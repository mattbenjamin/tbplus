// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#ifndef BPLUS_LEAF_H
#define BPLUS_LEAF_H

#include "compat.h"
#include <stdint.h>
#include <string>
#include <array>
#include <vector>
#include <iterator>
#include <mutex>
#include <limits>
#include <functional>
#include <utility>
#include <boost/variant.hpp>
#include <boost/blank.hpp>
#include <boost/algorithm/string.hpp>
#include <flatbuffers/flexbuffers.h>

namespace rgw { namespace bplus {

    namespace ba  = boost::algorithm;
    
    using std::vector;
    using std::string;

    class Pointer
    {
      string key;
      string oid; // path to on-disk rep
    };
    
    class Node
    {
    public:
      static constexpr uint32_t ondisk_version = 1;
      static constexpr uint32_t fanout = 100;

      static constexpr uint32_t FLAG_NONE =     0x0000;
      static constexpr uint32_t FLAG_REQUIRE_PREFIX = 0x0001;
      static constexpr uint32_t FLAG_LOCKED = 0x0002;

      enum class NodeType : uint8_t
      {
	Root,
	Leaf,
	Internal
      };

      uint32_t level; // per convention, 0 is a leaf

    private:
      using lock_guard = std::lock_guard<std::mutex>;
      using unique_lock = std::unique_lock<std::mutex>;

      mutable std::mutex mtx;

      NodeType type;

      // XXX: likely to change with key prefixing
      class KeyValueEntry
      {
      public:
	std::string key;
	std::string val;
	KeyValueEntry(const std::string& k, const std::string& v)
	  : key(k), val(v) {}
	KeyValueEntry(KeyValueEntry&& rhs)
	  : key(rhs.key), val(rhs.val) {}
	KeyValueEntry& operator=(KeyValueEntry&& rhs) {
	  key = rhs.key;
	  val = rhs.val;
	  return *this;
	}
      }; /* KeyValueEntry */

      // XXX: likely to change for indirection into local or flatbuffer rep
      class KeyEntry
      {
	friend class Node;
	size_t rep_off;
      public:
	KeyEntry(size_t _rep_off)
	  : rep_off(_rep_off) {}
	inline KeyValueEntry& elt(const Node* node) const {
	  return const_cast<KeyValueEntry&>(node->local_rep[rep_off]);
	}
	inline const std::string& key(const Node* node) const {
	  return elt(node).key;
	}
	inline const std::string& val(const Node* node) const {
	  return elt(node).val;
	}
      }; /* KeysViewEntry */

      vector<KeyValueEntry> local_rep; // unsorted vector of {k,v}
      vector<KeyEntry> keys_view; // sorted indirection vector

      using rep_iterator = decltype(local_rep)::iterator;
      using keysview_iterator = decltype(keys_view)::iterator;

      // indirect compf
      struct KeysViewLT
      {
      private:
	const Node* node;
      public:
	bool operator()(const KeyEntry& lhs, const KeyEntry& rhs) const
	  { return (lhs.key(node) < rhs.key(node)); }
	bool operator()(const std::string& k, const KeyEntry& rhs) const
	  { return (k < rhs.key(node)); }
	bool operator()(const KeyEntry& lhs, const std::string& k) const
	  { return (lhs.key(node) < k); }
	KeysViewLT(const Node* _node)
	  : node(_node) {}
      }; /* KeysViewLT */

      KeysViewLT keysviewLT;

    public:
      Node()
	: keysviewLT(this)
	{}

      Node(std::vector<uint8_t> flatv)
       : keysviewLT(this) {
	unserialize(flatv);
      }

      size_t size() const {
	lock_guard guard(mtx);
	return keys_view.size();
      } /* size */

      void clear(uint32_t flags = FLAG_NONE) {
	unique_lock uniq(mtx, std::defer_lock);
	if (likely(! (flags & FLAG_LOCKED))) {
	  uniq.lock();
	}
	keys_view.clear();
	local_rep.clear();
      } /* clear */

      int insert(const std::string& key, const std::string& value) {
	lock_guard guard(mtx);
	if (keys_view.size() == fanout) {
	  // oh, noes!  need split
	  return E2BIG;
	}
	keysview_iterator kv_it = std::lower_bound(
	  keys_view.begin(), keys_view.end(), key, keysviewLT);
	if (kv_it != keys_view.end()) {
	  if(unlikely(kv_it->key(this) == key)) {
	    return EEXIST;
	  }
	}
	// append to local rep vector (TODO: journal)
	local_rep.emplace_back(key, value);
	// now use kv_it to do a positional insert into keys_view
	keys_view.insert(kv_it, KeyEntry({local_rep.size()-1}));
	return 0;
      } /* insert */

      int remove(const std::string& key) {
	lock_guard guard(mtx);
	// TODO:  variant backing (local_rep and flatbuffer */
	keysview_iterator kv_it = std::lower_bound(
	  keys_view.begin(), keys_view.end(), key, keysviewLT);
	if (kv_it != keys_view.end() &&
	    kv_it->key(this) == key) {
	  /* remove from local_rep--to do this, swap the key to be
	   * deleted with the last element in the vector, then erase
	   * the element in last position */
	  auto& elt = kv_it->elt(this);
	  auto& elt2 = local_rep.back();
	  /* sadly, we don't know (though could save at the cost
	   * of significant space) the position of the keys_view
	   * record that points to local_rep.back(), and need it
	   * to execute the swap step */
	  keysview_iterator back_it = std::lower_bound(
	    keys_view.begin(), keys_view.end(), elt2.key, keysviewLT);
	  std::swap(elt, elt2);
	  local_rep.pop_back();
	  back_it->rep_off = kv_it->rep_off;
	  /* now erase kv_it from the sorted view */
	  keys_view.erase(kv_it);
	}
	return 0;
      } /* remove */

      int list(
	const std::optional<std::string>& prefix,
	std::function<int(const std::string*, const std::string*)> cb,
	std::optional<uint32_t> limit,
	uint32_t flags = FLAG_NONE) {
	uint32_t count{0};
	uint32_t lim  =
	  limit ? *limit : std::numeric_limits<uint32_t>::max() ;
	unique_lock uniq(mtx, std::defer_lock);
	if (likely(! (flags & FLAG_LOCKED))) {
	  uniq.lock();
	}

	keysview_iterator it = (prefix)
	  ? std::lower_bound(keys_view.begin(), keys_view.end(), *prefix,
			    keysviewLT)
	  : keys_view.begin();
	for (; it != keys_view.end() && count < lim; ++it) {
	  auto k = it->key(this);
	  // stop iteration iff prefix search and prefix not found
	  if (prefix && (flags & FLAG_REQUIRE_PREFIX) &&
	      !ba::starts_with(k, *prefix)) {
	    goto out;
	  }
	  auto ret = cb(&k, &it->val(this));
	  ++count;
	}
      out:
	return count;
      } /* list */

      std::vector<uint8_t> serialize() {
	lock_guard guard(mtx);
	flexbuffers::Builder fbb;
	/* TODO: recursion -> iteration */

	auto fkv =
	  [&fbb] (const std::string *k, const std::string *v) -> int {
	      fbb.String(*k);
	      fbb.String(*v);
	      return 0;
	  };

	fbb.Map(
	  [&node = *this, &fbb, fkv]() {
	    fbb.Map(
	      "rgw-bplus-leaf",
	      [&fbb, &node, fkv]() {
		fbb.Vector(
		  "header",
		  [&fbb, &node, fkv]() {
		    fbb.UInt(ondisk_version);
		    //fbb.String("more header fields");
		  });
		fbb.Vector(
		  "kv-data",
		  [&fbb, &node, fkv]() {
		    node.list({}, fkv, {}, Node::FLAG_LOCKED);
		  });
		/* XXXX actually this probably is another segment */
		fbb.Vector(
		  "update-log",
		  [&fbb]() {
		    fbb.String("update log records");
		  });
	      }); // Vector
	  }); // Map
	fbb.Finish();
	return fbb.GetBuffer();
      } /* serialize */

      int unserialize(std::vector<uint8_t> flatv) {
	/* destructively fill from serialized data, return size of
	 * node after fill */
	lock_guard guard(mtx);
	clear(Node::FLAG_LOCKED);
	auto map = flexbuffers::GetRoot(flatv).AsMap();
	auto vec = map["rgw-bplus-leaf"].AsVector();
	// header
	auto header = vec[0].AsVector();
	auto kv_data = vec[1].AsVector();
	// kv-data -- fill vectors, then sort keys_view
	for (int kv_ix = 0; kv_ix < kv_data.size(); kv_ix += 2) {
	  // TODO:  verify sharing rep (i.e., flatv)
	  auto key = kv_data[kv_ix].AsString().c_str();
	  auto val = kv_data[kv_ix+1].AsString().c_str();
	  local_rep.emplace_back(key, val);
	  keys_view.emplace_back(local_rep.size()-1);
	}
	// except keys_view is already sorted!
	//std::sort(keys_view.begin(), keys_view.end(), keysviewLT);
	// update log
	auto update_log = vec[2]; // XXX not used yet
	return kv_data.size();
      } /* unserialize */

    }; /* Node */

    class NonLeaf : public Node
    {
    public:
    };

    class Leaf : public Node
    {
      vector<string> values;
    public:
    };
    

}} /* namespace */

#endif /* BPLUS_LEAF_H */
