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

#ifndef BPLUS_NODE_H
#define BPLUS_NODE_H

#include "compat.h"
#include "bplus_key.h"
#include <stdint.h>
#include <string>
#include <string_view>
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

    using lock_guard = std::lock_guard<std::mutex>;
    using unique_lock = std::unique_lock<std::mutex>;

    static constexpr uint32_t ondisk_version = 1;

    static constexpr uint32_t FLAG_NONE = 0x0000;
    static constexpr uint32_t FLAG_REQUIRE_PREFIX = 0x0001;
    static constexpr uint32_t FLAG_LOCKED = 0x0002;
    static constexpr uint32_t FLAG_STOP = 0x0004;

    enum class NodeType : uint8_t
    {
      Leaf,
      Branch,
    };

    template <typename K, NodeType T>
    class Node
    {
    public:
      const NodeType type = T;
      const uint32_t fanout;
      const uint16_t prefix_min_len;

    private:
      mutable std::mutex mtx;

      fence_key lower_bound;
      fence_key upper_bound;

      class KVEntry
      {
      public:
	K key;
	std::string val;
	KVEntry(const K& k, const std::string& v)
	  : key(k), val(v) {}
	KVEntry(KVEntry&& rhs)
	  : key(rhs.key), val(rhs.val) {}
	KVEntry& operator=(KVEntry&& rhs) {
	  key = rhs.key;
	  val = rhs.val;
	  return *this;
	}
      }; /* KVEntry */

      vector<KVEntry> data; // sorted vector of {k,v}
      prefix_vector pv;

      using data_iterator = typename decltype(data)::iterator;

      // indirect compf
      struct KeysViewLT
      {
	prefix_vector& pv;
      public:
	KeysViewLT(prefix_vector& _pv)
	  : pv(_pv) {}

	bool operator()(const KVEntry& lhs, const KVEntry& rhs) const
	  { return less_than(pv, lhs.key, rhs.key); }
	bool operator()(const K& k, const KVEntry& rhs) const
	  { return less_than(pv, k, rhs.key); }
	bool operator()(const KVEntry& lhs, const K& k) const
	  { return less_than(pv, lhs.key, k); }
      }; /* KeysViewLT */

      struct KeysViewEQ
      {
	prefix_vector& pv;
      public:
	KeysViewEQ(prefix_vector& _pv)
	  : pv(_pv) {}
	bool operator()(const KVEntry& lhs, const KVEntry& rhs) const
	  { return equal_to(pv, lhs.key, rhs.key); }
	bool operator()(const K& k, const KVEntry& rhs) const
	  { return equal_to(pv, k, rhs.key); }
	bool operator()(const KVEntry& lhs, const K& k) const
	  { return equal_to(pv, lhs.key, k); }
      }; /* KeysViewEQ */

      KeysViewLT keysviewLT;
      KeysViewEQ keysviewEQ;

    public:
      Node(uint32_t _fanout, uint16_t _prefix_min_len)
	: fanout(_fanout), prefix_min_len(_prefix_min_len),
	  lower_bound(fence_key(key_range::unbounded)),
	  upper_bound(fence_key(key_range::unbounded)),
	  keysviewLT(pv), keysviewEQ(pv)
	{}

      Node(uint32_t _fanout, uint16_t _prefix_min_len,
	  const fence_key& lb, const fence_key& ub)
	: fanout(_fanout), prefix_min_len(_prefix_min_len),
	  lower_bound(lb), upper_bound(ub),
	  keysviewLT(pv), keysviewEQ(pv)
	{}

      size_t size() const {
	lock_guard guard(mtx);
	return data.size();
      } /* size */

      void dump_keys() {
	std::cout << " data vec: ";
	for (const auto& kv : data) {
	  std::cout << " " << kv.key;
	}
	std::cout << std::endl;
      }

      void clear(uint32_t flags = FLAG_NONE) {
	unique_lock uniq(mtx, std::defer_lock);
	if (likely(! (flags & FLAG_LOCKED))) {
	  uniq.lock();
	}
	data.clear();
      } /* clear */

      int insert(const K& key, const std::string& value) {
	lock_guard guard(mtx);
	if (data.size() == fanout) {
	  // oh, noes!  need split
	  return E2BIG;
	}
	data_iterator kv_it = std::lower_bound(
	  data.begin(), data.end(), key, keysviewLT);
	if (kv_it != data.end()) {
	  if(unlikely(equal_to(pv, kv_it->key, key))) {
	    return EEXIST;
	  }
	}
	// key prefixing
	K& ref_key = const_cast<K&>(key);
	if (kv_it != data.begin()) {
	  auto prev_it = std::prev(kv_it);
	  auto pref_key = make_prefix_key(
	    pv, key, prev_it->key, prefix_min_len);
	  if (pref_key) {
	    ref_key = *pref_key;
	  }
	}
	// now use kv_it to do a positional insert into keys_view
	data.insert(kv_it, {ref_key, value});
	return 0;
      } /* insert */

      int remove(const K& key) {
	lock_guard guard(mtx);
	// TODO:  variant backing (local_rep and flatbuffer) */
	data_iterator kv_it = std::lower_bound(
	  data.begin(), data.end(), key, keysviewLT);
	if (kv_it != data.end() &&
	    equal_to(pv, kv_it->key, key)) {
	  data.erase(kv_it);
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

	data_iterator it = (prefix)
	  ? std::lower_bound(data.begin(), data.end(), K(*prefix),
			     keysviewLT)
	  : data.begin();
	for (; it != data.end() && count < lim; ++it) {
	  auto k = it->key;
	  /* XXXX callback should not induce a string temporary */
	  auto str = k.to_string(pv);
	  // stop iteration iff prefix search and prefix not found
	  if (prefix && (flags & FLAG_REQUIRE_PREFIX) &&
	      !ba::starts_with(str, *prefix)) {
	    goto out;
	  }
	  //auto ret = cb(&k, &it->val);
	  auto ret = cb(&str /* XXX */, &it->val);
	  ++count;
	  /* terminate iteration ?*/
	  if (ret & FLAG_STOP) {
	    break;
	  }
	} /* foreach data */
      out:
	return count;
      } /* list */

      std::vector<uint8_t> serialize() {
	lock_guard guard(mtx);
	flexbuffers::Builder fbb;

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
		    fbb.UInt(uint8_t(node.type));
		    fbb.UInt(node.fanout);
		    fbb.UInt(node.prefix_min_len);
		    //fbb.String("more header fields");
		  });
		fbb.Vector(
		  "kv-data",
		  [&fbb, &node, fkv]() {
		    node.list({}, fkv, {}, FLAG_LOCKED);
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

      friend class node_factory;
    }; /* Node */

    using leaf_node = Node<leaf_key, NodeType::Leaf>;
    using branch_node = Node<fence_key, NodeType::Branch>;
    using node_ptr = std::variant<leaf_node*, branch_node*>;

    class node_factory {
    public:
      static node_ptr from_flexbuffers(std::vector<uint8_t> flatv) {
	node_ptr node;
	auto map = flexbuffers::GetRoot(flatv).AsMap();
	auto vec = map["rgw-bplus-leaf"].AsVector();
	// header
	auto header = vec[0].AsVector();
	uint32_t ondisk_version = header[0].AsUInt32();
	NodeType type = NodeType(header[1].AsUInt8());
	uint32_t fanout = header[2].AsUInt32();
	uint16_t prefix_min_len = header[3].AsUInt16();
	auto kv_data = vec[1].AsVector();
	// kv-data -- fill vectors, then sort keys_view
	switch(type) {
	case NodeType::Leaf:
	  node = new leaf_node(fanout, prefix_min_len);
	  break;
	case NodeType::Branch:
	  break;
	default:
	  // unknown type
	  // XXXX
	  break;
	};
	for (int kv_ix = 0; kv_ix < kv_data.size(); kv_ix += 2) {
	  // TODO:  verify sharing rep (i.e., flatv)
	  auto key = kv_data[kv_ix].AsString().c_str();
	  auto val = kv_data[kv_ix+1].AsString().c_str();
	  switch(type) {
	  case NodeType::Leaf:
	    get<leaf_node*>(node)->data.emplace_back(leaf_key(key), val);
	    break;
	  case NodeType::Branch:
	    break;
	  default:
	    // unknown type
	    break;
	  };
	}
	// except keys_view is already sorted!
	//std::sort(keys_view.begin(), keys_view.end(), keysviewLT);
	// update log
	auto update_log = vec[2]; // XXX not used yet
	return node;
      } /* from_flexbuffers */
    }; /* unserialize_node */

}} /* namespace */

#endif /* BPLUS_NODE_H */
