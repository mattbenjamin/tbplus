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

    static constexpr uint32_t FLAG_NONE =     0x0000;
    static constexpr uint32_t FLAG_REQUIRE_PREFIX = 0x0001;
    static constexpr uint32_t FLAG_LOCKED = 0x0002;

    enum class NodeType : uint8_t
    {
      Leaf,
      Branch,
    };

    template <typename K, NodeType T>
    class Node
    {
    public:
      const uint32_t fanout;
      uint32_t level; // per convention, 0 is a leaf

    private:
      mutable std::mutex mtx;
      const NodeType type = T;
      branch_key bounds;

      // XXX: likely to change with key prefixing
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

      vector<KVEntry> data; // unsorted vector of {k,v}

      using data_iterator = typename decltype(data)::iterator;

      // indirect compf
      struct KeysViewLT
      {
      public:
	bool operator()(const KVEntry& lhs, const KVEntry& rhs) const
	  { return (lhs.key < rhs.key); }
	bool operator()(const std::string& k, const KVEntry& rhs) const
	  { return (k < rhs.key); }
	bool operator()(const KVEntry& lhs, const std::string& k) const
	  { return (lhs.key < k); }
      }; /* KeysViewLT */

      KeysViewLT keysviewLT;

    public:
      Node(uint32_t _fanout)
	: fanout(_fanout), bounds(open_key_interval)
	{}

      Node(uint32_t _fanout, const branch_key& bounds)
	: fanout(_fanout), bounds(bounds)
	{}

      Node(uint32_t _fanout, std::vector<uint8_t> flatv)
	: fanout(_fanout), bounds(open_key_interval)
	{
	  unserialize(flatv);
	}

      size_t size() const {
	lock_guard guard(mtx);
	return data.size();
      } /* size */

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
	  if(unlikely(kv_it->key == key)) {
	    return EEXIST;
	  }
	}
	// now use kv_it to do a positional insert into keys_view
	data.insert(kv_it, {key, value});
	return 0;
      } /* insert */

      int remove(const K& key) {
	lock_guard guard(mtx);
	// TODO:  variant backing (local_rep and flatbuffer) */
	data_iterator kv_it = std::lower_bound(
	  data.begin(), data.end(), key, keysviewLT);
	if (kv_it != data.end() &&
	    kv_it->key == key) {
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
	  ? std::lower_bound(data.begin(), data.end(), *prefix,
			     keysviewLT)
	  : data.begin();
	for (; it != data.end() && count < lim; ++it) {
	  auto k = it->key;
	  // stop iteration iff prefix search and prefix not found
	  if (prefix && (flags & FLAG_REQUIRE_PREFIX) &&
	      !ba::starts_with(k, *prefix)) {
	    goto out;
	  }
	  auto ret = cb(&k, &it->val);
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

      int unserialize(std::vector<uint8_t> flatv) {
	/* destructively fill from serialized data, return size of
	 * node after fill */
	lock_guard guard(mtx);
	clear(FLAG_LOCKED);
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
	  data.emplace_back(key, val);
	}
	// except keys_view is already sorted!
	//std::sort(keys_view.begin(), keys_view.end(), keysviewLT);
	// update log
	auto update_log = vec[2]; // XXX not used yet
	return kv_data.size();
      } /* unserialize */

    }; /* Node */

    using leaf_node = Node<std::string, NodeType::Leaf>;
    using branch_node = Node<branch_key, NodeType::Branch>;
    using node_ptr = std::variant<leaf_node*, branch_node*>;

}} /* namespace */

#endif /* BPLUS_NODE_H */
