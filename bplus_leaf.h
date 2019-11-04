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
#include <mutex>
#include <limits>
#include <functional>
#include <boost/variant.hpp>
#include <boost/blank.hpp>
#include <boost/algorithm/string.hpp>

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
      static constexpr uint32_t fanout = 100;

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

      std::mutex mtx;


      vector<string> prefixes;
      vector<string> keys;
      vector<string> values;
      vector<string*> keys_view;

      using keys_iterator = decltype(keys)::iterator;
      
    public:
      const size_t size() const { return keys.size(); }

      int insert(const std::string& key, const std::string& value) {
	lock_guard guard(mtx);
	if (keys.size() == fanout) {
	  // oh, noes!  need split
	  return E2BIG;
	}
	keys_iterator it = std::lower_bound(keys.begin(), keys.end(), key);
	if (it == keys.end()) {
	  std::cout << "key not found" << std::endl;
	}
	if (it != keys.end()) {
	  if(unlikely(*it == key)) {
	    return EEXIST;
	  }
	}
	keys.insert(it, key);
	// TODO:  store value
	// TODO:  lift indirect
	return 0;
      } /* insert */

      int remove(const std::string& key) {
	lock_guard guard(mtx);
	// TODO:  lift indirect
	keys_iterator it = std::lower_bound(keys.begin(), keys.end(), key);
	if (it != keys.end() &&
	    *it == key) {
	  keys.erase(it);
	}
      } /* remove */

      int list(
	const std::optional<std::string>& prefix,
	std::function<int(const std::string*, const std::string*)> cb,
	std::optional<uint32_t> limit) {
	uint32_t count{0};
	uint32_t lim  =
	  limit ? *limit : std::numeric_limits<uint32_t>::max() ;
	keys_iterator it = (prefix)
	  ? std::lower_bound(keys.begin(), keys.end(), *prefix)
	  : keys.begin();
	for (; it != keys.end() && count < lim; ++it) {
	  auto k = *it;
	  // stop iteration iff prefix search and prefix not found
	  if (prefix && !ba::starts_with(k, *prefix)) {
	    goto out;
	  }
	  auto ret = cb(&k, &k);
	  ++count;
	}
      out:
	return count;
      } /* list */
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
