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
#include <functional>
#include <boost/variant.hpp>
#include <boost/blank.hpp>

namespace rgw { namespace bplus {

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

      int list(
	const std::optional<std::string>& prefix,
	std::function<int(const std::string*, const std::string*)> cb) {
	// TODO: seek to prefix
	keys_iterator it = keys.begin();
	for (; it != keys.end(); ++it) {
	  auto ret = cb(&*it, &*it);
	}
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
