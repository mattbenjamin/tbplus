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

    static constexpr uint32_t fanout = 100;

    class Pointer
    {
      string key;
      string oid; // path to on-disk rep
    };
    
    class Node
    {
      enum class NodeType : uint8_t
      {
	Root,
	Leaf,
	Internal
      };

      uint32_t level; // per convention, is a leaf

      vector<string> prefixes;
      vector<string> keys;
      vector<string*> keys_view;

    public:
      int insert(const std::string& key, const std::string& value) {

	return 0;
      }
    };

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
