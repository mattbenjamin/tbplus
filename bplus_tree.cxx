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

#include "bplus_tree.h"
#include "z85.hpp"

namespace rgw { namespace bplus {

    Tree::Tree(std::string _name, uint32_t _fanout)
      : name(_name), fanout(_fanout)
    {
    } /* Tree(std::string, uint32_t) */

    std::string Tree::root_name() const {
      std::string s{name_stem};
      s.reserve(64);
      s.reserve(s.length() + name.length() + 2 + 4);
      s += "-" + name + "-" + "root";
      return s;
    } /* root_name() */

    std::string Tree::gen_node_name() const {
      std::string s{name_stem};
      s.reserve(s.length() + name.length() + 2 + 24);
      s += "-" + name + "-" +
	z85::encode(io.random_bytes(16));
      return s;
    } /* gen_node_name() */

    node_ptr Tree::get_node_for_k(const std::string&) {
      // XXXX find or create node

      return static_cast<leaf_node*>(nullptr);
    } /* get_node_for_k */

    int Tree::insert(const std::string& key, const std::string& value)
    {
      // get_root();
      if (! root_node) {
	// find and ref node
	root_node = get_node_for_k(root_name());
	if (! root_node) {
	  return EIO;
	}
      }
      // traverse to candidate leaf
      // try-insert
      //    !full: insert, done
      //    full: <split>, choose-leaf, try-insert
      return 0;
    } /* insert */

    int Tree::list(const std::optional<std::string>& prefix,
		  std::function<int(const std::string*, const std::string*)> cb,
		  std::optional<uint32_t> limit,
		  uint32_t flags)
    {
      int count{0};
      // TODO: implement
      return count;
    } /* list */

}} /* namespace */
