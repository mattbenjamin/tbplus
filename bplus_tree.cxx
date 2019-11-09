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

namespace rgw { namespace bplus {

    Tree::Tree(std::string _name, uint32_t _fanout)
      : name(_name), fanout(_fanout)
    {
      // build node name stem (random_bytes())
    } /* Tree(std::string) */

    int Tree::insert(const std::string& key, const std::string& value)
    {
      // get_root();
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
