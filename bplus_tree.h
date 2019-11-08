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

#ifndef BPLUS_TREE_H
#define BPLUS_TREE_H

#include "bplus_leaf.h"
#include "bplus_io.h"

namespace rgw { namespace bplus {

class Tree
{
  std::string name;
public:
  Tree(std::string _name)
    : name(_name)
    {}

  /* api */

}; /* Tree */

}} /* namespace */

#endif /* BPLUS_TREE_H */
