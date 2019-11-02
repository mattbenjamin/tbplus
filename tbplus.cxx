// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "gtest/gtest.h"

#include <errno.h>
#include <iostream>
#include <string>
#include <vector>

#include "bplus_leaf.h"

#define dout_subsys ceph_subsys_rgw

namespace {

  using namespace rgw::bplus;
  using std::get;
  using std::string;

  /* test classes */
  class Node_Min1 : public ::testing::Test {
  protected:
  public:
    Node_Min1() {
    }
  };
} /* namespace */

TEST_F(Node_Min1, t1) {
  Node n{};
  string pref{"f_"};
  for (int ix = 0; ix < Node::fanout; ++ix) {
    string k = pref + std::to_string(ix);
    string v = "val for " + k;
    n.insert(k, v);
  }
  ASSERT_EQ(n.size(), Node::fanout);
}

int main(int argc, char **argv)
{

  return 0;
}
