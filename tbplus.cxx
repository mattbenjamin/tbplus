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
#include <optional>
#include <boost/program_options.hpp>

#include "bplus_leaf.h"

#define dout_subsys ceph_subsys_rgw

namespace {

  using namespace rgw::bplus;
  using std::get;
  using std::string;

  bool verbose = false;

  /* test classes */
  class Node_Min1 : public ::testing::Test {
  protected:
    string pref{"f_"};
  public:
    Node_Min1() {
    }
  };
  Node n{};
} /* namespace */

TEST_F(Node_Min1, fill1) {
  for (int ix = 0; ix < Node::fanout; ++ix) {
    string k = pref + std::to_string(ix);
    string v = "val for " + k;
    auto ret = n.insert(k, v);
    ASSERT_EQ(ret, 0);
    /* forbids duplicates */
    if (ix == 5) {
      ret = n.insert(k, v);
      ASSERT_EQ(ret, EEXIST);
    }
  }
  ASSERT_EQ(n.size(), Node::fanout);
  auto ret = n.insert("foo", "bar");
  ASSERT_EQ(ret, E2BIG);
}

TEST_F(Node_Min1, list1) {
  /* list all */
  ASSERT_EQ(n.size(), Node::fanout);
  int count{0};
  auto print_node =
    [&count] (const std::string *k, const std::string *v) -> int {
      if (verbose) {
	std::cout << "key: " << *k << " value: " << *v << std::endl;
      }
      ++count;
      return 0;
    };
    n.list({}, print_node, {});
    ASSERT_EQ(count, Node::fanout);
}

TEST_F(Node_Min1, list2) {
  /* list in a prefix */
  ASSERT_EQ(n.size(), Node::fanout);
  int count{0};
  auto print_node =
    [&count] (const std::string *k, const std::string *v) -> int {
      if (verbose) {
	std::cout << "key: " << *k << " value: " << *v << std::endl;
      }
      ++count;
      return 0;
    };
    n.list("f_9", print_node, {});
    ASSERT_EQ(count, 11);
}

TEST_F(Node_Min1, remove1) {
  ASSERT_EQ(n.size(), Node::fanout);
  for (auto ix : {92, 94, 97}) {
    string k = pref + std::to_string(ix);
    n.remove(k);
  }
  ASSERT_EQ(n.size(), Node::fanout - 3);
}

TEST_F(Node_Min1, list3) {
  /* list in a prefix */
  ASSERT_EQ(n.size(), Node::fanout - 3);
  int count{0};
  auto print_node =
    [&count] (const std::string *k, const std::string *v) -> int {
      if (verbose) {
	std::cout << "key: " << *k << " value: " << *v << std::endl;
      }
      ++count;
      return 0;
    };
    n.list("f_9", print_node, {});
    ASSERT_EQ(count, 8);
}

TEST_F(Node_Min1, serialize1) {
  auto bytes = n.serialize();
  ASSERT_GT(bytes.size(), 0);
}

int main(int argc, char **argv)
{
  int code = 0;
  namespace po = boost::program_options;

  po::options_description opts("program options");
  po::variables_map vm;

  try {

    opts.add_options()
      ("verbose", "be verbose about things")
      ;

    po::variables_map::iterator vm_iter;
    po::store(po::parse_command_line(argc, argv, opts), vm);

    if (vm.count("verbose")) {
      verbose = true;
    }

    po::notify(vm);

    ::testing::InitGoogleTest(&argc, argv);
    code = RUN_ALL_TESTS();
  }

  catch(po::error& e) {
    std::cout << "Error parsing opts " << e.what() << std::endl;
  }

  catch(...) {
    std::cout << "Unhandled exception in main()" << std::endl;
  }

  return code;
}
