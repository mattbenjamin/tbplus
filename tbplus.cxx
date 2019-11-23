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
#include "xxhash.h"

#include "bplus_tree.h"

#define dout_subsys ceph_subsys_rgw

namespace {

  using namespace rgw::bplus;
  using std::get;
  using std::string;

  bool verbose = false;
  static constexpr uint64_t seed = 8675309;

  /* test classes */

  class Strings_Min1 : public ::testing::Test {
  public:
    std::string s1{"/sub1/docrequest/D/DOC597z85"};
    std::string s2{"/sub1/docrequest/D/PDF448x79"};
    std::string s3{"orange carrot top"};
    std::string s4{"orange televised gawker trainwreck"};
  };

  class Node_Min1 : public ::testing::Test {
  public:
    static constexpr uint32_t fanout = 100;
    static constexpr uint16_t prefix_min_len = 2;
    string pref{"f_"};
  public:
    Node_Min1() {
    }
  };
  leaf_node n(Node_Min1::fanout, Node_Min1::prefix_min_len);
  std::vector<uint8_t> min1_serialized_bytes;

  class Tree_Min1 : public ::testing::Test {
  public:
    static constexpr uint32_t fanout = 10;
    string pref{"f_"};
  public:
    Tree_Min1() {
    }
  };
  Tree t1("Tree_Min1", Tree_Min1::fanout);
} /* namespace */

TEST_F(Node_Min1, fill1) {
  for (int ix = 0; ix < Node_Min1::fanout; ++ix) {
    string k = pref + std::to_string(ix);
    string v = "val for " + k;
    auto ret = n.insert(leaf_key(k), v);
    ASSERT_EQ(ret, 0);
    /* forbids duplicates */
    if (ix == 5) {
      ret = n.insert(k, v);
      ASSERT_EQ(ret, EEXIST);
    }
  }
  ASSERT_EQ(n.size(), Node_Min1::fanout);
  auto ret = n.insert(leaf_key("foo"), "bar");
  ASSERT_EQ(ret, E2BIG);
}

TEST_F(Node_Min1, list1) {
  /* list all */
  ASSERT_EQ(n.size(), Node_Min1::fanout);
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
    ASSERT_EQ(count, Node_Min1::fanout);
}

TEST_F(Node_Min1, list2) {
  /* list in a prefix */
  ASSERT_EQ(n.size(), Node_Min1::fanout);
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
  ASSERT_EQ(n.size(), Node_Min1::fanout);
  for (auto ix : {92, 94, 97}) {
    string k = pref + std::to_string(ix);
    if (verbose) {
      std::cout << "removing: " << k << std::endl;
    }
    n.remove(k);
  }
  ASSERT_EQ(n.size(), Node_Min1::fanout - 3);
}

TEST_F(Node_Min1, list3) {
  /* list in a prefix */
  ASSERT_EQ(n.size(), Node_Min1::fanout - 3);
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
    ASSERT_EQ(count, Node_Min1::fanout - 3);

    count = 0;
    n.list("f_9", print_node, {});
    ASSERT_EQ(count, 8);
}

TEST_F(Node_Min1, serialize1) {
  min1_serialized_bytes = n.serialize();
  ASSERT_GT(min1_serialized_bytes.size(), 0);
}

TEST_F(Node_Min1, unserialize1) {
  leaf_node* n2 = get<leaf_node*>(
    node_factory::from_flexbuffers(min1_serialized_bytes));
  ASSERT_NE(n2, nullptr);
  int count{0};
  auto print_node =
    [&count] (const std::string *k, const std::string *v) -> int {
      if (verbose) {
	std::cout << "key: " << *k << " value: " << *v << std::endl;
      }
      ++count;
      return 0;
    };
  n2->list({}, print_node, {});
  ASSERT_EQ(count, Node_Min1::fanout-3);
}

TEST_F(Node_Min1, list4) {
  /* list in a prefix */
  ASSERT_EQ(n.size(), Node_Min1::fanout - 3);
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
  ASSERT_EQ(count, Node_Min1::fanout - 3);
}

TEST_F(Tree_Min1, names) {
  for (int ix = 0; ix < 10; ++ix) {
    auto node_name = t1.gen_node_name();
    if (verbose) {
      std::cout << "rand node: " << node_name << std::endl;
    }
  }
}

TEST_F(Tree_Min1, names2) {
  std::array<int, 31> a1; a1.fill(0);
  std::array<int, 32> a2; a2.fill(0);
  for (int ix = 0; ix < 1000; ++ix) {
    auto name = t1.gen_node_name();
    uint64_t hash = XXH64(name.data(), name.length(), seed);
    (a1[hash % 31])++;
    (a2[hash % 32])++;
  }
  int ix;
  std::cout << "array1:" << std::endl;
  for (ix = 0; ix < 31; ++ix) {
    std::cout << /* " ix: " << ix << */ " val: " << a1[ix];
  }
  std::cout << std::endl;

 std::cout << "array2:" << std::endl;
  for (ix = 0; ix < 32; ++ix) {
    std::cout << /* " ix: " << ix << */ " val: " << a2[ix];
  }
  std::cout << std::endl;

}


TEST_F(Tree_Min1, fill1) {
  for (int ix = 0; ix < Tree_Min1::fanout; ++ix) {
    string k = pref + std::to_string(ix);
    string v = "val for " + k;
    auto ret = t1.insert(k, v);
    ASSERT_EQ(ret, 0);
    /* forbids duplicates */
    if (ix == 5) {
      ret = t1.insert(k, v);
      ASSERT_EQ(ret, EEXIST);
    }
  }
#if 0 /* get Node we have been inserting on, then: */
  ASSERT_EQ(t1.size(), Node_Min1::fanout);
  auto ret = t1.insert("foo", "bar");
  ASSERT_EQ(ret, E2BIG);
#endif
}

TEST_F(Strings_Min1, cpref1) {
  std::string r1 = common_prefix(s1, s2, 5);
  if (verbose) {
    std::cout << "cpref: " << r1 << " size: " << r1.length() << std::endl;
  }
  ASSERT_GE(r1.length(), 0);
  std::string r2 = common_prefix(s1, s2, 20);
  if (verbose) {
    std::cout << "cpref: " << r2 << " size: " << r2.length() << std::endl;
  }
  ASSERT_EQ(r2.length(), 0);
  std::string r3 = common_prefix(s3, s4, 5);
  std::string r4 = common_prefix(s4, s3, 5);
  if (verbose) {
    std::cout << "cpref: " << r3 << " " << r4 << std::endl;
  }
  ASSERT_EQ(r3, r4);
}

TEST_F(Strings_Min1, pref_eq1) {
  vector<string> pv = {"one", "one_"};
  string s1{"one_two_three"};
  leaf_key lk1{pv[0], "_two_three"};
  leaf_key lk2{pv[1], "two_three"};
  leaf_key lk3{0, "_two_three"};
  leaf_key lk4{1, "two_three"};
  ASSERT_TRUE(equal_to(pv, lk1, lk2));
  ASSERT_TRUE(equal_to(pv, lk1, lk3));
  ASSERT_TRUE(equal_to(pv, lk1, lk4));
  leaf_key lk5{pv[1], "_two_three"};
  ASSERT_FALSE(equal_to(pv, lk1, lk5));
  ASSERT_EQ(lk1.to_string(pv), s1);
  ASSERT_EQ(lk3.to_string(pv), s1);
}

TEST_F(Strings_Min1, pref_lt1) {
  vector<string> pv = {"abc", "def"};
  leaf_key lk1{pv[0], "_apple"};
  leaf_key lk2{pv[1], "_orange"};
  leaf_key lk3{0, "_apple"};
  leaf_key lk4{1, "_orange"};
  ASSERT_TRUE(less_than(pv, lk1, lk2));
  ASSERT_FALSE(less_than(pv, lk2, lk1));
  ASSERT_TRUE(less_than(pv, lk3, lk4));
  ASSERT_FALSE(less_than(pv, lk4, lk3)); 
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
