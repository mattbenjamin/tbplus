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
#include <string>
#include <string_view>
#include <vector>
#include <iterator>
#include <functional>
#include <utility>
#include <boost/blank.hpp>
#include <boost/algorithm/string.hpp>

namespace rgw::bplus {

  namespace ba = boost::algorithm;

  using prefix_vector = std::vector<std::string>;

  class leaf_key
  {
  public:
    std::optional<uint16_t> pref_off; // offset in local prefix vector
    std::string stem;
  }; /* leaf_key */

  enum class key_range : uint8_t {
    unbounded
    };

  using fence_key = std::variant<leaf_key, key_range>;

  class branch_key
  {
  public:
    fence_key upper;
    fence_key lower;
  }; /* branch_key */

}} /* namespace */

#endif /* BPLUS_LEAF_H */
