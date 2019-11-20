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

#ifndef BPLUS_KEY_H
#define BPLUS_KEY_H

#include "compat.h"
#include <array>
#include <string>
#include <string_view>
#include <vector>
#include <iterator>
#include <tuple>
#include <functional>
#include <utility>
#include <limits>
#include <variant>
#include <iostream>
#include <boost/blank.hpp>
#include <boost/algorithm/string.hpp>

namespace rgw::bplus {

  namespace ba = boost::algorithm;

  using std::get;
  using std::tuple;
  using prefix_vector = std::vector<std::string>;

  static constexpr std::string_view nullstr{""};

  static inline std::string common_prefix(
    const std::string& lhs, const std::string& rhs,
    uint16_t min_len) {
    std::array<const std::string*, 2> sv = {&lhs, &rhs};
    if (lhs.length() > rhs.length()) {
      std::swap(sv[0], sv[1]);
    }
    uint16_t cnt{0};
    min_len = std::min(min_len, std::numeric_limits<uint16_t>::max());
    for (int ix = 0; ix < lhs.length(); ++ix) {
      if ((*sv[0])[ix] == (*sv[1])[ix])
	++cnt;
      else
	break;
    }
    std::string s;
    if (cnt > min_len) {
      s = (*sv[0]).substr(0, cnt);
    }
    return s;
  } /* common_prefix */

  using sv_tuple = tuple<const std::string_view&, const std::string_view&>;

  static inline size_t len(const sv_tuple& tp) {
    return get<0>(tp).length() + get<1>(tp).length();
  } /* len */

  static inline char at(const sv_tuple& tp, int ix) {
    int llen = get<0>(tp).length();
    if ((llen > 0) &&
	(ix < llen)) {
      return get<0>(tp)[ix];
    }
    return get<1>(tp)[ix-llen];
  } /* at */

  /* XXX builtin operator< failed here (illegal access) */
  static inline bool less_than(const sv_tuple& lhs, const sv_tuple& rhs)
  {
    std::array<const sv_tuple*, 2> svt = {&lhs, &rhs};
    if (len(lhs) > len(rhs)) {
      std::swap(svt[0], svt[1]);
    }
    for (int ix = 0, lhs_len = len(lhs); ix < lhs_len; ++ix) {
      auto t1 = svt[0];
      auto t2 = svt[1];
      std::cout << t1 << " " << t2 << std::endl;
#if 0
      std::cout << " lhs: " << at(*svt[0], ix)
		<< " rhs: " << at(*svt[1], ix)
		<< std::endl;
#endif
    }
    return true;
  }

  class leaf_key
  {
  public:
    std::optional<
    std::variant<
      std::string /* string prefix */,
      uint16_t    /* offset in local prefix vector */
      >> prefix;
    std::string stem;
  public:
    leaf_key(const std::string& _str)
      : stem(_str) {}
    leaf_key(const std::string& _prefix, const std::string& _stem)
      : prefix(_prefix), stem(_stem) {}

    std::tuple<const std::string_view&, const std::string_view&>
    tie_prefix(const prefix_vector& pv) const {
      if (prefix) {
	// expanded prefix
	if (std::holds_alternative<std::string>(*prefix)) {
	  return std::tie(get<std::string>(*prefix), stem);
	}
	// satisfy prefix from pv
	return std::tie(pv[get<uint16_t>(*prefix)], stem);
      }
      // no prefix
      return std::tie(nullstr, stem);
    } /* tie_prefix */

    std::string to_string(const prefix_vector& pv) const {
      std::string str{};
      auto ps = tie_prefix(pv);
      str.reserve(get<0>(ps).length() + get<1>(ps).length());
      str += get<0>(ps);
      str += get<1>(ps);
      return str;
    } /* to_string*/
  }; /* leaf_key */

  enum class key_range : uint8_t {
    unbounded
    };

  using fence_key = std::variant<leaf_key, key_range>;

  class branch_key
  {
  public:
    /* XXX casey: need only lower bound? */
    fence_key upper;
    fence_key lower;
    branch_key(const fence_key& _upper, const fence_key& _lower)
      : upper(_upper), lower(_lower)
      {}
  }; /* branch_key */

  static inline branch_key open_key_interval(
    key_range::unbounded,
    key_range::unbounded);

  static inline bool less_than(
    const prefix_vector& pv, const std::string& lk, const std::string& rk) {
    return (lk < rk);
  }

  static inline bool less_than(
    const prefix_vector& pv, const leaf_key& lk, const leaf_key& rk) {
    auto ltied = lk.tie_prefix(pv);
    auto rtied = rk.tie_prefix(pv);
    bool eq = less_than(ltied, rtied);
    return (lk.tie_prefix(pv) < rk.tie_prefix(pv));
  }

  static inline bool less_than(
    const prefix_vector& pv, const branch_key& lk, const branch_key& rk) {
    return true; /* XXXX */
  }

  static inline bool equal_to(
    const prefix_vector& pv, const std::string& lk, const std::string& rk) {
    return (lk == rk);
  }

  static inline bool equal_to(
    const prefix_vector& pv, const leaf_key& lk, const leaf_key& rk) {
    /* XXX hmm */
    return (lk.tie_prefix(pv) == rk.tie_prefix(pv));
  }

  static inline bool equal_to(
    const prefix_vector& pv, const branch_key& lk, const branch_key& rk) {
    return true; /* XXXX */
  }

} /* namespace */

#endif /* BPLUS_KEY_H */
