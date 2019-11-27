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

  using sv_tuple = tuple<const std::string_view, const std::string_view>;

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

  inline std::ostream& operator<<(std::ostream &os, sv_tuple const &sv) {
    os << "<sv_tuple: lhs=";
    os << get<0>(sv);
    os << "; rhs=";
    os << get<1>(sv);
    os << ">";
    return os;
  } /* pretty-print sv_tuple */

  static inline bool less_than(const sv_tuple& lhs, const sv_tuple& rhs)
  {
    const auto lhs_len = len(lhs);
    const auto rhs_len = len(rhs);
    auto max = std::min(lhs_len, rhs_len);
    bool ident = true;
    for (decltype(max) ix = 0; ix < max; ++ix) {
      if (ident) {
	auto c1 = at(lhs, ix);
	auto c2 = at(rhs, ix);
	if (c1 != c2) {
	  ident = false;
	  if (c1 > c2)
	    return false;
	}
      }
    } /* for ix */
    if (ident && (lhs_len >= rhs_len)) {
      return false;
    }
    return true;
  } /* less_than(sv_tuple, sv_tuple) */

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

    leaf_key(uint16_t _prefix, const std::string& _stem)
      : prefix(_prefix), stem(_stem) {}

    std::tuple<const std::string_view, const std::string_view>
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

    friend std::ostream& operator<<(std::ostream &os, leaf_key const &lk);
  }; /* leaf_key */

  inline std::ostream& operator<<(std::ostream &os, leaf_key const &lk) {
    os << "<leaf_key: prefix=";
    if (lk.prefix) {
      if (std::holds_alternative<std::string>(*lk.prefix)) {
	os << get<std::string>(*lk.prefix);
      } else {
	os << get<uint16_t>(*lk.prefix);
      }
    } else {
      os << "<undef>";
    }
    os << "; stem=";
    os << lk.stem;
    os << ">";
    return os;
  } /* pretty-print leaf_key */

  enum class key_range : uint8_t {
    unbounded
    };

  class fence_key {
  public:
    std::variant<leaf_key, key_range> k;
  public:
    fence_key(const key_range _k) : k(_k) {}
    fence_key(const leaf_key& _k) : k(_k) {}

    inline bool unbounded() const {
      return std::holds_alternative<key_range>(k);
    }

    const leaf_key& as_leaf_key() const {
      return get<leaf_key>(k);
    }
  }; /* fence_key */

  static inline bool less_than(
    const prefix_vector& pv, const std::string& lk, const std::string& rk) {
    return (lk < rk);
  }

  static inline bool less_than(
    const prefix_vector& pv, const leaf_key& lk, const leaf_key& rk) {
    auto res = less_than(lk.tie_prefix(pv), rk.tie_prefix(pv));
    return (res);
  }

  static inline bool less_than(
    const prefix_vector& pv, const fence_key& lk, const fence_key& rk) {
    if (lk.unbounded()) {
      if (!rk.unbounded())
	return true;
      else
	return false;
    }
    /* lk is not unbounded so rk==unbounded > lk */
    if (rk.unbounded())
      return false;
    /* unbounded checks prove lk and rk are leaf_keys */
    auto& lfk = lk.as_leaf_key();
    auto& rfk = rk.as_leaf_key();
    auto res = less_than(pv, lfk, rfk);
    return (res);
  }

  static inline bool equal_to(
    const prefix_vector& pv, const std::string& lk, const std::string& rk) {
    return (lk == rk);
  }

  static inline bool equal_to(
    const prefix_vector& pv, const leaf_key& lk, const leaf_key& rk) {
    /* comparison is over the logical sequences, don't assume that
     * all leaf_keys are prefix-compressed */
    auto ltied = lk.tie_prefix(pv);
    auto rtied = rk.tie_prefix(pv);
    bool res{false};
    if (get<0>(ltied).length() == get<0>(rtied).length())
      res = (ltied == rtied);
    else
      res = (lk.to_string(pv) == rk.to_string(pv));
    return (res);
  }

  static inline bool equal_to(
    const prefix_vector& pv, const fence_key& lk, const fence_key& rk) {
    if (lk.unbounded()) {
      if (rk.unbounded())
	return true;
      else
	return false;
    }
    /* lk is not unbounded so rk==unbounded > lk */
    if (rk.unbounded())
      return false;
    /* unbounded checks prove lk and rk are leaf_keys */
    auto& lfk = lk.as_leaf_key();
    auto& rfk = rk.as_leaf_key();
    auto res = equal_to(pv, lfk, rfk);
    return (res);
  }

  /* make prefix keys */
  static inline std::optional<leaf_key> make_prefix_key(
    prefix_vector& pv, const leaf_key& k, const leaf_key& prevk,
    uint16_t min_len) {
    std::optional<leaf_key> res{};
    uint16_t pref_off{0};
    auto kstr = k.to_string(pv); // XXXX costly, wasteful
    /* case 1: carry forward existing prefix */
    if (prevk.prefix) {
      if(std::holds_alternative<uint16_t>(*prevk.prefix)) {
	pref_off = get<uint16_t>(*prevk.prefix);
	auto& prefstr = pv[pref_off];
	if (ba::starts_with(kstr, prefstr)) {
	  res = leaf_key(pref_off, kstr.substr(prefstr.length()));
	}
      }
    }
    /* case 2: unprefixed prefk shares a common prefix with k */
    auto cp = common_prefix(kstr, prevk.to_string(pv), min_len);
    if (cp.length() > 0) {
      if ((! res) ||
	  /* case 3: prefk is prefixed, k and prefk share a common
	   * prefix longer than prefk's prefix */
	  (cp.length() > pv[pref_off].length())) {
	pv.push_back(cp);
	res = leaf_key(pv.size()-1, kstr.substr(cp.length()));
      }
    }
    return res;
  } /* make_prefix_key(leaf_key) */
} /* namespace */

#endif /* BPLUS_KEY_H */
