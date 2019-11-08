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

#include "bplus_io.h"

#include <array>

namespace rgw { namespace bplus {

    IO::IO()
      : mt(nullptr)
    {
      /* seed sequence taken verbatim from
       * https://www.guyrutenberg.com/2014/05/03/c-mt19937-example/
       */
      std::array<int, 624> seed_data;
      std::random_device r;
      std::generate_n(seed_data.data(), seed_data.size(), std::ref(r));
      std::seed_seq seq(std::begin(seed_data), std::end(seed_data));
      mt = new std::mt19937(seq);
    }

    std::string IO::random_bytes(int cnt)
    {
      std::string s(cnt, ' ');
      std::generate(std::begin(s), std::end(s), std::ref(*mt));
      return std::move(s);
    } /* random_bytes */

}} /* namespace */
