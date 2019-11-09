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

#ifndef BPLUS_IO_H
#define BPLUS_IO_H

#include <string>
#include <map>
#include <iostream>
#include <random>
#include "bplus_leaf.h" // uses bplus::Node in the interface

namespace rgw { namespace bplus {

    class IO
    {
    private:
      std::mt19937* mt;
      std::map<std::string, Node*> node_cache;

    public:
      IO(void);

      std::string random_bytes(int cnt);

      /* api */

    }; /* IO */
    
    extern IO io;
    
}} /* namespace */

#endif /* BPLUS_IO_H */
