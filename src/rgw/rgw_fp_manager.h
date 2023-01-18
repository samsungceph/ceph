// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#ifndef CEPH_RGW_FP_MANAGER_H
#define CEPH_RGW_FP_MANAGER_H

#include "include/types.h"

using namespace std;


class RGWFPManager
{
public:
  RGWFPManager() {}
  RGWFPManager(const RGWFPManager& rhs) = delete;
  virtual ~RGWFPManager() {}

  void reset_fpmap();
  size_t find(string& fingerprint);
  void add(string& fingerprint);

private:
  std::shared_mutex fingerprint_lock;
  unordered_map<string, uint32_t> fp_map;
};

#endif
