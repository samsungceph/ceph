// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#ifndef CEPH_RGW_FP_MANAGER_H
#define CEPH_RGW_FP_MANAGER_H

#include "include/types.h"

using namespace std;

class RGWFPManager
{
public:
  RGWFPManager(uint32_t _dedup_threshold, uint64_t _memory_limit,
               uint32_t _low_watermark)
    : dedup_threshold(_dedup_threshold),
      memory_limit(_memory_limit),
      low_watermark(_low_watermark) {}
  RGWFPManager() = delete;
  RGWFPManager(const RGWFPManager& rhs) = delete;
  RGWFPManager& operator=(const RGWFPManager& rhs) = delete;
  virtual ~RGWFPManager() {}

  void reset_fpmap();
  size_t find(const string& fingerprint);
  void add(string& fingerprint);
  void check_memory_limit_and_do_evict();
  void set_low_watermark(const uint32_t new_low_wm);
  uint32_t get_fpmap_memory_size();
  size_t get_fpmap_size();

private:
  std::shared_mutex fingerprint_lock;
  uint32_t dedup_threshold;
  uint64_t memory_limit;
  uint32_t low_watermark;
  unordered_map<string, uint32_t> fp_map;
};

#endif
