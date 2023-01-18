// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <algorithm>
#include "rgw_fp_manager.h"

#define dout_subsys ceph_subsys_rgw

void RGWFPManager::reset_fpmap()
{
  fp_map.clear();
}

void RGWFPManager::set_low_watermark(const uint32_t new_low_wm)
{
  ceph_assert(new_low_wm <= 100);
  low_watermark = new_low_wm;
}

size_t RGWFPManager::find(const string& fingerprint)
{
  shared_lock lock(fingerprint_lock);
  auto found_item = fp_map.find(fingerprint);
  if ( found_item != fp_map.end() ) {
    return found_item->second;
  }
  return 0;
}

uint32_t RGWFPManager::get_fpmap_memory_size()
{
  if (fp_map.empty()) {
    return 0;
  }
  return fp_map.size() *
    (fp_map.begin()->first.length() + sizeof(fp_map.begin()->second));
}

size_t RGWFPManager::get_fpmap_size()
{
  return fp_map.size();
}

void RGWFPManager::check_memory_limit_and_do_evict()
{
  ceph_assert(memory_limit > 0);
  if (get_fpmap_memory_size() > memory_limit) {
    uint32_t current_dedup_threshold = dedup_threshold;
    uint32_t target_fpmap_size = memory_limit * low_watermark / 100;
    auto iter = fp_map.begin();
    while (get_fpmap_memory_size() > target_fpmap_size) {
      if (iter == fp_map.end()) {
        iter = fp_map.begin();
        ++current_dedup_threshold;
      }

      if (iter->second < current_dedup_threshold) {
        iter = fp_map.erase(iter);
      } else {
        ++iter;
      }
    }
  }
}

void RGWFPManager::add(string& fingerprint)
{
  unique_lock lock(fingerprint_lock);
  auto found_iter = fp_map.find(fingerprint);
  if (found_iter == fp_map.end()) {
    check_memory_limit_and_do_evict();
    fp_map.insert({fingerprint, 1});
  } else {
    ++found_iter->second;
  }
}

