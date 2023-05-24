// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <algorithm>
#include "rgw_fp_manager.h"

#define dout_subsys ceph_subsys_rgw


void RGWFPManager::reset_fpmap()
{
  fp_map.clear();
}

void RGWFPManager::set_low_watermark(uint32_t new_low_watermark)
{
  ceph_assert(new_low_watermark <= 100);
  low_watermark = new_low_watermark;
}

size_t RGWFPManager::find(string& fingerprint)
{
  shared_lock lock(fingerprint_lock);
  auto found_item = fp_map.find(fingerprint);
  if ( found_item != fp_map.end() ) {
    return found_item->second;
  } else {
    return 0;
  }
}

uint32_t RGWFPManager::get_fpmap_memory_size()
{
  if (fp_map.size() == 0) {
    return 0;
  }
  return fp_map.size() *
    (fp_map.begin()->first.length() + sizeof(fp_map.begin()->second));
}

void RGWFPManager::check_memory_limit_and_do_evict()
{
  ceph_assert(memory_limit > 0);
  if (get_fpmap_memory_size() > memory_limit) {
    uint32_t current_dedup_threshold = dedup_threshold;
    uint32_t target_fpmap_size = memory_limit * low_watermark / 100;
    while (get_fpmap_memory_size() > target_fpmap_size) {
      for (auto iter = fp_map.begin();
           iter != fp_map.end() && get_fpmap_memory_size() > target_fpmap_size;
           ++current_dedup_threshold) {
        if (iter->second < current_dedup_threshold) {
          iter = fp_map.erase(iter);
        } else {
          ++iter;
        } 
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
