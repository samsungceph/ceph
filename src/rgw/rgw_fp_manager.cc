// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <algorithm>
#include "rgw_fp_manager.h"


#define dout_subsys ceph_subsys_rgw


string RGWFPManager::get_chunk_algo()
{
  return chunk_algo;
}

void RGWFPManager::set_chunk_algo(string chunk_algo)
{
  ceph_assert(chunk_algo == "fixed" || chunk_algo == "fastcdc");
  chunk_algo = chunk_algo;
}

ssize_t RGWFPManager::get_chunk_size()
{
  return chunk_size;
}

void RGWFPManager::set_chunk_size(size_t chunk_size)
{
  ceph_assert(chunk_size > 0);
  chunk_size = chunk_size;
}

string RGWFPManager::get_fp_algo()
{
  return fp_algo;
}

void RGWFPManager::set_fp_algo(string fp_algo)
{
  ceph_assert(fp_algo == "sha1" || fp_algo == "sha256" || fp_algo == "sha512");
  fp_algo = fp_algo;
}

ssize_t RGWFPManager::get_dedup_threshold()
{
  return dedup_threshold;
}

void RGWFPManager::set_dedup_threshold(size_t dedup_threshold)
{
  ceph_assert(dedup_threshold > 0);
  dedup_threshold = dedup_threshold;
}

void RGWFPManager::reset_fpmap()
{
  fp_map.clear();
}

ssize_t RGWFPManager::get_fpmap_size()
{
  return fp_map.size();
}

uint32_t RGWFPManager::get_fpmap_memory_size()
{
  if (fp_map.size() == 0) {
    return 0;
  }
  return fp_map.size() * (fp_map.begin()->first.length() + sizeof(fp_map.begin()->second));
}

ssize_t RGWFPManager::find(string& fingerprint)
{
  shared_lock lock(fingerprint_lock);
  auto found_item = fp_map.find(fingerprint);
  
  if (found_item != fp_map.end()) {
    return found_item->second;
  } else {
    return 0;
  }
}

void RGWFPManager::check_memory_limit_and_do_evict()
{
  uint32_t  current_memory = get_fpmap_memory_size();

  if (current_memory > memory_limit) {
    bool memory_freed = false;
    int current_dedup_threshold = dedup_threshold;
    while (!memory_freed && fp_map.size() > 0) {
      for (auto iter = fp_map.begin(); iter != fp_map.end();) {
        if (iter->second < current_dedup_threshold) {
          iter = fp_map.erase(iter);
          memory_freed = true;
        } else {
          ++iter;
        }
      }
      if (!memory_freed) {
        current_dedup_threshold++;
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
