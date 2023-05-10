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

void RGWFPManager::set_chunk_size(ssize_t chunk_size)
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

void RGWFPManager::reset_fpmap()
{
  fp_map.clear();
}

ssize_t RGWFPManager::get_fpmap_size()
{
  return fp_map.size();
}

bool RGWFPManager::find(string& fingerprint)
{
  shared_lock lock(fingerprint_lock);
  auto found_item = fp_map.find(fingerprint);
  return found_item != fp_map.end();
}

void RGWFPManager::check_memory_limit_and_do_evict()
{
  uint64_t current_memory;
  if ( fp_algo == "sha1" ) {
    current_memory = 24 * fp_map.size();
  }
  else if ( fp_algo == "sha256" ) {
    current_memory = 36 * fp_map.size();
  }
  else if ( fp_algo == "sha512") {
    current_memory = 68 * fp_map.size();
  }
  
  if ( memory_limit > current_memory ) {
    bool memory_freed = false;
    int current_dedup_threshold = dedup_threshold;
    while (memory_freed == true && fp_map.size() != 0) {
      for (auto iter = fp_map.begin(); iter != fp_map.end();) {
        if (iter->second < dedup_threshold) {
          iter = fp_map.erase(iter);
          memory_freed = true;
        } else {
          current_dedup_threshold++;
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