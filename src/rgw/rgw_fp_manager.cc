// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <algorithm>

#include "rgw_fp_manager.h"
#include "rgw_rados.h"
#include "include/rados/librados.h"

#define dout_subsys ceph_subsys_rgw


string RGWFPManager::get_chunk_algo()
{
  return chunk_algo;
}

void RGWFPManager::set_chunk_algo(string chunk_algo)
{
  chunk_algo = chunk_algo;
  return;
}

ssize_t RGWFPManager::get_chunk_size()
{
  return chunk_size;
}

void RGWFPManager::set_chunk_size(ssize_t chunk_size)
{
  chunk_size = chunk_size;
  return;
}

string RGWFPManager::get_fp_algo()
{
  return fp_algo;
}

void RGWFPManager::set_fp_algo(string fp_algo)
{
  fp_algo = fp_algo;
  return;
}

void RGWFPManager::reset_fpmap()
{
  fp_map.clear();
  return;
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

void RGWFPManager::add(string& fingerprint)
{
  unique_lock lock(fingerprint_lock);
  auto found_iter = fp_map.find(fingerprint);
  ssize_t cur_reference = 1;

  if (found_iter == fp_map.end()) {
    fp_map.insert({fingerprint, 1});
  } else {
    cur_reference = ++found_iter->second;
  }

  return;
}