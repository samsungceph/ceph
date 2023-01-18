// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <algorithm>
#include "rgw_fp_manager.h"

#define dout_subsys ceph_subsys_rgw


void RGWFPManager::reset_fpmap()
{
  fp_map.clear();
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

void RGWFPManager::add(string& fingerprint)
{
  unique_lock lock(fingerprint_lock);
  auto found_iter = fp_map.find(fingerprint);
  if (found_iter == fp_map.end()) {
    fp_map.insert({fingerprint, 1});
  } else {
    ++found_iter->second;
  }
}
