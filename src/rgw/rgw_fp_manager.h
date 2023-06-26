// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#ifndef CEPH_RGW_FP_MANAGER_H
#define CEPH_RGW_FP_MANAGER_H

#include "include/types.h"

using namespace std;


class RGWFPManager
{
public:
  size_t find(string& fingerprint);
  void check_memory_limit_and_do_evict();
  void add(string& fingerprint);
  RGWFPManager(string _chunk_algo, size_t _chunk_size, string _fp_algo, size_t _dedup_threshold, size_t _memory_limit)
            : chunk_algo(_chunk_algo), chunk_size(_chunk_size), fp_algo(_fp_algo), dedup_threshold(_dedup_threshold), memory_limit(_memory_limit) {}
  string get_chunk_algo();
  void set_chunk_algo(string chunk_algo);
  uint32_t get_chunk_size();
  void set_chunk_size(size_t chunk_size);
  string get_fp_algo();
  void set_fp_algo(string fp_algo);
  ssize_t get_dedup_threshold();
  void set_dedup_threshold(size_t dedup_threshold);
  void reset_fpmap();
  size_t get_fpmap_size();
  uint32_t get_fpmap_memory_size();
private:
  std::shared_mutex fingerprint_lock;
  string chunk_algo;
  uint32_t chunk_size;
  string fp_algo;
  uint32_t dedup_threshold;
  uint32_t memory_limit;
  unordered_map<string, uint32_t> fp_map;
};

#endif
