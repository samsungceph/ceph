// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#ifndef CEPH_RGW_DEDUP_MANAGER_H
#define CEPH_RGW_DEDUP_MANAGER_H

#include "include/types.h"
#include "common/Cond.h"
#include "common/Thread.h"
#include "rgw_sal_rados.h"
#include "rgw_dedup_worker.h"

using namespace std;
using namespace librados;

extern const string DEFAULT_CHUNK_POOL_POSTFIX;
extern const string DEFAULT_COLD_POOL_POSTFIX;
extern const string DEFAULT_CHUNK_SIZE;
extern const string DEFAULT_CHUNK_ALGO;
extern const string DEFAULT_FP_ALGO;
extern const string DEFAULT_HITSET_TYPE;
extern const int DEFAULT_NUM_WORKERS;
extern const int DEFAULT_SAMPLING_RATIO;
extern const int DEFAULT_DEDUP_SCRUB_RATIO;

struct target_rados_object {
  string object_name;
  string pool_name;
};

class Worker;
class RGWDedupManager : public Thread
{
  const DoutPrefixProvider* dpp;
  CephContext* cct;
  rgw::sal::RadosStore* store;
  bool down_flag;
  vector<target_rados_object> rados_objs;
  vector<unique_ptr<Worker>> workers;

  string chunk_pool_postfix;
  string cold_pool_postfix;
  string chunk_size;
  string chunk_algo;
  string fp_algo;
  string hitset_type;
  int num_workers;
  int sampling_ratio;
  int dedup_scrub_ratio;
  int dedup_worked_cnt;

  /**
   *  There is a data_pool which is regarded as base-pool for a storage_classes.
   *  For dedup, a chunk-pool and a cold-pool are required for each base-pool.
   *  struct dedup_ioctx_set indicates the IoCtxs of the pools related to each other.
   */
  struct dedup_ioctx_set {
    IoCtx base_pool_ctx;
    IoCtx chunk_pool_ctx;
    IoCtx cold_pool_ctx;
  };
  // sc data pool (base-pool name) : ioctx_set
  map<string, dedup_ioctx_set> ioctx_map;

public:
  RGWDedupManager(const DoutPrefixProvider* _dpp,
                 CephContext* _cct,
                 rgw::sal::RadosStore* _store)
    : dpp(_dpp), cct(_cct), store(_store), down_flag(true),
      chunk_pool_postfix(DEFAULT_CHUNK_POOL_POSTFIX),
      cold_pool_postfix(DEFAULT_COLD_POOL_POSTFIX),
      chunk_size(DEFAULT_CHUNK_SIZE),
      chunk_algo(DEFAULT_CHUNK_ALGO),
      fp_algo(DEFAULT_FP_ALGO),
      hitset_type(DEFAULT_HITSET_TYPE),
      num_workers(DEFAULT_NUM_WORKERS),
      sampling_ratio(DEFAULT_SAMPLING_RATIO),
      dedup_scrub_ratio(DEFAULT_DEDUP_SCRUB_RATIO),
      dedup_worked_cnt(0) {}
  virtual ~RGWDedupManager() override {}
  virtual void* entry() override;

  void stop();
  void finalize();
  bool going_down();
  void initialize();
  void set_down_flag(bool new_flag) { down_flag = new_flag; }
  bool get_down_flag() { return down_flag; }

  void reset_workers(bool need_scrub);
  void set_dedup_tier(string base_pool_name);
  int prepare_dedup_work();
  IoCtx get_or_create_ioctx(rgw_pool pool);
  void append_ioctxs(rgw_pool base_pool);
  int get_rados_objects(RGWRados::Object::Stat& stat_op);
  vector<size_t> sample_rados_objects();
  void hand_out_objects(vector<size_t> sampled_indexes);

  int set_num_workers(int new_num_workers);
  int set_sampling_ratio(int new_sampling_ratio);
  void append_rados_obj(target_rados_object new_obj) { rados_objs.emplace_back(new_obj); }
  size_t get_num_rados_obj() { return rados_objs.size(); }
};

#endif
