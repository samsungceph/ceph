// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#ifndef CEPH_RGW_DEDUP_MANAGER_H
#define CEPH_RGW_DEDUP_MANAGER_H

#include "include/types.h"
#include "common/Cond.h"
#include "common/Thread.h"
#include "rgw_sal_rados.h"
#include "rgw_perf_counters.h"
#include "rgw_fp_manager.h"
#include "rgw_dedup_worker.h"

using namespace std;
using namespace librados;

extern const string DEFAULT_COLD_POOL_NAME;
extern const string DEFAULT_CHUNK_SIZE;
extern const string DEFAULT_CHUNK_ALGO;
extern const string DEFAULT_FP_ALGO;
extern const int DEFAULT_NUM_WORKERS;
extern const int DEFAULT_DEDUP_SCRUB_RATIO;

struct target_rados_object {
  string object_name;
  string pool_name;
};

class RGWFPManager;
class RGWDedupWorker;
class RGWChunkScrubWorker;

class RGWDedupManager : public Thread
{
  const DoutPrefixProvider* dpp;
  CephContext* cct;
  rgw::sal::RadosStore* store;
  bool down_flag;
  vector<target_rados_object> rados_objs;

  shared_ptr<RGWFPManager> fpmanager;
  vector<unique_ptr<RGWDedupWorker>> dedup_workers;
  vector<unique_ptr<RGWChunkScrubWorker>> scrub_workers;

  string cold_pool_name;
  string chunk_size;
  string chunk_algo;
  string fp_algo;
  int num_workers;
  int dedup_scrub_ratio;
  int dedup_worked_cnt;
  bool obj_scan_fwd;    // true: scan rados_objs forward, false: scan reverse

  /**
   *  There is a data_pool which is regarded as base-pool for a storage_classes.
   *  For a deduplication, a cold-pool is required for each base-pool.
   *  dedup_ioctx_set indicates the IoCtxs of the pools related to each other.
   */
  struct dedup_ioctx_set {
    IoCtx base_pool_ctx;
    IoCtx cold_pool_ctx;
  };
  map<string, dedup_ioctx_set> ioctx_map; // SC data pool (base-pool name):ioctxs

public:
  RGWDedupManager(const DoutPrefixProvider* _dpp,
                 CephContext* _cct,
                 rgw::sal::RadosStore* _store)
    : dpp(_dpp), cct(_cct), store(_store), down_flag(true),
      cold_pool_name(DEFAULT_COLD_POOL_NAME),
      chunk_size(DEFAULT_CHUNK_SIZE),
      chunk_algo(DEFAULT_CHUNK_ALGO),
      fp_algo(DEFAULT_FP_ALGO),
      num_workers(DEFAULT_NUM_WORKERS),
      dedup_scrub_ratio(DEFAULT_DEDUP_SCRUB_RATIO),
      dedup_worked_cnt(0),
      obj_scan_fwd(true) {}
  virtual ~RGWDedupManager() override {}
  virtual void* entry() override;

  void stop();
  void finalize();
  bool going_down();
  int initialize();
  void set_down_flag(bool new_flag) { down_flag = new_flag; }
  bool get_down_flag() { return down_flag; }
  size_t get_num_rados_obj() { return rados_objs.size(); }
  void set_dedup_tier(string base_pool_name);
  int append_ioctxs(rgw_pool base_pool);

  // add rados obj to Worker referring to objs_per_worker and update remain_objs
  int append_rados_obj(vector<unique_ptr<RGWDedupWorker>>::iterator& witer,
                       const target_rados_object& obj,
                       const size_t objs_per_worker,
                       int& remtin_objs);

  // distribute aggregated rados objects evenly to each RGWDedupWorker
  // reverse scanning rados_objs direction to avoid an issue that several objects
  // processed in an early stage can not be deduped because of insufficient fp info
  void hand_out_objects();

  // assign each worker's id
  void prepare_dedup(const int rgwdedup_cnt, const int cur_rgwdedup_id);
  void prepare_scrub(const int rgwdedup_cnt, const int cur_rgwdedup_id);

  string create_mon_cmd(const string& prefix, const vector<pair<string, string>>& options);
  string create_osd_pool_set_cmd(const string prefix, const string base_pool,
                                 const string var, const string val);

  int get_multi_rgwdedup_info(int& num_rgwdedups, int& cur_id);
  void update_base_pool_info();
};

#endif
