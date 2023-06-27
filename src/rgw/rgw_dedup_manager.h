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
  Rados* rados;

  shared_ptr<RGWFPManager> fpmanager;
  vector<unique_ptr<RGWDedupWorker>> dedup_workers;
  vector<unique_ptr<RGWChunkScrubWorker>> scrub_workers;

  string cold_pool_name;
  int num_workers;
  string chunk_algo;
  int chunk_size;
  string fp_algo;
  int dedup_threshold;
  int dedup_scrub_ratio;
  int fpmanager_memory_limit;
  
  int dedup_worked_cnt;
  bool obj_scan_fwd;    // true: scan rados_objs forward, false: scan reverse

  map<string, IoCtx> ioctx_map;   // base-pool name : base-pool IoCtx

public:
  RGWDedupManager(const DoutPrefixProvider* _dpp,
                 CephContext* _cct,
                 rgw::sal::RadosStore* _store)
    : dpp(_dpp), cct(_cct), store(_store), down_flag(true),
      rados(nullptr),
      cold_pool_name(DEFAULT_COLD_POOL_NAME),
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
  int append_ioctxs(rgw_pool base_pool);

  // assign each worker's id
  void prepare_dedup(const int rgwdedup_cnt, const int cur_rgwdedup_id);
  void prepare_scrub(const int rgwdedup_cnt, const int cur_rgwdedup_id);

  string create_cmd(const string& prefix, const vector<pair<string, string>>& options);
  string create_osd_pool_set_cmd(const string prefix, const string base_pool,
                                 const string var, const string val);

  void update_base_pool_info();
  template<class RadosClass>
  int get_multi_rgwdedup_info(int& num_rgwdedups, int& cur_id, RadosClass* rados);
};

#endif
