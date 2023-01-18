// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#ifndef CEPH_RGW_DEDUP_MANAGER_H
#define CEPH_RGW_DEDUP_MANAGER_H

#include "include/types.h"
#include "common/Cond.h"
#include "common/Thread.h"
#include "rgw_sal_rados.h"
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

  shared_ptr<RGWFPManager> fpmanager;
  vector<unique_ptr<RGWDedupWorker>> dedup_workers;
  vector<unique_ptr<RGWChunkScrubWorker>> scrub_workers;

  string cold_pool_name;
  size_t num_workers;
  string chunk_algo;
  size_t chunk_size;
  string fp_algo;
  size_t dedup_threshold;
  size_t dedup_scrub_ratio;
  
  int dedup_worked_cnt;
  bool obj_scan_fwd;    // true: scan rados_objs forward, false: scan reverse

  map<string, IoCtx> ioctx_map;   // base-pool name : base-pool IoCtx
  IoCtx cold_ioctx;

public:
  RGWDedupManager(const DoutPrefixProvider* _dpp,
                 CephContext* _cct,
                 rgw::sal::RadosStore* _store)
    : dpp(_dpp), cct(_cct), store(_store), down_flag(true),
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
  int get_rados_objects(RGWRados::Object::Stat& stat_op);

  // add rados obj to Worker referring to objs_per_worker and update remain_objs
  int append_rados_obj(vector<unique_ptr<RGWDedupWorker>>::iterator& witer,
                       const target_rados_object& obj,
                       const size_t objs_per_worker,
                       int& remtin_objs);

  // distribute aggregated rados objects evenly to each RGWDedupWorker
  // reverse scanning rados_objs direction to avoid an issue that several objects
  // processed in an early stage can not be deduped because of insufficient fp info
  void hand_out_objects();

  // get all rados objects to deduplicate
  int prepare_dedup();

  // get all chunk objects to scrub
  int prepare_scrub();

  string create_cmd(const string& prefix, const vector<pair<string, string>>& options);
  string create_osd_pool_set_cmd(const string prefix, const string base_pool,
                                 const string var, const string val);
};

#endif
