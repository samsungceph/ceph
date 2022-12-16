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

class RGWFPManager;
class RGWDedupWorker;
class RGWChunkScrubWorker;
class RGWDedupManager : public Thread
{
  const DoutPrefixProvider* dpp;
  CephContext* cct;
  rgw::sal::RadosStore* store;
  bool down_flag;
  Rados* rados;

  shared_ptr<RGWFPManager> fpmanager;
  vector<unique_ptr<RGWDedupWorker>> dedup_workers;
  vector<unique_ptr<RGWChunkScrubWorker>> scrub_workers;

  string cold_pool_name;
  string chunk_algo;
  string fp_algo;
  uint32_t num_workers;
  uint32_t chunk_size;
  uint32_t dedup_threshold;
  uint32_t dedup_scrub_ratio;
  bool obj_scan_fwd;    // true: scan forward, false: scan reverse

public:
  RGWDedupManager(const DoutPrefixProvider* _dpp,
                  CephContext* _cct,
                  rgw::sal::RadosStore* _store)
    : dpp(_dpp), cct(_cct), store(_store), down_flag(true),
      cold_pool_name(DEFAULT_COLD_POOL_NAME),
      obj_scan_fwd(true) {}
  RGWDedupManager(const RGWDedupManager& rhs) = delete;
  virtual ~RGWDedupManager() override {}
  virtual void* entry() override;

  void stop();
  int initialize();
  void finalize();
  void set_down_flag(bool new_flag);
  bool get_down_flag();

  bool need_scrub(const uint32_t dedup_worked_cnt);
  void run_dedup(uint32_t& dedup_worked_cnt);
  void run_scrub(uint32_t& dedup_worked_cnt);

  int append_ioctxs(rgw_pool base_pool);
  void update_base_pool_info();
  string create_cmd(const string& prefix,
                    const vector<pair<string, string>>& options);
  string create_osd_pool_set_cmd(const string prefix, const string base_pool,
                                 const string var, const string val);
};

#endif
