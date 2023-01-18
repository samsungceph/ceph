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

class RGWFPManager;
class RGWDedupWorker;
class RGWDedupManager : public Thread
{
  const DoutPrefixProvider* dpp;
  CephContext* cct;
  rgw::sal::RadosStore* store;
  bool down_flag;
  Rados* rados;

  shared_ptr<RGWFPManager> fpmanager;
  vector<unique_ptr<RGWDedupWorker>> dedup_workers;

  string cold_pool_name;
  string chunk_algo;
  string fp_algo;
  uint32_t num_workers;
  uint32_t chunk_size;
  uint32_t dedup_threshold;
  uint32_t dedup_scrub_ratio;
  uint64_t fpmanager_memory_limit;
  uint32_t fpmanager_low_watermark;

public:
  RGWDedupManager(const DoutPrefixProvider* _dpp,
                  CephContext* _cct,
                  rgw::sal::RadosStore* _store)
    : dpp(_dpp), cct(_cct), store(_store), down_flag(true) {}
  RGWDedupManager() = delete;
  RGWDedupManager(const RGWDedupManager& rhs) = delete;
  RGWDedupManager& operator=(const RGWDedupManager& rhs) = delete;
  virtual ~RGWDedupManager() override {}
  virtual void* entry() override;

  void stop();
  int initialize();
  void finalize();
  void set_down_flag(bool new_flag);
  bool get_down_flag();

  // WorkerType: RGWDedupWorker or RGWChunkScrubWorker
  template <typename WorkerType>
  void run_worker(vector<WorkerType>& workers, string tname_prefix);
  template <typename WorkerType>
  void wait_worker(vector<WorkerType>& workers);

  int append_ioctxs(rgw_pool base_pool);
  void update_base_pool_info();
  string create_cmd(const string& prefix,
                    const vector<pair<string, string>>& options);
  string create_osd_pool_set_cmd(const string prefix, const string base_pool,
                                 const string var, const string val);
};

#endif
