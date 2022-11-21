// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#ifndef CEPH_RGW_DEDUP_MANAGER_H
#define CEPH_RGW_DEDUP_MANAGER_H

#include "include/types.h"
#include "common/Cond.h"
#include "common/Thread.h"
#include "rgw_sal_rados.h"

using namespace std;

extern const string DEFAULT_CHUNK_POOL_POSTFIX;
extern const string DEFAULT_COLD_POOL_POSTFIX;
extern const string DEFAULT_CHUNK_SIZE;
extern const string DEFAULT_CHUNK_ALGO;
extern const string DEFAULT_FP_ALGO;
extern const string DEFAULT_HITSET_TYPE;

struct target_rados_object {
  string object_name;
  string pool_name;
};

class RGWDedupManager : public Thread
{
  const DoutPrefixProvider* dpp;
  CephContext* cct;
  rgw::sal::RadosStore* store;
  bool down_flag;
  vector<target_rados_object> rados_objs;

  string chunk_pool_postfix;
  string cold_pool_postfix;
  string chunk_size;
  string chunk_algo;
  string fp_algo;
  string hitset_type;

  /**
   *  There is a data_pool which is regarded as base-pool for a storage_classes.
   *  For dedup, a chunk-pool and a cold-pool are required for each base-pool.
   *  struct dedup_ioctx_set indicates the IoCtxs of the pools related to each other.
   */
  struct dedup_ioctx_set {
    librados::IoCtx base_pool_ctx;
    librados::IoCtx chunk_pool_ctx;
    librados::IoCtx cold_pool_ctx;
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
      hitset_type(DEFAULT_HITSET_TYPE) {}
  virtual ~RGWDedupManager() override {}
  virtual void* entry() override;

  void stop();
  void finalize();
  bool going_down();
  void initialize();
  void set_down_flag(bool new_flag) { down_flag = new_flag; }
  bool get_down_flag() { return down_flag; }

protected:
  void set_dedup_tier(string base_pool_name);
  int prepare_dedup_work();
  librados::IoCtx get_or_create_ioctx(rgw_pool pool);
  void append_ioctxs(rgw_pool base_pool);
  int get_rados_objects(RGWRados::Object::Stat& stat_op);
};

#endif
