// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#ifndef CEPH_RGW_DEDUP_WORKER_H
#define CEPH_RGW_DEDUP_WORKER_H

#include "cls/cas/cls_cas_internal.h"
#include "include/rados/librados.hpp"
#include "rgw_perf_counters.h"
#include "rgw_fp_manager.h"
#include "rgw_dedup_manager.h"
#include "common/CDC.h"

extern const int MAX_OBJ_SCAN_SIZE;

using namespace std;
using namespace librados;

struct target_rados_object;

class RGWFPManager;
class RGWSI_Zone;

class Worker : public Thread
{
protected:
  const DoutPrefixProvider* dpp;
  CephContext* cct;
  rgw::sal::RadosStore* store;
  bool is_run;

  // local worker id of a RGWDedup
  int id;
  // # workers throughout total RGWDedups (# RGWDedup * # workers)
  int num_total_workers = 0;
  // global worker id throughout total RGWDedups
  int gid = -1;
  map<uint64_t, IoCtx> base_ioctx_map;
  IoCtx cold_ioctx;

public:
  Worker(const DoutPrefixProvider* _dpp,
         CephContext* _cct,
         rgw::sal::RadosStore* _store,
         int _id,
         IoCtx _cold_ioctx)
    : dpp(_dpp), cct(_cct), store(_store), is_run(false), id(_id),
      cold_ioctx(_cold_ioctx) {}
  Worker(const Worker& rhs) = delete;
  virtual ~Worker() {}

  virtual void* entry() = 0;
  void stop();

  int get_id();
  void set_run(bool run_status);
  void prepare(const int new_total_workers, const int new_gid);
  void clear_base_ioctx_map();
  void append_base_ioctx(uint64_t name, IoCtx& ioctx);
};

// <chunk data, <offset, length>>
using ChunkInfoType = tuple<bufferlist, pair<uint64_t, uint64_t>>;
class RGWDedupWorker : public Worker
{
  bool obj_scan_dir;    // true: scan obj forward, false: scan object reverse
  shared_ptr<RGWFPManager> fpmanager;
  vector<target_rados_object> rados_objs;
  string chunk_algo;
  uint32_t chunk_size;
  string fp_algo;
  uint32_t dedup_threshold;

public:
  RGWDedupWorker(const DoutPrefixProvider* _dpp,
                 CephContext* _cct,
                 rgw::sal::RadosStore* _store,
                 int _id,
                 shared_ptr<RGWFPManager> _fpmanager,
                 IoCtx _cold_ioctx)
    : Worker(_dpp, _cct, _store, _id, _cold_ioctx),
      obj_scan_dir(true),
      fpmanager(_fpmanager) {}
  RGWDedupWorker(const RGWDedupWorker& rhs) = delete;
  virtual ~RGWDedupWorker() override {}

  struct chunk_t {
    size_t start = 0;
    size_t size = 0;
    string fingerprint = "";
    bufferlist data;
  };

  virtual void* entry() override;

  void append_obj(target_rados_object new_obj);
  size_t get_num_objs();
  void clear_objs();

  template <typename Iter>
  void try_object_dedup(IoCtx& base_ioctx, Iter begin, Iter end);
  bufferlist read_object_data(IoCtx &ioctx, string object_name);
  int write_object_data(IoCtx &ioctx, string object_name, bufferlist &data);
  int check_object_exists(IoCtx& ioctx, string object_name);
  int try_set_chunk(IoCtx& ioctx, IoCtx &cold_ioctx, string object_name,
                    chunk_t &chunk);
  void do_chunk_dedup(IoCtx &ioctx, IoCtx &cold_ioctx, string object_name,
                      list<chunk_t> redundant_chunks);
  void do_data_evict(IoCtx &ioctx, string object_name);
  int clear_manifest(IoCtx &ioctx, string object_name);
  int remove_object(IoCtx &ioctx, string object_name);
  vector<ChunkInfoType> do_cdc(bufferlist &data, string chunk_algo,
                               uint32_t chunk_size);
  string generate_fingerprint(bufferlist chunk_data, string fp_algo);
};

class RGWChunkScrubWorker : public Worker
{

public:
  RGWChunkScrubWorker(const DoutPrefixProvider* _dpp,
                      CephContext* _cct,
                      rgw::sal::RadosStore* _store,
                      int _id,
                      IoCtx _cold_ioctx)
    : Worker(_dpp, _cct, _store, _id, _cold_ioctx) {}
  RGWChunkScrubWorker(const RGWChunkScrubWorker& rhs) = delete;
  virtual ~RGWChunkScrubWorker() override {}
  
  virtual void* entry() override;

  // fix mismatched chunk reference
  int do_chunk_repair(IoCtx& cold_ioctx, const string chunk_obj_name,
		                  const hobject_t src_obj, int chunk_ref_cnt,
		                  int src_ref_cnt);
  
  // get references of chunk object
  int get_chunk_refs(IoCtx& chunk_ioctx, const string& chunk_oid, chunk_refs_t& refs);

  // check whether dedup reference is mismatched (false is mismatched) 
  int get_src_ref_cnt(const hobject_t& src_obj, const string& chunk_oid);
};

#endif
