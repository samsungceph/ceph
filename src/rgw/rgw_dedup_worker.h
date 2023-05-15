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

extern const int MAX_OBJ_SCAN_SIZE;

using namespace std;
using namespace librados;

class RGWFPManager;
class Worker : public Thread
{
protected:
  const DoutPrefixProvider* dpp;
  CephContext* cct;
  rgw::sal::RadosStore* store;

  // local worker id of a RGWDedup
  int id = -1;
  // # workers throughout total RGWDedups (# RGWDedup * # workers)
  int num_total_workers = 0;
  // global worker id throughout total RGWDedups
  int gid = -1;
  map<uint64_t, IoCtx> base_ioctx_map;
  IoCtx cold_ioctx;

  enum class MetadataObjType : int {
    New,
    Archived,
    Deduped,
    None
  };

public:
  Worker(const DoutPrefixProvider* _dpp,
         CephContext* _cct,
         rgw::sal::RadosStore* _store,
         int _id,
         IoCtx _cold_ioctx)
    : dpp(_dpp), cct(_cct), store(_store), id(_id),
      cold_ioctx(_cold_ioctx) {}
  Worker() = delete;
  Worker(const Worker& rhs) = delete;
  Worker& operator=(const Worker& rhs) = delete;
  virtual ~Worker() {}

  virtual void* entry() = 0;

  int get_id();
  void prepare(const int new_total_workers, const int new_gid);
  void clear_base_ioctx_map(uint64_t id, IoCtx& ioctx);
  void append_base_ioctx(uint64_t name, IoCtx& ioctx);
};

// <chunk data, <offset, length>>
using ChunkInfoType = tuple<bufferlist, pair<uint64_t, uint64_t>>;
class RGWDedupWorker : public Worker
{
  bool obj_scan_dir;    // true: scan obj forward, false: scan object reverse
  shared_ptr<RGWFPManager> fpmanager;
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
                 string _chunk_algo,
                 uint32_t _chunk_size,
                 string _fp_algo,
                 uint32_t _dedup_threshold,
                 IoCtx _cold_ioctx)
    : Worker(_dpp, _cct, _store, _id, _cold_ioctx),
      obj_scan_dir(true),
      fpmanager(_fpmanager),
      chunk_algo(_chunk_algo),
      chunk_size(_chunk_size),
      fp_algo(_fp_algo),
      dedup_threshold(_dedup_threshold) {}
  RGWDedupWorker() = delete;
  RGWDedupWorker(const RGWDedupWorker& rhs) = delete;
  RGWDedupWorker& operator=(const RGWDedupWorker& rhs) = delete;
  virtual ~RGWDedupWorker() override {}

  struct chunk_t {
    size_t start = 0;
    size_t size = 0;
    string fingerprint = "";
    bufferlist data;
  };

  virtual void* entry() override;
  void finalize();

  template <typename Iter>
  void try_object_dedup(IoCtx& base_ioctx, Iter begin, Iter end);
  bufferlist read_object_data(IoCtx& ioctx, string object_name);
  int write_object_data(IoCtx& ioctx, string object_name, bufferlist& data);
  int check_object_exists(IoCtx& ioctx, string object_name);
  MetadataObjType get_metadata_obj_type(object_info_t& oi,
                                        IoCtx& ioctx,
                                        const string obj_name,
                                        const uint32_t data_len);
  void do_chunk_dedup(IoCtx& ioctx, IoCtx& cold_ioctx, string object_name,
                      list<chunk_t> redundant_chunks,
                      map<uint64_t, chunk_info_t>& chunk_map);
  void do_data_evict(IoCtx& ioctx, string object_name);
  int remove_object(IoCtx &ioctx, string object_name);

  int try_set_chunk(IoCtx& ioctx, IoCtx& cold_ioctx, string object_name,
                    chunk_t& chunk);
  int clear_manifest(IoCtx& ioctx, string object_name);
  vector<ChunkInfoType> do_cdc(bufferlist& data, string chunk_algo,
                               uint32_t chunk_size);
  string generate_fingerprint(bufferlist chunk_data, string fp_algo);
  string get_archived_obj_name(IoCtx& ioctx, const string obj_name);
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
  RGWChunkScrubWorker() = delete;
  RGWChunkScrubWorker(const RGWChunkScrubWorker& rhs) = delete;
  RGWChunkScrubWorker& operator=(const RGWChunkScrubWorker& rhs) = delete;
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
