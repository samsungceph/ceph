// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#ifndef CEPH_RGW_DEDUP_WORKER_H
#define CEPH_RGW_DEDUP_WORKER_H

#include "cls/cas/cls_cas_internal.h"
#include "include/rados/librados.hpp"
#include "rgw_fp_manager.h"
#include "rgw_dedup_manager.h"
#include "common/CDC.h"

extern const int MAX_OBJ_SCAN_SIZE;

using namespace std;
using namespace librados;

struct target_rados_object;
struct dedup_ioctx_set;

class RGWFPManager;

class Worker : public Thread
{
protected:
  const DoutPrefixProvider* dpp;
  CephContext* cct;
  rgw::sal::RadosStore* store;
  int id;
  bool is_run;

public:
  Worker(const DoutPrefixProvider* _dpp,
         CephContext* _cct,
         rgw::sal::RadosStore* _store,
         int _id)
    : dpp(_dpp), cct(_cct), store(_store), id(_id), is_run(false) {}
  virtual ~Worker() {}

  virtual void* entry() = 0;
  virtual void finalize() = 0;
  void stop();

  virtual string get_id() = 0;
  void set_run(bool run_status);
};

struct chunk_t {
  size_t start = 0;
  size_t size = 0;
  string fingerprint = "";
  bufferlist data;
};
class RGWDedupWorker : public Worker
{
  shared_ptr<RGWFPManager> fpmanager;
  vector<target_rados_object> rados_objs;

public:
  RGWDedupWorker(const DoutPrefixProvider* _dpp,
                 CephContext* _cct,
                 rgw::sal::RadosStore* _store,
                 int _id,
                 shared_ptr<RGWFPManager> _fpmanager)
    : Worker(_dpp, _cct, _store, _id), fpmanager(_fpmanager) {}
  virtual ~RGWDedupWorker() override {}

  virtual void* entry() override;
  virtual void finalize() override;

  void append_obj(target_rados_object new_obj);
  size_t get_num_objs();
  void clear_objs();
  virtual string get_id() override;

  bufferlist read_object_data(IoCtx &ioctx, string object_name);
  int write_object_data(IoCtx &ioctx, string object_name, bufferlist &data);
  int check_object_exists(IoCtx& ioctx, string object_name);
  int try_set_chunk(IoCtx& ioctx, IoCtx &cold_ioctx, string object_name, chunk_t &chunk);
  void do_chunk_dedup(IoCtx &ioctx, IoCtx &cold_ioctx, string object_name, list<chunk_t> redundant_chunks);
  void do_data_evict(IoCtx &ioctx, string object_name);
  int clear_manifest(IoCtx &ioctx, string object_name);
  int remove_object(IoCtx &ioctx, string object_name);
  vector<tuple<bufferlist, pair<uint64_t, uint64_t>>> do_cdc(bufferlist &data, string chunk_algo, ssize_t chunk_size);
  string generate_fingerprint(bufferlist chunk_data, string fp_algo);
};

struct cold_pool_info_t
{
  IoCtx ioctx;
  uint64_t num_objs;
  ObjectCursor shard_begin;
  ObjectCursor shard_end;
};

class RGWChunkScrubWorker : public Worker
{
  int num_threads;
  vector<cold_pool_info_t> cold_pool_info;
  map<uint64_t, IoCtx> ioctx_map;

public:
  RGWChunkScrubWorker(const DoutPrefixProvider* _dpp,
                      CephContext* _cct,
                      rgw::sal::RadosStore* _store,
                      int _id,
                      int _num_threads)
    : Worker(_dpp, _cct, _store, _id),
      num_threads(_num_threads) {}
  virtual ~RGWChunkScrubWorker() override {}
  
  virtual void* entry() override;
  virtual void finalize() override;

  virtual string get_id() override;
  void append_cold_pool_info(cold_pool_info_t cold_pool_info);
  void clear_chunk_pool_info() {cold_pool_info.clear(); }

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
