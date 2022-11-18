// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#ifndef CEPH_RGW_DEDUP_WORKER_H
#define CEPH_RGW_DEDUP_WORKER_H

#include "include/rados/librados.hpp"
#include "rgw_dedup_manager.h"

using namespace std;
using namespace librados;

struct target_rados_object;
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

class RGWDedupWorker : public Worker
{
  vector<target_rados_object> rados_objs;

public:
  RGWDedupWorker(const DoutPrefixProvider* _dpp,
              CephContext* _cct,
              rgw::sal::RadosStore* _store,
              int _id)
    : Worker(_dpp, _cct, _store, _id) {}
  virtual ~RGWDedupWorker() override {}

  virtual void* entry() override;
  virtual void finalize() override;

  void append_obj(target_rados_object new_obj);
  size_t get_num_objs();
  void clear_objs();

  virtual string get_id() override;
};

class RGWChunkScrubWorker : public Worker
{
  int num_threads;

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
};

#endif
