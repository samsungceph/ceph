// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-                                            
// vim: ts=8 sw=2 smarttab ft=cpp

#ifndef CEPH_RGW_DEDUP_H
#define CEPH_RGW_DEDUP_H


#include <string>
#include <atomic>
#include <sstream>

#include "include/types.h"
#include "common/Cond.h"
#include "common/Thread.h"
#include "rgw_dedup_manager.h"

using namespace std;

class RGWDedup : public DoutPrefixProvider
{
  CephContext* cct;
  rgw::sal::RadosStore* store;
  unique_ptr<RGWDedupManager> dedup_manager;

public:
  RGWDedup() : cct(nullptr), store(nullptr) {}
  ~RGWDedup() override;

  int initialize(CephContext* _cct, rgw::sal::RadosStore* _store);
  void finalize();

  void start_dedup_manager();
  void stop_dedup_manager();

  CephContext* get_cct() const override;
  unsigned get_subsys() const override;
  std::ostream& gen_prefix(std::ostream& out) const override;
};

#endif

