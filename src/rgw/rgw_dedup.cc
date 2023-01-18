// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-                                                      
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_dedup.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;


void RGWDedup::initialize(CephContext* _cct, rgw::sal::RadosStore* _store)
{
  cct = _cct;
  store = _store;
  dedup_manager = make_unique<RGWDedupManager>(this, cct, store);
  dedup_manager->initialize();
}

void RGWDedup::finalize()
{
  dedup_manager.reset();
}

void RGWDedup::start_dedup_manager()
{
  assert(dedup_manager.get());
  dedup_manager->set_down_flag(false);
  dedup_manager->create("dedup_manager");
}

void RGWDedup::stop_dedup_manager()
{
  if (!dedup_manager->get_down_flag() && dedup_manager.get()) {
    dedup_manager->stop();
    dedup_manager->join();
    dedup_manager->finalize();
  }
}

RGWDedup::~RGWDedup()
{
  if (dedup_manager.get()) {
    stop_dedup_manager();
  }
  finalize();
  
  ldpp_dout(this, 2) << "stop RGWDedup done" << dendl;
}

unsigned RGWDedup::get_subsys() const
{
  return dout_subsys;
}


void RGWDedup::trace_obj(rgw_obj obj)
{
  dedup_manager->trace_obj(obj);
}