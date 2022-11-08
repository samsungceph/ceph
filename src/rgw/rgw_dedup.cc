// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-                                                      
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_dedup.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;


int RGWDedup::initialize(CephContext* _cct, rgw::sal::RadosStore* _store)
{
  cct = _cct;
  store = _store;
  dedup_manager = make_unique<RGWDedupManager>(this, cct, store);
  if (dedup_manager->initialize() < 0) {
    return -1;
  }
  return 0;
}

void RGWDedup::finalize()
{
  stop_dedup_manager();
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
  assert(dedup_manager.get());
  if (!dedup_manager->get_down_flag()) {
    dedup_manager->stop();
    dedup_manager->join();
    dedup_manager->finalize();
  }
}

RGWDedup::~RGWDedup()
{
  ldpp_dout(this, 2) << "stop RGWDedup done" << dendl;
}

unsigned RGWDedup::get_subsys() const
{
  return dout_subsys;
}

CephContext* RGWDedup::get_cct() const
{
  return cct;
}

std::ostream& RGWDedup::gen_prefix(std::ostream& out) const
{
  return out << "RGWDedup: ";
}
