// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_dedup_manager.h"
#include "rgw_rados.h"
#include "services/svc_zone.h"

#define dout_subsys ceph_subsys_rgw

const int DEDUP_INTERVAL = 3;

int RGWDedupManager::initialize()
{
  // initialize dedup parameters from rgw.yaml.in
  num_workers = cct->_conf->rgw_dedup_num_workers;
  ceph_assert(num_workers > 0);
  chunk_algo = cct->_conf->rgw_dedup_chunk_algo;
  ceph_assert(chunk_algo == "fastcdc" || chunk_algo == "fixed");
  chunk_size = cct->_conf->rgw_dedup_chunk_size;
  ceph_assert(chunk_size > 0);
  fp_algo = cct->_conf->rgw_dedup_fp_algo;
  ceph_assert(fp_algo == "sha1" || fp_algo == "sha256" || fp_algo == "sha512");
  dedup_threshold = cct->_conf->rgw_dedup_threshold;
  ceph_assert(dedup_threshold > 0);
  dedup_scrub_ratio = cct->_conf->rgw_dedup_scrub_ratio;
  ceph_assert(dedup_scrub_ratio > 0);
  cold_pool_name = cct->_conf->rgw_dedup_cold_pool_name;
  ceph_assert(!cold_pool_name.empty());

  rados = store->getRados()->get_rados_handle();
  ceph_assert(rados);

  // get cold pool ioctx
  IoCtx cold_ioctx;
  int ret = rgw_init_ioctx(dpp, rados, rgw_pool(cold_pool_name), cold_ioctx, false, false);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to get pool=" << cold_pool_name << dendl;
    return ret;
  }
  return 0;
}

string RGWDedupManager::create_cmd(const string& prefix,
                                   const vector<pair<string, string>>& options)
{
  ceph_assert(!prefix.empty());
  string cmd("{\"prefix\": \"" + prefix + "\"");
  for (auto& opt : options) {
    cmd.append(", \"" + opt.first + "\": \"" + opt.second + "\"");
  }
  cmd.append("}");
  return cmd;
}

void RGWDedupManager::update_base_pool_info()
{
  RGWZoneParams zone_params = store->svc()->zone->get_zone_params();
  map<std::string, RGWZonePlacementInfo> placement_pools = zone_params.placement_pools;
  for (auto& pp : placement_pools) {
    rgw_pool data_pool = pp.second.get_standard_data_pool();
    ldpp_dout(dpp, 20) << " placement: " << pp.first << ", data pool: "
      << data_pool << dendl;
    append_ioctxs(data_pool.name);
  }
}

void* RGWDedupManager::entry()
{
  ldpp_dout(dpp, 2) << "RGWDedupManager started" << dendl;
  uint32_t dedup_worked_cnt = 0;
  while (!get_down_flag()) {
    if (dedup_worked_cnt < dedup_scrub_ratio) {
      // do dedup
      update_base_pool_info();
      ++dedup_worked_cnt;
    } else {
      // do scrub
      dedup_worked_cnt = 0;
    }
    sleep(DEDUP_INTERVAL);
  }
  return nullptr;
}

void RGWDedupManager::stop()
{
  set_down_flag(true);
  ldpp_dout(dpp, 2) << "RGWDedupManager is set to be stopped" << dendl;
}

int RGWDedupManager::append_ioctxs(rgw_pool base_pool)
{
  ceph_assert(rados);
  IoCtx base_ioctx;
  int ret = rgw_init_ioctx(dpp, rados, base_pool, base_ioctx, false, false);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to get pool=" << base_pool.name << dendl;
    return ret;
  }
  return 0;
}

string RGWDedupManager::create_osd_pool_set_cmd(const string prefix, const string base_pool,
                                                const string var, const string val)
{
  vector<pair<string, string>> options;
  options.emplace_back(make_pair("pool", base_pool));
  options.emplace_back(make_pair("var", var));
  options.emplace_back(make_pair("val", val));
  return create_cmd(prefix, options);
}

void RGWDedupManager::set_down_flag(bool new_flag)
{
  down_flag = new_flag;
}

bool RGWDedupManager::get_down_flag()
{
  return down_flag;
}
