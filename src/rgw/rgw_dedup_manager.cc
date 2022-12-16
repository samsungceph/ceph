// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_dedup_manager.h"
#include "rgw_rados.h"
#include "services/svc_zone.h"
#include "include/rados/librados.h"

#define dout_subsys ceph_subsys_rgw

const int DEDUP_INTERVAL = 3;
const int MAX_OBJ_SCAN_SIZE = 100;
const uint32_t DEFAULT_DEDUP_SCRUB_RATIO = 5;

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
  fpmanager_memory_limit = cct->_conf->rgw_dedup_fpmanager_memory_limit;
  ceph_assert(fpmanager_memory_limit > 0);
  fpmanager_low_watermark = cct->_conf->rgw_dedup_fpmanager_low_watermark;
  ceph_assert(fpmanager_low_watermark > 0);

  rados = store->getRados()->get_rados_handle();
  ceph_assert(rados);

  // get cold pool ioctx
  IoCtx cold_ioctx;
  int ret = rgw_init_ioctx(dpp, rados, rgw_pool(cold_pool_name), cold_ioctx, false, false);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to get pool=" << cold_pool_name << dendl;
    return ret;
  }

  // initialize components
  fpmanager = make_shared<RGWFPManager>(
    dedup_threshold, fpmanager_memory_limit, fpmanager_low_watermark);

  for (uint32_t i = 0; i < num_workers; ++i) {
    append_dedup_worker(make_unique<RGWDedupWorker>(
      dpp, cct, store, i, num_workers, fpmanager, chunk_algo, chunk_size,
      fp_algo, dedup_threshold, cold_ioctx));
    append_scrub_worker(make_unique<RGWChunkScrubWorker>(
      dpp, cct, store, i, num_workers, cold_ioctx));
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

void RGWDedupManager::run_dedup(uint32_t& dedup_worked_cnt)
{
  ceph_assert(!dedup_workers.empty());
  for (auto& worker : dedup_workers) {
    ceph_assert(worker.get());
    worker->create(("DedupWorker_" + to_string(worker->get_id())).c_str());
  }
  ++dedup_worked_cnt;
}

void RGWDedupManager::run_scrub(uint32_t& dedup_worked_cnt)
{
  ceph_assert(!scrub_workers.empty());
  for (auto& worker : scrub_workers) {
    ceph_assert(worker.get());
    worker->create(("ScrubWorker_" + to_string(worker->get_id())).c_str());
  }
  dedup_worked_cnt = 0;
}

template <typename WorkerType>
void RGWDedupManager::wait_worker(vector<WorkerType>& workers)
{
  ceph_assert(!workers.empty());
  for (auto& worker : workers) {
    ceph_assert(worker.get());
    worker->join();
  }
}

void* RGWDedupManager::entry()
{
  ldpp_dout(dpp, 2) << "RGWDedupManager started" << dendl;
  uint32_t dedup_worked_cnt = 0;
  while (!get_down_flag()) {
    if (dedup_worked_cnt < dedup_scrub_ratio) {
      // do dedup
      ceph_assert(fpmanager.get());
      fpmanager->reset_fpmap();

      update_base_pool_info();
      run_dedup(dedup_worked_cnt);
      wait_worker(dedup_workers);
    } else {
      // do scrub
      run_scrub(dedup_worked_cnt);
      wait_worker(scrub_workers);
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

void RGWDedupManager::finalize()
{
  fpmanager->reset_fpmap();
  fpmanager.reset();

  for (uint32_t i = 0; i < num_workers; ++i) {
    dedup_workers[i]->finalize();
    dedup_workers[i].reset();
  }
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

  for (uint32_t i = 0; i < num_workers; ++i) {
    dedup_workers[i]->append_base_ioctx(base_ioctx.get_id(), base_ioctx);
    scrub_workers[i]->append_base_ioctx(base_ioctx.get_id(), base_ioctx);
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

void RGWDedupManager::append_dedup_worker(unique_ptr<RGWDedupWorker>&& new_worker)
{
  ceph_assert(new_worker.get());
  dedup_workers.emplace_back(move(new_worker));
}

void RGWDedupManager::append_scrub_worker(unique_ptr<RGWChunkScrubWorker>&& new_worker)
{
  ceph_assert(new_worker.get());
  scrub_workers.emplace_back(move(new_worker));
}

