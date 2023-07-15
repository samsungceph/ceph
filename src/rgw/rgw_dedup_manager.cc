// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_dedup_manager.h"
#include "rgw_rados.h"
#include "include/rados/librados.h"
#include "rgw_bucket.h"
#include "services/svc_zone.h"
#include "common/ceph_json.h"

#define dout_subsys ceph_subsys_rgw

const int RETRY_SLEEP_PERIOD = 5;
const int DEDUP_INTERVAL = 3;
const int MAX_OBJ_SCAN_SIZE = 100;
const int MAX_BUCKET_SCAN_SIZE = 100;
const uint32_t MAX_CHUNK_REF_SIZE = 10000;
const string DEFAULT_COLD_POOL_NAME = "default-cold-pool";

int RGWDedupManager::initialize()
{
  // create cold pool if not exist
  Rados* rados = store->getRados()->get_rados_handle();
  IoCtx cold_ioctx;
  int ret = rgw_init_ioctx(dpp, rados, rgw_pool(cold_pool_name), cold_ioctx, true, false);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to get_or_create ioctx pool="
      << cold_pool_name << dendl;
    return ret;
  }

  num_workers = cct->_conf->rgw_dedup_num_workers;
  chunk_algo = cct->_conf->rgw_dedup_chunk_algo;
  chunk_size = cct->_conf->rgw_dedup_chunk_size;
  fp_algo = cct->_conf->rgw_dedup_fp_algo;
  dedup_threshold = cct->_conf->rgw_dedup_threshold;
  dedup_scrub_ratio = cct->_conf->rgw_dedup_scrub_ratio;
  fpmanager_memory_limit = cct->_conf->rgw_dedup_fpmanager_memory_limit;

  // initialize components
  fpmanager = make_shared<RGWFPManager>(chunk_algo, chunk_size, fp_algo, dedup_threshold, fpmanager_memory_limit);

  for (uint32_t i = 0; i < num_workers; ++i) {
    dedup_workers.emplace_back(
      make_unique<RGWDedupWorker>(dpp, cct, store, i, fpmanager, cold_ioctx));
    scrub_workers.emplace_back(
      make_unique<RGWChunkScrubWorker>(dpp, cct, store, i, cold_ioctx));
  }
  return 0;
}

void RGWDedupManager::prepare_scrub(const int rgwdedup_cnt, const int cur_rgwdedup_id)
{
  for (auto& worker : scrub_workers) {
    ceph_assert(worker.get());
    worker->prepare(rgwdedup_cnt * num_workers,
        cur_rgwdedup_id * rgwdedup_cnt + worker->get_id());
  }
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
  for (uint32_t i = 0; i < num_workers; ++i) {
    dedup_workers[i]->clear_base_ioctx_map();
    scrub_workers[i]->clear_base_ioctx_map();
  }

  RGWZoneParams zone_params = store->svc()->zone->get_zone_params();
  map<std::string, RGWZonePlacementInfo> placement_pools = zone_params.placement_pools;
  for (auto& pp : placement_pools) {
    rgw_pool data_pool = pp.second.get_standard_data_pool();
    ldpp_dout(dpp, 20) << " placement: " << pp.first << ", data pool: "
      << data_pool << dendl;
    append_ioctxs(data_pool.name);
  }
}

int RGWDedupManager::get_multi_rgwdedup_info(int& num_rgwdedups, int& cur_id)
{
  bufferlist result;
  vector<pair<string, string>> options;
  options.emplace_back(make_pair("format", "json"));
  string cmd = create_cmd("service dump", options);

  Rados* rados = store->getRados()->get_rados_handle();
  if (rados->mgr_command(cmd, bufferlist(), &result, nullptr) < 0) {
    ldpp_dout(dpp, 0) << __func__ << " mgr_command " << cmd << " failed" << dendl;
    return -1;
  }

  string dump = result.to_str();
  JSONParser service_parser;
  if (!service_parser.parse(dump.c_str(), dump.size())) {
    return -1;
  }

  JSONFormattable f;
  try {
    decode_json_obj(f, &service_parser);
  } catch (JSONDecoder::err& e) {
    ldpp_dout(dpp, 2) << __func__ << " Failed to decode JSON object" << dendl;
  }

  if (!f.exists("services")) {
    return -1;
  }

  if (!f["services"].exists("rgw")) {
    return -1;
  }

  if (!f["services"]["rgw"].exists("daemons")) {
    return -1;
  }

  uint64_t rgw_gid = rados->get_instance_id();
  num_rgwdedups = f["services"]["rgw"]["daemons"].object().size();
  int idx = 0;
  for (const auto& [k, v] : f["services"]["rgw"]["daemons"].object()) {
    if (!v.exists("metadata") || !v["metadata"].exists("pid")) {
      --num_rgwdedups;
      continue;
    }

    if (rgw_gid == std::stoi(k)) {
      cur_id = idx;
    }
    ++idx;
  }

  // current RGWDedup not found in Ceph cluster
  if (cur_id == num_rgwdedups) {
    return -1;
  }
  return 0;
}

void* RGWDedupManager::entry()
{
  ldpp_dout(dpp, 2) << "RGWDedupManager started" << dendl;

  while (!get_down_flag()) {
    if (perfcounter) {
      perfcounter->set(l_rgw_dedup_worker_count, num_workers);
      perfcounter->set(l_rgw_dedup_scrub_ratio, dedup_scrub_ratio);

      if (chunk_algo == "fixed") {
        perfcounter->set(l_rgw_dedup_chunk_algo, 1);
      } else if (chunk_algo == "fastcdc") {
        perfcounter->set(l_rgw_dedup_chunk_algo, 2);
      }
      
      perfcounter->set(l_rgw_dedup_chunk_size, chunk_size);
      
      if (fp_algo == "sha1") {
        perfcounter->set(l_rgw_dedup_fp_algo, 1);
      } else if (fp_algo == "sha256") {
        perfcounter->set(l_rgw_dedup_fp_algo, 2);
      } else if (fp_algo == "sha512") {
        perfcounter->set(l_rgw_dedup_fp_algo, 3);
      }
    }

    int num_rgwdedup, cur_rgwdedup_id;
    if (get_multi_rgwdedup_info(num_rgwdedup, cur_rgwdedup_id) < 0) {
      ldpp_dout(dpp, 2) << "current RGWDedup thread not found yet in Ceph Cluster."
        << " Retry a few seconds later." << dendl;
      sleep(RETRY_SLEEP_PERIOD);
      continue;
    }
    ldpp_dout(dpp, 10) << "num rgwdedup: " << num_rgwdedup << ", cur rgwdedup id: "
      << cur_rgwdedup_id << dendl;

    update_base_pool_info(); 
    if (dedup_worked_cnt < dedup_scrub_ratio) {
      // dedup period
      if (perfcounter) {
        perfcounter->set(l_rgw_dedup_current_worker_mode, 1);
      }

      // set dedup worker id
      prepare_dedup(num_rgwdedup, cur_rgwdedup_id);

      // trigger RGWDedupWorkers
      for (auto& worker : dedup_workers) {
        ceph_assert(worker.get());
        fpmanager->reset_fpmap();
        worker->set_run(true);
        string name = "DedupWorker_" + to_string(worker->get_id());
        worker->create(name.c_str());
      }

      // all RGWDedupWorkers synchronozed here
      for (auto& worker: dedup_workers) {
        worker->join();
      }
      ++dedup_worked_cnt;
    } else {
      // scrub period
      if (perfcounter) {
        perfcounter->set(l_rgw_dedup_current_worker_mode, 2);
      }

      // set scrub worker id
      prepare_scrub(num_rgwdedup, cur_rgwdedup_id);

      // trigger RGWChunkScrubWorkers
      for (auto& worker : scrub_workers) {
        ceph_assert(worker.get());
        worker->set_run(true);
        string name = "ScrubWorker_" + to_string(worker->get_id());
        worker->create(name.c_str());
      }

      // all RGWChunkScrubWorkers synchronozed here
      for (auto& worker : scrub_workers) {
        worker->join();
      }
      dedup_worked_cnt = 0;
    }
    sleep(DEDUP_INTERVAL);
  }

  ldpp_dout(dpp, 2) << "RGWDedupManager is going down" << dendl;
  return nullptr;
}

void RGWDedupManager::stop()
{
  set_down_flag(true);
  ldpp_dout(dpp, 2) << "RGWDedupManager is set to be stopped" << dendl;
}

void RGWDedupManager::finalize()
{
  for (uint32_t i = 0; i < num_workers; ++i) {
    dedup_workers[i].reset();
    scrub_workers[i].reset();
  }
  fpmanager.reset();
  dedup_workers.clear();
  scrub_workers.clear();
}

int RGWDedupManager::append_ioctxs(rgw_pool base_pool)
{
  Rados* rados = store->getRados()->get_rados_handle();
  ceph_assert(rados);

  IoCtx base_ioctx;
  int ret = rgw_init_ioctx(dpp, rados, base_pool, base_ioctx, true, false);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to get_or_create ioctx pool="
      << base_pool.name << dendl;
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

void RGWDedupManager::prepare_dedup(const int rgwdedup_cnt, const int cur_rgwdedup_id)
{
  for (auto& worker : dedup_workers) {
    ceph_assert(worker.get());
    worker->prepare(rgwdedup_cnt * num_workers,
        cur_rgwdedup_id * num_workers + worker->get_id());
  }
}

