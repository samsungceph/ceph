// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <algorithm>

#include "rgw_dedup_manager.h"
#include "rgw_rados.h"
#include "include/rados/librados.h"

#define dout_subsys ceph_subsys_rgw

const int RETRY_SLEEP_PERIOD = 30;
const int DEDUP_INTERVAL = 3;
const int DEFAULT_NUM_WORKERS = 3;
const int MAX_OBJ_SCAN_SIZE = 100;
const int MAX_BUCKET_SCAN_SIZE = 100;
const int DEFAULT_DEDUP_SCRUB_RATIO = 10;
const string DEFAULT_COLD_POOL_POSTFIX = "_cold";
const string DEFAULT_CHUNK_SIZE = "16384";
const string DEFAULT_CHUNK_ALGO = "fastcdc";
const string DEFAULT_FP_ALGO = "sha1";

void RGWDedupManager::initialize()
{
  fpmanager = make_shared<RGWFPManager>(chunk_algo, stoi(chunk_size), fp_algo);

  for (int i = 0; i < num_workers; ++i) {
    dedup_workers.emplace_back(
      make_unique<RGWDedupWorker>(dpp, cct, store, i, fpmanager));
    scrub_workers.emplace_back(
      make_unique<RGWChunkScrubWorker>(dpp, cct, store, i, num_workers));
  }
}

int RGWDedupManager::append_rados_obj(vector<unique_ptr<RGWDedupWorker>>::iterator& witer,
                                      const target_rados_object& obj,
                                      const size_t objs_per_worker,
                                      int& remain_objs)
{
  ceph_assert(witer != dedup_workers.end());

  (*witer)->append_obj(obj);
  if (((*witer)->get_num_objs() == objs_per_worker)) {
    // append remain object for even distribution if remain_objs exists
    if (remain_objs > 0) {
      --remain_objs;
      return -1;
    }
    ++witer;
  } else if ((*witer)->get_num_objs() > objs_per_worker) {
    ++witer;
  }
  return 0;
}

void RGWDedupManager::hand_out_objects()
{
  size_t objs_per_worker = rados_objs.size() / num_workers;
  int remain_objs = rados_objs.size() % num_workers;
  for (auto& worker: dedup_workers) {
    worker->clear_objs();
  }

  vector<unique_ptr<RGWDedupWorker>>::iterator witer = dedup_workers.begin();
  if (obj_scan_fwd) {
    for (vector<target_rados_object>::iterator oiter = rados_objs.begin();
         oiter != rados_objs.end();
         ++oiter) {
      if (append_rados_obj(witer, *oiter, objs_per_worker, remain_objs) < 0) {
        continue;
      }
    }
  } else {  // reverse direction scan
    for (vector<target_rados_object>::reverse_iterator oiter = rados_objs.rbegin();
         oiter != rados_objs.rend();
         ++oiter) {
      if (append_rados_obj(witer, *oiter, objs_per_worker, remain_objs) < 0) {
        continue;
      }
    }
  }

  // reverse scanning direction
  obj_scan_fwd ^= 1;
}

struct cold_pool_info_t;
/*
 *  append cold pool information which is required to get chunk objects
 *  in order that each RGWChunkScrubWorker can get their own objects in cold pool
 */
int RGWDedupManager::prepare_scrub_work()
{
  Rados* rados = store->getRados()->get_rados_handle();
  cold_pool_info_t cold_pool_info;
  list<string> cold_pool_names;
  map<string, librados::pool_stat_t> cold_pool_stats;
  map<string, string> cold_to_base;   // cold_pool_name : base_pool_name

  for (const auto& [base_pool_name, ioctxs] : ioctx_map) {
    string cold_pool_name = ioctxs.cold_pool_ctx.get_pool_name();
    cold_pool_names.emplace_back(cold_pool_name);
    cold_to_base[cold_pool_name] = base_pool_name;
  }

  int ret = rados->get_pool_stats(cold_pool_names, cold_pool_stats);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "error fetching pool stats: " << cpp_strerror(ret) << dendl;
    return ret;
  }

  for (const auto& [cold_pool_name, pool_stat] : cold_pool_stats) {
    if (pool_stat.num_objects <= 0) {
      ldpp_dout(dpp, 2) << "cold pool (" << cold_pool_name << ") is empty" << dendl;
      continue;
    }

    cold_pool_info_t cold_pool_info;
    ObjectCursor pool_begin, pool_end;
    string base_pool_name = cold_to_base[cold_pool_name];

    IoCtx cold_ioctx = ioctx_map[base_pool_name].cold_pool_ctx;
    pool_begin = cold_ioctx.object_list_begin();
    pool_end = cold_ioctx.object_list_end();
    cold_pool_info.ioctx = cold_ioctx;
    cold_pool_info.num_objs = pool_stat.num_objects;

    for (int i = 0; i < num_workers; ++i) {
      ObjectCursor shard_begin, shard_end;
      cold_ioctx.object_list_slice(pool_begin, pool_end, i, num_workers,
                                   &shard_begin, &shard_end);
      cold_pool_info.shard_begin = shard_begin;
      cold_pool_info.shard_end = shard_end;

      scrub_workers[i]->append_cold_pool_info(cold_pool_info);
    }
  }
  return ret;
}

string RGWDedupManager::create_mon_cmd(const string& prefix,
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

void* RGWDedupManager::entry()
{
  ldpp_dout(dpp, 2) << "RGWDedupManager started" << dendl;

  while (!get_down_flag()) {
    if (dedup_worked_cnt < dedup_scrub_ratio) {
      int ret = prepare_dedup();
      if (ret < 0) {
        ldpp_dout(dpp, 0) << "prepare_dedup() failed" << dendl;
        return nullptr;
      } else if (ret == 0) {
        ldpp_dout(dpp, 2) << "rados object not found. retry" << dendl;
        sleep(RETRY_SLEEP_PERIOD);
        continue;
      }

      hand_out_objects();
      // trigger RGWDedupWorkers
      for (auto& worker : dedup_workers) {
        ceph_assert(worker.get());
        fpmanager->reset_fpmap();
        worker->set_run(true);
        string name = worker->get_id();
        worker->create(name.c_str());
      }

      // all RGWDedupWorkers synchronozed here
      for (auto& worker: dedup_workers) {
        worker->join();
      }
      ++dedup_worked_cnt;
    } else {
      for (auto& worker : scrub_workers) {
        ceph_assert(worker.get());
        worker->clear_chunk_pool_info();
      }
      prepare_scrub_work();

      // trigger RGWChunkScrubWorkers
      for (auto& worker : scrub_workers) {
        worker->set_run(true);
        string name = worker->get_id();
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
  for (int i = 0; i < num_workers; ++i) {
    dedup_workers[i].reset();
    scrub_workers[i].reset();
  }
  dedup_workers.clear();
  scrub_workers.clear();
}

int RGWDedupManager::append_ioctxs(rgw_pool base_pool)
{
  RGWRados* rados = store->getRados();
  ceph_assert(rados);

  librados::IoCtx base_ioctx;
  int ret = rgw_init_ioctx(dpp, rados->get_rados_handle(), base_pool,
                           base_ioctx, true, false);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to get_or_create ioctx pool="
      << base_pool.name << dendl;
    return ret;
  }

  librados::IoCtx cold_ioctx;
  ret = rgw_init_ioctx(dpp, rados->get_rados_handle(),
                       rgw_pool(base_pool.name + cold_pool_postfix), 
                       cold_ioctx, true, false);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to get_or_create ioctx pool="
      << base_pool.name + cold_pool_postfix << dendl;
    return ret;
  }

  dedup_ioctx_set pool_set{base_ioctx, cold_ioctx};
  ioctx_map.insert({base_pool.name, pool_set});
  return 0;
}

string RGWDedupManager::create_osd_pool_set_cmd(const string prefix, const string base_pool,
                                                const string var, const string val)
{
  vector<pair<string, string>> options;
  options.emplace_back(make_pair("pool", base_pool));
  options.emplace_back(make_pair("var", var));
  options.emplace_back(make_pair("val", val));
  return create_mon_cmd(prefix, options);
}

void RGWDedupManager::set_dedup_tier(string base_pool_name)
{
  auto handle = store->svc()->rados->handle();
  string cold_pool_name = base_pool_name + cold_pool_postfix;

  string cmd = create_osd_pool_set_cmd("osd pool set", base_pool_name, "dedup_tier",
                                       cold_pool_name);
  ldpp_dout(dpp, 0) << __func__ << " cmd: " << cmd << dendl;
  if (handle.mon_command(cmd, bufferlist(), nullptr, nullptr) < 0) {
    ldpp_dout(dpp, 0) << __func__ << " mon_command " << cmd << " failed" << dendl;
  }

  cmd = create_osd_pool_set_cmd("osd pool set", base_pool_name,
                                "dedup_chunk_algorithm", chunk_algo);
  ldpp_dout(dpp, 0) << __func__ << " cmd: " << cmd << dendl;
  if (handle.mon_command(cmd, bufferlist(), nullptr, nullptr) < 0) {
    ldpp_dout(dpp, 0) << __func__ << " mon_command " << cmd << " failed" << dendl;
  }

  cmd = create_osd_pool_set_cmd("osd pool set", base_pool_name,
                                "dedup_cdc_chunk_size", chunk_size);
  ldpp_dout(dpp, 0) << __func__ << " cmd: " << cmd << dendl;
  if (handle.mon_command(cmd, bufferlist(), nullptr, nullptr) < 0) {
    ldpp_dout(dpp, 0) << __func__ << " mon_command " << cmd << " failed" << dendl;
  }

  cmd = create_osd_pool_set_cmd("osd pool set", base_pool_name,
                                "fingerprint_algorithm", fp_algo);
  ldpp_dout(dpp, 0) << __func__ << " cmd: " << cmd << dendl;
  if (handle.mon_command(cmd, bufferlist(), nullptr, nullptr) < 0) {
    ldpp_dout(dpp, 0) << __func__ << " mon_command " << cmd << " failed" << dendl;
  }
}

int RGWDedupManager::get_rados_objects(RGWRados::Object::Stat& stat_op)
{
  int ret = stat_op.stat_async(dpp);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: stat_async() returned error: " <<
      cpp_strerror(-ret) << dendl;
    return ret;
  }
  ret = stat_op.wait(dpp);
  if (ret < 0) {
    if (ret != -ENOENT) {
      ldpp_dout(dpp, 0) << "ERROR: stat_async() returned error: " <<
        cpp_strerror(-ret) << dendl;
    }
    // clear error. continue processing directory
    ret = 0;
  }
  return ret;
}

int RGWDedupManager::prepare_dedup()
{
  void* handle = nullptr;
  bool has_remain_bkts = true;
  int total_obj_cnt = 0;

  rados_objs.clear();
  int ret = store->meta_list_keys_init(dpp, "bucket", string(), &handle);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: meta_list_keys_init() failed" << dendl;
    return ret;
  }

  while (has_remain_bkts) {
    list<string> bucket_list;
    ret = store->meta_list_keys_next(dpp, handle, MAX_BUCKET_SCAN_SIZE,
                                     bucket_list, &has_remain_bkts);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: meta_list_keys_next() failed" << dendl;
      store->meta_list_keys_complete(handle);
      return ret;
    }
    for (auto bucket_name : bucket_list) {
      unique_ptr<rgw::sal::Bucket> bucket;
      ret = store->get_bucket(dpp, nullptr, "", bucket_name, &bucket, null_yield);
      if (ret < 0) {
        ldpp_dout(dpp, 0) << "ERROR: get_bucket() failed" << dendl;
        store->meta_list_keys_complete(handle);
        return ret;
      }

      rgw::sal::Bucket::ListParams params;
      rgw::sal::Bucket::ListResults results;
      bool has_remain_objs = true;
      const string bucket_id = bucket->get_key().get_key();
      while (has_remain_objs) {
        ret = bucket->list(dpp, params, MAX_OBJ_SCAN_SIZE, results, null_yield);
        if (ret < 0) {
          ldpp_dout(dpp, 0) << "ERROR: list() failed" << dendl;
          store->meta_list_keys_complete(handle);
          return ret;
        }

        for (auto obj : results.objs) {
          ldpp_dout(dpp, 10) << "rgw_obj name: " << obj.key.name << dendl;
          RGWObjectCtx obj_ctx(store);
          unique_ptr<rgw::sal::Object> rgw_obj = bucket->get_object(obj.key);
          RGWRados::Object op_target(store->getRados(), bucket.get(),
                                     obj_ctx, rgw_obj.get());
          RGWRados::Object::Stat stat_op(&op_target);
          ret = get_rados_objects(stat_op);
          if (ret < 0) {
            store->meta_list_keys_complete(handle);
            return ret;
          }

          RGWObjManifest& manifest = *stat_op.result.manifest;
          RGWObjManifest::obj_iterator miter;
          for (miter = manifest.obj_begin(dpp);
               miter != manifest.obj_end(dpp);
               ++miter) {
            const rgw_raw_obj& rados_obj
              = miter.get_location()
                     .get_raw_obj(static_cast<rgw::sal::RadosStore*>(store));
            if (rados_obj == rgw_raw_obj()) {
              continue;
            }

            string base_pool_name = rados_obj.pool.name;
            if (ioctx_map.find(base_pool_name) == ioctx_map.end()) {
              if (append_ioctxs(rados_obj.pool) < 0) {
                continue;
              }
              set_dedup_tier(base_pool_name);
            }
            ++total_obj_cnt;
          }
        }

        has_remain_objs = results.is_truncated;
      }
    }
  }
  store->meta_list_keys_complete(handle);

  return total_obj_cnt;
}

