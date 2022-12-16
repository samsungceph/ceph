// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <algorithm>

#include "rgw_dedup_manager.h"
#include "rgw_rados.h"

#define dout_subsys ceph_subsys_rgw

const int DEFAULT_NUM_WORKERS = 3;
const int DEFAULT_SAMPLING_RATIO = 50;
const int MAX_OBJ_SCAN_SIZE = 100;
const int MAX_BUCKET_SCAN_SIZE = 100;
const int DEFAULT_DEDUP_SCRUB_RATIO = 10;
const string DEFAULT_COLD_POOL_POSTFIX = "_cold";
const string DEFAULT_CHUNK_SIZE = "16384";
const string DEFAULT_CHUNK_ALGO = "fastcdc";
const string DEFAULT_FP_ALGO = "sha1";

void RGWDedupManager::initialize()
{
  for (int i = 0; i < num_workers; i++) {
    auto worker = make_unique<RGWDedupWorker>(dpp, cct, store, i);
    workers.emplace_back(move(worker));
  }
}

vector<size_t> RGWDedupManager::sample_rados_objects()
{
  size_t num_objs = get_num_rados_obj();
  vector<size_t> sampled_indexes(num_objs);
  // fill out vector to get sampled indexes
  for (size_t i = 0; i < num_objs; i++) {
    sampled_indexes[i] = i;
  }

  unsigned seed = chrono::system_clock::now().time_since_epoch().count();
  shuffle(sampled_indexes.begin(), sampled_indexes.end(), default_random_engine(seed));
  size_t sampling_count = num_objs * sampling_ratio / 100;
  sampled_indexes.resize(sampling_count);

  return sampled_indexes;
}

void RGWDedupManager::hand_out_objects(vector<size_t> sampled_indexes)
{
  size_t num_objs_per_worker = sampled_indexes.size() / num_workers;
  int remain_objs = sampled_indexes.size() % num_workers;
  for (auto& worker: workers) {
    worker->clear_objs();
  }

  vector<unique_ptr<Worker>>::iterator it = workers.begin();
  for (auto idx : sampled_indexes) {
    (*it)->append_obj(rados_objs[idx]);
    if ((*it)->get_num_objs() >= num_objs_per_worker) {
      // append remain object for even distribution if remain_objs exists
      if (remain_objs > 0) {
        --remain_objs;
        continue;
      }
      ++it;
    }
  }
}

void* RGWDedupManager::entry()
{
  ldpp_dout(dpp, 2) << "RGWDedupManager started" << dendl;

  while (!get_down_flag()) {
    int ret = 0;
    if (dedup_worked_cnt < dedup_scrub_ratio) {
      ldpp_dout(dpp, 2) << "RGWDedupWorkers start" << dendl;

      assert(prepare_dedup_work() >= 0);
      if (ret == 0 && get_num_rados_obj() == 0) {
        ldpp_dout(dpp, 2) << "not a single rados object has been found. do retry" << dendl;
        sleep(60);
        continue;
      }

      vector<size_t> sampled_indexes = sample_rados_objects();
      hand_out_objects(sampled_indexes);

      // trigger RGWDedupWorkers
      for (auto& worker : dedup_workers) {
	worker->set_run(true);
	string name = worker->get_id();
	worker->create(name.c_str());
      }

      // all RGWDedupWorkers synchronized here
      for (auto& worker : dedup_workers) {
	worker->join();
      }
      ++dedup_worked_cnt;
    }
    else {
      ldpp_dout(dpp, 2) << "RGWChunkScrubWorkers start" << dendl;

      for (auto& worker : scrub_workers) {
        worker->initialize();
      }

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
    sleep(3);
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
  for (auto& worker : workers) {
    worker.reset();
  }
  workers.clear();
}

librados::IoCtx RGWDedupManager::get_or_create_ioctx(rgw_pool pool)
{
  librados::IoCtx ioctx;
  rgw_init_ioctx(dpp, store->getRados()->get_rados_handle(), pool,
                 ioctx, true, false);
  return ioctx;
}

void RGWDedupManager::append_ioctxs(rgw_pool base_pool)
{
  string base_pool_name = base_pool.name;
  librados::IoCtx base_ioctx = get_or_create_ioctx(base_pool);

  string cold_pool_name = base_pool_name + cold_pool_postfix;
  librados::IoCtx cold_ioctx = get_or_create_ioctx(rgw_pool(cold_pool_name));

  dedup_ioctx_set pool_set{base_ioctx, cold_ioctx};
  ioctx_map.insert({base_pool_name, pool_set});
}

void RGWDedupManager::set_dedup_tier(string base_pool_name)
{
  string cold_pool_name = ioctx_map[base_pool_name].cold_pool_ctx.get_pool_name();
  librados::Rados* rados = store->getRados()->get_rados_handle();
  bufferlist inbl;
  int ret = rados->mon_command(
    "{\"prefix\": \"osd pool set\", \"pool\": \"" + base_pool_name
    + "\",\"var\": \"dedup_tier\", \"val\": \"" + cold_pool_name
    + "\"}", inbl, nullptr, nullptr);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to set dedup_tier" << dendl;
  }

  ret = rados->mon_command(
    "{\"prefix\": \"osd pool set\", \"pool\": \"" + base_pool_name
    + "\",\"var\": \"dedup_chunk_algorithm\", \"val\": \"" + chunk_algo
    + "\"}", inbl, nullptr, nullptr);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to set dedup_chunk_algorithm" << dendl;
  }

  ret = rados->mon_command(
    "{\"prefix\": \"osd pool set\", \"pool\": \"" + base_pool_name
    + "\",\"var\": \"dedup_cdc_chunk_size\", \"val\": \"" + chunk_size
    + "\"}", inbl, nullptr, nullptr);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to set dedup_cdc_chunk_size" << dendl;
  }

  ret = rados->mon_command(
    "{\"prefix\": \"osd pool set\", \"pool\": \"" + base_pool_name
    + "\",\"var\": \"fingerprint_algorithm\", \"val\": \"" + fp_algo
    + "\"}", inbl, nullptr, nullptr);

  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to set fingerprint_algorithm" << dendl;
  }
}

int RGWDedupManager::get_rados_objects(RGWRados::Object::Stat& stat_op)
{
  int ret = 0;
  ret = stat_op.stat_async(dpp);
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
    return ret;
  }
  return ret;
}

int RGWDedupManager::prepare_dedup_work()
{
  void* handle = nullptr;
  bool bucket_trunc = true;
  int ret = 0;
  int total_obj_cnt = 0;

  ret = store->meta_list_keys_init(dpp, "bucket", string(), &handle);
  if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: meta_list_keys_init() failed" << dendl;
      return ret;
  }

  while (bucket_trunc) {
    list<string> bucket_list;
    ret = store->meta_list_keys_next(dpp, handle, MAX_BUCKET_SCAN_SIZE,
                                     bucket_list, &bucket_trunc);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: meta_list_keys_netx() failed" << dendl;
      return ret;
    }
    else {
      for (auto bucket_name : bucket_list) {
        unique_ptr<rgw::sal::Bucket> bucket;
        ret = store->get_bucket(dpp, nullptr, "", bucket_name, &bucket, null_yield);
        if (ret < 0) {
          ldpp_dout(dpp, 0) << "ERROR: get_bucket() failed" << dendl;
          return ret;
        }

        rgw::sal::Bucket::ListParams params;
        rgw::sal::Bucket::ListResults results;
        bool obj_trunc = true;
        const string bucket_id = bucket->get_key().get_key();

        while (obj_trunc) {
          ret = bucket->list(dpp, params, MAX_OBJ_SCAN_SIZE, results, null_yield);
          if (ret < 0) {
            ldpp_dout(dpp, 0) << "ERROR: list() failed" << dendl;
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

              // do not allow duplicated objects in rados_objs
              bool is_exist = false;
              for (auto& it : rados_objs) {
                if (it.object_name == rados_obj.oid) {
                  ldpp_dout(dpp, 20) << "get_raw_obj() got duplicated rados object ("
                                     << rados_obj.oid << ")" << dendl;
                  is_exist = true;
                  break;
                }
              }
              if (!is_exist) {
                target_rados_object obj{rados_obj.oid, rados_obj.pool.name};
                rados_objs.emplace_back(obj);
                ldpp_dout(dpp, 10) << "  rados_oid name: " << rados_obj.oid 
                                   << ", pool.name: " << rados_obj.pool.name << dendl;
              }

              string base_pool_name = rados_obj.pool.name;
              if (ioctx_map.find(base_pool_name) == ioctx_map.end()) {
                append_ioctxs(rados_obj.pool);
                set_dedup_tier(base_pool_name);
              }
            }
          }

          obj_trunc = results.is_truncated;
          total_obj_cnt += results.objs.size();
        }
      }
    }
  }
  store->meta_list_keys_complete(handle);

  return total_obj_cnt;
}

int RGWDedupManager::set_sampling_ratio(int new_sampling_ratio)
{
  if (new_sampling_ratio <= 0 || new_sampling_ratio > 100) {
    return -1;
  }
  sampling_ratio = new_sampling_ratio;
  return 0;
}
