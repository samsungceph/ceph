// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <algorithm>

#include "rgw_dedup_manager.h"
#include "rgw_rados.h"

#define dout_subsys ceph_subsys_rgw

const int MAX_OBJ_SCAN_SIZE = 100;
const int MAX_BUCKET_SCAN_SIZE = 100;
const string DEFAULT_CHUNK_POOL_POSTFIX = "_chunk";
const string DEFAULT_COLD_POOL_POSTFIX = "_cold";
const string DEFAULT_CHUNK_SIZE = "16384";
const string DEFAULT_CHUNK_ALGO = "fastcdc";
const string DEFAULT_FP_ALGO = "sha1";
const string DEFAULT_HITSET_TYPE = "bloom";

void RGWDedupManager::initialize()
{
  // TODO: initialize member variables of RGWDedupManager
}

void* RGWDedupManager::entry()
{
  ldpp_dout(dpp, 2) << "RGWDedupManager started" << dendl;

  while (!get_down_flag()) {
    int ret = 0;
    assert(prepare_dedup_work() >= 0);
    if (ret == 0) {
      ldpp_dout(dpp, 2) << "not a single rados object has been found. do retry" << dendl;
      sleep(3);
      continue;
    }

    // TODO: do dedup work
    
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
  // TODO: finalize member variables of RGWDedupManager
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

  string chunk_pool_name = base_pool_name + chunk_pool_postfix;
  librados::IoCtx chunk_ioctx = get_or_create_ioctx(rgw_pool(chunk_pool_name));

  string cold_pool_name = base_pool_name + cold_pool_postfix;
  librados::IoCtx cold_ioctx = get_or_create_ioctx(rgw_pool(cold_pool_name));

  dedup_ioctx_set pool_set{base_ioctx, chunk_ioctx, cold_ioctx};
  ioctx_map.insert({base_pool_name, pool_set});
}

void RGWDedupManager::set_dedup_tier(string base_pool_name)
{
  string chunk_pool_name = ioctx_map[base_pool_name].chunk_pool_ctx.get_pool_name();
  librados::Rados* rados = store->getRados()->get_rados_handle();
  bufferlist inbl;
  int ret = rados->mon_command(
    "{\"prefix\": \"osd pool set\", \"pool\": \"" + base_pool_name
    + "\",\"var\": \"dedup_tier\", \"val\": \"" + chunk_pool_name
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

  ret = rados->mon_command(
    "{\"prefix\": \"osd pool set\", \"pool\": \"" + base_pool_name
    + "\",\"var\": \"hit_set_type\", \"val\": \"" + hitset_type
    + "\"}", inbl, nullptr, nullptr);

  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to set hit_set_type" << dendl;
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
                  continue;
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
