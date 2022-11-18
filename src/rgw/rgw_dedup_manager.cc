// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_dedup_manager.h"
#include "rgw_rados.h"

#define dout_subsys ceph_subsys_rgw

const int MAX_OBJ_SCAN_SIZE = 100;
const int MAX_BUCKET_SCAN_SIZE = 100;

void RGWDedupManager::initialize()
{
  // TODO: initialize member variables of RGWDedupManager
}

void* RGWDedupManager::entry()
{
  ldpp_dout(dpp, 2) << "RGWDedupManager started" << dendl;

  while (!get_down_flag()) {
    int ret = get_rados_objects();

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

int RGWDedupManager::get_rados_objects()
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
      for (auto bucket_name : bucket_list){
        unique_ptr<rgw::sal::Bucket> bkt;
        ret = store->get_bucket(dpp, nullptr, "", bucket_name, &bkt, null_yield);
        if (ret < 0) {
          ldpp_dout(dpp, 0) << "ERROR: get_bucket() failed" << dendl;
          return ret;
        }

        rgw::sal::Bucket::ListParams params;
        rgw::sal::Bucket::ListResults results;
        bool obj_trunc = true;
        const std::string bucket_id = bkt->get_key().get_key();

        while (obj_trunc) {
          ret = bkt->list(dpp, params, MAX_OBJ_SCAN_SIZE, results, null_yield);
          if (ret < 0) {
            ldpp_dout(dpp, 0) << "ERROR: list() failed" << dendl;
            return ret;
          }

          for (auto obj : results.objs) {
            ldpp_dout(dpp, 0) << "rgw_obj name: " << obj.key.name << dendl;

            RGWObjectCtx obj_ctx(store);
            unique_ptr<rgw::sal::Object> rgw_obj = bkt->get_object(obj.key);
            RGWRados::Object op_target(store->getRados(), bkt.get(), obj_ctx, rgw_obj.get());
            RGWRados::Object::Stat stat_op(&op_target);
            ret = stat_op.stat_async(dpp);
            if (ret < 0) {
              ldpp_dout(dpp, -1) << "ERROR: stat_async() returned error: " <<
                cpp_strerror(-ret) << dendl;
              return ret;
            }
            ret = stat_op.wait(dpp);
            if (ret < 0) {
              if (ret != -ENOENT) {
                ldpp_dout(dpp, -1) << "ERROR: stat_async() returned error: " <<
                  cpp_strerror(-ret) << dendl;
              }
              return ret;
            }

            RGWObjManifest& manifest = *stat_op.result.manifest;
            RGWObjManifest::obj_iterator miter;
            for (miter = manifest.obj_begin(dpp); miter != manifest.obj_end(dpp); ++miter) {
              const rgw_raw_obj& loc = miter.get_location().get_raw_obj(static_cast<rgw::sal::RadosStore*>(store));
              rados_objs.emplace_back(loc.oid);
              ldpp_dout(dpp, 0) << "  rados_oid name: " << loc.oid << dendl;
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