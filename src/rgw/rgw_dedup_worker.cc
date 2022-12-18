// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_dedup_worker.h"
#include "cls/cas/cls_cas_internal.h"
#include "cls/cas/cls_cas_client.h"

#define dout_subsys ceph_subsys_rgw


void Worker::set_run(bool run_status)
{
  is_run = run_status;
}

void Worker::stop()
{
  is_run = false;
}


string RGWDedupWorker::get_id()
{
  return "DedupWorker_" + to_string(id);
}

void RGWDedupWorker::initialize()
{

}

void* RGWDedupWorker::entry()
{

  return nullptr;
}

void RGWDedupWorker::finalize()
{

}

void RGWDedupWorker::clear_objs()
{
  rados_objs.clear();
}

void RGWDedupWorker::append_obj(target_rados_object new_obj)
{
  rados_objs.emplace_back(new_obj);
}

size_t RGWDedupWorker::get_num_objs()
{
  return rados_objs.size();
}


string RGWChunkScrubWorker::get_id()
{
  return "ScrubWorker_" + to_string(id);
}

void RGWChunkScrubWorker::initialize()
{

}

int RGWChunkScrubWorker::do_chunk_repair(IoCtx& cold_ioctx,
					 const string chunk_obj_name,
					 const hobject_t src_obj,
					 int chunk_ref_cnt,
					 int source_ref_cnt)
{
  int ret = 0;
  assert(chunk_ref_cnt >= source_ref_cnt);

  while (chunk_ref_cnt != source_ref_cnt) {
    ObjectWriteOperation op;
    cls_cas_chunk_put_ref(op, src_obj);
    --chunk_ref_cnt;
    ret = cold_ioctx.operate(chunk_obj_name, &op);
    if (ret < 0) {
      return ret;
    }
  }
  return ret;
}

/*
 *  - chunk object: A part of the source object that is created by doing deup operation on it.
 *      It has a reference list containing its' source objects.
 *  - source object: An original object of its' chunk objects. It has its chunk information
 *      in a chunk_map.
 */
void* RGWChunkScrubWorker::entry()
{
  Rados* rados = store->getRados()->get_rados_handle();
  ldpp_dout(dpp, 10) << "ScrubWorker_" << id << " starts with " << cold_pool_info.size() 
    << " infos" << dendl;

  // get sharded chunk objects from all cold pools
  for (auto& cold_pool : cold_pool_info) {
    ldpp_dout(dpp, 10) << "cold pool (" << cold_pool.ioctx.get_pool_name()
       << ") has " << cold_pool.num_objs << " objects" << dendl;

    IoCtx cold_ioctx = cold_pool.ioctx;
    ObjectCursor obj_cursor = cold_pool.shard_begin;
    while (obj_cursor < cold_pool.shard_end) {
      vector<ObjectItem> obj_shard;
      int ret = cold_ioctx.object_list(obj_cursor, cold_pool.shard_end, MAX_OBJ_SCAN_SIZE, {},
                                       &obj_shard, &obj_cursor);
      if (ret < 0) {
        ldpp_dout(dpp, 0) << "error object_list: " << cpp_strerror(ret) << dendl;
        return nullptr;
      }

      for (const auto& obj : obj_shard) {
        auto cold_oid = obj.oid;

        chunk_refs_t refs;
        bufferlist bl;
        ret = cold_ioctx.getxattr(cold_oid, CHUNK_REFCOUNT_ATTR, bl);
        if (ret < 0) {
          // non-chunk objects are not targets of a RGWChunkScrubWorker
          ldpp_dout(dpp, 0) << "object " << cold_oid << " getxattr failed" << dendl;
          continue;
        }
        auto p = bl.cbegin();
        decode(refs, p);

        // do not allow other types except for TYPE_BY_OBJECT
        if (refs.get_type() != chunk_refs_t::TYPE_BY_OBJECT) {
          continue;
        }

        chunk_refs_by_object_t* chunk_refs =
          static_cast<chunk_refs_by_object_t*>(refs.r.get());

        set<hobject_t> src_obj_set(chunk_refs->by_object.begin(), 
                                   chunk_refs->by_object.end());
        for (auto& src_obj : src_obj_set) {
          IoCtx src_ioctx;
          // get reference count that chunk object is pointing a src object
          int chunk_ref_cnt = chunk_refs->by_object.count(src_obj);
          int src_ref_cnt = -1;

          ret = rados->ioctx_create2(src_obj.pool, src_ioctx);
          if (ret < 0) {
            ldpp_dout(dpp, 0) << cold_oid << " reference " << src_obj
	      << ": referencing pool does not exist" << dendl;
            src_ref_cnt = 0;
          }
          else {
            // get reference count that src object is pointing a chunk object
            src_ref_cnt = cls_cas_references_chunk(src_ioctx, src_obj.oid.name, cold_oid);
            if (src_ref_cnt < 0) {
              if (src_ref_cnt == -ENOENT || src_ref_cnt == -EINVAL) {
                ldpp_dout(dpp, 2) << "chunk (" << cold_oid << ") is referencing " << src_obj
                  << ": referencing object missing" << dendl;
                src_ref_cnt = 0;
              }
              else if (src_ref_cnt == -ENOLINK) {
                ldpp_dout(dpp, 2) << "chunk (" << cold_oid << ") is referencing " << src_obj
                  << ": referencing object does not reference this chunk" << dendl;
                src_ref_cnt = 0;
              }
              else {
                ldpp_dout(dpp, 2) << "cls_cas_references_chunk() fail: "
                  << strerror(src_ref_cnt) << dendl;
		continue;
              }
            }
          }

          ldpp_dout(dpp, 10) << "ScrubWorker_" << id << " chunk obj: " << cold_oid
	    << ", src obj: " << src_obj.oid.name << ", src pool: " << src_obj.pool
	    <<  ", chunk_ref_cnt: " << chunk_ref_cnt << ", src_ref_cnt: " << src_ref_cnt
	    << dendl;

          if (chunk_ref_cnt != src_ref_cnt) {
            ret = do_chunk_repair(cold_ioctx, cold_oid, src_obj, chunk_ref_cnt, src_ref_cnt);
            if (ret < 0) {
              ldpp_dout(dpp, 0) << "do_chunk_repair fail: " << cpp_strerror(ret) << dendl;
              continue;
            }
          }
        }
      }
    }
  }
  return nullptr;
}

void RGWChunkScrubWorker::finalize()
{

}

void RGWChunkScrubWorker::append_cold_pool_info(cold_pool_info_t new_pool_info)
{
  cold_pool_info.emplace_back(new_pool_info);
}
