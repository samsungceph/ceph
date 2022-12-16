// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_dedup_worker.h"
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

int Worker::get_id()
{
  return id;
}


void* RGWDedupWorker::entry()
{

  return nullptr;
}

void RGWDedupWorker::finalize()
{

}

void RGWDedupWorker::append_obj(target_rados_object new_obj)
{
  rados_objs.emplace_back(new_obj);
}

size_t RGWDedupWorker::get_num_objs()
{
  return rados_objs.size();
}

void RGWDedupWorker::clear_objs()
{
  rados_objs.clear();
}


int RGWChunkScrubWorker::do_chunk_repair(IoCtx& cold_ioctx,
					 const string chunk_obj_name,
					 const hobject_t src_obj,
					 int chunk_ref_cnt,
					 int source_ref_cnt)
{
  ceph_assert(chunk_ref_cnt >= source_ref_cnt);

  while (chunk_ref_cnt != source_ref_cnt) {
    ObjectWriteOperation op;
    cls_cas_chunk_put_ref(op, src_obj);
    --chunk_ref_cnt;
    int ret = cold_ioctx.operate(chunk_obj_name, &op);
    if (ret < 0) {
      return ret;
    }
  }
  return 0;
}

int RGWChunkScrubWorker::get_chunk_refs(IoCtx& chunk_ioctx,
                                        const string& chunk_oid,
                                        chunk_refs_t& refs)
{
  bufferlist bl;
  int ret = chunk_ioctx.getxattr(chunk_oid, CHUNK_REFCOUNT_ATTR, bl);
  if (ret < 0) {
    // non-chunk objects are not targets of a RGWChunkScrubWorker
    ldpp_dout(dpp, 0) << "object " << chunk_oid << " getxattr failed" << dendl;
    return -1;
  }
  auto p = bl.cbegin();
  decode(refs, p);

  if (refs.get_type() != chunk_refs_t::TYPE_BY_OBJECT) {
    ldpp_dout(dpp, 0) << "do not allow other types except for TYPE_BY_OBJECT" << dendl;
    return -1;
  }
  return 0;
}

int RGWChunkScrubWorker::get_src_ref_cnt(const hobject_t& src_obj,
                                         const string& chunk_oid)
{
  int src_ref_cnt = 0;
  Rados* rados = store->getRados()->get_rados_handle();

  IoCtx src_ioctx;
  if (ioctx_map.find(src_obj.pool) == ioctx_map.end()) {
    int ret = rados->ioctx_create2(src_obj.pool, src_ioctx);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << chunk_oid << " reference " << src_obj
              << ": referencing pool does not exist" << dendl;
      return ret;
    }
    ioctx_map.emplace(src_obj.pool, src_ioctx);
  } 

  src_ioctx = ioctx_map[src_obj.pool];
  // get reference count that src object is pointing a chunk object
  src_ref_cnt = cls_cas_references_chunk(src_ioctx, src_obj.oid.name, chunk_oid);
  if (src_ref_cnt < 0) {
    if (src_ref_cnt == -ENOENT || src_ref_cnt == -EINVAL) {
      ldpp_dout(dpp, 2) << "chunk (" << chunk_oid << ") is referencing " << src_obj
        << ": referencing object missing" << dendl;            
    } else if (src_ref_cnt == -ENOLINK) {
      ldpp_dout(dpp, 2) << "chunk (" << chunk_oid << ") is referencing " << src_obj
        << ": referencing object does not reference this chunk" << dendl;
    } else {
      ldpp_dout(dpp, 0) << "cls_cas_references_chunk() fail: "                    
        << strerror(src_ref_cnt) << dendl;
    }
    src_ref_cnt = 0;
  }
  return src_ref_cnt;
}

/*
 * - chunk object: A part of the source object that is created by doing deup operation on it.
 *     It has a reference list containing its' source objects.
 * - source object: An original object of its' chunk objects. It has its chunk information
 *     in a chunk_map.
 */
void* RGWChunkScrubWorker::entry()
{
  ldpp_dout(dpp, 20) << "RGWChunkScrubWorker_" << id << " starts" << dendl;

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
        ret = get_chunk_refs(cold_ioctx, cold_oid, refs);
        if (ret < 0) {
          continue;
        }

        chunk_refs_by_object_t* chunk_refs =
          static_cast<chunk_refs_by_object_t*>(refs.r.get());

        set<hobject_t> src_obj_set(chunk_refs->by_object.begin(), 
                                   chunk_refs->by_object.end());
        for (auto& src_obj : src_obj_set) {
          // get reference count that chunk object is pointing a src object
          int chunk_ref_cnt = chunk_refs->by_object.count(src_obj);
          int src_ref_cnt = get_src_ref_cnt(src_obj, cold_oid);

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
  cold_pool_info.clear();
}

void RGWChunkScrubWorker::append_cold_pool_info(cold_pool_info_t new_pool_info)
{
  cold_pool_info.emplace_back(new_pool_info);
}
