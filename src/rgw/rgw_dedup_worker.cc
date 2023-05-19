// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_dedup_worker.h"
#include "cls/cas/cls_cas_client.h"
#include "rgw_zone.h"
#include "services/svc_zone.h"

#define dout_subsys ceph_subsys_rgw

unsigned default_obj_read_size = 1 << 26;

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

void Worker::prepare(const int new_total_workers, const int new_gid)
{
  num_total_workers = new_total_workers;
  gid = new_gid;
}

void Worker::clear_base_ioctx_map()
{
  base_ioctx_map.clear();
}

void Worker::append_base_ioctx(uint64_t id, IoCtx& ioctx)
{
  base_ioctx_map.emplace(id, ioctx);
}

int Worker::get_chunk_refs(IoCtx& chunk_ioctx, const string& chunk_oid, chunk_refs_t& refs)
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


template <typename Iter>
void RGWDedupWorker::try_object_dedup(IoCtx& base_ioctx, Iter begin, Iter end)
{
  for (auto& obj = begin; obj != end; ++obj) {
    list<chunk_t> redundant_chunks;
    auto target_oid = obj->oid;
    ldpp_dout(dpp, 0) << "worker_" << id << "  oid: " << target_oid << dendl;
    bufferlist data = read_object_data(base_ioctx, target_oid);
    if (data.length() == 0) {
      ldpp_dout(dpp, 5) << "Skip dedup object "
        << target_oid << ", object data size is 0" << dendl;
      continue;
    }
    auto chunks = do_cdc(data, chunk_algo, chunk_size);

    // check if a chunk is duplicated in sampled objects
    for(auto &chunk : chunks) {
      auto &chunk_data = get<0>(chunk);
      string fingerprint = generate_fingerprint(chunk_data, fp_algo);
      
      if (fpmanager->find(fingerprint) >= dedup_threshold) {
        std::pair<uint64_t, uint64_t> chunk_boundary = std::get<1>(chunk);
        chunk_t chunk_info = {
          .start = chunk_boundary.first,
          .size = chunk_boundary.second,
          .fingerprint = fingerprint,
          .data = chunk_data
        };

        redundant_chunks.push_back(chunk_info);
      }

      fpmanager->add(fingerprint);
    }

    /**
    * Deduplication is performed according to TiDedup.
    * 
    * Object has 3 state, can be transferred to another state.
    * - New: Never scanned before, so it must be transferred to Cold or Deduped state
    * - Cold: No duplicated chunk has been found "so far", so entire object data is evicted to cold pool
    * - Deduped: Deduplication has occurred due to the duplication of the chunks of the object.
    *     Duplicated chunks data are evicted to cold pool
    *
    * example) 
    * +-----------+-----------+-----------+-----------+
    * |   object  |    New    |    Cold   |  Deduped  |
    * +-----------+-----------+-----------+-----------+
    * | base pool | 123456789 | --------- | 1-3456-89 |
    * +-----------+-----------+-----------+-----------+
    * | cold pool | --------- | 123456789 | -2----7-- |
    * +-----------+-----------+-----------+-----------+
    *
    * In single round, an object is deduplicated with the following policies:
    * 1) (New, Cold -> Cold) If all chunks are not duplicated, evict entire object data to the cold pool
    * 2) (New, Cold -> Deduped) If some chunks are found duplicated, deduplicate the chunks to the cold pool
    * 3) (Deduped -> Deduped) If the chunk was in deduped state, retain the Dedup state
    *       (even if can't find duplicate in this round)
    */

    int is_cold = check_object_exists(cold_ioctx, target_oid);

    // New -> Cold
    if (is_cold < 0 && redundant_chunks.size() <= 0) {
      int ret = write_object_data(cold_ioctx, target_oid, data);
      if (ret < 0) {
        continue;
      }

      chunk_t cold_object = {
        .start = 0,
        .size = data.length(),
        .fingerprint = target_oid,
        .data = data
      };

      ret = try_set_chunk(base_ioctx, cold_ioctx, target_oid, cold_object);
      if (ret < 0) { // Overlapped(Already Cold or Deduped)
        ret = remove_object(cold_ioctx, target_oid);
      }
      else {
        if (perfcounter) {
          perfcounter->inc(l_rgw_dedup_cold_data_size, data.length());
        }
      }
    }

    // New, Cold, Deduped -> Deduped
    else if (redundant_chunks.size() > 0) {
      if (is_cold >= 0) { // Cold
        int ret = clear_manifest(base_ioctx, target_oid);
        if (ret < 0) {
          continue;
        }

        if (perfcounter) {
          perfcounter->dec(l_rgw_dedup_cold_data_size, data.length());
        }
      }
      
      do_chunk_dedup(base_ioctx, cold_ioctx, target_oid, redundant_chunks);
    }

    do_data_evict(base_ioctx, target_oid);
  }
}

void* RGWDedupWorker::entry()
{
  ldpp_dout(dpp, 20) << "RGWDedupWorker_" << id << " start" << dendl;

  chunk_algo = fpmanager->get_chunk_algo();
  ceph_assert(chunk_algo == "fixed" || chunk_algo == "fastcdc");
  chunk_size = fpmanager->get_chunk_size();
  ceph_assert(chunk_size > 0);
  fp_algo = fpmanager->get_fp_algo();
  ceph_assert(fp_algo == "sha1" || fp_algo == "sha256" || fp_algo == "sha512");
  dedup_threshold = fpmanager->get_dedup_threshold();
  ceph_assert(dedup_threshold > 0);

  for (auto& iter : base_ioctx_map) {
    uint32_t num_objs = 0;
    IoCtx base_ioctx = iter.second;
    ObjectCursor pool_begin = base_ioctx.object_list_begin();
    ObjectCursor pool_end = base_ioctx.object_list_end();
    ObjectCursor shard_begin, shard_end;

    // get current worker's shard range of the base pool
    base_ioctx.object_list_slice(pool_begin, pool_end, gid, num_total_workers,
                                 &shard_begin, &shard_end);
    ldpp_dout(dpp, 20) << "gid/total: " << gid << "/" << num_total_workers
      << ", id: " << id << ", scan dir: " << obj_scan_dir << dendl;

    ObjectCursor obj_cursor = shard_begin;
    while (obj_cursor < shard_end) {
      vector<ObjectItem> obj_shard;
      int ret = base_ioctx.object_list(obj_cursor, shard_end, MAX_OBJ_SCAN_SIZE, {},
                                       &obj_shard, &obj_cursor);
      if (ret < 0) {
        ldpp_dout(dpp, 0) << "error object_list: " << cpp_strerror(ret) << dendl;
        return nullptr;
      }
      num_objs += obj_shard.size();

      if (obj_scan_dir) {
        try_object_dedup(base_ioctx, obj_shard.begin(), obj_shard.end());
      } else {
        try_object_dedup(base_ioctx, obj_shard.rbegin(), obj_shard.rend());
      }
    }

    ldpp_dout(dpp, 20) << "RGWDedupWorker_" << id << " pool: " << base_ioctx.get_pool_name()
      << ", num objs: " << num_objs << dendl;
  }

  // reverse object scan dir
  obj_scan_dir ^= 1;

  return nullptr;
}

void RGWDedupWorker::finalize()
{

}

bufferlist RGWDedupWorker::read_object_data(IoCtx& ioctx, string object_name)
{
  bufferlist whole_data;
  size_t offset = 0;
  int ret = -1;

  while (ret != 0) {
    bufferlist partial_data;
    ret = ioctx.read(object_name, partial_data, default_obj_read_size, offset);
    if(ret < 0) {
      ldpp_dout(dpp, 1) << "read object error pool_name: " << ioctx.get_pool_name()
        << ", object_name: " << object_name << ", offset: " << offset
        << ", size: " << default_obj_read_size << ", error:" << cpp_strerror(ret) 
        << dendl;
      bufferlist empty_buf;
      return empty_buf;
    }
    offset += ret;
    whole_data.claim_append(partial_data);
  }

  if (perfcounter) {
    perfcounter->inc(l_rgw_dedup_worker_read, whole_data.length());
  }

  return whole_data;
}

int RGWDedupWorker::write_object_data(IoCtx &ioctx, string object_name, bufferlist &data) {
  ObjectWriteOperation write_op;
  write_op.write_full(data);
  int ret = ioctx.operate(object_name, &write_op);
  if (ret < 0) {
    ldpp_dout(dpp, 1)
      << "Failed to write rados object, pool_name: "<< ioctx.get_pool_name()
      << ", object_name: " << object_name << ", ret: " << ret << dendl;
  }

  if (perfcounter) {
    perfcounter->inc(l_rgw_dedup_worker_write, data.length());
  }
  
  return ret;
}

int RGWDedupWorker::check_object_exists(IoCtx& ioctx, string object_name) {
  uint64_t size;
  time_t mtime;
  return ioctx.stat(object_name, &size, &mtime);
}

int RGWDedupWorker::try_set_chunk(IoCtx& ioctx, IoCtx &cold_ioctx, string object_name, chunk_t &chunk) {
  ObjectReadOperation chunk_op;
  chunk_op.set_chunk(
    chunk.start,
    chunk.size,
    cold_ioctx,
    chunk.fingerprint,
    0,
    CEPH_OSD_OP_FLAG_WITH_REFERENCE);
  Rados* rados = store->getRados()->get_rados_handle();
  AioCompletion* completion = rados->aio_create_completion();
  ioctx.aio_operate(object_name, completion, &chunk_op, OPERATION_IGNORE_CACHE, NULL);

  completion->wait_for_complete();
  int ret = completion->get_return_value();

  completion->release();
  return ret;
  //return ioctx.operate(object_name, &chunk_op, nullptr);
}

void RGWDedupWorker::do_chunk_dedup(IoCtx &ioctx,
                                    IoCtx &cold_ioctx,
                                    string object_name, list<chunk_t> redundant_chunks) {
  for (auto chunk : redundant_chunks) {
    if (check_object_exists(cold_ioctx, chunk.fingerprint) < 0) {
      int ret = write_object_data(cold_ioctx, chunk.fingerprint, chunk.data);
      if (ret < 0) {
        ldpp_dout(dpp, 1)
          << "Failed to write chunk to cold pool, cold_pool_name: "
          << cold_ioctx.get_pool_name()
          << ", fingerprint: " << chunk.fingerprint
          << ", ret: " << ret << dendl;
        return;
      }
      if (perfcounter) {
        perfcounter->inc(l_rgw_dedup_chunk_data_size, chunk.data.length());
      }
    } else {
      chunk_refs_t refs;
      if (get_chunk_refs(cold_ioctx, chunk.fingerprint, refs)) {
        continue;
      }
      chunk_refs_by_object_t* chunk_refs
        = static_cast<chunk_refs_by_object_t*>(refs.r.get());

      // # refs of chunk object must be less than MAX_CHUNK_REF_SIZE
      if (chunk_refs->by_object.size() >= MAX_CHUNK_REF_SIZE) {
        continue;
      }
    }
    int ret = try_set_chunk(ioctx, cold_ioctx, object_name, chunk);
    if (ret >= 0 && perfcounter) {
      perfcounter->inc(l_rgw_dedup_deduped_data_size, chunk.data.length());
    }
  }
}

void RGWDedupWorker::do_data_evict(IoCtx &ioctx, string object_name) {
  ObjectReadOperation tier_op;
  tier_op.tier_evict();
  int ret = ioctx.operate(object_name, &tier_op, nullptr);
  if (ret < 0) {
    ldpp_dout(dpp, 1) << "Failed to tier_evict rados object, pool_name: "
      << ioctx.get_pool_name() << ", object_name: "
      << object_name << ", ret: " << ret  
      << dendl;
  }
}

int RGWDedupWorker::clear_manifest(IoCtx &ioctx, string object_name) {
  ObjectWriteOperation promote_op;
  promote_op.tier_promote();
  int ret = ioctx.operate(object_name, &promote_op);
  if (ret < 0) {
    ldpp_dout(dpp, 1)
      << "Failed to tier promote rados object, pool_name: " << ioctx.get_pool_name()
      << ", object_name: " << object_name << ", ret: " << ret << dendl;
    return ret;
  }
        
  ObjectWriteOperation unset_op;
  unset_op.unset_manifest();
  ret = ioctx.operate(object_name, &unset_op);
  if (ret < 0) {
    ldpp_dout(dpp, 1)
      << "Failed to unset_manifest rados object, pool_name: " << ioctx.get_pool_name()
      << ", object_name: " << object_name << ", ret: " << ret << dendl;
  }
  return ret;
}

int RGWDedupWorker::remove_object(IoCtx &ioctx, string object_name) {
  int ret = ioctx.remove(object_name);
  if (ret < 0) {
    ldpp_dout(dpp, 1)
      << "Failed to remove entire object in pool, pool_name: "
       << ioctx.get_pool_name() << ", object_name: "
       << object_name << ", ret: " << ret << dendl;
  }
  return ret;
}

vector<tuple<bufferlist, pair<uint64_t, uint64_t>>> RGWDedupWorker::do_cdc(
  bufferlist &data, string chunk_algo, size_t chunk_size)
{
  vector<tuple<bufferlist, pair<uint64_t, uint64_t>>> ret;

  unique_ptr<CDC> cdc = CDC::create(chunk_algo, cbits(chunk_size) - 1);
  vector<pair<uint64_t, uint64_t>> chunks;
  cdc->calc_chunks(data, &chunks);

  for (auto &p : chunks) {
    bufferlist chunk;
    chunk.substr_of(data, p.first, p.second);
    ret.push_back(make_tuple(chunk, p));
  }
  return ret;
}

string RGWDedupWorker::generate_fingerprint(
  bufferlist chunk_data, string fp_algo)
{
  switch (pg_pool_t::get_fingerprint_from_str(fp_algo)) {
    case pg_pool_t::TYPE_FINGERPRINT_SHA1:
      return crypto::digest<crypto::SHA1>(chunk_data).to_str();

    case pg_pool_t::TYPE_FINGERPRINT_SHA256:
      return crypto::digest<crypto::SHA256>(chunk_data).to_str();

    case pg_pool_t::TYPE_FINGERPRINT_SHA512:
      return crypto::digest<crypto::SHA512>(chunk_data).to_str();

    default:
      ceph_assert(0 == "Invalid fp_algo type");
      break;
  }
  return string();
}

void RGWDedupWorker::set_chunk_size(uint32_t new_chunk_size)
{
  ceph_assert(new_chunk_size >= 4 && new_chunk_size <= 4194304);
  chunk_size = new_chunk_size;
}

void RGWDedupWorker::set_chunk_algorithm(string new_chunk_algo)
{
  ceph_assert(new_chunk_algo == "fastcdc" || new_chunk_algo == "fixed");
  chunk_algo = new_chunk_algo;
}

void RGWDedupWorker::set_fp_algorithm(string new_fp_algo)
{
  ceph_assert(new_fp_algo == "sha1" || new_fp_algo == "sha256" || new_fp_algo == "sha512");
  fp_algo = new_fp_algo;
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

int RGWChunkScrubWorker::get_src_ref_cnt(const hobject_t& src_obj,
                                         const string& chunk_oid)
{
  IoCtx src_ioctx;
  int src_ref_cnt = -1;
  if (base_ioctx_map.find(src_obj.pool) != base_ioctx_map.end()) {
    src_ioctx = base_ioctx_map[src_obj.pool];
  } else {
    // if base pool not found, try create ioctx
    Rados* rados = store->getRados()->get_rados_handle();
    if (rados->ioctx_create2(src_obj.pool, src_ioctx) < 0) {
      ldpp_dout(dpp, 1) << __func__ << " src pool " << src_obj.pool <<
        " doesn't exist" << dendl;
      return -1;
    }
  }

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

  // get sharded chunk objects from a cold pool
  ObjectCursor pool_begin = cold_ioctx.object_list_begin();
  ObjectCursor pool_end = cold_ioctx.object_list_end();
  ObjectCursor shard_begin, shard_end;

  // get current worker's shard range of the cold pool
  cold_ioctx.object_list_slice(pool_begin, pool_end, gid, num_total_workers,
                               &shard_begin, &shard_end);
  ObjectCursor obj_cursor = shard_begin;
  uint32_t num_objs = 0;
  while (obj_cursor < shard_end) {
    vector<ObjectItem> obj_shard;
    int ret = cold_ioctx.object_list(obj_cursor, shard_end, MAX_OBJ_SCAN_SIZE, {},
                                     &obj_shard, &obj_cursor);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "error object_list: " << cpp_strerror(ret) << dendl;
      return nullptr;
    }

    for (const auto& obj : obj_shard) {
      auto cold_oid = obj.oid;
      chunk_refs_t refs;
      if (get_chunk_refs(cold_ioctx, cold_oid, refs) < 0) {
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
          << ", chunk_ref_cnt: " << chunk_ref_cnt << ", src_ref_cnt: " << src_ref_cnt
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
    num_objs += obj_shard.size();
  }
  ldpp_dout(dpp, 20) << "RGWChunkScrubWorker_" << id << " pool: " << cold_ioctx.get_pool_name()
    << ", num objs: " << num_objs << dendl;

  return nullptr;
}

void RGWChunkScrubWorker::finalize()
{

}

