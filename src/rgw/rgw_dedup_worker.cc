// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_dedup_worker.h"
#include "cls/cas/cls_cas_client.h"

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


void* RGWDedupWorker::entry()
{
  ldpp_dout(dpp, 20) << "RGWDedupWorker_" << id << " start" << dendl;
  string chunk_algo = fpmanager->get_chunk_algo();
  ceph_assert(chunk_algo == "fixed" || chunk_algo == "fastcdc");
  ssize_t chunk_size = fpmanager->get_chunk_size();
  ceph_assert(chunk_size > 0);
  string fp_algo = fpmanager->get_fp_algo();
  ceph_assert(fp_algo == "sha1" || fp_algo == "sha256" || fp_algo == "sha512");

  map<string, pair<IoCtx, IoCtx>> ioctxs;

  for(auto rados_object : rados_objs) {
    librados::Rados* rados = store->getRados()->get_rados_handle();
    IoCtx ioctx;
    IoCtx cold_ioctx;

    // get ioctx
    if (ioctxs.find(rados_object.pool_name) != ioctxs.end()) {
      ioctx = ioctxs.find(rados_object.pool_name)->second.first;
      cold_ioctx = ioctxs.find(rados_object.pool_name)->second.second;
    } else {
      rados->ioctx_create(rados_object.pool_name.c_str(), ioctx);
      rados->ioctx_create((rados_object.pool_name + DEFAULT_COLD_POOL_POSTFIX).c_str(),
                            cold_ioctx);
      ioctxs.insert({rados_object.pool_name, {ioctx, cold_ioctx}});
    }

    list<chunk_t> redundant_chunks;

    bufferlist data = read_object_data(ioctx, rados_object.object_name);    

    if (data.length() == 0) {
      ldpp_dout(dpp, 5) << "Skip dedup object "
        << rados_object.object_name << ", object data size is 0" << dendl;
      continue;
    }

    auto chunks = do_cdc(data, chunk_algo, chunk_size);

    // check if a chunk is duplicated in sampled objects
    for(auto &chunk : chunks) {
      auto &chunk_data = get<0>(chunk);
      string fingerprint = generate_fingerprint(chunk_data, fp_algo);
      
      if (fpmanager->find(fingerprint)) {
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
    * - Deduped: Deduplication has occurred due to the duplication of the chunks of the object. Duplicated chunks data are evicted to cold pool
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
    * 3) (Deduped -> Deduped) If the chunk was in deduped state, retain the Dedup state (even if can't find duplicate in this round)
    */
    
    int is_cold = check_object_exists(cold_ioctx, rados_object.object_name);

    // New -> Cold
    if (is_cold < 0 && redundant_chunks.size() <= 0)
    {
      int ret = write_object_data(cold_ioctx, rados_object.object_name, data);
      if (ret < 0) {
        continue;
      }

      chunk_t cold_object = {
        .start = 0,
        .size = data.length(),
        .fingerprint = rados_object.object_name,
        .data = data
      };

      ret = try_set_chunk(ioctx, cold_ioctx, rados_object.object_name, cold_object);
      if (ret < 0) { // Overlapped(Already Cold or Deduped)
        ret = remove_object(cold_ioctx, rados_object.object_name);
      }
    }

    // New, Cold -> Deduped
    else if (redundant_chunks.size() > 0)
    {
      if (is_cold >= 0) { // Cold
        int ret = clear_manifest(ioctx, rados_object.object_name);
        if (ret < 0) {
          continue;
        }
      }
      
      do_chunk_dedup(ioctx, cold_ioctx, rados_object.object_name, redundant_chunks);
    }

    do_data_evict(ioctx, rados_object.object_name);
  }
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

string RGWDedupWorker::get_id()
{
  return "DedupWorker_" + to_string(id);
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
  return ret;
}

int RGWDedupWorker::check_object_exists(IoCtx& ioctx, string object_name) {
  uint64_t size;
  time_t mtime;
  int ret = ioctx.stat(object_name, &size, &mtime);
  return ret;
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
  int ret = ioctx.operate(object_name, &chunk_op, nullptr);
  return ret;
}

void RGWDedupWorker::do_chunk_dedup(IoCtx &ioctx, IoCtx &cold_ioctx, string object_name, list<chunk_t> redundant_chunks) {
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
    }
    try_set_chunk(ioctx, cold_ioctx, object_name, chunk);
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
  bufferlist &data, string chunk_algo, ssize_t chunk_size)
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
  string ret;

  switch (pg_pool_t::get_fingerprint_from_str(fp_algo)) {
    case pg_pool_t::TYPE_FINGERPRINT_SHA1:
      ret = crypto::digest<crypto::SHA1>(chunk_data).to_str();
      break;

    case pg_pool_t::TYPE_FINGERPRINT_SHA256:
      ret = crypto::digest<crypto::SHA256>(chunk_data).to_str();
      break;

    case pg_pool_t::TYPE_FINGERPRINT_SHA512:
      ret = crypto::digest<crypto::SHA512>(chunk_data).to_str();
      break;

    default:
      ceph_assert(0 == "Invalid fp_algo type");
      break;
  }
  return ret;
}


string RGWChunkScrubWorker::get_id()
{
  return "ScrubWorker_" + to_string(id);
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
  ldpp_dout(dpp, 10) << "ScrubWorker_" << id << " starts with " << cold_pool_info.size() 
    << " cold_pool_infos" << dendl;

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
