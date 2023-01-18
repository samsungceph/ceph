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

int Worker::get_id()
{
  return id;
}

void Worker::clear_base_ioctx_map(uint64_t id, IoCtx& ioctx)
{
  base_ioctx_map.clear();
}

void Worker::append_base_ioctx(uint64_t id, IoCtx& ioctx)
{
  base_ioctx_map.emplace(id, ioctx);
}


string RGWDedupWorker::get_archived_obj_name(IoCtx& ioctx, const string obj_name)
{
  ceph_assert(ioctx.get_id() > 0);
  ceph_assert(!obj_name.empty());
  return to_string(ioctx.get_id()) + ":" + obj_name;
}

RGWDedupWorker::MetadataObjType RGWDedupWorker::get_metadata_obj_type
  (object_info_t& oi, IoCtx& ioctx, const string obj_name, const uint32_t data_len)
{
  bufferlist bl;
  ioctx.getxattr(obj_name, "", bl);
  try {
    bufferlist::const_iterator bliter = bl.begin();
    decode(oi, bliter);
  } catch (...) {
    ldpp_dout(dpp, 0) << __func__ << " failed to get object_info_t of "
      << obj_name << dendl;
    return MetadataObjType::None;
  }

  if (!oi.has_manifest()) {
    return MetadataObjType::New;
  } else if (oi.has_manifest() && oi.manifest.is_chunked()) {
    const auto& first_entry = oi.manifest.chunk_map.begin();
    if (first_entry->first == 0 &&
        first_entry->second.length == data_len &&
        first_entry->second.oid.oid == get_archived_obj_name(ioctx, obj_name)) {
      return MetadataObjType::Archived;
    } else if (oi.manifest.chunk_map.size() > 1) {
      return MetadataObjType::Deduped;
    }
  }
  return MetadataObjType::None;
}

template <typename Iter>
void RGWDedupWorker::try_object_dedup(IoCtx& base_ioctx, Iter begin, Iter end)
{
  for (auto& obj = begin; obj != end; ++obj) {
    list<chunk_t> redundant_chunks;
    auto target_oid = obj->oid;
    ldpp_dout(dpp, 20) << "worker_" << id << "  oid: " << target_oid << dendl;

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
      fpmanager->add(fingerprint);
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
    }

    /**
    * Object has 3 state, can be transferred to another state.
    * - New: Never scanned before, so it must be transferred to Archived or Deduped state.
    * - Archived: No duplicated chunk has been found, entire object is evicted to cold pool.
    * - Deduped: Deduplication has occurred due to the duplicated chunks. Duplicated chunks
    *    have been evicted to cold pool
    *
    * example) 
    * +-----------+-----------+-----------+-----------+
    * |   object  |    New    |  Archive  |  Deduped  |
    * +-----------+-----------+-----------+-----------+
    * | base pool | 123456789 | --------- | 1-3456-89 |
    * +-----------+-----------+-----------+-----------+
    * | cold pool | --------- | 123456789 | -2----7-- |
    * +-----------+-----------+-----------+-----------+
    */

    object_info_t oi;
    MetadataObjType meta_obj_type
      = get_metadata_obj_type(oi, base_ioctx, target_oid, data.length());
    ldpp_dout(dpp, 20) << "oid: " << oi.soid << " state: "
      << static_cast<uint32_t>(meta_obj_type) << dendl;

    if (meta_obj_type == MetadataObjType::Archived) {
      if (redundant_chunks.empty()) {
        ldpp_dout(dpp, 20) << "oid: " << oi.soid << " Archived -> Archived" << dendl;
        // no need further operations
        continue;
      } else {
        // Archived -> Deduped
        ldpp_dout(dpp, 20) << "oid: " << oi.soid << " Archived -> Deduped" << dendl;
        clear_manifest(base_ioctx, target_oid);
      }
    } else if (meta_obj_type == MetadataObjType::Deduped) {
      if (redundant_chunks.empty()) {
        // Deduped -> Archived
        ldpp_dout(dpp, 20) << "oid: " << oi.soid << " Deduped -> Archived" << dendl;
        clear_manifest(base_ioctx, target_oid);
      }
    }

    if (redundant_chunks.empty()) {
      chunk_t archived_object = {
        .start = 0,
        .size = data.length(),
        .fingerprint = get_archived_obj_name(base_ioctx, target_oid),
        .data = data
      };
      redundant_chunks.push_back(archived_object);
    }

    do_chunk_dedup(base_ioctx, cold_ioctx, target_oid, redundant_chunks,
                   oi.manifest.chunk_map);
    do_data_evict(base_ioctx, target_oid);
  }
}

void* RGWDedupWorker::entry()
{
  ldpp_dout(dpp, 20) << "RGWDedupWorker_" << id << " starts" << dendl;

  ceph_assert(chunk_algo == "fixed" || chunk_algo == "fastcdc");
  ceph_assert(chunk_size > 0);
  ceph_assert(fp_algo == "sha1" || fp_algo == "sha256" || fp_algo == "sha512");
  ceph_assert(dedup_threshold > 0);

  for(auto& iter : base_ioctx_map) {
    uint32_t num_objs = 0;
    IoCtx base_ioctx = iter.second;
    ObjectCursor pool_begin = base_ioctx.object_list_begin();
    ObjectCursor pool_end = base_ioctx.object_list_end();
    ObjectCursor shard_begin, shard_end;

    // get current worker's shard range of the base pool
    base_ioctx.object_list_slice(pool_begin, pool_end, id, num_workers,
                                 &shard_begin, &shard_end);
    ldpp_dout(dpp, 20) << "id/# workers: " << id << "/" << num_workers
      << ", id: " << id << ", scan dir: " << obj_scan_dir << dendl;

    ObjectCursor obj_cursor = shard_begin;
    while (obj_cursor < shard_end) {
      vector<ObjectItem> obj_shard;
      int ret = base_ioctx.object_list(obj_cursor, shard_end, MAX_OBJ_SCAN_SIZE,
                                       {}, &obj_shard, &obj_cursor);
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

  // reverse object scanning direction
  obj_scan_dir ^= 1;
  return nullptr;
}

bufferlist RGWDedupWorker::read_object_data(IoCtx& ioctx, string object_name)
{
  bufferlist whole_data;
  uint64_t offset = 0;
  int ret = -1;

  while (ret != 0) {
    bufferlist partial_data;
    ret = ioctx.read(object_name, partial_data, default_obj_read_size, offset);
    if (ret < 0) {
      ldpp_dout(dpp, 1) << "read object error pool_name: " << ioctx.get_pool_name()
        << ", object_name: " << object_name << ", offset: " << offset
        << ", size: " << default_obj_read_size << ", error:" << cpp_strerror(ret) 
        << dendl;
      return bufferlist();
    }
    offset += ret;
    whole_data.claim_append(partial_data);
  }
  return whole_data;
}

int RGWDedupWorker::write_object_data(IoCtx &ioctx, string object_name, bufferlist &data)
{
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

int RGWDedupWorker::check_object_exists(IoCtx& ioctx, string object_name)
{
  uint64_t size;
  time_t mtime;
  int ret = ioctx.stat(object_name, &size, &mtime);
  return ret;
}

int RGWDedupWorker::try_set_chunk(IoCtx& ioctx, IoCtx& cold_ioctx,
                                  string object_name, chunk_t &chunk)
{
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

void RGWDedupWorker::do_chunk_dedup(IoCtx& ioctx, IoCtx& cold_ioctx,
                                    string object_name,
                                    list<chunk_t> redundant_chunks,
                                    map<uint64_t, chunk_info_t>& chunk_map)
{
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
    } else {
      if (chunk_map.find(chunk.start) != chunk_map.end() &&
          chunk_map[chunk.start].length == chunk.size &&
          chunk_map[chunk.start].oid.oid == chunk.fingerprint &&
          chunk_map[chunk.start].has_reference()) {
        // this chunk has already been deduped. skip this chunk
        ldpp_dout(dpp, 0) << chunk.fingerprint << " deduped -> deduped" << dendl;
        continue;
      }
    }
    try_set_chunk(ioctx, cold_ioctx, object_name, chunk);
  }
}

void RGWDedupWorker::do_data_evict(IoCtx &ioctx, string object_name)
{
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

int RGWDedupWorker::clear_manifest(IoCtx &ioctx, string object_name)
{
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

int RGWDedupWorker::remove_object(IoCtx &ioctx, string object_name)
{
  int ret = ioctx.remove(object_name);
  if (ret < 0) {
    ldpp_dout(dpp, 1)
      << "Failed to remove entire object in pool, pool_name: "
       << ioctx.get_pool_name() << ", object_name: "
       << object_name << ", ret: " << ret << dendl;
  }
  return ret;
}

vector<ChunkInfoType> RGWDedupWorker::do_cdc(bufferlist &data, string chunk_algo,
                                             uint32_t chunk_size)
{
  vector<ChunkInfoType> ret;
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

string RGWDedupWorker::generate_fingerprint(bufferlist chunk_data,
                                            string fp_algo)
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

void RGWDedupWorker::finalize()
{
  fpmanager.reset();
}

