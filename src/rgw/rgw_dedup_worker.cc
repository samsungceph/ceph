// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_dedup_worker.h"
#include "cls/cas/cls_cas_internal.h"
#include "cls/cas/cls_cas_client.h"

#define dout_subsys ceph_subsys_rgw

unsigned default_op_size = 1 << 26;

void Worker::set_run(bool run_status)
{
  is_run = run_status;
}

void Worker::stop()
{
  is_run = false;
}


void RGWDedupWorker::initialize()
{

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
    int ret = 0;

    // get ioctx
    if (ioctxs.find(rados_object.pool_name) != ioctxs.end()) {
      ioctx = ioctxs.find(rados_object.pool_name)->second.first;
      cold_ioctx = ioctxs.find(rados_object.pool_name)->second.second;
    }

    else {
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

    // move data(new <-> chunked <-> entire object) according to policy
    ret = check_object_exists(cold_ioctx, rados_object.object_name);
    if (redundant_chunks.size() > 0) {
      if (ret != -ENOENT) { // entire -> chunked
        ObjectWriteOperation promote_op;
        promote_op.tier_promote();
        ret = ioctx.operate(rados_object.object_name, &promote_op);
        if (ret < 0) {
          ldpp_dout(dpp, 1)
            << "Failed to promote rados object, pool_name: " << rados_object.pool_name
            << ", oid: " << rados_object.object_name 
            << ", ret: " << ret << dendl;
          continue;
        }
        
        ObjectWriteOperation unset_op;
        unset_op.unset_manifest();
        ret = ioctx.operate(rados_object.object_name, &unset_op);
        if (ret < 0) {
          ldpp_dout(dpp, 1)
            << "Failed to unset_manifest rados object, pool_name: " << rados_object.pool_name
            << ", oid: " << rados_object.object_name
            << ", ret: " << ret << dendl;
          continue;
        }

        for (auto chunk : redundant_chunks) {
          if (check_object_exists(cold_ioctx, chunk.fingerprint) < 0) {
            ret = write_object_data(cold_ioctx, chunk.fingerprint, chunk.data);
            if (ret < 0) {
              ldpp_dout(dpp, 1)
                << "Failed to write chunk to cold pool, cold_pool_name: "
                << rados_object.pool_name << DEFAULT_COLD_POOL_POSTFIX
                << ", fingerprint: " << chunk.fingerprint
                << ", ret: " << ret << dendl;
              continue;
            }
          }
          try_set_chunk(ioctx, cold_ioctx, rados_object.object_name, chunk);
        }
      } else if (ret == -ENOENT) { // new, chunked -> chunked
        for (auto chunk : redundant_chunks) {
          if (check_object_exists(cold_ioctx, chunk.fingerprint) == -ENOENT) {
            ret = write_object_data(cold_ioctx, chunk.fingerprint, chunk.data);
            if (ret < 0) {
              ldpp_dout(dpp, 1)
                << "Failed to write chunk to cold pool, cold_pool_name: "
                << rados_object.pool_name << DEFAULT_COLD_POOL_POSTFIX
                << ", fingerprint: " << chunk.fingerprint
                << ", ret: " << ret << dendl;
              continue;
            }
          }
          try_set_chunk(ioctx, cold_ioctx, rados_object.object_name, chunk);
        }
      }
    } else if (redundant_chunks.size() <= 0) { // new, whole -> whole, chunked -> chunked
      chunk_t chunk = {
        .start = 0,
        .size = data.length(),
        .fingerprint = rados_object.object_name,
        .data = data
      };

      if (ret == -ENOENT) { // new -> whole
        ret = write_object_data(cold_ioctx, rados_object.object_name, data);
        if (ret < 0) {
          ldpp_dout(dpp, 1)
            << "Failed to write entire object to cold pool, cold_pool_name: "
            << rados_object.pool_name << DEFAULT_COLD_POOL_POSTFIX
            << ", oid: " << rados_object.object_name 
            << ", ret: " << ret << dendl;
          continue;
        }

        ret = try_set_chunk(ioctx, cold_ioctx, rados_object.object_name, chunk);
        if (ret == -EOPNOTSUPP) { // chunked -> chunked
          ret = cold_ioctx.remove(rados_object.object_name);
          if (ret < 0) {
            ldpp_dout(dpp, 1)
                << "Failed to remove entire object in cold pool, cold_pool_name: "
                << rados_object.pool_name << DEFAULT_COLD_POOL_POSTFIX
                << ", oid: " << rados_object.object_name 
                << ", ret: " << ret << dendl;
            continue;
          }
        }
      }
    }

    ObjectReadOperation tier_op;
    tier_op.tier_evict();
    ret = ioctx.operate(rados_object.object_name, &tier_op, nullptr);
    if (ret < 0) {
      ldpp_dout(dpp, 1) << "Failed to tier_evict rados object, pool_name: "
        << rados_object.pool_name << ", oid: " 
        << ", ret: " << ret  
        << rados_object.object_name << dendl;
      continue;
    }
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

bufferlist RGWDedupWorker::read_object_data(IoCtx& ioctx, string oid)
{
  bufferlist whole_data;
  size_t offset = 0;
  int ret = -1;

  while (ret != 0) {
    bufferlist partial_data;
    ret = ioctx.read(oid, partial_data, default_op_size, offset);
    if(ret < 0)
    {
      ldpp_dout(dpp, 1) << "read object error " << oid << ", offset: " << offset
        << ", size: " << default_op_size << ", error:" << cpp_strerror(ret) 
        << dendl;
      bufferlist empty_buf;
      return empty_buf;
    }
    offset += ret;
    whole_data.claim_append(partial_data);
  }

  return whole_data;
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

int RGWDedupWorker::check_object_exists(IoCtx& ioctx, string object_name) {
  uint64_t size;
  time_t mtime;

  int result = ioctx.stat(object_name, &size, &mtime);

  return result;
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
  int result = ioctx.operate(object_name, &chunk_op, nullptr);
  
  return result;
}

int RGWDedupWorker::write_object_data(IoCtx &ioctx, string object_name, bufferlist &data) {
  ObjectWriteOperation write_op;
  write_op.write_full(data);
  int result = ioctx.operate(object_name, &write_op);

  return result;
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
