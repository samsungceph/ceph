// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "gtest/gtest.h"
#include "test/librados/test_cxx.h"
#include "test/librados/testcase_cxx.h"
#include "test_rgw_common.h"
#include "cls/cas/cls_cas_client.h"

#include "rgw/rgw_fp_manager.h"
#include "rgw/rgw_dedup_manager.h"
#include "rgw/rgw_dedup_worker.h"
#include "rgw/driver/rados/rgw_sal_rados.h"

#define dout_subsys ceph_subsys_rgw

auto cct = new CephContext(CEPH_ENTITY_TYPE_CLIENT);
const DoutPrefix dpp(cct, 1, "test rgw dedup: ");

static rgw::sal::RadosStore* store = nullptr;

class RGWDedupTestWithTwoPools : public RadosTestPP
{
public:
  RGWDedupTestWithTwoPools() {};
  ~RGWDedupTestWithTwoPools() override {};
protected:
  static void SetUpTestCase() {
    pool_name = get_temp_pool_name();
    ASSERT_EQ("", create_one_pool_pp(pool_name, s_cluster));
  }
  static void TearDownTestCase() {
    ASSERT_EQ(destroy_one_pool_pp(pool_name, s_cluster), 0);
  }
  string cold_pool_name;
  IoCtx cold_ioctx;

  void SetUp() override {
    RadosTestPP::SetUp();

    cold_pool_name = get_temp_pool_name();
    ASSERT_EQ(s_cluster.pool_create(cold_pool_name.c_str()), 0);
    ASSERT_EQ(cluster.ioctx_create(cold_pool_name.c_str(), cold_ioctx), 0);

    cold_ioctx.application_enable("rados", true);
    cold_ioctx.set_namespace(nspace);
  }
  void TearDown() override {
    // wait for maps to settle before next test
    cluster.wait_for_latest_osdmap();

    RadosTestPP::TearDown();

    cleanup_default_namespace(cold_ioctx);
    cleanup_namespace(cold_ioctx, nspace);

    cold_ioctx.close();
    ASSERT_EQ(s_cluster.pool_delete(cold_pool_name.c_str()), 0);
  }
};


// if there is any redundant chunk, it regards as deduplicated.
void get_chunk_map(const vector<tuple<bufferlist, pair<uint64_t, uint64_t>>> chunks,
                   RGWDedupWorker* worker,
                   const string fp_algorithm,
                   unordered_map<string, uint32_t>& chunk_map)
{
  for (auto& chunk : chunks) {
    string fp = worker->generate_fingerprint(get<0>(chunk), fp_algorithm);
    auto it = chunk_map.find(fp);
    if (it != chunk_map.end()) {
      it->second = ++chunk_map[fp];
    } else {
      chunk_map.emplace(fp, 1);
    }
  }
}

void read_deduped_data(
    RGWDedupWorker* worker,
    IoCtx ioctx,
    IoCtx cold_ioctx,
    const vector<tuple<bufferlist, pair<uint64_t, uint64_t>>>& chunks,
    unordered_map<string, uint32_t>& chunk_map,
    const string metadata_oid,
    const uint32_t metadata_obj_size,
    const string fp_algo,
    bufferlist& deduped_data)
{
  if (chunks.size() == chunk_map.size()) {
    // a metadata object has been archived
    ASSERT_GT(cold_ioctx.read(worker->get_archived_obj_name(ioctx, metadata_oid),
      deduped_data, metadata_obj_size, 0), 0);
  } else {
    // redundant chunks of a metadata object have been deduped
    for (const auto& chunk : chunks) {
      string fp = worker->generate_fingerprint(get<0>(chunk), fp_algo);
      bufferlist tmpbl;
      if (chunk_map[fp] == 1) {
        // chunk (fp) is not deduped. read from base-pool
        ASSERT_GT(ioctx.read(metadata_oid, tmpbl, get<1>(chunk).second, get<1>(chunk).first), 0);
        deduped_data.append(tmpbl);
      } else {
        // chunk (fp) has been deduped. read from cold-pool
        ASSERT_GT(cold_ioctx.read(fp, tmpbl, get<1>(chunk).second, 0), 0);
        deduped_data.append(tmpbl);
      }
    }
  }
}

void write_object(string obj_name, bufferlist& data, IoCtx& ioctx)
{
  ObjectWriteOperation wop;
  wop.write_full(data);
  ASSERT_EQ(ioctx.operate(obj_name, &wop), 0);
}

void try_dedup_for_all_objs(IoCtx& ioctx, RGWDedupWorker& worker)
{
  ObjectCursor shard_start, shard_end;
  ioctx.object_list_slice(ioctx.object_list_begin(), ioctx.object_list_end(),
                          0, 1, &shard_start, &shard_end);
  ObjectCursor cursor = shard_start;
  while (cursor < shard_end) {
    vector<ObjectItem> objs;
    ASSERT_GE(ioctx.object_list(cursor, shard_end, 100, {}, &objs, &cursor), 0);
    worker.try_object_dedup(ioctx, objs.begin(), objs.end());
  }
}

// RGWDedupWorker test
TEST_F(RGWDedupTestWithTwoPools, redundant_object_dedup)
{
  uint32_t dedup_threshold = 2;

  shared_ptr<RGWFPManager> fpmanager
    = make_shared<RGWFPManager>(dedup_threshold, 16 * 1024, 50);
  RGWDedupWorker worker(&dpp, cct, store, 0, fpmanager, "fastcdc", 1024, "sha1",
                        dedup_threshold, cold_ioctx);
  worker.append_base_ioctx(ioctx.get_id(), ioctx);

  // generate data which has redundancy a lot
  bufferlist data, tmpbl;
  generate_buffer(1024, &tmpbl, clock());
  for (int i = 0; i < 32; ++i) {
    data.append(tmpbl);
  }
  write_object("foo", data, ioctx);

  // redundant chunks will be deduped
  try_dedup_for_all_objs(ioctx, worker);

  auto chunks = worker.do_cdc(data, "fastcdc", 1024);
  unordered_map<string, uint32_t> chunk_map;
  get_chunk_map(chunks, &worker, "sha1", chunk_map);

  map<string, uint64_t> chunk_len_map;
  for (const auto& chunk : chunks) {
    string fp = worker.generate_fingerprint(get<0>(chunk), "sha1");
    uint64_t chunk_len = get<1>(chunk).second;
    chunk_len_map.emplace(fp, chunk_len);
  }

  for (const auto& [fp, cnt] : chunk_map) {
    bufferlist bl;
    int ret = cold_ioctx.read(fp, bl, 32 * 1024, 0);

    // check chunk is deduped and the size of chunk object
    if (ret < 0) {
      ASSERT_EQ(ret, -2);
    } else {
      // check chunk object size
      ASSERT_EQ(ret, chunk_len_map[fp]);

      // check ref cnt
      chunk_refs_t refs;
      worker.get_chunk_refs(cold_ioctx, fp, refs);
      chunk_refs_by_object_t* chunk_refs
        = static_cast<chunk_refs_by_object_t*>(refs.r.get());

      // need to consider dedup threshold
      ASSERT_EQ(chunk_refs->by_object.size(), cnt - dedup_threshold + 1);
    }
  }
}

TEST_F(RGWDedupTestWithTwoPools, unique_object_archiving)
{
  uint32_t dedup_threshold = 2;
  uint32_t obj_size = 32 * 1024;

  shared_ptr<RGWFPManager> fpmanager
    = make_shared<RGWFPManager>(dedup_threshold, 16 * 1024, 50);
  RGWDedupWorker worker(&dpp, cct, store, 0, fpmanager, "fastcdc", 1024,
                        "sha1", dedup_threshold, cold_ioctx);
  worker.append_base_ioctx(ioctx.get_id(), ioctx);

  // generate data which has no redundancy
  bufferlist data;
  generate_buffer(obj_size, &data, clock());
  write_object("foo", data, ioctx);

  // the object will be archived as cold object
  try_dedup_for_all_objs(ioctx, worker);

  auto chunks = worker.do_cdc(data, "fastcdc", 1024);
  unordered_map<string, uint32_t> chunk_map;
  get_chunk_map(chunks, &worker, "sha1", chunk_map);

  bufferlist bl;
  int ret = cold_ioctx.read(worker.get_archived_obj_name(ioctx, "foo"), bl, obj_size, 0);

  // check chunk object size
  ASSERT_EQ(ret, obj_size);

  // check ref cnt
  chunk_refs_t refs;
  worker.get_chunk_refs(cold_ioctx, worker.get_archived_obj_name(ioctx, "foo"), refs);
  chunk_refs_by_object_t* chunk_refs
    = static_cast<chunk_refs_by_object_t*>(refs.r.get());

  // need to consider dedup threshold
  ASSERT_EQ(chunk_refs->by_object.size(), 1);
}

#include "cls/cas/cls_cas_internal.h"
TEST_F(RGWDedupTestWithTwoPools, data_consistency_test_after_dedup)
{
  vector<string> fp_algos{"sha1", "sha256", "sha512"};
  for (auto fp_algo : fp_algos) {
    shared_ptr<RGWFPManager> fpmanager
      = make_shared<RGWFPManager>(2, 1024 * 1024, 50);
    RGWDedupWorker worker(&dpp, cct, store, 0, fpmanager, "fastcdc", 1024,
                          fp_algo, 2, cold_ioctx);
    worker.append_base_ioctx(ioctx.get_id(), ioctx);

    // Test an object not containing any redundancy (archived as a cold object)
    // generate random data
    bufferlist og_data;
    string rand_oid = "rand-data-" + fp_algo;
    generate_buffer(1024 * 16, &og_data, clock());
    write_object(rand_oid, og_data, ioctx);
    try_dedup_for_all_objs(ioctx, worker);

    // get chunk map in order to get a sequence of chunks
    auto chunks = worker.do_cdc(og_data, "fastcdc", 1024);
    unordered_map<string, uint32_t> chunk_map;
    get_chunk_map(chunks, &worker, fp_algo, chunk_map);

    // generate checksum of data before dedup
    string metadata_obj_checksum = worker.generate_fingerprint(og_data, fp_algo);
    ASSERT_NE(metadata_obj_checksum, string());

    // read data after try_object_dedup
    bufferlist chunk_data;
    read_deduped_data(&worker, ioctx, cold_ioctx, chunks, chunk_map, rand_oid,
                      1024 * 16, fp_algo, chunk_data);
    string chunk_obj_checksum = worker.generate_fingerprint(chunk_data, fp_algo);
    ASSERT_NE(chunk_obj_checksum, string());
    ASSERT_EQ(metadata_obj_checksum, chunk_obj_checksum);

    // reset variables
    og_data.clear();
    chunk_data.clear();
    chunk_map.clear();
    fpmanager->reset_fpmap();

    // Test an object containing redundant chunk
    // generate redundant data
    {
      bufferlist tmpbl;
      generate_buffer(1024, &tmpbl, clock());
      for (int i = 0; i < 16; ++i) {
        og_data.append(tmpbl);
      }
    }
    ASSERT_EQ(og_data.length(), 1024 * 16);

    string dup_oid = "dup-data-" + fp_algo;
    write_object(dup_oid, og_data, ioctx);
    try_dedup_for_all_objs(ioctx, worker);

    // get chunk map in order to get a sequence of chunks
    chunks = worker.do_cdc(og_data, "fastcdc", 1024);
    get_chunk_map(chunks, &worker, fp_algo, chunk_map);


    // generate checksum of data before dedup
    metadata_obj_checksum = worker.generate_fingerprint(og_data, fp_algo);
    ASSERT_NE(metadata_obj_checksum, string());

    // read data after try_object_dedup
    read_deduped_data(&worker, ioctx, cold_ioctx, chunks, chunk_map,
                      dup_oid, 1024 * 16, fp_algo, chunk_data);
    chunk_obj_checksum = worker.generate_fingerprint(chunk_data, fp_algo);
    ASSERT_NE(chunk_obj_checksum, string());
    ASSERT_EQ(metadata_obj_checksum, chunk_obj_checksum);

    // clear objects in base-pool and cold-pool
    cleanup_default_namespace(ioctx);
    cleanup_namespace(ioctx, nspace);
    cleanup_default_namespace(cold_ioctx);
    cleanup_namespace(cold_ioctx, nspace);
  }
}

TEST_F(RGWDedupTestWithTwoPools, chunk_obj_ref_size)
{
  store = new rgw::sal::RadosStore();
  ASSERT_NE(store, nullptr);
  RGWRados* rados = new RGWRados();
  ASSERT_NE(rados, nullptr);
  rados->set_context(cct);
  rados->init_rados();
  store->setRados(rados);
  rados->set_store(store);

  shared_ptr<RGWFPManager> fpmanager
    = make_shared<RGWFPManager>(2, 16 * 1024, 50);
  RGWDedupWorker worker(&dpp, cct, store, 0, fpmanager, "fastcdc", 1024,
                        "sha1", 2, cold_ioctx);
  worker.append_base_ioctx(ioctx.get_id(), ioctx);
  // scale down max_chunk_ref_size not to take too much time
  const uint32_t max_chunk_ref_size = 100;
  worker.set_max_chunk_ref_size(max_chunk_ref_size);

  // generate duplicated data
  bufferlist data, tmpbl;
  generate_buffer(1024, &tmpbl);
  for (int i = 0; i < 100; ++i) {
    data.append(tmpbl);
  }

  // create objects which have chunks larger than max_chunk_ref_size
  string obj_name = "dup-a-lot-obj-";
  for (int i = 0; i < 2; ++i) {
    write_object(obj_name + to_string(i), data, ioctx);
  }

  try_dedup_for_all_objs(ioctx, worker);

  ObjectCursor shard_start, shard_end;
  cold_ioctx.object_list_slice(cold_ioctx.object_list_begin(), ioctx.object_list_end(),
                               0, 1, &shard_start, &shard_end);
  ObjectCursor cursor = shard_start;
  while (cursor < shard_end) {
    vector<ObjectItem> objs;
    ASSERT_GE(cold_ioctx.object_list(cursor, shard_end, 100, {}, &objs, &cursor), 0);
    for (const auto& obj : objs) {
      chunk_refs_t refs;
      ASSERT_EQ(worker.get_chunk_refs(cold_ioctx, obj.oid, refs), 0);
      chunk_refs_by_object_t* chunk_refs
         = static_cast<chunk_refs_by_object_t*>(refs.r.get());
      ASSERT_LE(chunk_refs->by_object.size(), max_chunk_ref_size);
    }
  }
}

TEST_F(RGWDedupTestWithTwoPools, multi_base_pool)
{
  shared_ptr<RGWFPManager> fpmanager
    = make_shared<RGWFPManager>(2, 1024, 50);
  RGWDedupWorker worker(&dpp, cct, store, 0, fpmanager, "fastcdc", 1024,
                        "sha1", 2, cold_ioctx);
  worker.append_base_ioctx(ioctx.get_id(), ioctx);
  worker.prepare(1, 0);

  bufferlist data, tmpbl;
  generate_buffer(1024, &tmpbl, clock());
  for (int i = 0; i < 8; ++i) {
    data.append(tmpbl);
  }

  IoCtx ioctxs[10];
  vector<uint64_t> pool_ids;
  for (int i = 0; i < 10; ++i) {
    string pool_name = get_temp_pool_name();
    ASSERT_EQ(cluster.pool_create(pool_name.c_str()), 0);
    ASSERT_EQ(cluster.ioctx_create(pool_name.c_str(), ioctxs[i]), 0);
    ioctxs[i].application_enable("rados", true);

    worker.append_base_ioctx(ioctxs[i].get_id(), ioctxs[i]);
    string oid = "pool-" + to_string(i) + "-data";
    write_object(oid, data, ioctxs[i]);
    pool_ids.emplace_back(ioctxs[i].get_id());
  }

  // try dedup for all objects in base-pools
  worker.entry();

  // get chunk map in order to get a sequence of chunks
  auto chunks = worker.do_cdc(data, "fastcdc", 1024);
  unordered_map<string, uint32_t> chunk_map;
  get_chunk_map(chunks, &worker, "sha1", chunk_map);

  // verify chunk objects' references
  ObjectCursor shard_start, shard_end;
  cold_ioctx.object_list_slice(cold_ioctx.object_list_begin(), cold_ioctx.object_list_end(),
                               0, 1, &shard_start, &shard_end);
  ObjectCursor cursor = shard_start;
  while (cursor < shard_end) {
    vector<ObjectItem> objs;
    ASSERT_GE(cold_ioctx.object_list(cursor, shard_end, 100, {}, &objs, &cursor), 0);
    for (const auto& obj: objs) {
      chunk_refs_t refs;
      ASSERT_EQ(worker.get_chunk_refs(cold_ioctx, obj.oid, refs), 0);
      chunk_refs_by_object_t* chunk_refs
         = static_cast<chunk_refs_by_object_t*>(refs.r.get());

      for (auto ref : chunk_refs->by_object) {
        EXPECT_NE(find(pool_ids.begin(), pool_ids.end(), ref.pool), pool_ids.end());
      }
    }
  }

  // clear base-pools
  for (int i = 0; i < 10; ++i) {
    string pool_name = ioctxs[i].get_pool_name();
    cleanup_default_namespace(ioctxs[i]);
    ioctxs[i].close();
    ASSERT_EQ(s_cluster.pool_delete(pool_name.c_str()), 0);
  }
}


// RGWFPManager test
TEST_F(RGWDedupTestWithTwoPools, fpmap_memory_size)
{
  // limit fpmanager memory size upto 4096 bytes
  uint32_t memory_limit = 4096;
  RGWFPManager fpmanager(2, memory_limit, 50);
  RGWDedupWorker worker(&dpp, cct, store, 0, nullptr, "fastcdc", 1024, "sha1", 2, cold_ioctx);

  vector<string> fp_algos = {"sha1", "sha256", "sha512"};
  for (const auto& fp_algo : fp_algos) {
    fpmanager.reset_fpmap();
    bufferlist data;
    generate_buffer(8 * 1024, &data, clock());

    auto dup_chunks = worker.do_cdc(data, "fastcdc", 1024);
    uint32_t dup_chunk_cnt = dup_chunks.size();

    // make current chunks' fp duplicated
    for (int i = 0; i < 2; ++i) {
      for (const auto& chunk : dup_chunks) {
        string fp = worker.generate_fingerprint(get<0>(chunk), fp_algo);
        fpmanager.add(fp);
      }
    }
    ASSERT_EQ(fpmanager.get_fpmap_size(), dup_chunk_cnt);

    // add unique objects' chunks
    for (int i = 1; i <= 5; ++i) {
      data.clear();
      generate_buffer(16 * 1024, &data, clock());

      auto unique_chunks = worker.do_cdc(data, "fastcdc", 1024);
      for (const auto& chunk : unique_chunks) {
        string fp = worker.generate_fingerprint(get<0>(chunk), fp_algo);
        fpmanager.add(fp);
      }
    }

     // call once again because add() inserts a fp value
     //  into fpmap after check_memory_limit_and_do_evict()
    fpmanager.check_memory_limit_and_do_evict();
    ASSERT_LE(fpmanager.get_fpmap_memory_size(), memory_limit);
  }
}

TEST_F(RGWDedupTestWithTwoPools, thread_safe_fpmanager)
{
  RGWFPManager fpmanager(2, 10 * 1024 * 1024, 50);
  RGWDedupWorker worker(&dpp, cct, store, 0, nullptr, "fastcdc", 1024, "sha1", 2, cold_ioctx);

  // create redundant metadata object
  bufferlist data, tmp;
  generate_buffer(16 * 1024, &tmp, clock());
  for (int i = 0; i < 4; ++i) {
    data.append(tmp);
  }

  // get chunks by do_cdc()
  auto chunks = worker.do_cdc(data, "fastcdc", 1024);

  // run 10 threads which adds same chunk info simultaneously
  int num_workers = 10;
  vector<thread> threads;
  for (int i = 0; i < num_workers; ++i) {
    threads.emplace_back(thread( [] 
      (int id, RGWDedupWorker* worker,
       RGWFPManager* fpmanager,
       vector<tuple<bufferlist, pair<uint64_t, uint64_t>>> chunks) {
      for (const auto& chunk : chunks) {
        string fp = worker->generate_fingerprint(get<0>(chunk), "sha1");
        fpmanager->add(fp);
      }
    }, i, &worker, &fpmanager, chunks));
  }

  // join
  for (auto& t : threads) {
    t.join();
  }

  // when all threads are done, check chunk ref count
  unordered_map<string, uint32_t> chunk_map;
  get_chunk_map(chunks, &worker, "sha1", chunk_map);
  for (auto& [fp, cnt] : chunk_map) {
    size_t fp_cnt = fpmanager.find(fp);
    ASSERT_EQ(fp_cnt, cnt * num_workers);
  }
}

