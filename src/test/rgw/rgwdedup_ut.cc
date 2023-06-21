// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "gtest/gtest.h"

#include "common/common_init.h"
#include "test_rgw_common.h"
//#include "common/config.h"
#include "common/ceph_argparse.h"
//#include "common/debug.h"
#include "global/global_context.h"
#include "global/global_init.h"
#include "common/ceph_context.h"
//#include "rgw_common.h"
//#include "rgw_rados.h"
//#include "include/ceph_assert.h"
#include "test/librados/test_cxx.h"
#include "test/librados/testcase_cxx.h"
#include "common/CDC.h"
#include "include/rados/librados.hpp"

#include "rgw/driver/rados/rgw_rados.h"
#include "rgw/driver/rados/rgw_sal_rados.h"
#include "rgw/rgw_dedup_worker.h"
#include "rgw/rgw_fp_manager.h"


//#define dout_subsys ceph_subsys_rgw

using namespace std;
using namespace librados;

static constexpr auto dout_subsys = ceph_subsys_rgw;
static rgw::sal::RadosStore* store = nullptr;

auto cct = new CephContext(CEPH_ENTITY_TYPE_CLIENT);
//const NoDoutPrefix dpp(g_ceph_context, dout_subsys);
const DoutPrefix dpp(cct, 1, "test rgw dedup: ");

class RGWDedupUnitTest : public RadosTestPP
{
public:
  RGWDedupUnitTest() {};
  ~RGWDedupUnitTest() override {};
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
    cout << "pool_name: " << pool_name << ", namespace: " << nspace << std::endl;

    cold_pool_name = get_temp_pool_name();
    ASSERT_EQ(s_cluster.pool_create(cold_pool_name.c_str()), 0);
    ASSERT_EQ(cluster.ioctx_create(cold_pool_name.c_str(), cold_ioctx), 0);
    cout << "cold pool name: " << cold_pool_name << std::endl;

    cold_ioctx.application_enable("rados", true);
    cold_ioctx.set_namespace(nspace);

    store = new rgw::sal::RadosStore();
    ASSERT_NE(store, nullptr);
    RGWRados* rados = new RGWRados();
    ASSERT_NE(rados, nullptr);
    store->setRados(rados);
    rados->set_store(store);
  }
  void TearDown() override {
    // wait for maps to settle before next test
    cluster.wait_for_latest_osdmap();

    RadosTestPP::TearDown();

    cleanup_default_namespace(cold_ioctx);
    cleanup_namespace(cold_ioctx, nspace);

    cold_ioctx.close();
    ASSERT_EQ(0, s_cluster.pool_delete(cold_pool_name.c_str()));
  }
};

// if there is any redundant chunk, it regards as deduplicated.
void check_chunk_is_redundant(const vector<tuple<bufferlist, pair<uint64_t, uint64_t>>> chunks,
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
    ASSERT_GT(cold_ioctx.read(metadata_oid, deduped_data, metadata_obj_size, 0), 0);
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
        ASSERT_GT(cold_ioctx.read(fp, tmpbl, get<1>(chunk).first, 0), 0);
        deduped_data.append(tmpbl);
      }
    }
  }
}


// RGWDedupWorker test
#include "cls/cas/cls_cas_internal.h"
TEST_F(RGWDedupUnitTest, test_data_consistency_after_dedup)
{
  shared_ptr<RGWFPManager> fpmanager = make_shared<RGWFPManager>("fastcdc", 1024, "sha1", 2, 1024);
  RGWDedupWorker worker(&dpp, cct, store, 0, fpmanager, cold_ioctx);
  worker.set_chunk_algorithm("fastcdc");
  worker.set_chunk_size(1024);
  worker.append_base_ioctx(ioctx.get_id(), ioctx);
  worker.set_dedup_threshold(2);

  vector<string> fp_algos{"sha1", "sha256", "sha512"};
  for (auto fp_algo : fp_algos) {
    worker.set_fp_algorithm(fp_algo);

    // Test an object not containing any redundancy
    // generate random data
    bufferlist og_data;
    string rand_oid = "rand-data-" + fp_algo;
    generate_buffer(1024 * 16, &og_data);
    {
      ObjectWriteOperation wop;
      wop.write_full(og_data);
      ASSERT_EQ(ioctx.operate(rand_oid, &wop), 0);
    }

    ObjectCursor shard_start, shard_end;
    ObjectCursor cursor = ioctx.object_list_begin();
    ioctx.object_list_slice(ioctx.object_list_begin(), ioctx.object_list_end(), 0, 1, &shard_start, &shard_end);
    while (cursor < shard_end) {
      vector<ObjectItem> objs;
      ASSERT_GE(ioctx.object_list(cursor, shard_end, 100, {}, &objs, &cursor), 0);
      worker.try_object_dedup(ioctx, objs.begin(), objs.end());
    }

    // get chunk map in order to get a sequence of chunks
    auto chunks = worker.do_cdc(og_data, "fastcdc", 1024);
    unordered_map<string, uint32_t> chunk_map;
    check_chunk_is_redundant(chunks, &worker, fp_algo, chunk_map);

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
      generate_buffer(1024, &tmpbl);
      for (int i = 0; i < 16; ++i) {
        og_data.append(tmpbl);
      }
    }
    ASSERT_EQ(og_data.length(), 1024 * 16);

    string dup_oid = "dup-data-" + fp_algo;
    {
      ObjectWriteOperation wop;
      wop.write_full(og_data);
      ASSERT_EQ(ioctx.operate(dup_oid, &wop), 0);
    }

    cursor = ioctx.object_list_begin();
    ioctx.object_list_slice(ioctx.object_list_begin(), ioctx.object_list_end(), 0, 1, &shard_start, &shard_end);
    while (cursor < shard_end) {
      vector<ObjectItem> objs;
      ASSERT_GE(ioctx.object_list(cursor, shard_end, 100, {}, &objs, &cursor), 0);
      worker.try_object_dedup(ioctx, objs.begin(), objs.end());
    }

    // get chunk map in order to get a sequence of chunks
    chunks = worker.do_cdc(og_data, "fastcdc", 1024);
    check_chunk_is_redundant(chunks, &worker, fp_algo, chunk_map);

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


TEST_F(RGWDedupUnitTest, chunk_obj_ref_size)
{
  shared_ptr<RGWFPManager> fpmanager = make_shared<RGWFPManager>("fastcdc", 1024, "sha1", 2, 16384);
  RGWDedupWorker worker(&dpp, cct, store, 0, fpmanager, cold_ioctx);
  worker.set_chunk_algorithm("fastcdc");
  worker.set_chunk_size(1024);
  worker.append_base_ioctx(ioctx.get_id(), ioctx);
  worker.set_fp_algorithm("sha1");
  worker.set_dedup_threshold(2);

  bufferlist data, tmpbl;
  generate_buffer(1024, &tmpbl);
  for (int i = 0; i < 4 * 1024; ++i) {
    data.append(tmpbl);
  }

  // create objects which have chunks larger than MAX_REF_CHUNK_SIZE
  // these objects contain about 1280 same chunks
  string obj_name = "dup-a-lot-obj-";
  for (int i = 0; i < 5; ++i) {
    ObjectWriteOperation wop;
    wop.write_full(data);
    ASSERT_EQ(ioctx.operate(obj_name + to_string(i), &wop), 0);
  }

  //ObjectCursor shard_start, shard_end;
  //ioctx.object_list_slice(ioctx.object_list_begin(), ioctx.object_list_end(),
                          //0, 1, &shard_start, &shard_end);
  cout << "base pool after write objs" << std::endl;
  //while (cursor < shard_end) {
  {
    ObjectCursor cursor;
    vector<ObjectItem> objs;
    ASSERT_GE(ioctx.object_list(ioctx.object_list_begin(), ioctx.object_list_end(), 100, {}, &objs, &cursor), 0);
    worker.try_object_dedup(ioctx, objs.begin(), objs.end());
    for (auto& obj : objs) {
      cout << "obj: " << obj.oid << std::endl;
    }
  }
  //}


  //cursor = cold_ioctx.object_list_begin();
  //cold_ioctx.object_list_slice(cold_ioctx.object_list_begin(),
                               //cold_ioctx.object_list_end(),
                               //0, 1, &shard_start, &shard_end);
  cout << "MAX_CHUNK_REF_SIZE: " << MAX_CHUNK_REF_SIZE << std::endl;
  cout << "cold pool after dedup" << std::endl;
  {
    vector<ObjectItem> objs;
    ASSERT_GE(cold_ioctx.object_list(cold_ioctx.object_list_begin(), cold_ioctx.object_list_end(), 100, {}, &objs, &cursor), 0);
    for (auto& obj : objs) {
      cout << "obj: " << obj.oid << std::endl;
      chunk_refs_t refs;
      ASSERT_EQ(worker.get_chunk_refs(cold_ioctx, obj.oid, refs), 0);
      chunk_refs_by_object_t* chunk_refs
         = static_cast<chunk_refs_by_object_t*>(refs.r.get());
      cout << "obj: " << obj.oid << " ref cnt: " << chunk_refs->by_object.size() << std::endl;
      ASSERT_LE(chunk_refs->by_object.size(), MAX_CHUNK_REF_SIZE); 
    }
  }
}

TEST_F(RGWDedupUnitTest, scrub_test)
{
  RGWChunkScrubWorker scrub_worker(&dpp, cct, store, 0, cold_ioctx);
  shared_ptr<RGWFPManager> fpmanager = make_shared<RGWFPManager>("fastcdc", 1024, "sha1", 2, 1024);
  RGWDedupWorker worker(&dpp, cct, store, 0, fpmanager, cold_ioctx);
  worker.set_chunk_algorithm("fastcdc");
  worker.set_chunk_size(1024);
  worker.append_base_ioctx(ioctx.get_id(), ioctx);
  worker.set_fp_algorithm("sha1");
  worker.set_dedup_threshold(2);

  // create object
  bufferlist data;
  generate_buffer(1024 * 1024, &data);
  cout << "data size: " << data.length() << std::endl;
  
  ObjectWriteOperation wop;
  wop.write_full(data);
  ASSERT_EQ(ioctx.operate("metadata-obj", &wop), 0);

  // dedup objects
  ObjectCursor cursor;
  vector<ObjectItem> objs;
  ASSERT_GE(ioctx.object_list(ioctx.object_list_begin(), ioctx.object_list_end(), 100, {}, &objs, &cursor), 0);
  worker.try_object_dedup(ioctx, objs.begin(), objs.end());
  
  
  // check the number of chunk objects' references


  // inject not available pool fault into chunk object


  // run chunk scrub

  
  // check reference count
  

  // inject not available oid fault into chunk object
  


  // inject count mismatch fault into chunk object



}

TEST_F(RGWDedupUnitTest, fpmap_size_test)
{
  
}
