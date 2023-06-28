// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "gtest/gtest.h"
#include "gmock/gmock.h"

#include "common/common_init.h"
#include "test_rgw_common.h"
#include "common/ceph_argparse.h"
#include "global/global_context.h"
#include "global/global_init.h"
#include "common/ceph_context.h"
//#include "rgw_common.h"
//#include "rgw_rados.h"
#include "test/librados/test_cxx.h"
#include "test/librados/testcase_cxx.h"
#include "common/CDC.h"
#include "include/rados/librados.hpp"

#include "rgw/driver/rados/rgw_rados.h"
#include "rgw/driver/rados/rgw_sal_rados.h"
#include "rgw/rgw_dedup_worker.h"
#include "rgw/rgw_fp_manager.h"
#include "rgw/rgw_dedup_manager.h"
#include "rgw/rgw_dedup_manager.cc"

#include <ctime>
#include <random>


using namespace std;
using namespace librados;

//static constexpr auto dout_subsys = ceph_subsys_rgw;
static rgw::sal::RadosStore* store = nullptr;

auto cct = new CephContext(CEPH_ENTITY_TYPE_CLIENT);
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

    cold_pool_name = get_temp_pool_name();
    ASSERT_EQ(s_cluster.pool_create(cold_pool_name.c_str()), 0);
    ASSERT_EQ(cluster.ioctx_create(cold_pool_name.c_str(), cold_ioctx), 0);

    cold_ioctx.application_enable("rados", true);
    cold_ioctx.set_namespace(nspace);

    /*
    store = new rgw::sal::RadosStore();
    ASSERT_NE(store, nullptr);
    RGWRados* rados = new RGWRados();
    rados->set_context(cct);
    rados->init_rados();
    ASSERT_NE(rados, nullptr);
    store->setRados(rados);
    rados->set_store(store);
    */
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

/*
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
        ASSERT_GT(cold_ioctx.read(fp, tmpbl, get<1>(chunk).second, 0), 0);
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
    time_t curtime = time(0);
    generate_buffer(1024 * 16, &og_data, curtime);
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
    cout << "checksum of metadata object: " << metadata_obj_checksum << std::endl;
    cout << "checksum of total chunk objs: " << chunk_obj_checksum << std::endl;

    // reset variables
    og_data.clear();
    chunk_data.clear();
    chunk_map.clear();
    fpmanager->reset_fpmap();

    // Test an object containing redundant chunk
    // generate redundant data
    {
      bufferlist tmpbl;
      time_t curtime = time(0);
      generate_buffer(1024, &tmpbl, curtime);
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
    get_chunk_map(chunks, &worker, fp_algo, chunk_map);
    //for (auto& [fp, cnt] : chunk_map) {
      //cout << "fp: " << fp << ", cnt: " << cnt << std::endl;
    //}
    

    // generate checksum of data before dedup
    metadata_obj_checksum = worker.generate_fingerprint(og_data, fp_algo);
    ASSERT_NE(metadata_obj_checksum, string());

    // read data after try_object_dedup
    read_deduped_data(&worker, ioctx, cold_ioctx, chunks, chunk_map,
                      dup_oid, 1024 * 16, fp_algo, chunk_data);
    chunk_obj_checksum = worker.generate_fingerprint(chunk_data, fp_algo);
    ASSERT_NE(chunk_obj_checksum, string());
    ASSERT_EQ(metadata_obj_checksum, chunk_obj_checksum);
    cout << "checksum of metadata objects: " << metadata_obj_checksum << std::endl;
    cout << "checksum of total chunk objects: " << chunk_obj_checksum << std::endl;

    // clear objects in base-pool and cold-pool
    cleanup_default_namespace(ioctx);
    cleanup_namespace(ioctx, nspace);
    cleanup_default_namespace(cold_ioctx);
    cleanup_namespace(cold_ioctx, nspace);
  }
}

TEST_F(RGWDedupUnitTest, chunk_obj_ref_size)
{
  store = new rgw::sal::RadosStore();
  ASSERT_NE(store, nullptr);
  RGWRados* rados = new RGWRados();
  ASSERT_NE(rados, nullptr);
  rados->set_context(cct);
  rados->init_rados();
  store->setRados(rados);
  rados->set_store(store);

  shared_ptr<RGWFPManager> fpmanager = make_shared<RGWFPManager>("fastcdc", 1024, "sha1", 2, 16 * 1024);
  RGWDedupWorker worker(&dpp, cct, store, 0, fpmanager, cold_ioctx);
  worker.set_chunk_algorithm("fastcdc");
  worker.set_chunk_size(16384);
  worker.append_base_ioctx(ioctx.get_id(), ioctx);
  worker.set_fp_algorithm("sha1");
  worker.set_dedup_threshold(2);

  // scale down max_chunk_ref_size not to take too much time
  worker.set_max_chunk_ref_size(100);

  bufferlist data, tmpbl;
  generate_buffer(1024, &tmpbl);
  for (int i = 0; i < 4096; ++i) {
    data.append(tmpbl);
  }

  // create objects which have chunks larger than MAX_REF_CHUNK_SIZE
  // these objects contain about 320 same chunks
  string obj_name = "dup-a-lot-obj-";
  for (int i = 0; i < 5; ++i) {
    ObjectWriteOperation wop;
    wop.write_full(data);
    ASSERT_EQ(ioctx.operate(obj_name + to_string(i), &wop), 0);
  }

  ObjectCursor cursor;
  vector<ObjectItem> objs;
  ASSERT_GE(ioctx.object_list(ioctx.object_list_begin(), ioctx.object_list_end(), 100, {}, &objs, &cursor), 0);
  worker.try_object_dedup(ioctx, objs.begin(), objs.end());

  objs.clear();
  ASSERT_GE(cold_ioctx.object_list(cold_ioctx.object_list_begin(), cold_ioctx.object_list_end(), 100, {}, &objs, &cursor), 0);
  for (const auto& obj : objs) {
    chunk_refs_t refs;
    ASSERT_EQ(worker.get_chunk_refs(cold_ioctx, obj.oid, refs), 0);
    chunk_refs_by_object_t* chunk_refs
       = static_cast<chunk_refs_by_object_t*>(refs.r.get());
    ASSERT_LE(chunk_refs->by_object.size(), worker.get_max_chunk_ref_size());
    cout << "max_chunk_ref_size: " << worker.get_max_chunk_ref_size() << ", chunk("
      << obj.oid << ") refcnt: " << chunk_refs->by_object.size() << std::endl;
  }
}

string get_target_chunk_oid(const map<string, uint32_t>& chunk_ref_cnt_map)
{
  random_device rd;
  mt19937 gen(rd());
  uniform_int_distribution<int> dis(0, chunk_ref_cnt_map.size() - 1);
  int target_idx = dis(gen);
  string target_chunk_oid;
  cout << "random index: " << target_idx << std::endl;

  for (const auto& [fp, cnt] : chunk_ref_cnt_map) {
    if (target_idx-- <= 0) {
      target_chunk_oid = fp;
      break;
    }
  }
  cout << "target oid: " << target_chunk_oid << std::endl;
  return target_chunk_oid;
}

#include "cls/cas/cls_cas_client.h"
TEST_F(RGWDedupUnitTest, scrub_test)
{
  RGWChunkScrubWorker scrub_worker(&dpp, cct, store, 0, cold_ioctx);
  scrub_worker.prepare(1, 0);
  scrub_worker.append_base_ioctx(ioctx.get_id(), ioctx);
  shared_ptr<RGWFPManager> fpmanager = make_shared<RGWFPManager>("fastcdc", 1024, "sha1", 1, 8 * 1024);
  RGWDedupWorker dedup_worker(&dpp, cct, store, 0, fpmanager, cold_ioctx);
  dedup_worker.set_chunk_algorithm("fastcdc");
  dedup_worker.set_chunk_size(8 * 1024);
  dedup_worker.append_base_ioctx(ioctx.get_id(), ioctx);
  dedup_worker.set_fp_algorithm("sha1");

  // allow unconditional dedup for all chunks
  dedup_worker.set_dedup_threshold(1);

  // create object
  bufferlist data;
  time_t curtime = time(0);
  generate_buffer(512 * 1024, &data, curtime);
  
  ObjectWriteOperation wop;
  wop.write_full(data);
  ASSERT_EQ(ioctx.operate("metadata-obj", &wop), 0);

  // dedup objects
  ObjectCursor cursor;
  vector<ObjectItem> objs;
  ASSERT_GE(ioctx.object_list(ioctx.object_list_begin(), ioctx.object_list_end(), 100, {}, &objs, &cursor), 0);
  dedup_worker.try_object_dedup(ioctx, objs.begin(), objs.end());
  
  // check the number of chunk objects' references
  hobject_t metadata_obj;
  objs.clear();
  map<string, uint32_t> chunk_ref_cnt_map;
  ASSERT_GE(cold_ioctx.object_list(cold_ioctx.object_list_begin(), cold_ioctx.object_list_end(), 100, {}, &objs, &cursor), 0);
  for (const auto& obj : objs) {
    chunk_refs_t refs;
    ASSERT_EQ(dedup_worker.get_chunk_refs(cold_ioctx, obj.oid, refs), 0);
    chunk_refs_by_object_t* chunk_refs
       = static_cast<chunk_refs_by_object_t*>(refs.r.get());
    //cout << "obj: " << obj.oid << " ref cnt: " << chunk_refs->by_object.size() << std::endl;
    chunk_ref_cnt_map.emplace(obj.oid, chunk_refs->by_object.size());

    if (metadata_obj.oid.name.empty()) {
      metadata_obj = *(chunk_refs->by_object.begin());
    }
  }
  
  // inject not available pool fault into chunk object
  string pool_fault_injected_oid = get_target_chunk_oid(chunk_ref_cnt_map);
  uint32_t pool_fault_injected_oid_ref_cnt = chunk_ref_cnt_map[pool_fault_injected_oid];
  uint32_t hash;
  ASSERT_GE(cold_ioctx.get_object_hash_position2(pool_fault_injected_oid, &hash), 0);
  hobject_t invalid_pool_ref(sobject_t("invalid-pool-fault-obj", CEPH_NOSNAP), 
                             "", hash, cold_ioctx.get_id() + 1, "");
  {
    ObjectWriteOperation wop;
    cls_cas_chunk_get_ref(wop, invalid_pool_ref);
    ASSERT_GE(cold_ioctx.operate(pool_fault_injected_oid, &wop), 0);
  }

  // inject not available oid fault into chunk object
  string oid_fault_injected_oid = get_target_chunk_oid(chunk_ref_cnt_map);
  uint32_t oid_fault_injected_oid_ref_cnt = chunk_ref_cnt_map[oid_fault_injected_oid];
  ASSERT_GE(cold_ioctx.get_object_hash_position2(oid_fault_injected_oid, &hash), 0);
  hobject_t invalid_oid_ref(sobject_t("invalid-oid-fault-obj", CEPH_NOSNAP),
                            "", hash, ioctx.get_id(), "");
  {
    ObjectWriteOperation wop;
    cls_cas_chunk_get_ref(wop, invalid_oid_ref);
    ASSERT_GE(cold_ioctx.operate(oid_fault_injected_oid, &wop), 0);
  }

  // inject count mismatch fault into chunk object
  string dummy_ref_injected_oid = get_target_chunk_oid(chunk_ref_cnt_map);
  uint32_t dummy_ref_injected_oid_ref_cnt = chunk_ref_cnt_map[dummy_ref_injected_oid];
  ASSERT_GE(cold_ioctx.get_object_hash_position2(dummy_ref_injected_oid, &hash), 0);
  hobject_t dummy_ref(sobject_t("metadata-obj", CEPH_NOSNAP),
                      "", hash, ioctx.get_id(), "");
  {
    ObjectWriteOperation wop;
    cls_cas_chunk_get_ref(wop, metadata_obj);
    ASSERT_GE(cold_ioctx.operate(dummy_ref_injected_oid, &wop), 0);
  }

  // run chunk scrub
  scrub_worker.entry();

  // check reference count
  {
    chunk_refs_t refs;
    ASSERT_EQ(dedup_worker.get_chunk_refs(cold_ioctx, pool_fault_injected_oid, refs), 0);
    chunk_refs_by_object_t* chunk_refs
      = static_cast<chunk_refs_by_object_t*>(refs.r.get());
    ASSERT_EQ(pool_fault_injected_oid_ref_cnt, chunk_refs->by_object.size());
  }

  {
    chunk_refs_t refs;
    ASSERT_EQ(dedup_worker.get_chunk_refs(cold_ioctx, oid_fault_injected_oid, refs), 0);
    chunk_refs_by_object_t* chunk_refs
      = static_cast<chunk_refs_by_object_t*>(refs.r.get());
    ASSERT_EQ(oid_fault_injected_oid_ref_cnt, chunk_refs->by_object.size());
  }

  {
    chunk_refs_t refs;
    ASSERT_EQ(dedup_worker.get_chunk_refs(cold_ioctx, dummy_ref_injected_oid, refs), 0);
    chunk_refs_by_object_t* chunk_refs
      = static_cast<chunk_refs_by_object_t*>(refs.r.get());
    ASSERT_EQ(dummy_ref_injected_oid_ref_cnt, chunk_refs->by_object.size());
  }
}

TEST_F(RGWDedupUnitTest, fpmap_memory_size_test)
{
  // limit fpmanager memory size upto 2048 bytes
  RGWFPManager fpmanager("fastcdc", 1024, "sha1", 2, 2048);
  RGWDedupWorker worker(&dpp, cct, store, 0, nullptr, cold_ioctx);

  vector<string> fp_algos = {"sha1", "sha256", "sha512"};
  for (const auto& fp_algo : fp_algos) {
    fpmanager.reset_fpmap();
    bufferlist data;
    {
      time_t curtime = time(0);
      generate_buffer(8 * 1024, &data, curtime);
    }

    auto dup_chunks = worker.do_cdc(data, "fastcdc", 1024);
    uint32_t dup_chunk_cnt = dup_chunks.size();

    // make current chunks' fp duplicated
    for (int i = 0; i < 2; i++) {
      for (const auto& chunk : dup_chunks) {
        string fp = worker.generate_fingerprint(get<0>(chunk), fp_algo);
        fpmanager.add(fp);
      }
    }
    ASSERT_EQ(fpmanager.get_fpmap_size(), dup_chunk_cnt);

    // add unique objects' chunks
    for (int i = 1; i <= 5; i++) {
      data.clear();
      {
        time_t curtime = time(0);
        generate_buffer(16 * 1024, &data, curtime + i);
      }

      auto unique_chunks = worker.do_cdc(data, "fastcdc", 1024);
      for (const auto& chunk : unique_chunks) {
        string fp = worker.generate_fingerprint(get<0>(chunk), fp_algo);
        fpmanager.add(fp);
      }
    }

     // call once again because add() inserts a fp value
     //  into fpmap after check_memory_limit_and_do_evict()
    fpmanager.check_memory_limit_and_do_evict();
    ASSERT_LE(fpmanager.get_fpmap_memory_size(), 2048);
  }
}
*/

class MockRados {
public:
  void set_mgr_command_ret(string json_service_dump) {
    service_dump.clear();
    service_dump.append(json_service_dump);
  }

  int mgr_command(string cmd, const bufferlist& inbl, bufferlist* outbl, string* outs) {
    outbl->clear();
    outbl->append(service_dump);
    return 0;
  }

private:
  bufferlist service_dump;
};

string create_service_dump(int num_rgws, int& cur_rgw_idx, bool is_valid)
{
  string dump = R"(
{
  "epoch": 1,
  "modified": "2023-06-28T01:27:02.781234+0000",
  "services": {
    "osd": {
      "daemons": {
        "0": {
          "start_epoch": 0,
          "start_stamp": "0.000000",
          "gid": 0,
          "addr": "(unrecognized address family 0)/0",
          "metadata": {},
          "task_status": {}
        },
        "1": {
          "start_epoch": 0,
          "start_stamp": "0.000000",
          "gid": 0,
          "addr": "(unrecognized address family 0)/0",
          "metadata": {},
          "task_status": {}
        },
        "summary": ""
      }
    },
    "rgw": {
      "daemons": {
        )";

  // randomly generate an index of RGW
  random_device rd;
  mt19937 gen(rd());
  uniform_int_distribution<int> dis(0, num_rgws - 1);
  cur_rgw_idx = dis(gen);

  for (int i = 0; i < num_rgws; ++i) {
    dump += "\"" + to_string(i);
    string rgw_instance = R"(": {
        "start_epoch": 1,
        "start_stamp": "2023-06-28T02:10:28.913702+0000",
        "gid": 4201,
        "addr": "X.X.X.X:Y/Z",
        "metadata": {
          "arch": "x86_64",
          "ceph_release": "reef",
          "ceph_version": "ceph version 10.2.0-85555-g96d92456cbd (96d92456cbd400df6d59ebf3b3a7c79ef9acb486) reef",
          "ceph_version_short": "10.2.0-85555-g96d92456cbd",
          "cpu": "Intel(R) Xeon(R) Gold 6342 CPU @ 2.80GHz",
          "distro": "ubuntu",
          "distro_description": "Ubuntu 22.04.1 LTS",
          "distro_version": "22.04",
          "frontend_config#0": "beast port=8000",
          "frontend_type#0": "beast",
          "hostname": "XXXX",
          "id": "8000",
          "kernel_description": "#80-Ubuntu SMP Mon May 15 15:18:26 UTC 2023",
          "kernel_version": "5.15.0-73-generic",
          "mem_swap_kb": "8388604",
          "mem_total_kb": "527791192",
          "num_handles": "1",
          "os": "Linux",
          "pid": ")";

    if (is_valid && cur_rgw_idx == i) {
      rgw_instance += to_string(getpid()) + "\",";
    } else {
      rgw_instance += to_string(-1) + "\",";
    }

    rgw_instance += R"(
            "realm_id": "",
            "realm_name": "",
            "zone_id": "f525ddcb-a19b-4bf5-a3df-36164db44ecf",
            "zone_name": "default",
            "zonegroup_id": "83d5a044-c81a-4061-a322-57dcec6d7e1b",
            "zonegroup_name": "default"
          },
          "task_status": {}
        },
    )";

    dump += rgw_instance;
  }
  dump += R"(
      }
    }
  }
}
  )";

  return dump;
}

TEST_F(RGWDedupUnitTest, service_dump_test)
{
  RGWDedupManager manager(&dpp, cct, store);
  MockRados mock_rados;

  int num_rgwdedup = -1;
  int cur_id = -1;
  string service_dump= "";
  mock_rados.set_mgr_command_ret(service_dump);
  ASSERT_EQ(manager.get_multi_rgwdedup_info(num_rgwdedup, cur_id, &mock_rados), -1);

  // no service in service_dump
  num_rgwdedup = -1;
  cur_id = -1;
  service_dump = R"({"epoch":1,"modified":"2023-06-28T01:27:02.781234+0000"})";
  mock_rados.set_mgr_command_ret(service_dump);
  ASSERT_EQ(manager.get_multi_rgwdedup_info(num_rgwdedup, cur_id, &mock_rados), -1);

  // no rgw
  num_rgwdedup = -1;
  cur_id = -1;
  service_dump = R"(
{
  "epoch": 1,
  "modified": "2023-06-28T01:27:02.781234+0000",
  "services": {
    "osd": {
      "daemons": {
        "0": {
          "start_epoch": 0,
          "start_stamp": "0.000000",
          "gid": 0,
          "addr": "(unrecognized address family 0)/0",
          "metadata": {},
          "task_status": {}
        },
        "1": {
          "start_epoch": 0,
          "start_stamp": "0.000000",
          "gid": 0,
          "addr": "(unrecognized address family 0)/0",
          "metadata": {},
          "task_status": {}
        },
        "summary": ""
      }
    }
  }
}
  )";
  mock_rados.set_mgr_command_ret(service_dump);
  ASSERT_EQ(manager.get_multi_rgwdedup_info(num_rgwdedup, cur_id, &mock_rados), -1);

  // no daemons
  num_rgwdedup = -1;
  cur_id = -1;
  service_dump = R"(
{
  "epoch": 1,
  "modified": "2023-06-28T02:10:47.289105+0000",
  "services": {
    "osd": {
      "daemons": {
        "0": {
          "start_epoch": 0,
          "start_stamp": "0.000000",
          "gid": 0,
          "addr": "(unrecognized address family 0)/0",
          "metadata": {},
          "task_status": {}
        },
        "1": {
          "start_epoch": 0,
          "start_stamp": "0.000000",
          "gid": 0,
          "addr": "(unrecognized address family 0)/0",
          "metadata": {},
          "task_status": {}
        },
        "summary": ""
      }
    },
    "rgw": {
    }
  }
}
  )";
  mock_rados.set_mgr_command_ret(service_dump);
  ASSERT_EQ(manager.get_multi_rgwdedup_info(num_rgwdedup, cur_id, &mock_rados), -1);

  // test invalid service dump which doesn't include current RGW's pid which containing RGWDedup
  // This presumes that current RGW info has not been recognized by MGR yet.
  // In case of this situation RGWDedupManager make fail.
  int cur_rgw_idx;
  for (int i = 1, num_rgwdedup = -1, cur_id = -1; i <= 10; ++i) {
    // generate invalid service dump not including current RGW's pid
    service_dump = create_service_dump(i, cur_rgw_idx, false);
    mock_rados.set_mgr_command_ret(service_dump);
    ASSERT_EQ(manager.get_multi_rgwdedup_info(num_rgwdedup, cur_id, &mock_rados), -1);
  }

  // test valid service dump including from 1 to 10 RGWs
  for (int i = 1, num_rgwdedup = -1, cur_id = -1; i <= 10; ++i) {
    // generate current RGW's index randomly
    service_dump = create_service_dump(i, cur_rgw_idx, true);
    mock_rados.set_mgr_command_ret(service_dump);
    ASSERT_EQ(manager.get_multi_rgwdedup_info(num_rgwdedup, cur_id, &mock_rados), 0);
    ASSERT_EQ(num_rgwdedup, i);
    ASSERT_EQ(cur_id, cur_rgw_idx);
  }
}

TEST_F(RGWDedupUnitTest, object_distribution_test)
{

}
