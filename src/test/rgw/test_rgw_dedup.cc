// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "gtest/gtest.h"
#include "test/librados/test_cxx.h"
#include "test_rgw_common.h"
#include "rgw/rgw_dedup_manager.h"
#include "rgw/rgw_dedup_worker.h"
#include "rgw/rgw_sal_rados.h"
#include "common/dout.h"


auto cct = new CephContext(CEPH_ENTITY_TYPE_CLIENT);
const DoutPrefix dp(cct, 1, "test rgw dedup: ");

class RGWDedupTest : public ::testing::Test
{
protected:
  rgw::sal::RadosStore store;

  static void SetUpTestCase() {}
  static void TearDownTestCase() {}

  void SetUp() override {}
  void TearDown() override {}

public:
  RGWDedupTest() {}
  ~RGWDedupTest() override {}
};

TEST_F(RGWDedupTest, set_sampling_ratio)
{
  RGWDedupManager dedupmanager(&dp, cct, &store);

  EXPECT_EQ(0, dedupmanager.set_sampling_ratio(1));
  EXPECT_EQ(0, dedupmanager.set_sampling_ratio(100));
  EXPECT_EQ(-1, dedupmanager.set_sampling_ratio(0));
  EXPECT_EQ(-1, dedupmanager.set_sampling_ratio(101));
  EXPECT_EQ(-1, dedupmanager.set_sampling_ratio(-1000));
  EXPECT_EQ(-1, dedupmanager.set_sampling_ratio(1000));
}

TEST_F(RGWDedupTest, sample_objects)
{
  RGWDedupManager dedup_manager(&dp, cct, &store);

  int num_objs = 10;
  for (int i = 0; i < num_objs; ++i) {
    string oid = "obj_" + to_string(i);
    target_rados_object obj{oid, "test_pool"};
    dedup_manager.append_rados_obj(obj);
  }
  EXPECT_EQ(num_objs, dedup_manager.get_num_rados_obj());

  int sampling_ratio = 30;
  EXPECT_EQ(0, dedup_manager.set_sampling_ratio(sampling_ratio));
  vector<size_t> sampled_idx = dedup_manager.sample_rados_objects();
  EXPECT_EQ(num_objs * sampling_ratio / 100, sampled_idx.size());

  sampling_ratio = 100;
  EXPECT_EQ(0, dedup_manager.set_sampling_ratio(sampling_ratio));
  sampled_idx.clear();
  sampled_idx = dedup_manager.sample_rados_objects();
  EXPECT_EQ(num_objs * sampling_ratio / 100, sampled_idx.size());
}


TEST_F(RGWDedupTest, get_worker_id)
{
  RGWDedupWorker dedupworker(&dp, cct, &store, 1234);
  EXPECT_EQ("DedupWorker_1234", dedupworker.get_id());

  RGWChunkScrubWorker scrubworker(&dp, cct, &store, 1234, 12345);
  EXPECT_EQ("ScrubWorker_1234", scrubworker.get_id());
}


int main (int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
