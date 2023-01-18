// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "gtest/gtest.h"
#include "test/librados/test_cxx.h"
#include "test_rgw_common.h"
#include "rgw/rgw_fp_manager.h"
#include "rgw/rgw_dedup_manager.h"
#include "rgw/rgw_dedup_worker.h"
#include "rgw/rgw_dedup_iotracker.h"
#include "rgw/rgw_sal_rados.h"
#include "common/dout.h"
#define dout_subsys ceph_subsys_rgw

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


TEST_F(RGWDedupTest, iotracker)
{
  RGWIOTracker iotracker(&dp);
  iotracker.set_hit_set_count(2);
  iotracker.set_hit_set_period(2);
  iotracker.set_hit_set_target_size(2);
  iotracker.set_hit_set_type(HitSet::TYPE_EXPLICIT_HASH);
  iotracker.initialize();

  string bucket_id = "test_bucket_id";
  rgw_bucket bucket("tenant", "test_bucket", bucket_id);

  rgw_obj obj_01(bucket, "ABCDEF");
  rgw_obj obj_02(bucket, "BCDEFG");
  rgw_obj obj_03(bucket, "CDEFGH");
  rgw_obj obj_04(bucket, "DEFGHI");
  rgw_obj obj_05(bucket, "EFGHIJ");

  EXPECT_EQ(false, iotracker.is_hot(rgw_obj()));
  EXPECT_EQ(false, iotracker.is_hot(obj_01));

  // spacial locality test
  iotracker.insert(obj_01);
  EXPECT_EQ(true, iotracker.is_hot(obj_01));
  iotracker.insert(obj_02);
  EXPECT_EQ(true, iotracker.is_hot(obj_02));
  iotracker.insert(obj_03);
  EXPECT_EQ(true, iotracker.is_hot(obj_03));
  iotracker.insert(obj_04);
  EXPECT_EQ(true, iotracker.is_hot(obj_04));
  iotracker.insert(obj_05);
  EXPECT_EQ(true, iotracker.is_hot(obj_05));

  EXPECT_EQ(false, iotracker.is_hot(obj_01));
  EXPECT_EQ(false, iotracker.is_hot(obj_02));
  EXPECT_EQ(true, iotracker.is_hot(obj_03));
  EXPECT_EQ(true, iotracker.is_hot(obj_04));
  EXPECT_EQ(true, iotracker.is_hot(obj_05));
  
  // temporal locality test
  iotracker.insert(obj_01);
  sleep(4);
  EXPECT_EQ(false, iotracker.is_hot(obj_01));
}

TEST_F(RGWDedupTest, fpmanager_add)
{
  RGWFPManager *fpmanager = new RGWFPManager("testchunkalgo", 1234, "testfpalgo");

  string teststring1 = "1234";
  string teststring2 = "5678";

  EXPECT_EQ(0, fpmanager->get_fpmap_size());
  
  fpmanager->add(teststring1);
  EXPECT_EQ(1, fpmanager->get_fpmap_size());

  fpmanager->add(teststring1);
  EXPECT_EQ(1, fpmanager->get_fpmap_size());

  fpmanager->add(teststring2);
  EXPECT_EQ(2, fpmanager->get_fpmap_size());

  fpmanager->add(teststring1);
  EXPECT_EQ(2, fpmanager->get_fpmap_size());
}

TEST_F(RGWDedupTest, fpmanager_find)
{
  RGWFPManager *fpmanager = new RGWFPManager("testchunkalgo", 1234, "testfpalgo");
  string teststring1 = "1234";
  string teststring2 = "5678";
  string teststring3 = "asdf";

  fpmanager->add(teststring1);
  fpmanager->add(teststring2);

  EXPECT_EQ(true, fpmanager->find(teststring1));
  EXPECT_EQ(true, fpmanager->find(teststring2));
  EXPECT_EQ(false, fpmanager->find(teststring3));
}

TEST_F(RGWDedupTest, reset_fpmap)
{
  RGWFPManager *fpmanager = new RGWFPManager("testchunkalgo", 1234, "testfpalgo");
  string teststring1 = "1234";
  string teststring2 = "5678";
  
  fpmanager->add(teststring1);
  fpmanager->add(teststring2);

  EXPECT_EQ(2, fpmanager->get_fpmap_size());

  fpmanager->reset_fpmap();
  EXPECT_EQ(0, fpmanager->get_fpmap_size());
}

int main (int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
