// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "gtest/gtest.h"
#include "test/librados/test_cxx.h"
#include "test_rgw_common.h"
#include "rgw/rgw_fp_manager.h"
#include "rgw/rgw_dedup_manager.h"
#include "rgw/rgw_dedup_worker.h"
#include "rgw/driver/rados/rgw_sal_rados.h"
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


TEST_F(RGWDedupTest, fpmanager_add)
{
  shared_ptr<RGWFPManager> fpmanager = make_shared<RGWFPManager>("testchunkalgo", 16384, "testfpalgo");

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
  shared_ptr<RGWFPManager> fpmanager = make_shared<RGWFPManager>("testchunkalgo", 16384, "testfpalgo");

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
  shared_ptr<RGWFPManager> fpmanager = make_shared<RGWFPManager>("testchunkalgo", 16384, "testfpalgo");

  string teststring1 = "1234";
  string teststring2 = "5678";
  
  fpmanager->add(teststring1);
  fpmanager->add(teststring2);

  EXPECT_EQ(2, fpmanager->get_fpmap_size());

  fpmanager->reset_fpmap();
  EXPECT_EQ(0, fpmanager->get_fpmap_size());
}

TEST_F(RGWDedupTest, generate_fingerprint)
{
  shared_ptr<RGWFPManager> fpmanager = make_shared<RGWFPManager>("testchunkalgo", 16384, "testfpalgo");
  RGWDedupWorker dedupworker(&dp, cct, &store, 1234, fpmanager);

  bufferlist data1;
  data1.append("");
  EXPECT_EQ("da39a3ee5e6b4b0d3255bfef95601890afd80709",
              dedupworker.generate_fingerprint(data1, "sha1"));
  EXPECT_EQ("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
              dedupworker.generate_fingerprint(data1, "sha256"));
  EXPECT_EQ("cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e",
              dedupworker.generate_fingerprint(data1, "sha512"));

  bufferlist data2;
  data2.append("1234");
  EXPECT_EQ("7110eda4d09e062aa5e4a390b0a572ac0d2c0220",
              dedupworker.generate_fingerprint(data2, "sha1"));
  EXPECT_EQ("03ac674216f3e15c761ee1a5e255f067953623c8b388b4459e13f978d7c846f4",
              dedupworker.generate_fingerprint(data2, "sha256"));
  EXPECT_EQ("d404559f602eab6fd602ac7680dacbfaadd13630335e951f097af3900e9de176b6db28512f2e000b9d04fba5133e8b1c6e8df59db3a8ab9d60be4b97cc9e81db",
              dedupworker.generate_fingerprint(data2, "sha512"));

  bufferlist data3;
  data3.append("1234!@#$qwerQWER");
  EXPECT_EQ("4a8a52f40333d4a0a6b252eea92a157f655c0368",
              dedupworker.generate_fingerprint(data3, "sha1"));
  EXPECT_EQ("2b1f6dcffcc7cf39bb3b6a202e694699a57caadfa77236360e7934abb760a374",
              dedupworker.generate_fingerprint(data3, "sha256"));
  EXPECT_EQ("40b4d8d9a012f401488b0d3175cda012310e544dca3697f72554986d3acdbb2afd045370547b8438e9f66c9bf2b52043ff9616da251632d178916f5e9f4b0a65",
              dedupworker.generate_fingerprint(data3, "sha512"));
}

int main (int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
