// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "gtest/gtest.h"
#include "test/librados/test_cxx.h"
#include "test_rgw_common.h"
#include "rgw/rgw_dedup_manager.h"
#include "rgw/rgw_dedup_worker.h"
#include "rgw/rgw_sal_rados.h"
#include "common/dout.h"
#define dout_subsys ceph_subsys_rgw

auto cct = new CephContext(CEPH_ENTITY_TYPE_CLIENT);
const DoutPrefix dp(cct, 1, "test rgw dedup: ");

class RGWDedupTest : public ::testing::Test {
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
// librados::IoCtx RGWDedupTest::ioctx;
// librados::IoCtx RGWDedupTest::ioctx_cold;

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
  FPManager *fpmanager;
  RGWDedupWorker dedupworker(&dp, cct, &store, 1234, fpmanager);
  EXPECT_EQ("DedupWorker_1234", dedupworker.get_id());

  RGWChunkScrubWorker scrubworker(&dp, cct, &store, 1234, 12345);
  EXPECT_EQ("ScrubWorker_1234", scrubworker.get_id());
}

TEST_F(RGWDedupTest, fpmanager_add)
{
  // FPManager *fpmanager = new FPManager("testchunkalgo", 1234, "testfpalgo");
  // FPManager fpmanager = FPManager("testchunkalgo", 1234, "testfpalgo");
  string teststring1 = "1234";
  string teststring2 = "5678";

  EXPECT_EQ(0, fpmanager.get_fpmap_size());
  
  fpmanager.add(teststring1);
  EXPECT_EQ(1, fpmanager.get_fpmap_size());

  fpmanager.add(teststring1);
  EXPECT_EQ(1, fpmanager.get_fpmap_size());

  fpmanager.add(teststring2);
  EXPECT_EQ(2, fpmanager.get_fpmap_size());

  fpmanager.add(teststring1);
  EXPECT_EQ(2, fpmanager.get_fpmap_size());
}

TEST_F(RGWDedupTest, fpmanager_find)
{
  FPManager *fpmanager = new FPManager("testchunkalgo", 1234, "testfpalgo");
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
  FPManager *fpmanager = new FPManager("testchunkalgo", 1234, "testfpalgo");
  string teststring1 = "1234";
  string teststring2 = "5678";
  
  fpmanager->add(teststring1);
  fpmanager->add(teststring2);

  EXPECT_EQ(2, fpmanager->get_fpmap_size());

  fpmanager->reset_fpmap();
  EXPECT_EQ(0, fpmanager->get_fpmap_size());
}

TEST_F(RGWDedupTest, do_cdc)
{
  FPManager *fpmanager;
  RGWDedupWorker dedupworker(&dp, cct, &store, 1234, fpmanager);

  bufferlist bl;
  generate_buffer(4*1024*1024, &bl);

  vector<pair<uint64_t, uint64_t>> fixed_expected = { {0, 262144}, {262144, 262144}, {524288, 262144}, {786432, 262144}, {1048576, 262144}, {1310720, 262144}, {1572864, 262144}, {1835008, 262144}, {2097152, 262144}, {2359296, 262144}, {2621440, 262144}, {2883584, 262144}, {3145728, 262144}, {3407872, 262144}, {3670016, 262144}, {3932160, 262144} };
  vector<pair<uint64_t, uint64_t>> fastcdc_expected = { {0, 151460}, {151460, 441676}, {593136, 407491}, {1000627, 425767}, {1426394, 602875}, {2029269, 327307}, {2356576, 155515}, {2512091, 159392}, {2671483, 829416}, {3500899, 539667}, {4040566, 153738} };
  
  auto fixed_chunks = dedupworker.do_cdc(bl, "fixed", 262144);
  ASSERT_EQ(fixed_expected.size(), fixed_chunks.size());
  for (int i = 0; i < fixed_chunks.size(); i++) {
    EXPECT_EQ(fixed_expected[i], std::get<1>(fixed_chunks[i]));
  }

  auto fastcdc_chunks = dedupworker.do_cdc(bl, "fastcdc", 262144);
  ASSERT_EQ(fastcdc_expected.size(), fastcdc_chunks.size());
  for (int i = 0; i < fastcdc_chunks.size(); i++) {
    EXPECT_EQ(fastcdc_expected[i], std::get<1>(fastcdc_chunks[i]));
  }

}

TEST_F(RGWDedupTest, generate_fingerprint)
{
  FPManager *fpmanager;
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

// YDGYDG
// int main (int argc, char** argv) {
//   ::testing::InitGoogleTest(&argc, argv);
//   return RUN_ALL_TESTS();
// }
