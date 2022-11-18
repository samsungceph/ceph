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


int main (int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
