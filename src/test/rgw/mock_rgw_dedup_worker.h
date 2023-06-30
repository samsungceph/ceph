
#include "rgw/rgw_dedup_worker.h"

class MockDedupWorker : public RGWDedupWorker {
public:
  void* entry() override {
    return nullptr;
  }

  MockDedupWorker(int id)
    : RGWDedupWorker(nullptr, nullptr, nullptr, id, nullptr, "", 0, "", 1, IoCtx()) {}
  virtual ~MockDedupWorker() {}
};

class MockScrubWorker : public RGWChunkScrubWorker {
public:
  void* entry() override {
    return nullptr;
  }

  MockScrubWorker(int id) : RGWChunkScrubWorker(nullptr, nullptr, nullptr, id, IoCtx()) {}
  virtual ~MockScrubWorker() {}
};
