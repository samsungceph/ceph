// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_DEDUP_IOTRACKER_H
#define CEPH_RGW_DEDUP_IOTRACKER_H

#include <map>
#include <shared_mutex>

#include "osd/HitSet.h"
#include "include/utime.h"
#include "rgw_common.h"
#include "rgw_perf_counters.h"

extern const uint32_t DEFAULT_HITSET_COUNT;
extern const uint32_t DEFAULT_HITSET_PERIOD;
extern const uint64_t DEFAULT_HITSET_TARGET_SIZE;
extern const double DEFAULT_HITSET_FPP;

using namespace std;

class RGWIOTracker
{
protected:
  const DoutPrefixProvider* dpp;

  HitSetRef hit_set;              // an active HitSet
  utime_t hit_set_start_stamp;    // creation time of an active HitSet
  uint32_t hit_set_count;         // num of in-memory HitSets
  uint32_t hit_set_period;        // active period of HitSet (second)
  uint64_t hit_set_target_size;   // # allowed objects in a HitSet
  double hit_set_fpp;             // false positive rate of a HitSet
  HitSet::impl_type_t hit_set_type;
  map<utime_t, HitSetRef> hit_set_map;	// hot objects

  std::shared_mutex iotracker_lock;     // rw-lock (read: estimate, write: insert, trim)

public:
  RGWIOTracker(const DoutPrefixProvider* _dpp)
    : dpp(_dpp), hit_set_count(DEFAULT_HITSET_COUNT),
      hit_set_period(DEFAULT_HITSET_PERIOD),
      hit_set_target_size(DEFAULT_HITSET_TARGET_SIZE),
      hit_set_fpp(DEFAULT_HITSET_FPP),
      hit_set_type(HitSet::TYPE_BLOOM) {}
  ~RGWIOTracker() {}

  void initialize();
  void finalize();

  // add object to active HitSet
  void insert(rgw_obj obj);

  // check if HitSet contains a rgw_obj
  bool is_hot(rgw_obj obj);

  // discard expired in-memory HitSet
  void remove_oldest_hit_set();

  // create and reset a new active HitSet
  void create_hit_set();

  // deactivate active HitSet if needed
  void check_hit_set_valid(utime_t now, bool is_full = false);

  void set_hit_set_count(const uint32_t new_count);
  void set_hit_set_period(const uint32_t new_period);
  void set_hit_set_target_size(const uint32_t new_target_size);
  void set_hit_set_type(const HitSet::impl_type_t new_type);
};

#endif
