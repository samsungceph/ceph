// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_dedup_iotracker.h"

void RGWIOTracker::initialize()
{
  create_hit_set();
  hit_set_map.clear();
}

void RGWIOTracker::remove_oldest_hit_set()
{
  if (!hit_set_map.empty()) {
    map<utime_t, HitSetRef>::iterator it = hit_set_map.begin();
    hit_set_map.erase(it);
  }
}

void RGWIOTracker::create_hit_set()
{
  utime_t now = ceph_clock_now();
  BloomHitSet* bl_hitset = new BloomHitSet(hit_set_target_size,
                                           hit_set_fpp,
                                           now.sec());
  hit_set.reset(new HitSet(bl_hitset));
  hit_set_start_stamp = now;
}

void RGWIOTracker::check_hit_set_valid(utime_t now, bool is_full)
{
  assert(hit_set_start_stamp > utime_t());
  assert(hit_set_count> 0);

  // active HitSet only case. hit_set_map doesn't need.
  if (hit_set_count == 1) {
    return;
  }

  if (is_full || hit_set_start_stamp + hit_set_period < now) {
    {
      std::unique_lock<std::shared_mutex> lock(iotracker_lock);
      hit_set_map.emplace(hit_set_start_stamp, hit_set);
      hit_set_map.rbegin()->second->seal();
      create_hit_set();
    }
    
    while (hit_set_map.size() >= hit_set_count) {
      remove_oldest_hit_set();
    }
  }
}

void RGWIOTracker::insert(rgw_obj obj)
{
  assert(hit_set.get());

  utime_t now = ceph_clock_now();
  if (obj.bucket.bucket_id != "" && obj.get_oid() != "") {
    bool is_full = false;
    {
      std::unique_lock<std::shared_mutex> lock(iotracker_lock);
      hit_set->insert_string(obj.bucket.bucket_id + ":" + obj.get_oid());
      is_full = hit_set->is_full();
    }
    check_hit_set_valid(now, is_full);
  }
}

bool RGWIOTracker::estimate_temp(rgw_obj obj)
{
  assert(hit_set.get());

  utime_t now = ceph_clock_now();
  check_hit_set_valid(now);

  if (obj.bucket.bucket_id != "" && obj.get_oid() != "") {
    {
      std::shared_lock<std::shared_mutex> lock(iotracker_lock);
      if (hit_set->contains_string(obj.bucket.bucket_id+ ":" + obj.get_oid())) {
        ldpp_dout(dpp, 10) << obj << " found in active HitSet" << dendl;
        return true;
      }
    }

    // serach from the latest
    for (map<utime_t, HitSetRef>::reverse_iterator p = hit_set_map.rbegin();
         p != hit_set_map.rend();
         ++p) {
      // ignore too old HitSets
      if (p->first + (hit_set_count * hit_set_period) < now) {
        break;
      }
      if (p->second->contains_string(obj.bucket.bucket_id + ":" + obj.get_oid())) {
        ldpp_dout(dpp, 10) << obj << " found in hit_set_map" << dendl;
        return true;
      }
    }
  }
  ldpp_dout(dpp, 10) << obj << " not exists" << dendl;
  return false;
}

void RGWIOTracker::finalize()
{
  hit_set_map.clear();
  hit_set.reset();
}

void RGWIOTracker::set_hit_set_count(const uint32_t new_count)
{
  assert(new_count > 0);
  hit_set_count = new_count;
}

void RGWIOTracker::set_hit_set_period(const uint32_t new_period)
{
  assert(new_period > 0);
  hit_set_period = new_period;
}

void RGWIOTracker::set_hit_set_target_size(const uint32_t new_target_size)
{
  assert(new_target_size > 0);
  hit_set_target_size = new_target_size;
}
