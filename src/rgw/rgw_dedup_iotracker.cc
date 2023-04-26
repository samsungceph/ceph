// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_dedup_iotracker.h"
#include "include/ceph_hash.h"

void RGWIOTracker::initialize()
{
  create_hit_set();
  hit_set_map.clear();
  if (perfcounter) {
    perfcounter->set(l_rgw_dedup_hitset_count, hit_set_count);
    perfcounter->set(l_rgw_dedup_hitset_period, hit_set_period);
    perfcounter->set(l_rgw_dedup_hitset_target_size, hit_set_target_size);
    perfcounter->set(l_rgw_dedup_hitset_fpp, hit_set_fpp * 100);
  }
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
  HitSet::Params hitset_param;

  switch (hit_set_type) {
  case HitSet::TYPE_EXPLICIT_HASH:
    hitset_param = HitSet::Params(new ExplicitHashHitSet::Params);
    break;
  case HitSet::TYPE_EXPLICIT_OBJECT:
    hitset_param = HitSet::Params(new ExplicitObjectHitSet::Params);
    break;
  case HitSet::TYPE_BLOOM:
    ceph_assert(hit_set_fpp > 0);
    hitset_param = HitSet::Params(new BloomHitSet::Params(hit_set_fpp,
                                                          hit_set_target_size,
                                                          now.nsec()));
    break;
  case HitSet::TYPE_NONE:
    ldpp_dout(dpp, 0) << "create_hit_set failed" << dendl;
    return;
  }

  hit_set.reset(new HitSet(hitset_param));
  hit_set_start_stamp = now;
}

void RGWIOTracker::check_hit_set_valid(utime_t now, bool is_full)
{
  ceph_assert(!hit_set_start_stamp.is_zero());
  ceph_assert(hit_set_count > 0);

  // active HitSet only case. hit_set_map doesn't need.
  if (hit_set_count == 1) {
    if (is_full) {
      std::unique_lock<std::shared_mutex> lock(iotracker_lock);
      if (hit_set->insert_count() >= hit_set_target_size) {
        create_hit_set();
      }
    }
    return;
  }

  if (is_full || hit_set_start_stamp + utime_t(hit_set_period, 0) < now) {
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
  ceph_assert(hit_set.get());

  utime_t now = ceph_clock_now();
  bool is_full = hit_set->insert_count() >= hit_set_target_size;
  check_hit_set_valid(now, is_full);

  if (!obj.bucket.bucket_id.empty() && !obj.get_oid().empty()) {
    {
      std::unique_lock<std::shared_mutex> lock(iotracker_lock);
      string obj_key = obj.bucket.bucket_id + ":" + obj.get_oid();
      hit_set->insert(hobject_t("", "", 0, ceph_str_hash_linux(obj_key.c_str(),
                      obj_key.length()), 0, ""));
    }
  }
}

bool RGWIOTracker::is_hot(rgw_obj obj)
{
  ceph_assert(hit_set.get());

  utime_t now = ceph_clock_now();
  check_hit_set_valid(now);

  if (!obj.bucket.bucket_id.empty() && !obj.get_oid().empty()) {
    string obj_key = obj.bucket.bucket_id + ":" + obj.get_oid();
    {
      std::shared_lock<std::shared_mutex> lock(iotracker_lock);
      if (hit_set->contains(hobject_t("", "", 0, ceph_str_hash_linux(obj_key.c_str(),
          obj_key.length()), 0, ""))) {
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
      if (p->second->contains(hobject_t("", "", 0, ceph_str_hash_linux(obj_key.c_str(), obj_key.length()), 0, ""))) {
        ldpp_dout(dpp, 10) << obj << " found in hit_set_map" << dendl;
        return true;
      }
    }
  }
  ldpp_dout(dpp, 10) << obj << " not in IOTracker" << dendl;
  return false;
}

void RGWIOTracker::finalize()
{
  hit_set_map.clear();
  hit_set.reset();
}

void RGWIOTracker::set_hit_set_count(const uint32_t new_count)
{
  ceph_assert(new_count > 0);
  hit_set_count = new_count;
  if (perfcounter) {
    perfcounter->set(l_rgw_dedup_hitset_count, hit_set_count);
  }
}

void RGWIOTracker::set_hit_set_period(const uint32_t new_period)
{
  ceph_assert(new_period > 0);
  hit_set_period = new_period;
  if (perfcounter) {
    perfcounter->set(l_rgw_dedup_hitset_period, hit_set_period);
  }
}

void RGWIOTracker::set_hit_set_target_size(const uint32_t new_target_size)
{
  ceph_assert(new_target_size > 0);
  hit_set_target_size = new_target_size;
  if (perfcounter) {
    perfcounter->set(l_rgw_dedup_hitset_target_size, hit_set_target_size);
  }
}

void RGWIOTracker::set_hit_set_type(const HitSet::impl_type_t new_type)
{
  ceph_assert(new_type == HitSet::TYPE_EXPLICIT_HASH
              || new_type == HitSet::TYPE_EXPLICIT_OBJECT
              || new_type == HitSet::TYPE_BLOOM);
  hit_set_type = new_type;
}
