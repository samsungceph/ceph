// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_perf_counters.h"
#include "common/perf_counters.h"
#include "common/ceph_context.h"

PerfCounters *perfcounter = NULL;

int rgw_perf_start(CephContext *cct)
{
  PerfCountersBuilder plb(cct, "rgw", l_rgw_first, l_rgw_last);

  // RGW emits comparatively few metrics, so let's be generous
  // and mark them all USEFUL to get transmission to ceph-mgr by default.
  plb.set_prio_default(PerfCountersBuilder::PRIO_USEFUL);

  plb.add_u64_counter(l_rgw_req, "req", "Requests");
  plb.add_u64_counter(l_rgw_failed_req, "failed_req", "Aborted requests");

  plb.add_u64_counter(l_rgw_get, "get", "Gets");
  plb.add_u64_counter(l_rgw_get_b, "get_b", "Size of gets");
  plb.add_time_avg(l_rgw_get_lat, "get_initial_lat", "Get latency");
  plb.add_u64_counter(l_rgw_put, "put", "Puts");
  plb.add_u64_counter(l_rgw_put_b, "put_b", "Size of puts");
  plb.add_time_avg(l_rgw_put_lat, "put_initial_lat", "Put latency");

  plb.add_u64(l_rgw_qlen, "qlen", "Queue length");
  plb.add_u64(l_rgw_qactive, "qactive", "Active requests queue");

  plb.add_u64_counter(l_rgw_cache_hit, "cache_hit", "Cache hits");
  plb.add_u64_counter(l_rgw_cache_miss, "cache_miss", "Cache miss");

  plb.add_u64(l_rgw_dedup_original_data_size, "dedup_original_data_size", "Total RGW object size before deduplication");
  plb.add_u64(l_rgw_dedup_archived_data_size, "dedup_archived_data_size", "Total archived object size during deduplication");
  plb.add_u64(l_rgw_dedup_chunk_data_size, "dedup_chunk_data_size", "Total chunk object size during deduplication");
  plb.add_u64(l_rgw_dedup_deduped_data_size, "dedup_deduped_data_size", "Current deduped data size during deduplication");
  plb.add_u64(l_rgw_dedup_current_worker_mode, "dedup_current_worker_mode", "Current worker mode(dedup | scrub)");
  plb.add_u64_counter(l_rgw_dedup_worker_read, "dedup_worker_read", "RGWDedupWorker's read size to base pools");
  plb.add_u64_counter(l_rgw_dedup_worker_write, "dedup_worker_write", "RGWDedupWorker's write size to cold pools");
  plb.add_u64(l_rgw_dedup_worker_count, "dedup_worker_count", "Configuration value of RGWDedupWorker count");
  plb.add_u64(l_rgw_dedup_scrub_ratio, "dedup_scrub_ratio", "Configuration value of dedup_scrub_ratio");
  plb.add_u64(l_rgw_dedup_chunk_algo, "dedup_chunk_algo", "Configuration value of chunk_algo");
  plb.add_u64(l_rgw_dedup_chunk_size, "dedup_chunk_size", "Configuration value of chunk_size");
  plb.add_u64(l_rgw_dedup_fp_algo, "dedup_fp_algo", "Configuration value of fp_algo");
  plb.add_u64(l_rgw_dedup_hitset_count, "dedup_hitset_count", "Configuration value of hitset_count");
  plb.add_u64(l_rgw_dedup_hitset_period, "dedup_hitset_period", "Configuration value of hitset_period");
  plb.add_u64(l_rgw_dedup_hitset_target_size, "dedup_hitset_target_size", "Configuration value of hitset_target_size");
  plb.add_u64(l_rgw_dedup_hitset_fpp, "dedup_hitset_fpp", "Configuration value of hitset_target_fpp");

  plb.add_u64_counter(l_rgw_keystone_token_cache_hit, "keystone_token_cache_hit", "Keystone token cache hits");
  plb.add_u64_counter(l_rgw_keystone_token_cache_miss, "keystone_token_cache_miss", "Keystone token cache miss");

  plb.add_u64_counter(l_rgw_gc_retire, "gc_retire_object", "GC object retires");

  plb.add_u64_counter(l_rgw_lc_expire_current, "lc_expire_current",
		      "Lifecycle current expiration");
  plb.add_u64_counter(l_rgw_lc_expire_noncurrent, "lc_expire_noncurrent",
		      "Lifecycle non-current expiration");
  plb.add_u64_counter(l_rgw_lc_expire_dm, "lc_expire_dm",
		      "Lifecycle delete-marker expiration");
  plb.add_u64_counter(l_rgw_lc_transition_current, "lc_transition_current",
		      "Lifecycle current transition");
  plb.add_u64_counter(l_rgw_lc_transition_noncurrent,
		      "lc_transition_noncurrent",
		      "Lifecycle non-current transition");
  plb.add_u64_counter(l_rgw_lc_abort_mpu, "lc_abort_mpu",
		      "Lifecycle abort multipart upload");

  plb.add_u64_counter(l_rgw_pubsub_event_triggered, "pubsub_event_triggered", "Pubsub events with at least one topic");
  plb.add_u64_counter(l_rgw_pubsub_event_lost, "pubsub_event_lost", "Pubsub events lost");
  plb.add_u64_counter(l_rgw_pubsub_store_ok, "pubsub_store_ok", "Pubsub events successfully stored");
  plb.add_u64_counter(l_rgw_pubsub_store_fail, "pubsub_store_fail", "Pubsub events failed to be stored");
  plb.add_u64(l_rgw_pubsub_events, "pubsub_events", "Pubsub events in store");
  plb.add_u64_counter(l_rgw_pubsub_push_ok, "pubsub_push_ok", "Pubsub events pushed to an endpoint");
  plb.add_u64_counter(l_rgw_pubsub_push_failed, "pubsub_push_failed", "Pubsub events failed to be pushed to an endpoint");
  plb.add_u64(l_rgw_pubsub_push_pending, "pubsub_push_pending", "Pubsub events pending reply from endpoint");
  plb.add_u64_counter(l_rgw_pubsub_missing_conf, "pubsub_missing_conf", "Pubsub events could not be handled because of missing configuration");
  
  plb.add_u64_counter(l_rgw_lua_script_ok, "lua_script_ok", "Successfull executions of lua scripts");
  plb.add_u64_counter(l_rgw_lua_script_fail, "lua_script_fail", "Failed executions of lua scripts");
  plb.add_u64(l_rgw_lua_current_vms, "lua_current_vms", "Number of Lua VMs currently being executed");
  
  perfcounter = plb.create_perf_counters();
  cct->get_perfcounters_collection()->add(perfcounter);
  return 0;
}

void rgw_perf_stop(CephContext *cct)
{
  ceph_assert(perfcounter);
  cct->get_perfcounters_collection()->remove(perfcounter);
  delete perfcounter;
}

