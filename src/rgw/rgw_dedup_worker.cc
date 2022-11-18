// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_dedup_worker.h"

#define dout_subsys ceph_subsys_rgw


void Worker::set_run(bool run_status)
{
  is_run = run_status;
}

void Worker::stop()
{
  is_run = false;
}


void* RGWDedupWorker::entry()
{

  return nullptr;
}

void RGWDedupWorker::finalize()
{

}

void RGWDedupWorker::append_obj(target_rados_object new_obj)
{
  rados_objs.emplace_back(new_obj);
}

size_t RGWDedupWorker::get_num_objs()
{
  return rados_objs.size();
}

void RGWDedupWorker::clear_objs()
{
  rados_objs.clear();
}

string RGWDedupWorker::get_id()
{
  return "DedupWorker_" + to_string(id);
}


string RGWChunkScrubWorker::get_id()
{
  return "ScrubWorker_" + to_string(id);
}

void* RGWChunkScrubWorker::entry()
{

  return nullptr;
}

void RGWChunkScrubWorker::finalize()
{

}

