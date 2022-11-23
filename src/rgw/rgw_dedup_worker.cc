// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_dedup_worker.h"

#define dout_subsys ceph_subsys_rgw

void Worker::clear_objs()
{
  rados_objs.clear();
}

void Worker::append_obj(target_rados_object new_obj)
{
  rados_objs.emplace_back(new_obj);
}

size_t Worker::get_num_objs()
{
  return rados_objs.size();
}

void Worker::set_run(bool run_status)
{
  is_run = run_status;
}


string RGWDedupWorker::get_id()
{
  return "DedupWorker_" + to_string(id);
}

void RGWDedupWorker::initialize()
{

}

void* RGWDedupWorker::entry()
{

  return nullptr;
}

void RGWDedupWorker::stop()
{

}

void RGWDedupWorker::finalize()
{

}


string RGWChunkScrubWorker::get_id()
{
  return "ScrubWorker_" + to_string(id);
}

void RGWChunkScrubWorker::initialize()
{

}

void* RGWChunkScrubWorker::entry()
{

  return nullptr;
}

void RGWChunkScrubWorker::stop()
{

}

void RGWChunkScrubWorker::finalize()
{

}

