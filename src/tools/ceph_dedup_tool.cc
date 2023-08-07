// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 * * Author: Myoungwon Oh <ohmyoungwon@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#include "include/types.h"

#include "include/rados/buffer.h"
#include "include/rados/librados.hpp"
#include "include/rados/rados_types.hpp"

#include "common/Cond.h"
#include "common/Formatter.h"
#include "common/ceph_argparse.h"
#include "common/ceph_crypto.h"
#include "global/global_context.h"
#include "global/global_init.h"
#include "osd/osd_types.h"

#include <iostream>
#include <stdlib.h>
#include <time.h>
#include <sstream>
#include <math.h>
#include <mutex>
#include <condition_variable>
#include <vector>

#include "include/stringify.h"
#include "global/signal_handler.h"
#include "common/CDC.h"
#include "common/Preforker.h"

#include <boost/program_options/variables_map.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/filesystem.hpp>

using namespace std;
namespace po = boost::program_options;
namespace fs = boost::filesystem;

struct EstimateResult {
  std::unique_ptr<CDC> cdc;

  uint64_t chunk_size;

  mutex lock;

  // < key, <count, chunk_size> >
  map< string, pair <uint64_t, uint64_t> > chunk_statistics;
  uint64_t total_bytes = 0;
  std::atomic<uint64_t> total_objects = {0};

  EstimateResult(std::string alg, int chunk_size, int window_bits)
    : cdc(CDC::create(alg, chunk_size, window_bits)),
      chunk_size(1ull << chunk_size) {}

  void add_chunk(bufferlist& chunk, const std::string& fp_algo) {
    string fp;
    if (fp_algo == "sha1") {
      ceph::crypto::SHA1 sha;
      char buf[CEPH_CRYPTO_SHA1_DIGESTSIZE];
      memset(buf, 0x00, sizeof(buf));
      sha.Update((byte*)chunk.c_str(), chunk.length());
      sha.Final((byte*)buf);
      //fp = ceph::crypto::digest<ceph::crypto::SHA1>(chunk).to_str();
      fp = string(buf);
    } else if (fp_algo == "sha256") {
      ceph::crypto::SHA256 sha;
      char buf[CEPH_CRYPTO_SHA256_DIGESTSIZE];
      memset(buf, 0x00, sizeof(buf));
      sha.Update((byte*)chunk.c_str(), chunk.length());
      sha.Final((byte*)buf);
      //fp = ceph::crypto::digest<ceph::crypto::SHA256>(chunk).to_str();
      fp = string(buf);
    } else {
      ceph_assert(0 == "no support fingerperint algorithm");
    }

    lock_guard<mutex> l(mutex);
    auto p = chunk_statistics.find(fp);
    if (p != chunk_statistics.end()) {
      p->second.first++;
      if (p->second.second != chunk.length()) {
        cerr << "warning: hash collision on " << fp
             << ": was " << p->second.second
             << " now " << chunk.length() << std::endl;
      }
    } else {
      chunk_statistics[fp] = make_pair(1, chunk.length());
    }
    total_bytes += chunk.length();
  }

  void dump(Formatter *f) const {
    f->dump_unsigned("target_chunk_size", chunk_size);

    uint64_t dedup_bytes = 0;
    uint64_t dedup_objects = chunk_statistics.size();
    for (auto& j : chunk_statistics) {
      dedup_bytes += j.second.second;
    }
    f->dump_unsigned("dedup_bytes", dedup_bytes);
    //f->dump_unsigned("original_bytes", total_bytes);
    f->dump_float("dedup_bytes_ratio",
                  (double)dedup_bytes / (double)total_bytes);
    f->dump_float("dedup_objects_ratio",
                  (double)dedup_objects / (double)total_objects);

    uint64_t avg = total_bytes / dedup_objects;
    uint64_t sqsum = 0;
    for (auto& j : chunk_statistics) {
      sqsum += (avg - j.second.second) * (avg - j.second.second);
    }
    uint64_t stddev = sqrt(sqsum / dedup_objects);
    f->dump_unsigned("chunk_size_average", avg);
    f->dump_unsigned("chunk_size_stddev", stddev);
  }
};

map<uint64_t, EstimateResult> dedup_estimates; // chunk size -> result

using namespace librados;
unsigned default_op_size = 1 << 26;
unsigned default_max_thread = 2;
int32_t default_report_period = 10;
mutex glock;
//Mutex glock("glock");

po::options_description make_usage() {
  po::options_description desc("Usage");
  desc.add_options()
    ("help,h", ": produce help message")
    ("op estimate --pool <POOL> --chunk-size <CHUNK_SIZE> --chunk-algorithm <ALGO> --fingerprint-algorithm <FP_ALGO>",
     ": estimate how many chunks are redundant")
    ;
  po::options_description op_desc("Opational arguments");
  op_desc.add_options()
    ("op", po::value<std::string>(), ": estimate")
    ("chunk-size", po::value<int>(), ": chunk size (byte)")
    ("chunk-algorithm", po::value<std::string>(), ": <fixed|fastcdc>, set chunk-algorithm")
    ("fingerprint-algorithm", po::value<std::string>(), ": <sha1|sha256|sha512>, set fingerprint-algorithm")
    ("max-thread", po::value<int>(), ": set max thread")
    ("report-period", po::value<int>(), ": set report-period")
    ("max-seconds", po::value<int>(), ": set max runtime")
    ("max-read-size", po::value<int>(), ": set max read size")
    ("pool", po::value<std::string>(), ": set pool name")
    ("min-chunk-size", po::value<int>(), ": min chunk size (byte)")
    ("max-chunk-size", po::value<int>(), ": max chunk size (byte)")
    ("debug", ": enable debug")
    ("chunk-dedup-threshold", po::value<uint32_t>(), ": set the threshold for chunk dedup (number of duplication) ")
    ("daemon", ": execute sample dedup in daemon mode")
    ("num-dedup-tool", po::value<int>(), ": set the number of ceph-dedup-tool processes when estimates in parallel")
    ("dedup-tool-id", po::value<int>(), ": set the id of current ceph-dedup-tool's process when estimates in parallel")
    ("window-bits", po::value<int>(), ": set the window bits parameter of FastCDC")
    ("output-dir", po::value<std::string>(), ": set a directory path where estimation output is stored")
  ;
  desc.add(op_desc);
  return desc;
}

class EstimateDedupRatio;
class CrawlerThread : public Thread
{
  IoCtx io_ctx;
  int n;
  int m;
  ObjectCursor begin;
  ObjectCursor end;
  mutex m_lock;
  //Mutex m_lock("CrawlerThread::Locker");
  condition_variable m_cond;
  int32_t report_period;
  bool m_stop = false;
  uint64_t total_bytes = 0;
  uint64_t total_objects = 0;
  uint64_t examined_objects = 0;
  uint64_t examined_bytes = 0;
  uint64_t max_read_size = 0;
  bool debug = false;
#define COND_WAIT_INTERVAL 10

public:
  CrawlerThread(IoCtx& io_ctx, int n, int m,
                ObjectCursor begin, ObjectCursor end, int32_t report_period,
                uint64_t num_objects, uint64_t max_read_size = default_op_size):
    io_ctx(io_ctx), n(n), m(m), begin(begin), end(end),
    report_period(report_period), total_objects(num_objects), max_read_size(max_read_size)
  {}

  void signal(int signum) {
    lock_guard<mutex> l(m_lock);
    m_stop = true;
    m_cond.notify_all();
  }
  virtual void print_status(Formatter *f, ostream &out) {}
  uint64_t get_examined_objects() { return examined_objects; }
  uint64_t get_examined_bytes() { return examined_bytes; }
  uint64_t get_total_bytes() { return total_bytes; }
  uint64_t get_total_objects() { return total_objects; }
  void set_debug(const bool debug_) { debug = debug_; }
  friend class EstimateDedupRatio;
};

class EstimateDedupRatio : public CrawlerThread
{
  string chunk_algo;
  string fp_algo;
  uint64_t chunk_size;
  uint64_t max_seconds;
  uint32_t dedup_tool_id = 0;
  uint32_t num_dedup_tool = 1;

public:
  EstimateDedupRatio(
    IoCtx& io_ctx, int n, int m, ObjectCursor begin, ObjectCursor end,
    string chunk_algo, string fp_algo, uint64_t chunk_size, int32_t report_period,
    uint64_t num_objects, uint64_t max_read_size,
    uint64_t max_seconds,
    uint32_t dedup_tool_id, uint32_t num_dedup_tool):
    CrawlerThread(io_ctx, n, m, begin, end, report_period, num_objects,
                  max_read_size),
    chunk_algo(chunk_algo),
    fp_algo(fp_algo),
    chunk_size(chunk_size),
    max_seconds(max_seconds),
    dedup_tool_id(dedup_tool_id),
    num_dedup_tool(num_dedup_tool) {
  }

  void* entry() {
    estimate_dedup_ratio();
    return NULL;
  }
  void estimate_dedup_ratio();
};

std::vector<std::unique_ptr<CrawlerThread>> estimate_threads;

static void print_dedup_estimate(std::ostream& out, std::string chunk_algo, bool chunk_info_include)
{
  /*
  uint64_t total_bytes = 0;
  uint64_t total_objects = 0;
  */
  uint64_t examined_objects = 0;
  uint64_t examined_bytes = 0;

  for (auto &et : estimate_threads) {
    examined_objects += et->get_examined_objects();
    examined_bytes += et->get_examined_bytes();
  }

  auto f = Formatter::create("json-pretty");
  f->open_object_section("results");
  f->dump_string("chunk_algo", chunk_algo);
  f->open_array_section("chunk_sizes");
  if (chunk_info_include) {
    for (auto& i : dedup_estimates) {
      f->dump_object("chunker", i.second);
    }
  }

  f->close_section();

  f->open_object_section("summary");
  f->dump_unsigned("examined_objects", examined_objects);
  f->dump_unsigned("examined_bytes", examined_bytes);

  uint32_t fp_map_size = 0;
  for (const auto& i : dedup_estimates) {
    fp_map_size += i.second.chunk_statistics.size()
      * (i.second.chunk_statistics.begin()->first.length() + 1
      + sizeof(i.second.chunk_statistics.begin()->second));
  }
  f->dump_unsigned("fp_map_size", fp_map_size);

  /*
  f->dump_unsigned("total_objects", total_objects);
  f->dump_unsigned("total_bytes", total_bytes);
  f->dump_float("examined_ratio", (float)examined_bytes / (float)total_bytes);
  */
  f->close_section();
  f->close_section();
  f->flush(out);
}

static void handle_signal(int signum)
{
  lock_guard<mutex> l(glock);
  //glock.Lock();
  for (auto &p : estimate_threads) {
    p->signal(signum);
  }
}

void EstimateDedupRatio::estimate_dedup_ratio()
{
  ObjectCursor proc_shard_start;
  ObjectCursor proc_shard_end;
  ObjectCursor shard_start;
  ObjectCursor shard_end;

  // slice for multi-dedup-tool process
  io_ctx.object_list_slice(
    begin,
    end,
    dedup_tool_id,
    num_dedup_tool,
    &proc_shard_start,
    &proc_shard_end);

  // slice for EstimateDedupRatio thread
  io_ctx.object_list_slice(
    proc_shard_start,
    proc_shard_end,
    n,
    m,
    &shard_start,
    &shard_end);

  utime_t start = ceph_clock_now();
  utime_t end;
  if (max_seconds) {
    end = start;
    end += max_seconds;
  }

  utime_t next_report;
  if (report_period) {
    next_report = start;
    next_report += report_period;
  }

  ObjectCursor c(shard_start);
  while (c < shard_end)
  {
    std::vector<ObjectItem> result;
    int r = io_ctx.object_list(c, shard_end, 12, {}, &result, &c);
    if (r < 0 ){
      cerr << "error object_list : " << cpp_strerror(r) << std::endl;
      return;
    }

    unsigned op_size = max_read_size;

    for (const auto & i : result) {
      const auto &oid = i.oid;

      utime_t now = ceph_clock_now();
      if (max_seconds && now > end) {
        m_stop = true;
      }
      if (m_stop) {
        return;
      }
      if (n == 0 && // first thread only
          next_report != utime_t() && now > next_report) {
        cerr << (int)(now - start) << "s : read "
             << dedup_estimates.begin()->second.total_bytes << " bytes so far..."
             << std::endl;
        print_dedup_estimate(cerr, chunk_algo, false);
        next_report = now;
        next_report += report_period;
      }

      // read entire object
      bufferlist bl;
      uint64_t offset = 0;
      while (true) {
        bufferlist t;
        int ret = io_ctx.read(oid, t, op_size, offset);
        if (ret <= 0) {
          break;
        }
        offset += ret;
        bl.claim_append(t);
      }
      examined_objects++;
      examined_bytes += bl.length();

      // do the chunking
      for (auto& i : dedup_estimates) {
        vector<pair<uint64_t, uint64_t>> chunks;
        i.second.cdc->calc_chunks(bl, &chunks);
        for (auto& p : chunks) {
          bufferlist chunk;
          chunk.substr_of(bl, p.first, p.second);
          i.second.add_chunk(chunk, fp_algo);
          if (debug) {
            cout << " " << oid <<  " " << p.first << "~" << p.second << std::endl;
          }
        }
        ++i.second.total_objects;
      }
    }
  }
}

string get_opts_pool_name(const po::variables_map &opts) {
  if (opts.count("pool")) {
    return opts["pool"].as<string>();
  }
  cerr << "must specify pool name" << std::endl;
  exit(1);
}

string get_opts_chunk_algo(const po::variables_map &opts) {
  if (opts.count("chunk-algorithm")) {
    string chunk_algo = opts["chunk-algorithm"].as<string>();
    if (!CDC::create(chunk_algo, 12)) {
      cerr << "unrecognized chunk-algorithm " << chunk_algo << std::endl;
      exit(1);
    }
    return chunk_algo;
  }
  cerr << "must specify chunk-algorithm" << std::endl;
  exit(1);
}

string get_opts_fp_algo(const po::variables_map &opts) {
  if (opts.count("fingerprint-algorithm")) {
    string fp_algo = opts["fingerprint-algorithm"].as<string>();
    if (fp_algo != "sha1"
        && fp_algo != "sha256" && fp_algo != "sha512") {
      cerr << "unrecognized fingerprint-algorithm " << fp_algo << std::endl;
      exit(1);
    }
    return fp_algo;
  }
  cout << "SHA1 is set as fingerprint algorithm by default" << std::endl;
  return string("sha1");
}

string get_opts_op_name(const po::variables_map &opts) {
  if (opts.count("op")) {
    return opts["op"].as<string>();
  } else {
    cerr << "must specify op" << std::endl;
    exit(1);
  }
}

int get_opts_max_thread(const po::variables_map &opts) {
  if (opts.count("max-thread")) {
    return opts["max-thread"].as<int>();
  } else {
    cout << "2 is set as the number of threads by default" << std::endl;
    return 2;
  }
}

int get_opts_report_period(const po::variables_map &opts) {
  if (opts.count("report-period")) {
    return opts["report-period"].as<int>();
  } else {
    cout << "10 seconds is set as report period by default" << std::endl;
    return 10;
  }
}

void write_fp_info_file(string out_dir, uint32_t id)
{
  if (out_dir[out_dir.length() - 1] != '/') {
    out_dir.append("/");
  }

  for (const auto& i : dedup_estimates) {
    string file_name = "fp_info_" + to_string(i.first) + "_" + to_string(id) + ".csv";
    ofstream outfile(out_dir + file_name);
    string buf;
    int count = 0;

    //for (auto iter = i.second.chunk_statistics.begin();
         //iter != i.second.chunk_statistics.end();
         //++iter) {
    for (auto iter : i.second.chunk_statistics) {
      if (count >= 500) {
        // write to file
        outfile << buf;
        buf.clear();
        count = 0;
      }

      // buffer append
      buf.append(iter.first + "," + to_string(iter.second.second) + "\n");
      ++count;
    }
    outfile.close();
  }
}

int estimate_dedup_ratio(const po::variables_map &opts)
{
  Rados rados;
  IoCtx io_ctx;
  std::string chunk_algo = "fastcdc";
  string fp_algo = "sha1";
  string pool_name;
  uint64_t chunk_size = 0;
  uint64_t min_chunk_size = 8192;
  uint64_t max_chunk_size = 4*1024*1024;
  unsigned max_thread = default_max_thread;
  uint32_t report_period = default_report_period;
  uint64_t max_read_size = default_op_size;
  uint64_t max_seconds = 0;
  uint32_t num_dedup_tool = 1;
  uint32_t dedup_tool_id = 0;
  int window_bits = 0;
  string output_dir = "";
  int ret;
  std::map<std::string, std::string>::const_iterator i;
  bool debug = false;
  ObjectCursor begin;
  ObjectCursor end;
  librados::pool_stat_t s;
  list<string> pool_names;
  map<string, librados::pool_stat_t> stats;

  pool_name = get_opts_pool_name(opts);
  if (opts.count("chunk-algorithm")) {
    chunk_algo = opts["chunk-algorithm"].as<string>();
    if (!CDC::create(chunk_algo, 12)) {
      cerr << "unrecognized chunk-algorithm " << chunk_algo << std::endl;
      exit(1);
    }
  } else {
    cerr << "must specify chunk-algorithm" << std::endl;
    exit(1);
  }
  fp_algo = get_opts_fp_algo(opts);
  if (opts.count("chunk-size")) {
    chunk_size = opts["chunk-size"].as<int>();
  } else {
    cout << "8192 is set as chunk size by default" << std::endl;
  }
  if (opts.count("min-chunk-size")) {
    min_chunk_size = opts["min-chunk-size"].as<int>();
  } else {
    cout << "8192 is set as min chunk size by default" << std::endl;
  }
  if (opts.count("max-chunk-size")) {
    max_chunk_size = opts["max-chunk-size"].as<int>();
  } else {
    cout << "4MB is set as max chunk size by default" << std::endl;
  }
  max_thread = get_opts_max_thread(opts);
  report_period = get_opts_report_period(opts);
  if (opts.count("max-seconds")) {
    max_seconds = opts["max-seconds"].as<int>();
  } else {
    cout << "max seconds is not set" << std::endl;
  }
  if (opts.count("max-read-size")) {
    max_read_size = opts["max-read-size"].as<int>();
  } else {
    cout << default_op_size << " is set as max-read-size by default" << std::endl;
  }
  if (opts.count("debug")) {
    debug = true;
  }
  boost::optional<pg_t> pgid(opts.count("pgid"), pg_t());
  if (opts.count("num-dedup-tool")) {
    num_dedup_tool = opts["num-dedup-tool"].as<int>();
  }
  if (opts.count("dedup-tool-id")) {
    dedup_tool_id = opts["dedup-tool-id"].as<int>();
  }
  ceph_assert(num_dedup_tool > dedup_tool_id);
  if (opts.count("window-bits")) {
    window_bits = opts["window-bits"].as<int>();
  }
  ceph_assert(window_bits >= 0);
  if (opts.count("output-dir")) {
    output_dir= opts["output-dir"].as<string>();
    fs::path p = output_dir;
    if (!fs::exists(p)) {
      cerr << "error output dir not exists" << std::endl;
    }
  }

  ret = rados.init_with_context(g_ceph_context);
  if (ret < 0) {
     cerr << "couldn't initialize rados: " << cpp_strerror(ret) << std::endl;
     goto out;
  }
  ret = rados.connect();
  if (ret) {
     cerr << "couldn't connect to cluster: " << cpp_strerror(ret) << std::endl;
     ret = -1;
     goto out;
  }
  if (pool_name.empty()) {
    cerr << "--create-pool requested but pool_name was not specified!" << std::endl;
    exit(1);
  }
  ret = rados.ioctx_create(pool_name.c_str(), io_ctx);
  if (ret < 0) {
    cerr << "error opening pool "
         << pool_name << ": "
         << cpp_strerror(ret) << std::endl;
    goto out;
  }

  // set up chunkers
  if (chunk_size) {
    dedup_estimates.emplace(std::piecewise_construct,
                            std::forward_as_tuple(chunk_size),
                            std::forward_as_tuple(chunk_algo, cbits(chunk_size)-1, window_bits));
  } else {
    for (size_t cs = min_chunk_size; cs <= max_chunk_size; cs *= 2) {
      dedup_estimates.emplace(std::piecewise_construct,
                              std::forward_as_tuple(cs),
                              std::forward_as_tuple(chunk_algo, cbits(cs)-1, window_bits));
    }
  }

  glock.lock();
  begin = io_ctx.object_list_begin();
  end = io_ctx.object_list_end();
  pool_names.push_back(pool_name);
  ret = rados.get_pool_stats(pool_names, stats);
  if (ret < 0) {
    cerr << "error fetching pool stats: " << cpp_strerror(ret) << std::endl;
    glock.unlock();
    return ret;
  }
  if (stats.find(pool_name) == stats.end()) {
    cerr << "stats can not find pool name: " << pool_name << std::endl;
    glock.unlock();
    return ret;
  }
  s = stats[pool_name];

  for (unsigned i = 0; i < max_thread; i++) {
    std::unique_ptr<CrawlerThread> ptr (
      new EstimateDedupRatio(io_ctx, i, max_thread, begin, end,
                             chunk_algo, fp_algo, chunk_size,
                             report_period, s.num_objects, max_read_size,
                             max_seconds, dedup_tool_id, num_dedup_tool));
    ptr->create("estimate_thread");
    ptr->set_debug(debug);
    estimate_threads.push_back(move(ptr));
  }
  glock.unlock();

  for (auto &p : estimate_threads) {
    p->join();
  }

  // writing fingrtprint information into file
  if (!output_dir.empty()) {
    write_fp_info_file(output_dir, dedup_tool_id);
  }

  print_dedup_estimate(cout, chunk_algo, true);

 out:
  return (ret < 0) ? 1 : 0;
}

string make_pool_str(string pool, string var, string val)
{
  return string("{\"prefix\": \"osd pool set\",\"pool\":\"") + pool
    + string("\",\"var\": \"") + var + string("\",\"val\": \"")
    + val + string("\"}");
}

string make_pool_str(string pool, string var, int val)
{
  return make_pool_str(pool, var, stringify(val));
}

int main(int argc, const char **argv)
{
  std::vector<const char*> args;
  argv_to_vec(argc, argv, args);
  if (args.empty()) {
    cerr << argv[0] << ": -h or --help for usage" << std::endl;
    exit(1);
  }

  po::variables_map opts;
  po::positional_options_description p;
  p.add("command", 1);
  po::options_description desc = make_usage();
  try {
    po::parsed_options parsed =
      po::command_line_parser(argc, argv).options(desc).positional(p).allow_unregistered().run();
    po::store(parsed, opts);
    po::notify(opts);
  } catch(po::error &e) {
    std::cerr << e.what() << std::endl;
    return 1;
  }
  if (opts.count("help") || opts.count("h")) {
    cout<< desc << std::endl;
    exit(0);
  }

  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
                        CODE_ENVIRONMENT_DAEMON,
                        CINIT_FLAG_UNPRIVILEGED_DAEMON_DEFAULTS);

  Preforker forker;
  if (global_init_prefork(g_ceph_context) >= 0) {
    std::string err;
    int r = forker.prefork(err);
    if (r < 0) {
      cerr << err << std::endl;
      return r;
    }
    if (forker.is_parent()) {
      g_ceph_context->_log->start();
      if (forker.parent_wait(err) != 0) {
        return -ENXIO;
      }
      return 0;
    }
    global_init_postfork_start(g_ceph_context);
  }
  common_init_finish(g_ceph_context);
  if (opts.count("daemon")) {
    global_init_postfork_finish(g_ceph_context);
    forker.daemonize();
  }
  init_async_signal_handler();
  register_async_signal_handler_oneshot(SIGINT, handle_signal);
  register_async_signal_handler_oneshot(SIGTERM, handle_signal);

  string op_name = get_opts_op_name(opts);
  int ret = 0;
  if (op_name == "estimate") {
    ret = estimate_dedup_ratio(opts);
  } else {
    cerr << "unrecognized op " << op_name << std::endl;
    exit(1);
  }

  unregister_async_signal_handler(SIGINT, handle_signal);
  unregister_async_signal_handler(SIGTERM, handle_signal);
  shutdown_async_signal_handler();

  return forker.signal_exit(ret);
}
