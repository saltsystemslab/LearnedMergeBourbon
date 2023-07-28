#include <cassert>
#include <chrono>
#include <fstream>
#include <iostream>
#include <set>
#include <string>
#include <vector>

#include "leveldb/db.h"

#include "util.h"

// #include "mod/config.h"
#include "mod/plr.h"
#include "mod/ycsb.cc"

using namespace std;

static const char* YCSB = "ycsb";

static const char* FLAGS_benchmark = "random";
static const char* FLAGS_ycsb_workload_type = "";
static const char* FLAGS_ycsb_key_type = "";
static const char* FLAGS_ycsb_access_pattern = "";
static int FLAGS_num_keys = 100000000;
static int FLAGS_key_size_bytes = 10;
static uint64_t FLAGS_universe_size = 10000000000;
static bool FLAGS_check_values_in_db = true;
static double FLAGS_plr_error = 10.0;

const std::string DB_NAME = "./DB";

bool is_flag(const char* arg, const char* flag) {
  return strncmp(arg, flag, strlen(flag)) == 0;
}

string generate_key(uint64_t key_value) {
  string key = to_string(key_value);
  string result = string(FLAGS_key_size_bytes - key.length(), '0') + key;
  return std::move(result);
}

void generate_random_keys(vector<std::string>& keys) {
  // set<int64_t> s;
  for (int i = 0; i < FLAGS_num_keys; i++) {
    int k = random() % FLAGS_universe_size;
    //  while (s.find(k) != s.end()) {
    //    k = random() % FLAGS_universe_size;
    //  }
    // if(s.find(k) != s.end()){
    //   std::cout<<"duplicate"<<std::endl;
    // }
    keys.push_back(generate_key(k));
    // s.insert(k);
  }
  // std::cout << "done generating" << std::endl;
  return;
}

void generate_ycsb_keys(vector<std::string>& load_keys,
                        vector<std::string>& run_keys,
                        vector<int>& run_phase_ops) {
  const char* workload_type = FLAGS_ycsb_workload_type;
  const char* key_type = FLAGS_ycsb_key_type;
  const char* access_pattern = FLAGS_ycsb_access_pattern;

  std::vector<uint64_t> load_phase_keys;
  std::vector<uint64_t> run_phase_keys;
  load_phase_keys = (ycsb_main(workload_type, key_type, access_pattern,
                               run_phase_keys, run_phase_ops))[0];

  for (auto key : load_phase_keys) {
    load_keys.push_back(generate_key(key));
  }
  for (auto key : run_phase_keys) {
    run_keys.push_back(generate_key(key));
  }
  return;
}

int main(int argc, char** argv) {
  std::ofstream stats;
  stats.open("stats.csv", std::ofstream::out);
  stats << "LEVEL"
        << ",";
  stats << "COMP_COUNT"
        << ",";
  stats << "LEARNED_COMP_COUNT"
        << ",";
  stats << "CDF_ABS_ERROR"
        << ",";
  stats << "NUM_ITERATORS"
        << ",";
  stats << "NUM_ITEMS"
        << "\n";
  // stats << "NUM_ITEMS_PER_LIST"
  //       << "\n ";

  // stats << "STANDARD_MERGER_DURATION"
  //       << ",";
  // stats << "LEARNED_MERGER_DURATION"
  //       << "\n";
  stats.close();

  for (int i = 1; i < argc; i++) {
    double m;
    long long n;
    char junk;
    if (is_flag(argv[i], "--benchmark=")) {
      FLAGS_benchmark = argv[i] + strlen("--benchmark=");
    } else if (is_flag(argv[i], "--ycsb_workload_type=")) {
      FLAGS_ycsb_workload_type = argv[i] + strlen("--ycsb_workload_type=");
    } else if (is_flag(argv[i], "--ycsb_key_type=")) {
      FLAGS_ycsb_key_type = argv[i] + strlen("--ycsb_key_type=");
    } else if (is_flag(argv[i], "--ycsb_access_pattern=")) {
      FLAGS_ycsb_access_pattern = argv[i] + strlen("--ycsb_access_pattern=");
    } else if (sscanf(argv[i], "--num_keys=%lld%c", &n, &junk) == 1) {
      FLAGS_num_keys = n;
    } else if (sscanf(argv[i], "--key_size_bytes=%lld%c", &n, &junk) == 1) {
      FLAGS_key_size_bytes = n;
    } else if (sscanf(argv[i], "--universe_size=%lld%c", &n, &junk) == 1) {
      FLAGS_universe_size = n;
    } else if (sscanf(argv[i], "--check_values=%lld%c", &n, &junk) == 1) {
      FLAGS_check_values_in_db = n;
    } else if (sscanf(argv[i], "--max_plr_error=%lf%c", &m, &junk) == 1) {
      FLAGS_plr_error = m;
    } else {
      printf("WARNING: unrecognized flag %s\n", argv[i]);
    }
  }

  vector<std::string> load_keys;
  vector<std::string> run_keys;
  vector<int> run_phase_ops;
  if (strcmp(FLAGS_benchmark, "random") == 0) {
    generate_random_keys(load_keys);
  } else if (strcmp(FLAGS_benchmark, YCSB) == 0) {
    // abort();
    generate_ycsb_keys(load_keys, run_keys, run_phase_ops);
  } else {
    std::cout << "Unrecognized benchmark!" << std::endl;
    return 1;
  }

  leveldb::Options options;
  leveldb::Status status = leveldb::DestroyDB(DB_NAME, options);
  assert(status.ok() || status.IsNotFound());

  leveldb::DB* db;
  options.create_if_missing = true;
  // options.max_plr_error = FLAGS_plr_error;
  status = leveldb::DB::Open(options, DB_NAME, &db);
  assert(status.ok());
  auto compactionStart = std::chrono::high_resolution_clock::now();
  int cnt = 0;
  if (strcmp(FLAGS_benchmark, YCSB) == 0) {
    // load phase
    for (auto k : load_keys) {
      db->Put(leveldb::WriteOptions(), k, k);
    }
    // run phase
    
    uint64_t lookup_duration_ns;
    for (int i = 0; i < run_phase_ops.size(); i++) {
      if (run_phase_ops[i] == 0 || run_phase_ops[i] == 1) {
        db->Put(leveldb::WriteOptions(), run_keys[i], run_keys[i]);
      } else if (run_phase_ops[i] == 2) {
        std::string value;
        auto lookup_start = std::chrono::high_resolution_clock::now();
        db->Get(leveldb::ReadOptions(), run_keys[i], &value);
        auto lookup_end = std::chrono::high_resolution_clock::now();
        lookup_duration_ns +=
            std::chrono::duration_cast<std::chrono::nanoseconds>(lookup_end -
                                                                 lookup_start)
                .count();
      } else {
        std::cout << "key operation is not valid" << std::endl;
      }
    }
    float lookup_duration_sec = lookup_duration_ns / 1e9;
    std::cout << "Lookup duration:" << lookup_duration_sec << std::endl;
  } else {
    // random
    for (auto k : load_keys) {
      db->Put(leveldb::WriteOptions(), k, k);
    }
  }
  auto compactionEnd = std::chrono::high_resolution_clock::now();
  uint64_t compaction_duration_ns =
      std::chrono::duration_cast<std::chrono::nanoseconds>(compactionEnd -
                                                           compactionStart)
          .count();
  float compaction_duration_sec = compaction_duration_ns / 1e9;
  std::cout << "Compaction duration:" << compaction_duration_sec << std::endl;
  if (FLAGS_check_values_in_db) {
    for (int i = 0; i < 5; i++) {
      for (auto k : load_keys) {
        std::string value;
        status = db->Get(leveldb::ReadOptions(), k, &value);
        assert(status.ok() && value == k);
      }
    }
  }

  std::cout << "DB Stats" << std::endl;
  std::string db_stats;
  std::cout << db->GetProperty("leveldb.stats", &db_stats);
  std::cout << db_stats << std::endl;

  cout << "Ok!" << endl;
}
