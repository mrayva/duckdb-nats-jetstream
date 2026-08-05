#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "nats_connection.hpp"
#include <condition_variable>
#include <mutex>
#include <nats/nats.h>
#include <thread>
#include <unordered_map>

namespace duckdb {

class ExtensionLoader;
class DatabaseInstance;

class NatsKvWatchFunction {
public:
    static void Register(ExtensionLoader &loader);
};

struct NatsKvWatchConfig {
    string job_name;
    string target_table;
    NatsConnectionConfig connection;
    string bucket;
    string key_filter;
    uint64_t batch_size = 256;
    int64_t poll_ms = 1000;
    bool updates_only = false;
    bool ignore_deletes = false;
    bool create_target_table = false;
    string key_column = "key";
    string value_column = "value";
};

struct NatsKvWatchProgress {
    bool running = false;
    bool paused = false;
    bool pause_requested = false;
    bool stop_requested = false;
    bool failed = false;
    uint64_t rows_inserted = 0;
    uint64_t batches_committed = 0;
    uint64_t entries_delivered = 0;
    bool connected = false;
    bool reconnecting = false;
    uint64_t reconnect_count = 0;
    string last_error;
    timestamp_t last_start_time;
    timestamp_t last_commit_time;
    timestamp_t last_error_time;
    timestamp_t last_message_time;
    timestamp_t last_reconnect_time {0};
};

struct NatsKvWatchJobState {
    explicit NatsKvWatchJobState(NatsKvWatchConfig config);
    ~NatsKvWatchJobState();

    NatsKvWatchConfig config;
    NatsKvWatchProgress progress;
    // Weak: this job can outlive the DatabaseInstance's normal owner (e.g. it sits
    // stopped-but-not-removed in NatsKvWatchManager's static map). Holding a strong
    // ref here would make this job the last owner, deferring ~DatabaseInstance() to
    // static teardown at process exit, which crashes. Lock and null-check on use.
    weak_ptr<DatabaseInstance> db;

    std::mutex mutex;
    std::condition_variable cv;
    std::thread worker;
    natsConnection *conn = nullptr;
    jsCtx *js = nullptr;
    kvStore *kv = nullptr;
    kvWatcher *watcher = nullptr;
};

class NatsKvWatchManager {
public:
    static NatsKvWatchManager &Get();

    shared_ptr<NatsKvWatchJobState> CreateJob(NatsKvWatchConfig config);
    shared_ptr<NatsKvWatchJobState> GetJob(const string &job_name);
    vector<shared_ptr<NatsKvWatchJobState>> ListJobs();
    bool PauseJob(const string &job_name);
    bool ResumeJob(const string &job_name);
    bool StopJob(const string &job_name);
    bool RemoveJob(const string &job_name);

private:
    std::mutex mutex_;
    unordered_map<string, shared_ptr<NatsKvWatchJobState>> jobs_;
};

} // namespace duckdb
