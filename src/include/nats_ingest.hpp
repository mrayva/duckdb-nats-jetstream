#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "nats_connection.hpp"
#include <google/protobuf/descriptor.h>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <nats/nats.h>

namespace duckdb {

class ExtensionLoader;
class DatabaseInstance;

class NatsIngestFunction {
public:
    static void Register(ExtensionLoader &loader);
};

struct NatsIngestConfig {
    string job_name;
    string stream_name;
    string target_table;
    string durable_name;
    NatsConnectionConfig connection;
    string subject_contains;
    string nats_subject;
    uint64_t start_seq = 1;
    uint64_t batch_size = 1000;
    int64_t poll_ms = 100;
    int64_t fetch_timeout_ms = 1000;
    bool resume_from_checkpoint = true;
    bool create_target_table = false;
    vector<string> json_fields;
    vector<string> msgpack_fields;
    vector<vector<string>> msgpack_field_paths;
    vector<string> cbor_fields;
    vector<vector<string>> cbor_field_paths;
    vector<string> flexbuffers_fields;
    vector<vector<string>> flexbuffers_field_paths;
    string proto_file;
    string proto_message;
    vector<string> proto_fields;
    vector<vector<const google::protobuf::FieldDescriptor *>> proto_field_paths;
};

struct NatsIngestProgress {
    bool running = false;
    bool stop_requested = false;
    bool stopped = false;
    bool failed = false;
    bool paused = false;
    bool pause_requested = false;
    uint64_t last_committed_seq = 0;
    uint64_t last_delivered_seq = 0;
    uint64_t rows_inserted = 0;
    uint64_t duplicates_skipped = 0;
    uint64_t batches_committed = 0;
    uint64_t fetches_completed = 0;
    uint64_t last_batch_rows = 0;
    uint64_t checkpoint_seq = 0;
    bool connected = false;
    bool reconnecting = false;
    uint64_t reconnect_count = 0;
    timestamp_t last_reconnect_time {0};
    timestamp_t last_start_time;
    timestamp_t last_fetch_time;
    timestamp_t last_ack_time;
    timestamp_t last_commit_time;
    timestamp_t last_error_time;
    string last_error;
};

struct NatsIngestJobState {
    explicit NatsIngestJobState(NatsIngestConfig config);
    ~NatsIngestJobState();

    NatsIngestConfig config;
    NatsIngestProgress progress;
    // Weak: this job can outlive the DatabaseInstance's normal owner (e.g. it sits
    // stopped-but-not-removed in NatsIngestManager's static map). Holding a strong
    // ref here would make this job the last owner, deferring ~DatabaseInstance() to
    // static teardown at process exit, which crashes. Lock and null-check on use.
    weak_ptr<DatabaseInstance> db;
    std::mutex job_mutex;
    std::condition_variable cv;
    std::thread worker;
    string lease_owner_id;
    uint64_t fencing_token = 0;
    int lease_fd = -1;
    natsConnection *conn = nullptr;
    jsCtx *js = nullptr;
    natsSubscription *sub = nullptr;
};

class NatsIngestManager {
public:
    static NatsIngestManager &Get();

    shared_ptr<NatsIngestJobState> CreateJob(NatsIngestConfig config);
    shared_ptr<NatsIngestJobState> GetJob(const string &job_name);
    vector<shared_ptr<NatsIngestJobState>> ListJobs();
    bool PauseJob(const string &job_name);
    bool ResumeJob(const string &job_name);
    bool StopJob(const string &job_name);
    bool RemoveJob(const string &job_name);

private:
    std::mutex mutex_;
    unordered_map<string, shared_ptr<NatsIngestJobState>> jobs_;
};

} // namespace duckdb
