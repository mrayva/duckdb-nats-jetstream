#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "nats_connection.hpp"
#include "nats_message_decode.hpp"
#include <condition_variable>
#include <google/protobuf/descriptor.h>
#include <mutex>
#include <nats/nats.h>
#include <thread>
#include <unordered_map>

namespace duckdb {

class ExtensionLoader;
class DatabaseInstance;

class NatsSubscribeFunction {
public:
    static void Register(ExtensionLoader &loader);
};

struct NatsSubscribeConfig {
    string job_name;
    string target_table;
    NatsConnectionConfig connection;
    string subject;
    string queue_group;
    uint64_t batch_size = 1024;
    int64_t poll_ms = 100;
    int pending_message_limit = 65536;
    int pending_bytes_limit = 64 * 1024 * 1024;
    bool create_target_table = false;
    string subject_column = "subject";
    string payload_column = "payload";
    vector<string> json_fields;
    vector<string> msgpack_fields;
    vector<vector<string>> msgpack_field_paths;
    vector<string> cbor_fields;
    vector<vector<string>> cbor_field_paths;
    string proto_file;
    string proto_message;
    vector<string> proto_fields;
    vector<vector<const google::protobuf::FieldDescriptor *>> proto_field_paths;
};

struct NatsSubscribeProgress {
    bool running = false;
    bool paused = false;
    bool pause_requested = false;
    bool stop_requested = false;
    bool failed = false;
    bool resubscribe_requested = false;
    uint64_t rows_inserted = 0;
    uint64_t batches_committed = 0;
    uint64_t pending_messages = 0;
    uint64_t pending_bytes = 0;
    uint64_t max_pending_messages = 0;
    uint64_t max_pending_bytes = 0;
    uint64_t messages_delivered = 0;
    uint64_t messages_dropped = 0;
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

struct NatsSubscribeJobState {
    explicit NatsSubscribeJobState(NatsSubscribeConfig config);
    ~NatsSubscribeJobState();

    NatsSubscribeConfig config;
    NatsSubscribeProgress progress;
    shared_ptr<DatabaseInstance> db;

    std::mutex mutex;
    std::condition_variable cv;
    std::thread worker;
    natsConnection *conn = nullptr;
    natsSubscription *sub = nullptr;
};

class NatsSubscribeManager {
public:
    static NatsSubscribeManager &Get();

    shared_ptr<NatsSubscribeJobState> CreateJob(NatsSubscribeConfig config);
    shared_ptr<NatsSubscribeJobState> GetJob(const string &job_name);
    vector<shared_ptr<NatsSubscribeJobState>> ListJobs();
    bool PauseJob(const string &job_name);
    bool ResumeJob(const string &job_name);
    bool StopJob(const string &job_name);
    bool RemoveJob(const string &job_name);

private:
    std::mutex mutex_;
    unordered_map<string, shared_ptr<NatsSubscribeJobState>> jobs_;
};

} // namespace duckdb
