#include "nats_subscribe.hpp"

#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

#include <algorithm>
#include <chrono>
#include <limits>
#include <sstream>

namespace duckdb {

NatsSubscribeJobState::NatsSubscribeJobState(NatsSubscribeConfig config_p) : config(std::move(config_p)) {
}

NatsSubscribeJobState::~NatsSubscribeJobState() {
    if (worker.joinable()) {
        {
            std::lock_guard<std::mutex> guard(mutex);
            progress.stop_requested = true;
            cv.notify_all();
        }
        worker.join();
    }
    if (sub != nullptr) {
        natsSubscription_Destroy(sub);
        sub = nullptr;
    }
    if (conn != nullptr) {
        natsConnection_Destroy(conn);
        conn = nullptr;
    }
}

NatsSubscribeManager &NatsSubscribeManager::Get() {
    static NatsSubscribeManager instance;
    return instance;
}

shared_ptr<NatsSubscribeJobState> NatsSubscribeManager::CreateJob(NatsSubscribeConfig config) {
    auto job = make_shared_ptr<NatsSubscribeJobState>(std::move(config));
    lock_guard<std::mutex> guard(mutex_);
    auto insert_result = jobs_.emplace(job->config.job_name, job);
    if (!insert_result.second) {
        throw BinderException("subscribe job '%s' already exists", job->config.job_name);
    }
    return job;
}

shared_ptr<NatsSubscribeJobState> NatsSubscribeManager::GetJob(const string &job_name) {
    lock_guard<std::mutex> guard(mutex_);
    auto entry = jobs_.find(job_name);
    if (entry == jobs_.end()) {
        return nullptr;
    }
    return entry->second;
}

vector<shared_ptr<NatsSubscribeJobState>> NatsSubscribeManager::ListJobs() {
    lock_guard<std::mutex> guard(mutex_);
    vector<shared_ptr<NatsSubscribeJobState>> result;
    result.reserve(jobs_.size());
    for (auto &entry : jobs_) {
        result.push_back(entry.second);
    }
    return result;
}

bool NatsSubscribeManager::PauseJob(const string &job_name) {
    auto job = GetJob(job_name);
    if (!job) {
        return false;
    }
    lock_guard<std::mutex> guard(job->mutex);
    job->progress.paused = true;
    job->progress.pause_requested = true;
    job->cv.notify_all();
    return true;
}

bool NatsSubscribeManager::ResumeJob(const string &job_name) {
    auto job = GetJob(job_name);
    if (!job) {
        return false;
    }
    lock_guard<std::mutex> guard(job->mutex);
    job->progress.paused = false;
    job->progress.pause_requested = false;
    job->cv.notify_all();
    return true;
}

bool NatsSubscribeManager::StopJob(const string &job_name) {
    auto job = GetJob(job_name);
    if (!job) {
        return false;
    }
    lock_guard<std::mutex> guard(job->mutex);
    job->progress.stop_requested = true;
    job->cv.notify_all();
    return true;
}

bool NatsSubscribeManager::RemoveJob(const string &job_name) {
    lock_guard<std::mutex> guard(mutex_);
    return jobs_.erase(job_name) > 0;
}

struct NatsSubscribeSnapshot {
    string job_name;
    string target_table;
    string nats_url;
    string subject;
    string queue_group;
    bool running = false;
    bool paused = false;
    bool pause_requested = false;
    bool stop_requested = false;
    bool failed = false;
    bool create_target_table = false;
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

struct NatsSubscribeBindData : public TableFunctionData {
    NatsSubscribeConfig config;
};

struct NatsSubscribeJobNameBindData : public TableFunctionData {
    string job_name;
};

struct NatsSubscribeJobsBindData : public TableFunctionData {
};

struct NatsSubscribePauseResumeBindData : public TableFunctionData {
    string job_name;
};

struct NatsSubscribeControlGlobalState : public GlobalTableFunctionState {
    bool done = false;
    idx_t row_idx = 0;
    vector<shared_ptr<NatsSubscribeJobState>> jobs;

    idx_t MaxThreads() const override {
        return 1;
    }
};

static void AddSubscribeSnapshotColumns(vector<LogicalType> &return_types, vector<string> &names) {
    return_types = {LogicalType(LogicalTypeId::VARCHAR),  LogicalType(LogicalTypeId::VARCHAR),
                    LogicalType(LogicalTypeId::VARCHAR),  LogicalType(LogicalTypeId::VARCHAR),
                    LogicalType(LogicalTypeId::VARCHAR),  LogicalType(LogicalTypeId::BOOLEAN),
                    LogicalType(LogicalTypeId::BOOLEAN),   LogicalType(LogicalTypeId::BOOLEAN),
                    LogicalType(LogicalTypeId::BOOLEAN),   LogicalType(LogicalTypeId::BOOLEAN),
                    LogicalType(LogicalTypeId::UBIGINT),    LogicalType(LogicalTypeId::UBIGINT),
                    LogicalType(LogicalTypeId::UBIGINT),    LogicalType(LogicalTypeId::UBIGINT),
                    LogicalType(LogicalTypeId::UBIGINT),    LogicalType(LogicalTypeId::UBIGINT),
                    LogicalType(LogicalTypeId::UBIGINT),    LogicalType(LogicalTypeId::UBIGINT),
                    LogicalType(LogicalTypeId::TIMESTAMP),  LogicalType(LogicalTypeId::TIMESTAMP),
                    LogicalType(LogicalTypeId::TIMESTAMP),  LogicalType(LogicalTypeId::TIMESTAMP),
                    LogicalType(LogicalTypeId::VARCHAR),    LogicalType(LogicalTypeId::BOOLEAN),
                    LogicalType(LogicalTypeId::BOOLEAN),     LogicalType(LogicalTypeId::UBIGINT),
                    LogicalType(LogicalTypeId::TIMESTAMP)};
    names = {"job_name",         "target_table",     "nats_url",        "subject",       "queue_group",
             "running",          "paused",           "pause_requested", "stop_requested", "failed",
             "rows_inserted",    "batches_committed", "pending_messages", "pending_bytes",
             "max_pending_messages", "max_pending_bytes", "messages_delivered", "messages_dropped",
             "last_start_time", "last_commit_time", "last_error_time", "last_message_time", "last_error",
             "connected", "reconnecting", "reconnect_count", "last_reconnect_time"};
}

static void FillSubscribeSnapshotColumns(DataChunk &output, idx_t row, const NatsSubscribeSnapshot &snapshot) {
    output.SetValue(0, row, Value(snapshot.job_name));
    output.SetValue(1, row, Value(snapshot.target_table));
    output.SetValue(2, row, Value(snapshot.nats_url));
    output.SetValue(3, row, Value(snapshot.subject));
    output.SetValue(4, row, Value(snapshot.queue_group));
    output.SetValue(5, row, Value(snapshot.running));
    output.SetValue(6, row, Value(snapshot.paused));
    output.SetValue(7, row, Value(snapshot.pause_requested));
    output.SetValue(8, row, Value(snapshot.stop_requested));
    output.SetValue(9, row, Value(snapshot.failed));
    output.SetValue(10, row, Value::UBIGINT(snapshot.rows_inserted));
    output.SetValue(11, row, Value::UBIGINT(snapshot.batches_committed));
    output.SetValue(12, row, Value::UBIGINT(snapshot.pending_messages));
    output.SetValue(13, row, Value::UBIGINT(snapshot.pending_bytes));
    output.SetValue(14, row, Value::UBIGINT(snapshot.max_pending_messages));
    output.SetValue(15, row, Value::UBIGINT(snapshot.max_pending_bytes));
    output.SetValue(16, row, Value::UBIGINT(snapshot.messages_delivered));
    output.SetValue(17, row, Value::UBIGINT(snapshot.messages_dropped));
    output.SetValue(18, row, Value::TIMESTAMP(snapshot.last_start_time));
    output.SetValue(19, row, Value::TIMESTAMP(snapshot.last_commit_time));
    output.SetValue(20, row, Value::TIMESTAMP(snapshot.last_error_time));
    output.SetValue(21, row, Value::TIMESTAMP(snapshot.last_message_time));
    output.SetValue(22, row, Value(snapshot.last_error));
    output.SetValue(23, row, Value(snapshot.connected));
    output.SetValue(24, row, Value(snapshot.reconnecting));
    output.SetValue(25, row, Value::UBIGINT(snapshot.reconnect_count));
    if (snapshot.last_reconnect_time.value == 0) {
        FlatVector::SetNull(output.data[26], row, true);
    } else {
        output.SetValue(26, row, Value::TIMESTAMP(snapshot.last_reconnect_time));
    }
}

static NatsSubscribeSnapshot SnapshotJob(const shared_ptr<NatsSubscribeJobState> &job) {
    lock_guard<std::mutex> guard(job->mutex);
    NatsSubscribeSnapshot snapshot;
    snapshot.job_name = job->config.job_name;
    snapshot.target_table = job->config.target_table;
    snapshot.nats_url = job->config.connection.url;
    snapshot.subject = job->config.subject;
    snapshot.queue_group = job->config.queue_group;
    snapshot.running = job->progress.running;
    snapshot.paused = job->progress.paused;
    snapshot.pause_requested = job->progress.pause_requested;
    snapshot.stop_requested = job->progress.stop_requested;
    snapshot.failed = job->progress.failed;
    snapshot.create_target_table = job->config.create_target_table;
    snapshot.rows_inserted = job->progress.rows_inserted;
    snapshot.batches_committed = job->progress.batches_committed;
    snapshot.pending_messages = job->progress.pending_messages;
    snapshot.pending_bytes = job->progress.pending_bytes;
    snapshot.max_pending_messages = job->progress.max_pending_messages;
    snapshot.max_pending_bytes = job->progress.max_pending_bytes;
    snapshot.messages_delivered = job->progress.messages_delivered;
    snapshot.messages_dropped = job->progress.messages_dropped;
    snapshot.connected = job->progress.connected;
    snapshot.reconnecting = job->progress.reconnecting;
    snapshot.reconnect_count = job->progress.reconnect_count;
    snapshot.last_error = job->progress.last_error;
    snapshot.last_start_time = job->progress.last_start_time;
    snapshot.last_commit_time = job->progress.last_commit_time;
    snapshot.last_error_time = job->progress.last_error_time;
    snapshot.last_message_time = job->progress.last_message_time;
    snapshot.last_reconnect_time = job->progress.last_reconnect_time;
    return snapshot;
}

static string GetNamedString(const case_insensitive_map_t<Value> &named_parameters, const string &name,
                             const string &default_value = "") {
    auto entry = named_parameters.find(name);
    if (entry == named_parameters.end() || entry->second.IsNull()) {
        return default_value;
    }
    return StringValue::Get(entry->second);
}

static uint64_t GetNamedUBigInt(const case_insensitive_map_t<Value> &named_parameters, const string &name,
                                uint64_t default_value) {
    auto entry = named_parameters.find(name);
    if (entry == named_parameters.end() || entry->second.IsNull()) {
        return default_value;
    }
    return UBigIntValue::Get(entry->second);
}

static int64_t GetNamedBigInt(const case_insensitive_map_t<Value> &named_parameters, const string &name,
                              int64_t default_value) {
    auto entry = named_parameters.find(name);
    if (entry == named_parameters.end() || entry->second.IsNull()) {
        return default_value;
    }
    return BigIntValue::Get(entry->second);
}

static bool GetNamedBool(const case_insensitive_map_t<Value> &named_parameters, const string &name,
                         bool default_value) {
    auto entry = named_parameters.find(name);
    if (entry == named_parameters.end() || entry->second.IsNull()) {
        return default_value;
    }
    return BooleanValue::Get(entry->second);
}

static vector<string> GetNamedStringList(const case_insensitive_map_t<Value> &named_parameters, const string &name) {
    vector<string> result;
    auto entry = named_parameters.find(name);
    if (entry == named_parameters.end() || entry->second.IsNull()) {
        return result;
    }
    auto &list = ListValue::GetChildren(entry->second);
    for (auto &child : list) {
        result.push_back(StringValue::Get(child));
    }
    return result;
}

static NatsSubscribeConfig ParseSubscribeConfig(TableFunctionBindInput &input) {
    NatsSubscribeConfig config;
    bool has_job_name = false;
    bool has_target_table = false;
    bool has_subject = false;
    for (auto &kv : input.named_parameters) {
        if (kv.first == "job_name") {
            config.job_name = StringValue::Get(kv.second);
            has_job_name = true;
        } else if (kv.first == "target_table") {
            config.target_table = StringValue::Get(kv.second);
            has_target_table = true;
        } else if (ParseNatsConnectionParameter(config.connection, kv.first, kv.second)) {
        } else if (kv.first == "subject") {
            config.subject = StringValue::Get(kv.second);
            has_subject = !config.subject.empty();
        } else if (kv.first == "queue_group") {
            config.queue_group = StringValue::Get(kv.second);
        } else if (kv.first == "batch_size") {
            config.batch_size = UBigIntValue::Get(kv.second);
        } else if (kv.first == "poll_ms") {
            config.poll_ms = BigIntValue::Get(kv.second);
        } else if (kv.first == "pending_message_limit") {
            auto value = BigIntValue::Get(kv.second);
            if (value < -1 || value == 0 || value > std::numeric_limits<int>::max()) {
                throw std::runtime_error("pending_message_limit must be -1 or between 1 and 2147483647");
            }
            config.pending_message_limit = static_cast<int>(value);
        } else if (kv.first == "pending_bytes_limit") {
            auto value = BigIntValue::Get(kv.second);
            if (value < -1 || value == 0 || value > std::numeric_limits<int>::max()) {
                throw std::runtime_error("pending_bytes_limit must be -1 or between 1 and 2147483647");
            }
            config.pending_bytes_limit = static_cast<int>(value);
        } else if (kv.first == "create_target_table") {
            config.create_target_table = BooleanValue::Get(kv.second);
        } else if (kv.first == "subject_column") {
            config.subject_column = StringValue::Get(kv.second);
        } else if (kv.first == "payload_column") {
            config.payload_column = StringValue::Get(kv.second);
        } else if (kv.first == "json_extract") {
            config.json_fields = GetNamedStringList(input.named_parameters, "json_extract");
        } else if (kv.first == "msgpack_extract") {
            config.msgpack_fields = GetNamedStringList(input.named_parameters, "msgpack_extract");
        } else if (kv.first == "proto_file") {
            config.proto_file = StringValue::Get(kv.second);
        } else if (kv.first == "proto_message") {
            config.proto_message = StringValue::Get(kv.second);
        } else if (kv.first == "proto_extract") {
            config.proto_fields = GetNamedStringList(input.named_parameters, "proto_extract");
        }
    }
    if (!has_job_name) {
        throw std::runtime_error("job_name parameter is required");
    }
    if (!has_target_table) {
        throw std::runtime_error("target_table parameter is required");
    }
    if (!has_subject) {
        throw std::runtime_error("subject parameter is required");
    }
    if (!config.proto_fields.empty()) {
        throw std::runtime_error("core NATS live subscriptions currently support JSON and MessagePack extraction only");
    }
    if (!config.json_fields.empty() && !config.msgpack_fields.empty()) {
        throw std::runtime_error("Cannot combine json_extract with msgpack_extract");
    }
    if (!config.msgpack_fields.empty()) {
        config.msgpack_field_paths = SplitMsgpackFieldPaths(config.msgpack_fields);
    }
    if (config.batch_size == 0 || config.batch_size > 65536) {
        throw std::runtime_error("batch_size must be between 1 and 65536");
    }
    if (config.poll_ms < 1) {
        throw std::runtime_error("poll_ms must be at least 1");
    }
    if (config.subject_column.empty()) {
        throw std::runtime_error("subject_column must not be empty");
    }
    if (config.payload_column.empty()) {
        throw std::runtime_error("payload_column must not be empty");
    }
    ValidateNatsConnectionConfig(config.connection);
    return config;
}

static string QuoteIdentifier(const string &identifier) {
    string result = "\"";
    for (char ch : identifier) {
        result += ch == '"' ? "\"\"" : string(1, ch);
    }
    result += "\"";
    return result;
}

static string QuoteQualifiedTableName(const QualifiedName &name) {
    std::ostringstream result;
    if (!name.catalog.empty()) {
        result << QuoteIdentifier(name.catalog) << ".";
    }
    if (!name.schema.empty()) {
        result << QuoteIdentifier(name.schema) << ".";
    }
    result << QuoteIdentifier(name.name);
    return result.str();
}

static void EnsureTargetTable(Connection &conn, const NatsSubscribeConfig &config) {
    if (!config.create_target_table) {
        return;
    }
    auto qualified_name = QualifiedName::Parse(config.target_table);
    std::ostringstream sql;
    sql << "CREATE TABLE IF NOT EXISTS " << QuoteQualifiedTableName(qualified_name) << " ("
        << QuoteIdentifier(config.subject_column) << " VARCHAR,"
        << QuoteIdentifier(config.payload_column) << " BLOB,"
        << "received_at TIMESTAMP";
    const auto &extracted_fields = config.json_fields.empty() ? config.msgpack_fields : config.json_fields;
    for (const auto &field : extracted_fields) {
        sql << "," << QuoteIdentifier(field) << " VARCHAR";
    }
    sql << ")";
    auto result = conn.Query(sql.str());
    if (result->HasError()) {
        throw std::runtime_error("Failed to create subscription target table: " + result->GetError());
    }
}

static void DisconnectNats(natsConnection **conn) {
    if (conn && *conn) {
        natsConnection_Destroy(*conn);
        *conn = nullptr;
    }
}

static unique_ptr<Appender> CreateSubscribeAppender(Connection &conn, const string &target_table) {
    auto qualified_name = QualifiedName::Parse(target_table);
    if (!qualified_name.catalog.empty()) {
        return make_uniq<Appender>(conn, qualified_name.catalog, qualified_name.schema, qualified_name.name);
    }
    if (!qualified_name.schema.empty()) {
        return make_uniq<Appender>(conn, qualified_name.schema, qualified_name.name);
    }
    return make_uniq<Appender>(conn, qualified_name.name);
}

static void FlushSubscribeBatch(Connection &conn, const NatsSubscribeConfig &config,
                                const vector<pair<string, string>> &batch) {
    if (batch.empty()) {
        return;
    }
    auto appender = CreateSubscribeAppender(conn, config.target_table);
    const auto &types = appender->GetActiveTypes();
    const auto &extracted_fields = config.json_fields.empty() ? config.msgpack_fields : config.json_fields;
    if (types.size() != 3 + extracted_fields.size() || types[0].id() != LogicalTypeId::VARCHAR ||
        (types[1].id() != LogicalTypeId::VARCHAR && types[1].id() != LogicalTypeId::BLOB) ||
        types[2].id() != LogicalTypeId::TIMESTAMP) {
        throw std::runtime_error("Subscription target table must have subject, payload, received_at, and the "
                                 "configured extraction columns");
    }
    for (idx_t i = 0; i < extracted_fields.size(); i++) {
        if (types[3 + i].id() != LogicalTypeId::VARCHAR) {
            throw std::runtime_error("MessagePack extraction columns must be VARCHAR");
        }
    }
    if (!extracted_fields.empty()) {
        vector<idx_t> output_columns;
        output_columns.reserve(extracted_fields.size());
        for (idx_t i = 0; i < extracted_fields.size(); i++) {
            output_columns.push_back(3 + i);
        }
        DataChunk chunk;
        chunk.Initialize(Allocator::Get(*conn.context), types, batch.size());
        for (idx_t row = 0; row < batch.size(); row++) {
            const auto &entry = batch[row];
            chunk.SetValue(0, row, Value(entry.first));
            chunk.SetValue(1, row, Value::BLOB_RAW(entry.second));
            chunk.SetValue(2, row, Value::TIMESTAMP(Timestamp::GetCurrentTimestamp()));
            NatsPayloadView payload {entry.second.data(), entry.second.size()};
            if (!config.json_fields.empty()) {
                DecodeJsonFieldsToChunk(chunk, row, 3, payload, config.json_fields);
            } else {
                DecodeMsgpackFieldPathsToChunk(chunk, row, output_columns, payload, config.msgpack_field_paths);
            }
        }
        chunk.SetCardinality(batch.size());
        appender->AppendDataChunk(chunk);
        appender->Flush();
        return;
    }
    for (auto &entry : batch) {
        auto payload = types[1].id() == LogicalTypeId::BLOB ? Value::BLOB_RAW(entry.second) : Value(entry.second);
        appender->AppendRow(Value(entry.first), payload, Value::TIMESTAMP(Timestamp::GetCurrentTimestamp()));
    }
    appender->Flush();
}

static void RefreshSubscribeMetrics(const shared_ptr<NatsSubscribeJobState> &job) {
    int pending_messages = 0;
    int pending_bytes = 0;
    int max_pending_messages = 0;
    int max_pending_bytes = 0;
    int64_t messages_delivered = 0;
    int64_t messages_dropped = 0;
    auto status = natsSubscription_GetStats(job->sub, &pending_messages, &pending_bytes, &max_pending_messages,
                                            &max_pending_bytes, &messages_delivered, &messages_dropped);
    if (status != NATS_OK) {
        return;
    }
    lock_guard<std::mutex> guard(job->mutex);
    job->progress.pending_messages = static_cast<uint64_t>(std::max(pending_messages, 0));
    job->progress.pending_bytes = static_cast<uint64_t>(std::max(pending_bytes, 0));
    job->progress.max_pending_messages = static_cast<uint64_t>(std::max(max_pending_messages, 0));
    job->progress.max_pending_bytes = static_cast<uint64_t>(std::max(max_pending_bytes, 0));
    job->progress.messages_delivered = static_cast<uint64_t>(std::max<int64_t>(messages_delivered, 0));
    job->progress.messages_dropped = static_cast<uint64_t>(std::max<int64_t>(messages_dropped, 0));
}

static void SubscribeDisconnected(natsConnection *, void *closure) {
    auto *job = static_cast<NatsSubscribeJobState *>(closure);
    lock_guard<std::mutex> guard(job->mutex);
    job->progress.connected = false;
    job->progress.reconnecting = true;
    job->progress.resubscribe_requested = true;
    job->cv.notify_all();
}

static void SubscribeReconnected(natsConnection *, void *closure) {
    auto *job = static_cast<NatsSubscribeJobState *>(closure);
    lock_guard<std::mutex> guard(job->mutex);
    job->progress.connected = true;
    job->progress.reconnecting = false;
    job->progress.reconnect_count++;
    job->progress.last_reconnect_time = Timestamp::GetCurrentTimestamp();
    job->cv.notify_all();
}

static void SubscribeClosed(natsConnection *, void *closure) {
    auto *job = static_cast<NatsSubscribeJobState *>(closure);
    lock_guard<std::mutex> guard(job->mutex);
    job->progress.connected = false;
    job->progress.reconnecting = false;
    if (!job->progress.stop_requested) {
        job->progress.last_error = "NATS connection closed after reconnect attempts";
        job->progress.last_error_time = Timestamp::GetCurrentTimestamp();
    }
    job->cv.notify_all();
}

static bool IsTransientNatsStatus(natsStatus status) {
    return status == NATS_CONNECTION_DISCONNECTED || status == NATS_IO_ERROR ||
           status == NATS_STALE_CONNECTION || status == NATS_NOT_YET_CONNECTED;
}

static void CreateSubscribeSubscription(const shared_ptr<NatsSubscribeJobState> &job) {
    natsConnection *conn = nullptr;
    {
        lock_guard<std::mutex> guard(job->mutex);
        conn = job->conn;
    }
    if (conn == nullptr || natsConnection_IsClosed(conn)) {
        throw std::runtime_error("Cannot create NATS subscription while connection is closed");
    }

    natsSubscription *sub = nullptr;
    natsStatus status = NATS_OK;
    if (!job->config.queue_group.empty()) {
        status = natsConnection_QueueSubscribeSync(&sub, conn, job->config.subject.c_str(),
                                                   job->config.queue_group.c_str());
    } else {
        status = natsConnection_SubscribeSync(&sub, conn, job->config.subject.c_str());
    }
    if (status != NATS_OK) {
        throw std::runtime_error(std::string("Failed to create NATS subscription: ") + natsStatus_GetText(status));
    }
    status = natsSubscription_SetPendingLimits(sub, job->config.pending_message_limit,
                                                job->config.pending_bytes_limit);
    if (status != NATS_OK) {
        natsSubscription_Destroy(sub);
        throw std::runtime_error(std::string("Failed to set NATS subscription pending limits: ") +
                                 natsStatus_GetText(status));
    }
    status = natsConnection_FlushTimeout(conn, 5000);
    if (status != NATS_OK) {
        natsSubscription_Destroy(sub);
        throw std::runtime_error(std::string("Failed to flush NATS subscription: ") + natsStatus_GetText(status));
    }
    {
        lock_guard<std::mutex> guard(job->mutex);
        job->sub = sub;
        job->progress.resubscribe_requested = false;
    }
}

static void RunSubscribeWorker(const shared_ptr<NatsSubscribeJobState> &job) {
    try {
        Connection conn(*job->db);
        EnsureTargetTable(conn, job->config);
        NatsConnectionCallbacks callbacks;
        callbacks.disconnected = SubscribeDisconnected;
        callbacks.reconnected = SubscribeReconnected;
        callbacks.closed = SubscribeClosed;
        callbacks.closure = job.get();
        ConnectNats(job->config.connection, &job->conn, 20, &callbacks);

        CreateSubscribeSubscription(job);

        {
            lock_guard<std::mutex> guard(job->mutex);
            job->progress.running = true;
            job->progress.connected = true;
            job->progress.reconnecting = false;
            job->progress.last_start_time = Timestamp::GetCurrentTimestamp();
        }
        job->cv.notify_all();

        vector<pair<string, string>> batch;
        batch.reserve(job->config.batch_size);
        natsStatus s = NATS_OK;

        while (true) {
            bool resubscribe_requested = false;
            {
                unique_lock<std::mutex> lock(job->mutex);
                if (job->progress.stop_requested) {
                    break;
                }
                resubscribe_requested = job->progress.resubscribe_requested;
                if (job->progress.paused) {
                    lock.unlock();
                    RefreshSubscribeMetrics(job);
                    lock.lock();
                    job->cv.wait_for(lock, std::chrono::milliseconds(job->config.poll_ms), [&]() {
                        return !job->progress.paused || job->progress.stop_requested;
                    });
                    if (job->progress.stop_requested) {
                        break;
                    }
                    if (job->progress.paused) {
                        continue;
                    }
                }
            }

            if (resubscribe_requested) {
                natsSubscription *old_sub = nullptr;
                {
                    lock_guard<std::mutex> guard(job->mutex);
                    old_sub = job->sub;
                    job->sub = nullptr;
                }
                if (old_sub != nullptr) {
                    natsSubscription_Destroy(old_sub);
                }
                CreateSubscribeSubscription(job);
                continue;
            }

            natsMsg *msg = nullptr;
            s = natsSubscription_NextMsg(&msg, job->sub, job->config.poll_ms);
            RefreshSubscribeMetrics(job);
            if (s == NATS_TIMEOUT) {
                if (!batch.empty()) {
                    FlushSubscribeBatch(conn, job->config, batch);
                    {
                        lock_guard<std::mutex> guard(job->mutex);
                        job->progress.batches_committed++;
                        job->progress.rows_inserted += batch.size();
                        job->progress.last_commit_time = Timestamp::GetCurrentTimestamp();
                    }
                    job->cv.notify_all();
                    batch.clear();
                }
                continue;
            }
            if (s == NATS_SLOW_CONSUMER) {
                continue;
            }
            if (IsTransientNatsStatus(s) && job->conn != nullptr && !natsConnection_IsClosed(job->conn)) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                continue;
            }
            if (s != NATS_OK) {
                throw std::runtime_error(std::string("Failed to receive NATS message: ") + natsStatus_GetText(s));
            }

            string subject = natsMsg_GetSubject(msg);
            string payload;
            const char *data = static_cast<const char *>(natsMsg_GetData(msg));
            int len = natsMsg_GetDataLength(msg);
            if (data && len > 0) {
                payload.assign(data, data + len);
            }
            natsMsg_Destroy(msg);

            {
                lock_guard<std::mutex> guard(job->mutex);
                job->progress.last_message_time = Timestamp::GetCurrentTimestamp();
            }

            batch.emplace_back(std::move(subject), std::move(payload));
            if (batch.size() >= job->config.batch_size) {
                FlushSubscribeBatch(conn, job->config, batch);
                {
                    lock_guard<std::mutex> guard(job->mutex);
                    job->progress.batches_committed++;
                    job->progress.rows_inserted += batch.size();
                    job->progress.last_commit_time = Timestamp::GetCurrentTimestamp();
                }
                job->cv.notify_all();
                batch.clear();
            }
        }

        if (!batch.empty()) {
            FlushSubscribeBatch(conn, job->config, batch);
            lock_guard<std::mutex> guard(job->mutex);
            job->progress.batches_committed++;
            job->progress.rows_inserted += batch.size();
            job->progress.last_commit_time = Timestamp::GetCurrentTimestamp();
        }
        RefreshSubscribeMetrics(job);

        {
            lock_guard<std::mutex> guard(job->mutex);
            job->progress.running = false;
            job->progress.connected = false;
            job->progress.reconnecting = false;
        }
    } catch (std::exception &ex) {
        lock_guard<std::mutex> guard(job->mutex);
        job->progress.running = false;
        job->progress.failed = true;
        job->progress.last_error = ex.what();
        job->progress.last_error_time = Timestamp::GetCurrentTimestamp();
    }
    if (job->sub != nullptr) {
        natsSubscription_Destroy(job->sub);
        job->sub = nullptr;
    }
    DisconnectNats(&job->conn);
    job->cv.notify_all();
}

static unique_ptr<FunctionData> NatsSubscribeStartBind(ClientContext &, TableFunctionBindInput &input,
                                                       vector<LogicalType> &return_types, vector<string> &names) {
    auto config = ParseSubscribeConfig(input);
    AddSubscribeSnapshotColumns(return_types, names);
    auto bind_data = make_uniq<NatsSubscribeBindData>();
    bind_data->config = std::move(config);
    return bind_data;
}

static unique_ptr<GlobalTableFunctionState> NatsSubscribeStartInitGlobal(ClientContext &context,
                                                                         TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<NatsSubscribeBindData>();
    auto job = NatsSubscribeManager::Get().CreateJob(bind_data.config);
    job->db = context.db;
    job->worker = std::thread(RunSubscribeWorker, job);

    auto state = make_uniq<NatsSubscribeControlGlobalState>();
    state->jobs.push_back(job);
    return state;
}

static void NatsSubscribeStartExecute(ClientContext &, TableFunctionInput &data_p, DataChunk &output) {
    auto &state = data_p.global_state->Cast<NatsSubscribeControlGlobalState>();
    if (state.done) {
        output.SetCardinality(0);
        return;
    }
    auto snapshot = SnapshotJob(state.jobs[0]);
    FillSubscribeSnapshotColumns(output, 0, snapshot);
    output.SetCardinality(1);
    state.done = true;
}

static unique_ptr<FunctionData> NatsSubscribeStopBind(ClientContext &, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<string> &names) {
    string job_name;
    bool has_job_name = false;
    for (auto &kv : input.named_parameters) {
        if (kv.first == "job_name") {
            job_name = StringValue::Get(kv.second);
            has_job_name = true;
        }
    }
    if (!has_job_name) {
        throw std::runtime_error("job_name parameter is required");
    }
    AddSubscribeSnapshotColumns(return_types, names);
    auto bind_data = make_uniq<NatsSubscribeJobNameBindData>();
    bind_data->job_name = std::move(job_name);
    return bind_data;
}

static unique_ptr<GlobalTableFunctionState> NatsSubscribeStopInitGlobal(ClientContext &, TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<NatsSubscribeJobNameBindData>();
    auto job = NatsSubscribeManager::Get().GetJob(bind_data.job_name);
    if (!job) {
        throw std::runtime_error("Subscribe job '" + bind_data.job_name + "' does not exist");
    }
    NatsSubscribeManager::Get().StopJob(bind_data.job_name);
    auto state = make_uniq<NatsSubscribeControlGlobalState>();
    state->jobs.push_back(job);
    return state;
}

static void NatsSubscribeStopExecute(ClientContext &, TableFunctionInput &data_p, DataChunk &output) {
    auto &state = data_p.global_state->Cast<NatsSubscribeControlGlobalState>();
    if (state.done) {
        output.SetCardinality(0);
        return;
    }
    auto snapshot = SnapshotJob(state.jobs[0]);
    FillSubscribeSnapshotColumns(output, 0, snapshot);
    output.SetCardinality(1);
    state.done = true;
}

static unique_ptr<FunctionData> NatsSubscribeStatusBind(ClientContext &, TableFunctionBindInput &input,
                                                        vector<LogicalType> &return_types, vector<string> &names) {
    string job_name;
    bool has_job_name = false;
    for (auto &kv : input.named_parameters) {
        if (kv.first == "job_name") {
            job_name = StringValue::Get(kv.second);
            has_job_name = true;
        }
    }
    if (!has_job_name) {
        throw std::runtime_error("job_name parameter is required");
    }
    AddSubscribeSnapshotColumns(return_types, names);
    auto bind_data = make_uniq<NatsSubscribeJobNameBindData>();
    bind_data->job_name = std::move(job_name);
    return bind_data;
}

static unique_ptr<FunctionData> NatsSubscribePauseBind(ClientContext &, TableFunctionBindInput &input,
                                                       vector<LogicalType> &return_types, vector<string> &names) {
    string job_name;
    bool has_job_name = false;
    for (auto &kv : input.named_parameters) {
        if (kv.first == "job_name") {
            job_name = StringValue::Get(kv.second);
            has_job_name = true;
        }
    }
    if (!has_job_name) {
        throw std::runtime_error("job_name parameter is required");
    }
    AddSubscribeSnapshotColumns(return_types, names);
    auto bind_data = make_uniq<NatsSubscribePauseResumeBindData>();
    bind_data->job_name = std::move(job_name);
    return bind_data;
}

static unique_ptr<GlobalTableFunctionState> NatsSubscribePauseInitGlobal(ClientContext &, TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<NatsSubscribePauseResumeBindData>();
    auto job = NatsSubscribeManager::Get().GetJob(bind_data.job_name);
    if (!job) {
        throw std::runtime_error("Subscribe job '" + bind_data.job_name + "' does not exist or is not running");
    }
    {
        lock_guard<std::mutex> guard(job->mutex);
        if (!job->progress.running || job->progress.stop_requested || job->progress.failed) {
            throw std::runtime_error("Subscribe job '" + bind_data.job_name + "' is not running");
        }
    }
    NatsSubscribeManager::Get().PauseJob(bind_data.job_name);
    auto state = make_uniq<NatsSubscribeControlGlobalState>();
    state->jobs.push_back(job);
    return state;
}

static void NatsSubscribePauseExecute(ClientContext &, TableFunctionInput &data_p, DataChunk &output) {
    auto &state = data_p.global_state->Cast<NatsSubscribeControlGlobalState>();
    if (state.done) {
        output.SetCardinality(0);
        return;
    }
    auto job = state.jobs[0];
    {
        unique_lock<std::mutex> lock(job->mutex);
        if (!job->cv.wait_for(lock, std::chrono::seconds(30), [&]() {
                return job->progress.paused || job->progress.failed || job->progress.stop_requested;
            })) {
            throw std::runtime_error("Timed out waiting for subscribe job '" + job->config.job_name + "' to pause");
        }
        if (!job->progress.paused && !job->progress.failed && !job->progress.stop_requested) {
            throw std::runtime_error("Subscribe job '" + job->config.job_name + "' did not pause");
        }
    }
    auto snapshot = SnapshotJob(job);
    FillSubscribeSnapshotColumns(output, 0, snapshot);
    output.SetCardinality(1);
    state.done = true;
}

static unique_ptr<FunctionData> NatsSubscribeResumeBind(ClientContext &, TableFunctionBindInput &input,
                                                        vector<LogicalType> &return_types, vector<string> &names) {
    string job_name;
    bool has_job_name = false;
    for (auto &kv : input.named_parameters) {
        if (kv.first == "job_name") {
            job_name = StringValue::Get(kv.second);
            has_job_name = true;
        }
    }
    if (!has_job_name) {
        throw std::runtime_error("job_name parameter is required");
    }
    AddSubscribeSnapshotColumns(return_types, names);
    auto bind_data = make_uniq<NatsSubscribePauseResumeBindData>();
    bind_data->job_name = std::move(job_name);
    return bind_data;
}

static unique_ptr<GlobalTableFunctionState> NatsSubscribeResumeInitGlobal(ClientContext &, TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<NatsSubscribePauseResumeBindData>();
    auto job = NatsSubscribeManager::Get().GetJob(bind_data.job_name);
    if (!job) {
        throw std::runtime_error("Subscribe job '" + bind_data.job_name + "' does not exist or is not running");
    }
    {
        lock_guard<std::mutex> guard(job->mutex);
        if (!job->progress.running || job->progress.stop_requested || job->progress.failed) {
            throw std::runtime_error("Subscribe job '" + bind_data.job_name + "' is not running");
        }
    }
    NatsSubscribeManager::Get().ResumeJob(bind_data.job_name);
    auto state = make_uniq<NatsSubscribeControlGlobalState>();
    state->jobs.push_back(job);
    return state;
}

static void NatsSubscribeResumeExecute(ClientContext &, TableFunctionInput &data_p, DataChunk &output) {
    auto &state = data_p.global_state->Cast<NatsSubscribeControlGlobalState>();
    if (state.done) {
        output.SetCardinality(0);
        return;
    }
    auto job = state.jobs[0];
    {
        unique_lock<std::mutex> lock(job->mutex);
        if (!job->cv.wait_for(lock, std::chrono::seconds(30), [&]() {
                return !job->progress.paused || job->progress.failed || job->progress.stop_requested;
            })) {
            throw std::runtime_error("Timed out waiting for subscribe job '" + job->config.job_name + "' to resume");
        }
        if (job->progress.paused && !job->progress.failed && !job->progress.stop_requested) {
            throw std::runtime_error("Subscribe job '" + job->config.job_name + "' did not resume");
        }
    }
    auto snapshot = SnapshotJob(job);
    FillSubscribeSnapshotColumns(output, 0, snapshot);
    output.SetCardinality(1);
    state.done = true;
}

static unique_ptr<GlobalTableFunctionState> NatsSubscribeStatusInitGlobal(ClientContext &, TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<NatsSubscribeJobNameBindData>();
    auto job = NatsSubscribeManager::Get().GetJob(bind_data.job_name);
    if (!job) {
        throw std::runtime_error("Subscribe job '" + bind_data.job_name + "' does not exist");
    }
    auto state = make_uniq<NatsSubscribeControlGlobalState>();
    state->jobs.push_back(job);
    return state;
}

static void NatsSubscribeStatusExecute(ClientContext &, TableFunctionInput &data_p, DataChunk &output) {
    auto &state = data_p.global_state->Cast<NatsSubscribeControlGlobalState>();
    if (state.done) {
        output.SetCardinality(0);
        return;
    }
    auto snapshot = SnapshotJob(state.jobs[0]);
    FillSubscribeSnapshotColumns(output, 0, snapshot);
    output.SetCardinality(1);
    state.done = true;
}

static unique_ptr<FunctionData> NatsSubscribeJobsBind(ClientContext &, TableFunctionBindInput &,
                                                      vector<LogicalType> &return_types, vector<string> &names) {
    AddSubscribeSnapshotColumns(return_types, names);
    return make_uniq<NatsSubscribeJobsBindData>();
}

static unique_ptr<GlobalTableFunctionState> NatsSubscribeJobsInitGlobal(ClientContext &, TableFunctionInitInput &) {
    auto state = make_uniq<NatsSubscribeControlGlobalState>();
    state->jobs = NatsSubscribeManager::Get().ListJobs();
    return state;
}

static void NatsSubscribeJobsExecute(ClientContext &, TableFunctionInput &data_p, DataChunk &output) {
    auto &state = data_p.global_state->Cast<NatsSubscribeControlGlobalState>();
    idx_t row = 0;
    while (row < STANDARD_VECTOR_SIZE && state.row_idx < state.jobs.size()) {
        auto snapshot = SnapshotJob(state.jobs[state.row_idx]);
        FillSubscribeSnapshotColumns(output, row, snapshot);
        row++;
        state.row_idx++;
    }
    output.SetCardinality(row);
}

void NatsSubscribeFunction::Register(ExtensionLoader &loader) {
    TableFunction start_fn("nats_start_subscribe", {}, NatsSubscribeStartExecute, NatsSubscribeStartBind,
                           NatsSubscribeStartInitGlobal);
    start_fn.named_parameters["job_name"] = LogicalType(LogicalTypeId::VARCHAR);
    start_fn.named_parameters["target_table"] = LogicalType(LogicalTypeId::VARCHAR);
    RegisterNatsConnectionParameters(start_fn);
    start_fn.named_parameters["subject"] = LogicalType(LogicalTypeId::VARCHAR);
    start_fn.named_parameters["queue_group"] = LogicalType(LogicalTypeId::VARCHAR);
    start_fn.named_parameters["batch_size"] = LogicalType(LogicalTypeId::UBIGINT);
    start_fn.named_parameters["poll_ms"] = LogicalType(LogicalTypeId::BIGINT);
    start_fn.named_parameters["pending_message_limit"] = LogicalType(LogicalTypeId::BIGINT);
    start_fn.named_parameters["pending_bytes_limit"] = LogicalType(LogicalTypeId::BIGINT);
    start_fn.named_parameters["create_target_table"] = LogicalType(LogicalTypeId::BOOLEAN);
    start_fn.named_parameters["subject_column"] = LogicalType(LogicalTypeId::VARCHAR);
    start_fn.named_parameters["payload_column"] = LogicalType(LogicalTypeId::VARCHAR);
    start_fn.named_parameters["json_extract"] = LogicalType::LIST(LogicalType(LogicalTypeId::VARCHAR));
    start_fn.named_parameters["msgpack_extract"] = LogicalType::LIST(LogicalType(LogicalTypeId::VARCHAR));
    start_fn.named_parameters["proto_file"] = LogicalType(LogicalTypeId::VARCHAR);
    start_fn.named_parameters["proto_message"] = LogicalType(LogicalTypeId::VARCHAR);
    start_fn.named_parameters["proto_extract"] = LogicalType::LIST(LogicalType(LogicalTypeId::VARCHAR));
    loader.RegisterFunction(start_fn);

    TableFunction stop_fn("nats_stop_subscribe", {}, NatsSubscribeStopExecute, NatsSubscribeStopBind,
                          NatsSubscribeStopInitGlobal);
    stop_fn.named_parameters["job_name"] = LogicalType(LogicalTypeId::VARCHAR);
    loader.RegisterFunction(stop_fn);

    TableFunction status_fn("nats_subscribe_status", {}, NatsSubscribeStatusExecute, NatsSubscribeStatusBind,
                            NatsSubscribeStatusInitGlobal);
    status_fn.named_parameters["job_name"] = LogicalType(LogicalTypeId::VARCHAR);
    loader.RegisterFunction(status_fn);

    TableFunction pause_fn("nats_pause_subscribe", {}, NatsSubscribePauseExecute, NatsSubscribePauseBind,
                           NatsSubscribePauseInitGlobal);
    pause_fn.named_parameters["job_name"] = LogicalType(LogicalTypeId::VARCHAR);
    loader.RegisterFunction(pause_fn);

    TableFunction resume_fn("nats_resume_subscribe", {}, NatsSubscribeResumeExecute, NatsSubscribeResumeBind,
                            NatsSubscribeResumeInitGlobal);
    resume_fn.named_parameters["job_name"] = LogicalType(LogicalTypeId::VARCHAR);
    loader.RegisterFunction(resume_fn);

    TableFunction jobs_fn("nats_subscribe_jobs", {}, NatsSubscribeJobsExecute, NatsSubscribeJobsBind,
                          NatsSubscribeJobsInitGlobal);
    loader.RegisterFunction(jobs_fn);
}

} // namespace duckdb
