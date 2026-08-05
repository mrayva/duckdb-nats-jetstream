#include "nats_kv_watch.hpp"
#include "nats_duckdb_compat.hpp"
#include "nats_message_decode.hpp"

#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

#include <chrono>
#include <sstream>

namespace duckdb {

namespace {

constexpr int64_t NATS_KV_WATCH_SETUP_TIMEOUT_MS = 5000;

struct NatsKvWatchRow {
    string key;
    string value;
    uint64_t revision = 0;
    int64_t created_ns = 0;
    kvOperation operation = kvOp_Unknown;
};

bool IsTransientNatsStatus(natsStatus status) {
    return status == NATS_CONNECTION_DISCONNECTED || status == NATS_IO_ERROR ||
           status == NATS_STALE_CONNECTION || status == NATS_NOT_YET_CONNECTED;
}

string KvWatchOperationName(kvOperation op) {
    switch (op) {
    case kvOp_Put:
        return "put";
    case kvOp_Delete:
        return "delete";
    case kvOp_Purge:
        return "purge";
    default:
        return "unknown";
    }
}

} // namespace

NatsKvWatchJobState::NatsKvWatchJobState(NatsKvWatchConfig config_p) : config(std::move(config_p)) {
}

NatsKvWatchJobState::~NatsKvWatchJobState() {
    if (worker.joinable()) {
        {
            std::lock_guard<std::mutex> guard(mutex);
            progress.stop_requested = true;
            cv.notify_all();
        }
        if (worker.get_id() != std::this_thread::get_id()) {
            worker.join();
        }
    }
    if (watcher != nullptr) {
        kvWatcher_Destroy(watcher);
        watcher = nullptr;
    }
    if (kv != nullptr) {
        kvStore_Destroy(kv);
        kv = nullptr;
    }
    if (js != nullptr) {
        jsCtx_Destroy(js);
        js = nullptr;
    }
    if (conn != nullptr) {
        natsConnection_Destroy(conn);
        conn = nullptr;
    }
}

NatsKvWatchManager &NatsKvWatchManager::Get() {
    static NatsKvWatchManager instance;
    return instance;
}

shared_ptr<NatsKvWatchJobState> NatsKvWatchManager::CreateJob(NatsKvWatchConfig config) {
    auto job = make_shared_ptr<NatsKvWatchJobState>(std::move(config));
    lock_guard<std::mutex> guard(mutex_);
    auto it = jobs_.find(job->config.job_name);
    if (it != jobs_.end()) {
        bool existing_running;
        {
            lock_guard<std::mutex> job_guard(it->second->mutex);
            existing_running = it->second->progress.running;
        }
        if (existing_running) {
            throw BinderException("kv watch job '%s' already exists", job->config.job_name);
        }
        // A previous job under this name has already stopped or failed: drop it so its
        // connection/watcher/thread are released and the name can be reused.
        jobs_.erase(it);
    }
    jobs_.emplace(job->config.job_name, job);
    return job;
}

shared_ptr<NatsKvWatchJobState> NatsKvWatchManager::GetJob(const string &job_name) {
    lock_guard<std::mutex> guard(mutex_);
    auto entry = jobs_.find(job_name);
    if (entry == jobs_.end()) {
        return nullptr;
    }
    return entry->second;
}

vector<shared_ptr<NatsKvWatchJobState>> NatsKvWatchManager::ListJobs() {
    lock_guard<std::mutex> guard(mutex_);
    vector<shared_ptr<NatsKvWatchJobState>> result;
    result.reserve(jobs_.size());
    for (auto &entry : jobs_) {
        result.push_back(entry.second);
    }
    return result;
}

bool NatsKvWatchManager::PauseJob(const string &job_name) {
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

bool NatsKvWatchManager::ResumeJob(const string &job_name) {
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

bool NatsKvWatchManager::StopJob(const string &job_name) {
    auto job = GetJob(job_name);
    if (!job) {
        return false;
    }
    lock_guard<std::mutex> guard(job->mutex);
    job->progress.stop_requested = true;
    job->cv.notify_all();
    return true;
}

bool NatsKvWatchManager::RemoveJob(const string &job_name) {
    lock_guard<std::mutex> guard(mutex_);
    auto it = jobs_.find(job_name);
    if (it == jobs_.end()) {
        return false;
    }
    {
        lock_guard<std::mutex> job_guard(it->second->mutex);
        if (it->second->progress.running) {
            throw std::runtime_error("KV watch job '" + job_name + "' is still running; stop it before removing");
        }
    }
    jobs_.erase(it);
    return true;
}

namespace {

struct NatsKvWatchSnapshot {
    string job_name;
    string target_table;
    string nats_url;
    string bucket;
    string key_filter;
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

struct NatsKvWatchBindData : public TableFunctionData {
    NatsKvWatchConfig config;
};

struct NatsKvWatchJobNameBindData : public TableFunctionData {
    string job_name;
};

struct NatsKvWatchJobsBindData : public TableFunctionData {
};

struct NatsKvWatchControlGlobalState : public GlobalTableFunctionState {
    bool done = false;
    idx_t row_idx = 0;
    vector<shared_ptr<NatsKvWatchJobState>> jobs;

    idx_t MaxThreads() const override {
        return 1;
    }
};

void AddKvWatchSnapshotColumns(vector<LogicalType> &return_types, vector<string> &names) {
    return_types = {LogicalType(LogicalTypeId::VARCHAR),  LogicalType(LogicalTypeId::VARCHAR),
                    LogicalType(LogicalTypeId::VARCHAR),  LogicalType(LogicalTypeId::VARCHAR),
                    LogicalType(LogicalTypeId::VARCHAR),  LogicalType(LogicalTypeId::BOOLEAN),
                    LogicalType(LogicalTypeId::BOOLEAN),   LogicalType(LogicalTypeId::BOOLEAN),
                    LogicalType(LogicalTypeId::BOOLEAN),   LogicalType(LogicalTypeId::BOOLEAN),
                    LogicalType(LogicalTypeId::UBIGINT),    LogicalType(LogicalTypeId::UBIGINT),
                    LogicalType(LogicalTypeId::UBIGINT),    LogicalType(LogicalTypeId::TIMESTAMP),
                    LogicalType(LogicalTypeId::TIMESTAMP),  LogicalType(LogicalTypeId::TIMESTAMP),
                    LogicalType(LogicalTypeId::TIMESTAMP),  LogicalType(LogicalTypeId::VARCHAR),
                    LogicalType(LogicalTypeId::BOOLEAN),    LogicalType(LogicalTypeId::BOOLEAN),
                    LogicalType(LogicalTypeId::UBIGINT),    LogicalType(LogicalTypeId::TIMESTAMP)};
    names = {"job_name",       "target_table",   "nats_url",       "bucket",          "key_filter",
             "running",        "paused",         "pause_requested", "stop_requested", "failed",
             "rows_inserted",  "batches_committed", "entries_delivered",
             "last_start_time", "last_commit_time", "last_error_time", "last_message_time", "last_error",
             "connected",      "reconnecting",   "reconnect_count", "last_reconnect_time"};
}

void FillKvWatchSnapshotColumns(DataChunk &output, idx_t row, const NatsKvWatchSnapshot &snapshot) {
    output.SetValue(0, row, Value(snapshot.job_name));
    output.SetValue(1, row, Value(snapshot.target_table));
    output.SetValue(2, row, Value(snapshot.nats_url));
    output.SetValue(3, row, Value(snapshot.bucket));
    output.SetValue(4, row, Value(snapshot.key_filter));
    output.SetValue(5, row, Value(snapshot.running));
    output.SetValue(6, row, Value(snapshot.paused));
    output.SetValue(7, row, Value(snapshot.pause_requested));
    output.SetValue(8, row, Value(snapshot.stop_requested));
    output.SetValue(9, row, Value(snapshot.failed));
    output.SetValue(10, row, Value::UBIGINT(snapshot.rows_inserted));
    output.SetValue(11, row, Value::UBIGINT(snapshot.batches_committed));
    output.SetValue(12, row, Value::UBIGINT(snapshot.entries_delivered));
    output.SetValue(13, row, Value::TIMESTAMP(snapshot.last_start_time));
    output.SetValue(14, row, Value::TIMESTAMP(snapshot.last_commit_time));
    output.SetValue(15, row, Value::TIMESTAMP(snapshot.last_error_time));
    output.SetValue(16, row, Value::TIMESTAMP(snapshot.last_message_time));
    output.SetValue(17, row, Value(snapshot.last_error));
    output.SetValue(18, row, Value(snapshot.connected));
    output.SetValue(19, row, Value(snapshot.reconnecting));
    output.SetValue(20, row, Value::UBIGINT(snapshot.reconnect_count));
    if (snapshot.last_reconnect_time.value == 0) {
        FlatVector::SetNull(output.data[21], row, true);
    } else {
        output.SetValue(21, row, Value::TIMESTAMP(snapshot.last_reconnect_time));
    }
}

NatsKvWatchSnapshot SnapshotKvWatchJob(const shared_ptr<NatsKvWatchJobState> &job) {
    lock_guard<std::mutex> guard(job->mutex);
    NatsKvWatchSnapshot snapshot;
    snapshot.job_name = job->config.job_name;
    snapshot.target_table = job->config.target_table;
    snapshot.nats_url = job->config.connection.url;
    snapshot.bucket = job->config.bucket;
    snapshot.key_filter = job->config.key_filter;
    snapshot.running = job->progress.running;
    snapshot.paused = job->progress.paused;
    snapshot.pause_requested = job->progress.pause_requested;
    snapshot.stop_requested = job->progress.stop_requested;
    snapshot.failed = job->progress.failed;
    snapshot.rows_inserted = job->progress.rows_inserted;
    snapshot.batches_committed = job->progress.batches_committed;
    snapshot.entries_delivered = job->progress.entries_delivered;
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

NatsKvWatchConfig ParseKvWatchConfig(TableFunctionBindInput &input) {
    NatsKvWatchConfig config;
    bool has_job_name = false;
    bool has_target_table = false;
    bool has_bucket = false;
    for (auto &kv : input.named_parameters) {
        if (kv.first == "job_name") {
            config.job_name = StringValue::Get(kv.second);
            has_job_name = true;
        } else if (kv.first == "target_table") {
            config.target_table = StringValue::Get(kv.second);
            has_target_table = true;
        } else if (ParseNatsConnectionParameter(config.connection, string(kv.first), kv.second)) {
        } else if (kv.first == "bucket") {
            config.bucket = StringValue::Get(kv.second);
            has_bucket = !config.bucket.empty();
        } else if (kv.first == "key_filter") {
            config.key_filter = StringValue::Get(kv.second);
        } else if (kv.first == "batch_size") {
            config.batch_size = UBigIntValue::Get(kv.second);
        } else if (kv.first == "poll_ms") {
            config.poll_ms = BigIntValue::Get(kv.second);
        } else if (kv.first == "updates_only") {
            config.updates_only = BooleanValue::Get(kv.second);
        } else if (kv.first == "ignore_deletes") {
            config.ignore_deletes = BooleanValue::Get(kv.second);
        } else if (kv.first == "create_target_table") {
            config.create_target_table = BooleanValue::Get(kv.second);
        } else if (kv.first == "key_column") {
            config.key_column = StringValue::Get(kv.second);
        } else if (kv.first == "value_column") {
            config.value_column = StringValue::Get(kv.second);
        }
    }
    if (!has_job_name) {
        throw std::runtime_error("job_name parameter is required");
    }
    if (!has_target_table) {
        throw std::runtime_error("target_table parameter is required");
    }
    if (!has_bucket) {
        throw std::runtime_error("bucket parameter is required");
    }
    if (config.batch_size == 0 || config.batch_size > 65536) {
        throw std::runtime_error("batch_size must be between 1 and 65536");
    }
    if (config.poll_ms < 1) {
        throw std::runtime_error("poll_ms must be at least 1");
    }
    if (config.key_column.empty()) {
        throw std::runtime_error("key_column must not be empty");
    }
    if (config.value_column.empty()) {
        throw std::runtime_error("value_column must not be empty");
    }
    if (StringUtil::CIEquals(config.key_column, config.value_column)) {
        throw std::runtime_error("key_column and value_column must be different");
    }
    ValidateNatsConnectionConfig(config.connection);
    return config;
}

string QuoteKvWatchIdentifier(const string &identifier) {
    string result = "\"";
    for (char ch : identifier) {
        result += ch == '"' ? "\"\"" : string(1, ch);
    }
    result += "\"";
    return result;
}

string QuoteKvWatchQualifiedTableName(const QualifiedName &name) {
    std::ostringstream result;
    if (!NatsQualifiedCatalog(name).empty()) {
        result << QuoteKvWatchIdentifier(string(NatsQualifiedCatalog(name))) << ".";
    }
    if (!NatsQualifiedSchema(name).empty()) {
        result << QuoteKvWatchIdentifier(string(NatsQualifiedSchema(name))) << ".";
    }
    result << QuoteKvWatchIdentifier(string(NatsQualifiedTable(name)));
    return result.str();
}

void EnsureKvWatchTargetTable(Connection &conn, const NatsKvWatchConfig &config) {
    if (!config.create_target_table) {
        return;
    }
    auto qualified_name = QualifiedName::Parse(config.target_table);
    std::ostringstream sql;
    sql << "CREATE TABLE IF NOT EXISTS " << QuoteKvWatchQualifiedTableName(qualified_name) << " ("
        << QuoteKvWatchIdentifier(config.key_column) << " VARCHAR,"
        << QuoteKvWatchIdentifier(config.value_column) << " BLOB,"
        << "revision UBIGINT,"
        << "created TIMESTAMP,"
        << "operation VARCHAR,"
        << "received_at TIMESTAMP)";
    auto result = conn.Query(sql.str());
    if (result->HasError()) {
        throw std::runtime_error("Failed to create KV watch target table: " + result->GetError());
    }
}

unique_ptr<Appender> CreateKvWatchAppender(Connection &conn, const string &target_table) {
    auto qualified_name = QualifiedName::Parse(target_table);
    if (!NatsQualifiedCatalog(qualified_name).empty()) {
        return make_uniq<Appender>(conn, NatsQualifiedCatalog(qualified_name), NatsQualifiedSchema(qualified_name),
                                   NatsQualifiedTable(qualified_name));
    }
    if (!NatsQualifiedSchema(qualified_name).empty()) {
        return make_uniq<Appender>(conn, NatsQualifiedSchema(qualified_name), NatsQualifiedTable(qualified_name));
    }
    return make_uniq<Appender>(conn, NatsQualifiedTable(qualified_name));
}

void FlushKvWatchBatch(Connection &conn, const NatsKvWatchConfig &config, const vector<NatsKvWatchRow> &batch) {
    if (batch.empty()) {
        return;
    }
    auto appender = CreateKvWatchAppender(conn, config.target_table);
    const auto &types = appender->GetActiveTypes();
    if (types.size() != 6 || types[0].id() != LogicalTypeId::VARCHAR ||
        (types[1].id() != LogicalTypeId::VARCHAR && types[1].id() != LogicalTypeId::BLOB) ||
        types[2].id() != LogicalTypeId::UBIGINT || types[3].id() != LogicalTypeId::TIMESTAMP ||
        types[4].id() != LogicalTypeId::VARCHAR || types[5].id() != LogicalTypeId::TIMESTAMP) {
        throw std::runtime_error("KV watch target table schema does not match (key VARCHAR, value VARCHAR/BLOB, "
                                 "revision UBIGINT, created TIMESTAMP, operation VARCHAR, received_at TIMESTAMP)");
    }
    DataChunk chunk;
    chunk.Initialize(Allocator::Get(*conn.context), types, batch.size());
    auto key_data = GetNatsMutableVectorData<string_t>(chunk.data[0]);
    auto value_data = GetNatsMutableVectorData<string_t>(chunk.data[1]);
    auto revision_data = GetNatsMutableVectorData<uint64_t>(chunk.data[2]);
    auto created_data = GetNatsMutableVectorData<timestamp_t>(chunk.data[3]);
    auto operation_data = GetNatsMutableVectorData<string_t>(chunk.data[4]);
    auto received_at_data = GetNatsMutableVectorData<timestamp_t>(chunk.data[5]);
    for (idx_t row = 0; row < batch.size(); row++) {
        auto &entry = batch[row];
        key_data[row] = StringVector::AddString(chunk.data[0], entry.key);
        if (types[1].id() == LogicalTypeId::BLOB) {
            value_data[row] = StringVector::AddStringOrBlob(chunk.data[1], entry.value.data(), entry.value.size());
        } else {
            value_data[row] = StringVector::AddString(chunk.data[1], entry.value);
        }
        revision_data[row] = entry.revision;
        created_data[row] = entry.created_ns > 0 ? timestamp_t(entry.created_ns / 1000) : timestamp_t(0);
        if (entry.created_ns <= 0) {
            FlatVector::SetNull(chunk.data[3], row, true);
        }
        operation_data[row] = StringVector::AddString(chunk.data[4], KvWatchOperationName(entry.operation));
        received_at_data[row] = Timestamp::GetCurrentTimestamp();
    }
    chunk.SetCardinality(batch.size());
    appender->AppendDataChunk(chunk);
    appender->Flush();
}

void KvWatchDisconnected(natsConnection *, void *closure) {
    auto *job = static_cast<NatsKvWatchJobState *>(closure);
    lock_guard<std::mutex> guard(job->mutex);
    job->progress.connected = false;
    job->progress.reconnecting = true;
    job->cv.notify_all();
}

void KvWatchReconnected(natsConnection *, void *closure) {
    auto *job = static_cast<NatsKvWatchJobState *>(closure);
    lock_guard<std::mutex> guard(job->mutex);
    job->progress.connected = true;
    job->progress.reconnecting = false;
    job->progress.reconnect_count++;
    job->progress.last_reconnect_time = Timestamp::GetCurrentTimestamp();
    job->cv.notify_all();
}

void KvWatchClosed(natsConnection *, void *closure) {
    auto *job = static_cast<NatsKvWatchJobState *>(closure);
    lock_guard<std::mutex> guard(job->mutex);
    job->progress.connected = false;
    job->progress.reconnecting = false;
    if (!job->progress.stop_requested && !job->progress.failed) {
        job->progress.last_error = "NATS connection closed after reconnect attempts";
        job->progress.last_error_time = Timestamp::GetCurrentTimestamp();
    }
    job->cv.notify_all();
}

void RunKvWatchWorker(const shared_ptr<NatsKvWatchJobState> &job) {
    try {
        auto db_instance = job->db.lock();
        if (!db_instance) {
            throw std::runtime_error("KV watch job '" + job->config.job_name +
                                     "' database instance is no longer available");
        }
        Connection conn(*db_instance);
        EnsureKvWatchTargetTable(conn, job->config);

        NatsConnectionCallbacks callbacks;
        callbacks.disconnected = KvWatchDisconnected;
        callbacks.reconnected = KvWatchReconnected;
        callbacks.closed = KvWatchClosed;
        callbacks.closure = job.get();
        ConnectJetStream(job->config.connection, &job->conn, &job->js, 20, &callbacks);

        auto kv_status = js_KeyValue(&job->kv, job->js, job->config.bucket.c_str());
        if (kv_status != NATS_OK) {
            throw std::runtime_error("Failed to open KV bucket '" + job->config.bucket +
                                     "': " + natsStatus_GetText(kv_status));
        }

        kvWatchOptions watch_opts;
        kvWatchOptions_Init(&watch_opts);
        watch_opts.Timeout = NATS_KV_WATCH_SETUP_TIMEOUT_MS;
        watch_opts.UpdatesOnly = job->config.updates_only;
        watch_opts.IgnoreDeletes = job->config.ignore_deletes;

        natsStatus watch_status;
        if (job->config.key_filter.empty()) {
            watch_status = kvStore_WatchAll(&job->watcher, job->kv, &watch_opts);
        } else {
            watch_status = kvStore_Watch(&job->watcher, job->kv, job->config.key_filter.c_str(), &watch_opts);
        }
        if (watch_status != NATS_OK) {
            throw std::runtime_error("Failed to start watcher on bucket '" + job->config.bucket +
                                     "': " + natsStatus_GetText(watch_status));
        }

        {
            lock_guard<std::mutex> guard(job->mutex);
            job->progress.running = true;
            job->progress.connected = true;
            job->progress.reconnecting = false;
            job->progress.last_start_time = Timestamp::GetCurrentTimestamp();
        }
        job->cv.notify_all();

        vector<NatsKvWatchRow> batch;
        batch.reserve(job->config.batch_size);

        while (true) {
            {
                unique_lock<std::mutex> lock(job->mutex);
                if (job->progress.stop_requested) {
                    break;
                }
                if (job->progress.paused) {
                    lock.unlock();
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

            kvEntry *entry = nullptr;
            auto s = kvWatcher_Next(&entry, job->watcher, job->config.poll_ms);
            if (s == NATS_TIMEOUT) {
                if (!batch.empty()) {
                    FlushKvWatchBatch(conn, job->config, batch);
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
            if (IsTransientNatsStatus(s) && job->conn != nullptr && !natsConnection_IsClosed(job->conn)) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                continue;
            }
            if (s != NATS_OK) {
                throw std::runtime_error(std::string("Failed to receive KV watch entry: ") + natsStatus_GetText(s));
            }
            if (entry == nullptr) {
                // Marker for "initial snapshot delivered"; no row to insert.
                continue;
            }

            {
                lock_guard<std::mutex> guard(job->mutex);
                job->progress.last_message_time = Timestamp::GetCurrentTimestamp();
                job->progress.entries_delivered++;
            }

            NatsKvWatchRow row;
            row.key = kvEntry_Key(entry);
            row.revision = kvEntry_Revision(entry);
            row.created_ns = kvEntry_Created(entry);
            row.operation = kvEntry_Operation(entry);
            if (row.operation == kvOp_Put) {
                row.value.assign(static_cast<const char *>(kvEntry_Value(entry)),
                                 static_cast<size_t>(kvEntry_ValueLen(entry)));
            }
            kvEntry_Destroy(entry);
            batch.push_back(std::move(row));

            if (batch.size() >= job->config.batch_size) {
                FlushKvWatchBatch(conn, job->config, batch);
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
            FlushKvWatchBatch(conn, job->config, batch);
            lock_guard<std::mutex> guard(job->mutex);
            job->progress.batches_committed++;
            job->progress.rows_inserted += batch.size();
            job->progress.last_commit_time = Timestamp::GetCurrentTimestamp();
        }

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
    if (job->watcher != nullptr) {
        kvWatcher_Destroy(job->watcher);
        job->watcher = nullptr;
    }
    if (job->kv != nullptr) {
        kvStore_Destroy(job->kv);
        job->kv = nullptr;
    }
    if (job->js != nullptr) {
        jsCtx_Destroy(job->js);
        job->js = nullptr;
    }
    if (job->conn != nullptr) {
        natsConnection_Destroy(job->conn);
        job->conn = nullptr;
    }
    job->cv.notify_all();
}

unique_ptr<FunctionData> NatsKvWatchStartBind(ClientContext &, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<string> &names) {
    auto config = ParseKvWatchConfig(input);
    AddKvWatchSnapshotColumns(return_types, names);
    auto bind_data = make_uniq<NatsKvWatchBindData>();
    bind_data->config = std::move(config);
    return bind_data;
}

unique_ptr<GlobalTableFunctionState> NatsKvWatchStartInitGlobal(ClientContext &context,
                                                                TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<NatsKvWatchBindData>();
    auto job = NatsKvWatchManager::Get().CreateJob(bind_data.config);
    job->db = context.db;
    job->worker = std::thread(RunKvWatchWorker, job);

    auto state = make_uniq<NatsKvWatchControlGlobalState>();
    state->jobs.push_back(job);
    return state;
}

void NatsKvWatchStartExecute(ClientContext &, TableFunctionInput &data_p, DataChunk &output) {
    auto &state = data_p.global_state->Cast<NatsKvWatchControlGlobalState>();
    if (state.done) {
        output.SetCardinality(0);
        return;
    }
    auto snapshot = SnapshotKvWatchJob(state.jobs[0]);
    FillKvWatchSnapshotColumns(output, 0, snapshot);
    output.SetCardinality(1);
    state.done = true;
}

unique_ptr<FunctionData> NatsKvWatchJobNameBind(ClientContext &, TableFunctionBindInput &input,
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
    AddKvWatchSnapshotColumns(return_types, names);
    auto bind_data = make_uniq<NatsKvWatchJobNameBindData>();
    bind_data->job_name = std::move(job_name);
    return bind_data;
}

unique_ptr<GlobalTableFunctionState> NatsKvWatchStopInitGlobal(ClientContext &, TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<NatsKvWatchJobNameBindData>();
    auto job = NatsKvWatchManager::Get().GetJob(bind_data.job_name);
    if (!job) {
        throw std::runtime_error("KV watch job '" + bind_data.job_name + "' does not exist");
    }
    NatsKvWatchManager::Get().StopJob(bind_data.job_name);
    auto state = make_uniq<NatsKvWatchControlGlobalState>();
    state->jobs.push_back(job);
    return state;
}

void NatsKvWatchControlExecute(ClientContext &, TableFunctionInput &data_p, DataChunk &output) {
    auto &state = data_p.global_state->Cast<NatsKvWatchControlGlobalState>();
    if (state.done) {
        output.SetCardinality(0);
        return;
    }
    auto snapshot = SnapshotKvWatchJob(state.jobs[0]);
    FillKvWatchSnapshotColumns(output, 0, snapshot);
    output.SetCardinality(1);
    state.done = true;
}

unique_ptr<GlobalTableFunctionState> NatsKvWatchRemoveInitGlobal(ClientContext &, TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<NatsKvWatchJobNameBindData>();
    auto job = NatsKvWatchManager::Get().GetJob(bind_data.job_name);
    if (!job) {
        throw std::runtime_error("KV watch job '" + bind_data.job_name + "' does not exist");
    }
    auto state = make_uniq<NatsKvWatchControlGlobalState>();
    state->jobs.push_back(job);
    NatsKvWatchManager::Get().RemoveJob(bind_data.job_name);
    return state;
}

unique_ptr<GlobalTableFunctionState> NatsKvWatchStatusInitGlobal(ClientContext &, TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<NatsKvWatchJobNameBindData>();
    auto job = NatsKvWatchManager::Get().GetJob(bind_data.job_name);
    if (!job) {
        throw std::runtime_error("KV watch job '" + bind_data.job_name + "' does not exist");
    }
    auto state = make_uniq<NatsKvWatchControlGlobalState>();
    state->jobs.push_back(job);
    return state;
}

unique_ptr<GlobalTableFunctionState> NatsKvWatchPauseInitGlobal(ClientContext &, TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<NatsKvWatchJobNameBindData>();
    auto job = NatsKvWatchManager::Get().GetJob(bind_data.job_name);
    if (!job) {
        throw std::runtime_error("KV watch job '" + bind_data.job_name + "' does not exist or is not running");
    }
    {
        lock_guard<std::mutex> guard(job->mutex);
        if (!job->progress.running || job->progress.stop_requested || job->progress.failed) {
            throw std::runtime_error("KV watch job '" + bind_data.job_name + "' is not running");
        }
    }
    NatsKvWatchManager::Get().PauseJob(bind_data.job_name);
    auto state = make_uniq<NatsKvWatchControlGlobalState>();
    state->jobs.push_back(job);
    return state;
}

void NatsKvWatchPauseExecute(ClientContext &, TableFunctionInput &data_p, DataChunk &output) {
    auto &state = data_p.global_state->Cast<NatsKvWatchControlGlobalState>();
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
            throw std::runtime_error("Timed out waiting for KV watch job '" + job->config.job_name + "' to pause");
        }
        if (!job->progress.paused && !job->progress.failed && !job->progress.stop_requested) {
            throw std::runtime_error("KV watch job '" + job->config.job_name + "' did not pause");
        }
    }
    auto snapshot = SnapshotKvWatchJob(job);
    FillKvWatchSnapshotColumns(output, 0, snapshot);
    output.SetCardinality(1);
    state.done = true;
}

unique_ptr<GlobalTableFunctionState> NatsKvWatchResumeInitGlobal(ClientContext &, TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<NatsKvWatchJobNameBindData>();
    auto job = NatsKvWatchManager::Get().GetJob(bind_data.job_name);
    if (!job) {
        throw std::runtime_error("KV watch job '" + bind_data.job_name + "' does not exist or is not running");
    }
    {
        lock_guard<std::mutex> guard(job->mutex);
        if (!job->progress.running || job->progress.stop_requested || job->progress.failed) {
            throw std::runtime_error("KV watch job '" + bind_data.job_name + "' is not running");
        }
    }
    NatsKvWatchManager::Get().ResumeJob(bind_data.job_name);
    auto state = make_uniq<NatsKvWatchControlGlobalState>();
    state->jobs.push_back(job);
    return state;
}

void NatsKvWatchResumeExecute(ClientContext &, TableFunctionInput &data_p, DataChunk &output) {
    auto &state = data_p.global_state->Cast<NatsKvWatchControlGlobalState>();
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
            throw std::runtime_error("Timed out waiting for KV watch job '" + job->config.job_name + "' to resume");
        }
        if (job->progress.paused && !job->progress.failed && !job->progress.stop_requested) {
            throw std::runtime_error("KV watch job '" + job->config.job_name + "' did not resume");
        }
    }
    auto snapshot = SnapshotKvWatchJob(job);
    FillKvWatchSnapshotColumns(output, 0, snapshot);
    output.SetCardinality(1);
    state.done = true;
}

unique_ptr<FunctionData> NatsKvWatchJobsBind(ClientContext &, TableFunctionBindInput &,
                                             vector<LogicalType> &return_types, vector<string> &names) {
    AddKvWatchSnapshotColumns(return_types, names);
    return make_uniq<NatsKvWatchJobsBindData>();
}

unique_ptr<GlobalTableFunctionState> NatsKvWatchJobsInitGlobal(ClientContext &, TableFunctionInitInput &) {
    auto state = make_uniq<NatsKvWatchControlGlobalState>();
    state->jobs = NatsKvWatchManager::Get().ListJobs();
    return state;
}

void NatsKvWatchJobsExecute(ClientContext &, TableFunctionInput &data_p, DataChunk &output) {
    auto &state = data_p.global_state->Cast<NatsKvWatchControlGlobalState>();
    idx_t row = 0;
    while (row < STANDARD_VECTOR_SIZE && state.row_idx < state.jobs.size()) {
        auto snapshot = SnapshotKvWatchJob(state.jobs[state.row_idx]);
        FillKvWatchSnapshotColumns(output, row, snapshot);
        row++;
        state.row_idx++;
    }
    output.SetCardinality(row);
}

} // namespace

void NatsKvWatchFunction::Register(ExtensionLoader &loader) {
    TableFunction start_fn("nats_start_kv_watch", {}, NatsKvWatchStartExecute, NatsKvWatchStartBind,
                           NatsKvWatchStartInitGlobal);
    start_fn.named_parameters["job_name"] = LogicalType(LogicalTypeId::VARCHAR);
    start_fn.named_parameters["target_table"] = LogicalType(LogicalTypeId::VARCHAR);
    RegisterNatsConnectionParameters(start_fn);
    start_fn.named_parameters["bucket"] = LogicalType(LogicalTypeId::VARCHAR);
    start_fn.named_parameters["key_filter"] = LogicalType(LogicalTypeId::VARCHAR);
    start_fn.named_parameters["batch_size"] = LogicalType(LogicalTypeId::UBIGINT);
    start_fn.named_parameters["poll_ms"] = LogicalType(LogicalTypeId::BIGINT);
    start_fn.named_parameters["updates_only"] = LogicalType(LogicalTypeId::BOOLEAN);
    start_fn.named_parameters["ignore_deletes"] = LogicalType(LogicalTypeId::BOOLEAN);
    start_fn.named_parameters["create_target_table"] = LogicalType(LogicalTypeId::BOOLEAN);
    start_fn.named_parameters["key_column"] = LogicalType(LogicalTypeId::VARCHAR);
    start_fn.named_parameters["value_column"] = LogicalType(LogicalTypeId::VARCHAR);
    loader.RegisterFunction(start_fn);

    TableFunction stop_fn("nats_stop_kv_watch", {}, NatsKvWatchControlExecute, NatsKvWatchJobNameBind,
                          NatsKvWatchStopInitGlobal);
    stop_fn.named_parameters["job_name"] = LogicalType(LogicalTypeId::VARCHAR);
    loader.RegisterFunction(stop_fn);

    TableFunction remove_fn("nats_remove_kv_watch", {}, NatsKvWatchControlExecute, NatsKvWatchJobNameBind,
                            NatsKvWatchRemoveInitGlobal);
    remove_fn.named_parameters["job_name"] = LogicalType(LogicalTypeId::VARCHAR);
    loader.RegisterFunction(remove_fn);

    TableFunction status_fn("nats_kv_watch_status", {}, NatsKvWatchControlExecute, NatsKvWatchJobNameBind,
                            NatsKvWatchStatusInitGlobal);
    status_fn.named_parameters["job_name"] = LogicalType(LogicalTypeId::VARCHAR);
    loader.RegisterFunction(status_fn);

    TableFunction pause_fn("nats_pause_kv_watch", {}, NatsKvWatchPauseExecute, NatsKvWatchJobNameBind,
                           NatsKvWatchPauseInitGlobal);
    pause_fn.named_parameters["job_name"] = LogicalType(LogicalTypeId::VARCHAR);
    loader.RegisterFunction(pause_fn);

    TableFunction resume_fn("nats_resume_kv_watch", {}, NatsKvWatchResumeExecute, NatsKvWatchJobNameBind,
                            NatsKvWatchResumeInitGlobal);
    resume_fn.named_parameters["job_name"] = LogicalType(LogicalTypeId::VARCHAR);
    loader.RegisterFunction(resume_fn);

    TableFunction jobs_fn("nats_kv_watch_jobs", {}, NatsKvWatchJobsExecute, NatsKvWatchJobsBind,
                          NatsKvWatchJobsInitGlobal);
    loader.RegisterFunction(jobs_fn);
}

} // namespace duckdb
