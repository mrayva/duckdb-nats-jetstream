#include "nats_ingest.hpp"
#include "nats_message_decode.hpp"
#include "nats_source.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/common/types/vector.hpp"
#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <sstream>
#include <unordered_set>
#include <nats/nats.h>
#include <google/protobuf/compiler/importer.h>
#include <google/protobuf/dynamic_message.h>

// Windows defines GetMessage as a macro (GetMessageA/GetMessageW)
// This conflicts with protobuf's Reflection::GetMessage() method
#ifdef GetMessage
#undef GetMessage
#endif

using namespace google::protobuf;
using namespace google::protobuf::compiler;

namespace duckdb {

class ProtobufErrorCollector : public MultiFileErrorCollector {
public:
#if GOOGLE_PROTOBUF_VERSION >= 3022000
    void RecordError(absl::string_view filename, int line, int column, absl::string_view message) override {
        errors.push_back(string(filename) + ":" + std::to_string(line) + ":" + std::to_string(column) + ": " + string(message));
    }
#else
    void AddError(const std::string &filename, int line, int column, const std::string &message) override {
        errors.push_back(filename + ":" + std::to_string(line) + ":" + std::to_string(column) + ": " + message);
    }
#endif

    string GetErrors() const {
        string result;
        for (const auto &err : errors) {
            result += err + "\n";
        }
        return result;
    }

    bool HasErrors() const {
        return !errors.empty();
    }

private:
    vector<string> errors;
};

struct NatsIngestStartBindData : public TableFunctionData {
    NatsIngestConfig config;
};

struct NatsIngestStopBindData : public TableFunctionData {
    string job_name;
};

struct NatsIngestPauseBindData : public TableFunctionData {
    string job_name;
};

struct NatsIngestResumeBindData : public TableFunctionData {
    string job_name;
};

struct NatsIngestStatusBindData : public TableFunctionData {
    string job_name;
};

struct NatsIngestJobsBindData : public TableFunctionData {
};

struct NatsIngestSnapshot {
    string job_name;
    string stream_name;
    string target_table;
    string durable_name;
    string nats_url = "nats://localhost:4222";
    string credentials_file;
    string tls_ca_file;
    string tls_cert_file;
    string tls_key_file;
    string tls_server_name;
    bool tls_skip_verify = false;
    uint64_t start_seq = 1;
    uint64_t batch_size = 1000;
    int64_t poll_ms = 100;
    int64_t fetch_timeout_ms = 1000;
    bool resume_from_checkpoint = true;
    bool create_target_table = false;
    string subject_contains;
    string nats_subject;
    vector<string> json_fields;
    string proto_file;
    string proto_message;
    vector<string> proto_fields;
    bool running = false;
    bool stop_requested = false;
    bool stopped = false;
    bool failed = false;
    bool paused = false;
    bool pause_requested = false;
    uint64_t last_committed_seq = 0;
    uint64_t last_delivered_seq = 0;
    uint64_t rows_inserted = 0;
    uint64_t batches_committed = 0;
    uint64_t fetches_completed = 0;
    uint64_t last_batch_rows = 0;
    uint64_t sequence_lag = 0;
    timestamp_t last_start_time;
    timestamp_t last_fetch_time;
    timestamp_t last_ack_time;
    timestamp_t last_commit_time;
    timestamp_t last_error_time;
    string last_error;
};

struct NatsIngestControlGlobalState : public GlobalTableFunctionState {
    bool done = false;
    idx_t row_idx = 0;
    vector<shared_ptr<NatsIngestJobState>> jobs;
    vector<NatsIngestSnapshot> registry_rows;

    idx_t MaxThreads() const override {
        return 1;
    }
};

static constexpr const char *NATS_INGEST_CHECKPOINT_TABLE = "duckdb_nats_ingest_checkpoints";
static constexpr const char *NATS_INGEST_REGISTRY_TABLE = "duckdb_nats_ingest_jobs";

static NatsIngestSnapshot SnapshotJob(const shared_ptr<NatsIngestJobState> &job);
static NatsIngestConfig SnapshotToConfig(const NatsIngestSnapshot &snapshot);
static void RestoreJobProgress(const shared_ptr<NatsIngestJobState> &job, const NatsIngestSnapshot &snapshot);
static void RunIngestWorker(const shared_ptr<NatsIngestJobState> &job);
static TableCatalogEntry &ResolveTargetTable(ClientContext &context, const string &target_table);
static void EnsureTargetTable(Connection &conn, NatsIngestConfig &config);

static string SqlStringLiteral(const string &value) {
    string result = "'";
    for (auto ch : value) {
        if (ch == '\'') {
            result += "''";
        } else {
            result += ch;
        }
    }
    result += "'";
    return result;
}

static string SqlListLiteral(const vector<string> &values) {
    if (values.empty()) {
        return "LIST_VALUE()";
    }
    std::ostringstream result;
    result << "LIST_VALUE(";
    for (idx_t i = 0; i < values.size(); i++) {
        if (i > 0) {
            result << ", ";
        }
        result << SqlStringLiteral(values[i]);
    }
    result << ")";
    return result.str();
}

static void ExecuteOrThrow(Connection &conn, const string &sql, const string &action) {
    auto result = conn.Query(sql);
    if (result->HasError()) {
        throw std::runtime_error(action + ": " + result->GetError());
    }
}

static void EnsureActiveTransaction(Connection &conn) {
    if (!conn.HasActiveTransaction()) {
        conn.BeginTransaction();
    }
}

static void MarkTransactionWrite(Connection &conn) {
    auto &catalog = Catalog::GetCatalog(*conn.context, INVALID_CATALOG);
    auto &transaction = Transaction::Get(*conn.context, catalog);
    transaction.SetReadWrite();
}

static void EnsureCheckpointTable(Connection &conn) {
    std::ostringstream sql;
    sql << "CREATE TABLE IF NOT EXISTS " << NATS_INGEST_CHECKPOINT_TABLE << " ("
        << "stream_name VARCHAR NOT NULL,"
        << "durable_name VARCHAR NOT NULL,"
        << "job_name VARCHAR NOT NULL,"
        << "last_committed_seq UBIGINT NOT NULL,"
        << "last_delivered_seq UBIGINT NOT NULL,"
        << "rows_inserted UBIGINT NOT NULL,"
        << "batches_committed UBIGINT NOT NULL,"
        << "updated_at TIMESTAMP NOT NULL,"
        << "PRIMARY KEY(stream_name, durable_name)"
        << ")";
    ExecuteOrThrow(conn, sql.str(), "Failed to create ingest checkpoint table");
}

static void EnsureRegistryTable(Connection &conn) {
    std::ostringstream sql;
    sql << "CREATE TABLE IF NOT EXISTS " << NATS_INGEST_REGISTRY_TABLE << " ("
        << "job_name VARCHAR PRIMARY KEY,"
        << "stream_name VARCHAR NOT NULL,"
        << "target_table VARCHAR NOT NULL,"
        << "durable_name VARCHAR NOT NULL,"
        << "nats_url VARCHAR NOT NULL,"
        << "start_seq UBIGINT NOT NULL,"
        << "batch_size UBIGINT NOT NULL,"
        << "poll_ms BIGINT NOT NULL,"
        << "fetch_timeout_ms BIGINT NOT NULL,"
        << "resume_from_checkpoint BOOLEAN NOT NULL,"
        << "create_target_table BOOLEAN NOT NULL,"
        << "subject_contains VARCHAR,"
        << "nats_subject VARCHAR,"
        << "json_fields VARCHAR[],"
        << "proto_file VARCHAR,"
        << "proto_message VARCHAR,"
        << "proto_fields VARCHAR[],"
        << "running BOOLEAN NOT NULL,"
        << "stop_requested BOOLEAN NOT NULL,"
        << "stopped BOOLEAN NOT NULL,"
        << "failed BOOLEAN NOT NULL,"
        << "paused BOOLEAN NOT NULL,"
        << "pause_requested BOOLEAN NOT NULL,"
        << "last_committed_seq UBIGINT NOT NULL,"
        << "last_delivered_seq UBIGINT NOT NULL,"
        << "rows_inserted UBIGINT NOT NULL,"
        << "batches_committed UBIGINT NOT NULL,"
        << "fetches_completed UBIGINT NOT NULL,"
        << "last_batch_rows UBIGINT NOT NULL,"
        << "sequence_lag UBIGINT NOT NULL,"
        << "last_start_time TIMESTAMP,"
        << "last_fetch_time TIMESTAMP,"
        << "last_ack_time TIMESTAMP,"
        << "last_commit_time TIMESTAMP,"
        << "last_error_time TIMESTAMP,"
        << "last_error VARCHAR,"
        << "credentials_file VARCHAR,"
        << "tls_ca_file VARCHAR,"
        << "tls_cert_file VARCHAR,"
        << "tls_key_file VARCHAR,"
        << "tls_server_name VARCHAR,"
        << "tls_skip_verify BOOLEAN NOT NULL DEFAULT FALSE,"
        << "updated_at TIMESTAMP NOT NULL"
        << ")";
    ExecuteOrThrow(conn, sql.str(), "Failed to create ingest registry table");
    ExecuteOrThrow(conn,
                   "ALTER TABLE " + string(NATS_INGEST_REGISTRY_TABLE) +
                       " ADD COLUMN IF NOT EXISTS create_target_table BOOLEAN",
                   "Failed to migrate ingest registry table");
    ExecuteOrThrow(conn,
                   "UPDATE " + string(NATS_INGEST_REGISTRY_TABLE) +
                       " SET create_target_table = COALESCE(create_target_table, FALSE)",
                   "Failed to backfill ingest registry table");
    for (const auto &column : {"credentials_file VARCHAR", "tls_ca_file VARCHAR", "tls_cert_file VARCHAR",
                               "tls_key_file VARCHAR", "tls_server_name VARCHAR",
                               "tls_skip_verify BOOLEAN DEFAULT FALSE"}) {
        ExecuteOrThrow(conn, "ALTER TABLE " + string(NATS_INGEST_REGISTRY_TABLE) + " ADD COLUMN IF NOT EXISTS " + column,
                       "Failed to migrate ingest registry connection settings");
    }
    ExecuteOrThrow(conn,
                   "UPDATE " + string(NATS_INGEST_REGISTRY_TABLE) +
                       " SET tls_skip_verify = COALESCE(tls_skip_verify, FALSE)",
                   "Failed to backfill ingest registry TLS settings");
}

static void UpsertRegistry(Connection &conn, const NatsIngestSnapshot &snapshot) {
    std::ostringstream sql;
    sql << "INSERT INTO " << NATS_INGEST_REGISTRY_TABLE
        << " (job_name, stream_name, target_table, durable_name, nats_url, start_seq, batch_size, poll_ms, "
           "fetch_timeout_ms, resume_from_checkpoint, create_target_table, subject_contains, nats_subject, json_fields, proto_file, "
           "proto_message, proto_fields, running, stop_requested, stopped, failed, paused, pause_requested, "
           "last_committed_seq, last_delivered_seq, rows_inserted, batches_committed, fetches_completed, last_batch_rows, "
           "sequence_lag, last_start_time, last_fetch_time, last_ack_time, last_commit_time, last_error_time, last_error, "
           "credentials_file, tls_ca_file, tls_cert_file, tls_key_file, tls_server_name, tls_skip_verify, updated_at) VALUES ("
        << SqlStringLiteral(snapshot.job_name) << ", "
        << SqlStringLiteral(snapshot.stream_name) << ", "
        << SqlStringLiteral(snapshot.target_table) << ", "
        << SqlStringLiteral(snapshot.durable_name) << ", "
        << SqlStringLiteral(snapshot.nats_url) << ", "
        << snapshot.start_seq << ", "
        << snapshot.batch_size << ", "
        << snapshot.poll_ms << ", "
        << snapshot.fetch_timeout_ms << ", "
        << (snapshot.resume_from_checkpoint ? "TRUE" : "FALSE") << ", "
        << (snapshot.create_target_table ? "TRUE" : "FALSE") << ", "
        << (snapshot.subject_contains.empty() ? "NULL" : SqlStringLiteral(snapshot.subject_contains)) << ", "
        << (snapshot.nats_subject.empty() ? "NULL" : SqlStringLiteral(snapshot.nats_subject)) << ", "
        << SqlListLiteral(snapshot.json_fields) << ", "
        << (snapshot.proto_file.empty() ? "NULL" : SqlStringLiteral(snapshot.proto_file)) << ", "
        << (snapshot.proto_message.empty() ? "NULL" : SqlStringLiteral(snapshot.proto_message)) << ", "
        << SqlListLiteral(snapshot.proto_fields) << ", "
        << (snapshot.running ? "TRUE" : "FALSE") << ", "
        << (snapshot.stop_requested ? "TRUE" : "FALSE") << ", "
        << (snapshot.stopped ? "TRUE" : "FALSE") << ", "
        << (snapshot.failed ? "TRUE" : "FALSE") << ", "
        << (snapshot.paused ? "TRUE" : "FALSE") << ", "
        << (snapshot.pause_requested ? "TRUE" : "FALSE") << ", "
        << snapshot.last_committed_seq << ", "
        << snapshot.last_delivered_seq << ", "
        << snapshot.rows_inserted << ", "
        << snapshot.batches_committed << ", "
        << snapshot.fetches_completed << ", "
        << snapshot.last_batch_rows << ", "
        << snapshot.sequence_lag << ", "
        << (snapshot.last_start_time.value == 0 ? "NULL" : SqlStringLiteral(Timestamp::ToString(snapshot.last_start_time))) << ", "
        << (snapshot.last_fetch_time.value == 0 ? "NULL" : SqlStringLiteral(Timestamp::ToString(snapshot.last_fetch_time))) << ", "
        << (snapshot.last_ack_time.value == 0 ? "NULL" : SqlStringLiteral(Timestamp::ToString(snapshot.last_ack_time))) << ", "
        << (snapshot.last_commit_time.value == 0 ? "NULL" : SqlStringLiteral(Timestamp::ToString(snapshot.last_commit_time))) << ", "
        << (snapshot.last_error_time.value == 0 ? "NULL" : SqlStringLiteral(Timestamp::ToString(snapshot.last_error_time))) << ", "
        << (snapshot.last_error.empty() ? "NULL" : SqlStringLiteral(snapshot.last_error)) << ", "
        << (snapshot.credentials_file.empty() ? "NULL" : SqlStringLiteral(snapshot.credentials_file)) << ", "
        << (snapshot.tls_ca_file.empty() ? "NULL" : SqlStringLiteral(snapshot.tls_ca_file)) << ", "
        << (snapshot.tls_cert_file.empty() ? "NULL" : SqlStringLiteral(snapshot.tls_cert_file)) << ", "
        << (snapshot.tls_key_file.empty() ? "NULL" : SqlStringLiteral(snapshot.tls_key_file)) << ", "
        << (snapshot.tls_server_name.empty() ? "NULL" : SqlStringLiteral(snapshot.tls_server_name)) << ", "
        << (snapshot.tls_skip_verify ? "TRUE" : "FALSE") << ", "
        << "CURRENT_TIMESTAMP"
        << ") ON CONFLICT(job_name) DO UPDATE SET "
        << "stream_name = excluded.stream_name, "
        << "target_table = excluded.target_table, "
        << "durable_name = excluded.durable_name, "
        << "nats_url = excluded.nats_url, "
        << "start_seq = excluded.start_seq, "
        << "batch_size = excluded.batch_size, "
        << "poll_ms = excluded.poll_ms, "
        << "fetch_timeout_ms = excluded.fetch_timeout_ms, "
        << "resume_from_checkpoint = excluded.resume_from_checkpoint, "
        << "create_target_table = excluded.create_target_table, "
        << "subject_contains = excluded.subject_contains, "
        << "nats_subject = excluded.nats_subject, "
        << "json_fields = excluded.json_fields, "
        << "proto_file = excluded.proto_file, "
        << "proto_message = excluded.proto_message, "
        << "proto_fields = excluded.proto_fields, "
        << "running = excluded.running, "
        << "stop_requested = excluded.stop_requested, "
        << "stopped = excluded.stopped, "
        << "failed = excluded.failed, "
        << "paused = excluded.paused, "
        << "pause_requested = excluded.pause_requested, "
        << "last_committed_seq = excluded.last_committed_seq, "
        << "last_delivered_seq = excluded.last_delivered_seq, "
        << "rows_inserted = excluded.rows_inserted, "
        << "batches_committed = excluded.batches_committed, "
        << "fetches_completed = excluded.fetches_completed, "
        << "last_batch_rows = excluded.last_batch_rows, "
        << "sequence_lag = excluded.sequence_lag, "
        << "last_start_time = excluded.last_start_time, "
        << "last_fetch_time = excluded.last_fetch_time, "
        << "last_ack_time = excluded.last_ack_time, "
        << "last_commit_time = excluded.last_commit_time, "
        << "last_error_time = excluded.last_error_time, "
        << "last_error = excluded.last_error, "
        << "credentials_file = excluded.credentials_file, "
        << "tls_ca_file = excluded.tls_ca_file, "
        << "tls_cert_file = excluded.tls_cert_file, "
        << "tls_key_file = excluded.tls_key_file, "
        << "tls_server_name = excluded.tls_server_name, "
        << "tls_skip_verify = excluded.tls_skip_verify, "
        << "updated_at = excluded.updated_at";
    ExecuteOrThrow(conn, sql.str(), "Failed to update ingest registry");
}

static void PersistRegistry(Connection &conn, const shared_ptr<NatsIngestJobState> &job) {
    UpsertRegistry(conn, SnapshotJob(job));
}

static bool LoadRegistrySnapshot(Connection &conn, const string &job_name, NatsIngestSnapshot &snapshot) {
    std::ostringstream sql;
    sql << "SELECT job_name, stream_name, target_table, durable_name, nats_url, start_seq, batch_size, poll_ms, "
           "fetch_timeout_ms, resume_from_checkpoint, create_target_table, subject_contains, nats_subject, json_fields, proto_file, "
           "proto_message, proto_fields, running, stop_requested, stopped, failed, paused, pause_requested, last_committed_seq, "
           "last_delivered_seq, rows_inserted, batches_committed, fetches_completed, last_batch_rows, sequence_lag, last_start_time, "
           "last_fetch_time, last_ack_time, last_commit_time, last_error_time, last_error, credentials_file, tls_ca_file, "
           "tls_cert_file, tls_key_file, tls_server_name, tls_skip_verify "
        << "FROM " << NATS_INGEST_REGISTRY_TABLE << " WHERE job_name = " << SqlStringLiteral(job_name);

    auto result = conn.Query(sql.str());
    if (result->HasError()) {
        throw std::runtime_error("Failed to load ingest registry entry: " + result->GetError());
    }

    auto chunk = result->Fetch();
    if (!chunk || chunk->size() == 0) {
        return false;
    }

    snapshot.job_name = chunk->GetValue(0, 0).GetValue<string>();
    snapshot.stream_name = chunk->GetValue(1, 0).GetValue<string>();
    snapshot.target_table = chunk->GetValue(2, 0).GetValue<string>();
    snapshot.durable_name = chunk->GetValue(3, 0).GetValue<string>();
    snapshot.nats_url = chunk->GetValue(4, 0).GetValue<string>();
    snapshot.start_seq = chunk->GetValue(5, 0).GetValue<uint64_t>();
    snapshot.batch_size = chunk->GetValue(6, 0).GetValue<uint64_t>();
    snapshot.poll_ms = chunk->GetValue(7, 0).GetValue<int64_t>();
    snapshot.fetch_timeout_ms = chunk->GetValue(8, 0).GetValue<int64_t>();
    snapshot.resume_from_checkpoint = chunk->GetValue(9, 0).GetValue<bool>();
    snapshot.create_target_table = chunk->GetValue(10, 0).GetValue<bool>();
    if (!chunk->GetValue(11, 0).IsNull()) {
        snapshot.subject_contains = chunk->GetValue(11, 0).GetValue<string>();
    }
    if (!chunk->GetValue(12, 0).IsNull()) {
        snapshot.nats_subject = chunk->GetValue(12, 0).GetValue<string>();
    }
    if (!chunk->GetValue(13, 0).IsNull()) {
        for (auto &entry : ListValue::GetChildren(chunk->GetValue(13, 0))) {
            snapshot.json_fields.push_back(entry.GetValue<string>());
        }
    }
    if (!chunk->GetValue(14, 0).IsNull()) {
        snapshot.proto_file = chunk->GetValue(14, 0).GetValue<string>();
    }
    if (!chunk->GetValue(15, 0).IsNull()) {
        snapshot.proto_message = chunk->GetValue(15, 0).GetValue<string>();
    }
    if (!chunk->GetValue(16, 0).IsNull()) {
        for (auto &entry : ListValue::GetChildren(chunk->GetValue(16, 0))) {
            snapshot.proto_fields.push_back(entry.GetValue<string>());
        }
    }
    snapshot.running = chunk->GetValue(17, 0).GetValue<bool>();
    snapshot.stop_requested = chunk->GetValue(18, 0).GetValue<bool>();
    snapshot.stopped = chunk->GetValue(19, 0).GetValue<bool>();
    snapshot.failed = chunk->GetValue(20, 0).GetValue<bool>();
    snapshot.paused = chunk->GetValue(21, 0).GetValue<bool>();
    snapshot.pause_requested = chunk->GetValue(22, 0).GetValue<bool>();
    snapshot.last_committed_seq = chunk->GetValue(23, 0).GetValue<uint64_t>();
    snapshot.last_delivered_seq = chunk->GetValue(24, 0).GetValue<uint64_t>();
    snapshot.rows_inserted = chunk->GetValue(25, 0).GetValue<uint64_t>();
    snapshot.batches_committed = chunk->GetValue(26, 0).GetValue<uint64_t>();
    snapshot.fetches_completed = chunk->GetValue(27, 0).GetValue<uint64_t>();
    snapshot.last_batch_rows = chunk->GetValue(28, 0).GetValue<uint64_t>();
    snapshot.sequence_lag = chunk->GetValue(29, 0).GetValue<uint64_t>();
    if (!chunk->GetValue(30, 0).IsNull()) {
        snapshot.last_start_time = chunk->GetValue(30, 0).GetValue<timestamp_t>();
    }
    if (!chunk->GetValue(31, 0).IsNull()) {
        snapshot.last_fetch_time = chunk->GetValue(31, 0).GetValue<timestamp_t>();
    }
    if (!chunk->GetValue(32, 0).IsNull()) {
        snapshot.last_ack_time = chunk->GetValue(32, 0).GetValue<timestamp_t>();
    }
    if (!chunk->GetValue(33, 0).IsNull()) {
        snapshot.last_commit_time = chunk->GetValue(33, 0).GetValue<timestamp_t>();
    }
    if (!chunk->GetValue(34, 0).IsNull()) {
        snapshot.last_error_time = chunk->GetValue(34, 0).GetValue<timestamp_t>();
    }
    if (!chunk->GetValue(35, 0).IsNull()) {
        snapshot.last_error = chunk->GetValue(35, 0).GetValue<string>();
    }
    if (!chunk->GetValue(36, 0).IsNull()) {
        snapshot.credentials_file = chunk->GetValue(36, 0).GetValue<string>();
    }
    if (!chunk->GetValue(37, 0).IsNull()) {
        snapshot.tls_ca_file = chunk->GetValue(37, 0).GetValue<string>();
    }
    if (!chunk->GetValue(38, 0).IsNull()) {
        snapshot.tls_cert_file = chunk->GetValue(38, 0).GetValue<string>();
    }
    if (!chunk->GetValue(39, 0).IsNull()) {
        snapshot.tls_key_file = chunk->GetValue(39, 0).GetValue<string>();
    }
    if (!chunk->GetValue(40, 0).IsNull()) {
        snapshot.tls_server_name = chunk->GetValue(40, 0).GetValue<string>();
    }
    snapshot.tls_skip_verify = chunk->GetValue(41, 0).GetValue<bool>();
    return true;
}

static vector<NatsIngestSnapshot> LoadRegistrySnapshots(Connection &conn) {
    std::ostringstream sql;
    sql << "SELECT job_name, stream_name, target_table, durable_name, nats_url, start_seq, batch_size, poll_ms, "
           "fetch_timeout_ms, resume_from_checkpoint, create_target_table, subject_contains, nats_subject, json_fields, proto_file, "
           "proto_message, proto_fields, running, stop_requested, stopped, failed, paused, pause_requested, last_committed_seq, "
           "last_delivered_seq, rows_inserted, batches_committed, fetches_completed, last_batch_rows, sequence_lag, last_start_time, "
           "last_fetch_time, last_ack_time, last_commit_time, last_error_time, last_error, credentials_file, tls_ca_file, "
           "tls_cert_file, tls_key_file, tls_server_name, tls_skip_verify "
        << "FROM " << NATS_INGEST_REGISTRY_TABLE << " ORDER BY job_name";

    auto result = conn.Query(sql.str());
    if (result->HasError()) {
        throw std::runtime_error("Failed to load ingest registry entries: " + result->GetError());
    }

    vector<NatsIngestSnapshot> snapshots;
    for (auto chunk = result->Fetch(); chunk && chunk->size() > 0; chunk = result->Fetch()) {
        for (idx_t row = 0; row < chunk->size(); row++) {
            NatsIngestSnapshot snapshot;
            snapshot.job_name = chunk->GetValue(0, row).GetValue<string>();
            snapshot.stream_name = chunk->GetValue(1, row).GetValue<string>();
            snapshot.target_table = chunk->GetValue(2, row).GetValue<string>();
            snapshot.durable_name = chunk->GetValue(3, row).GetValue<string>();
            snapshot.nats_url = chunk->GetValue(4, row).GetValue<string>();
            snapshot.start_seq = chunk->GetValue(5, row).GetValue<uint64_t>();
            snapshot.batch_size = chunk->GetValue(6, row).GetValue<uint64_t>();
            snapshot.poll_ms = chunk->GetValue(7, row).GetValue<int64_t>();
            snapshot.fetch_timeout_ms = chunk->GetValue(8, row).GetValue<int64_t>();
            snapshot.resume_from_checkpoint = chunk->GetValue(9, row).GetValue<bool>();
            snapshot.create_target_table = chunk->GetValue(10, row).GetValue<bool>();
            if (!chunk->GetValue(11, row).IsNull()) {
                snapshot.subject_contains = chunk->GetValue(11, row).GetValue<string>();
            }
            if (!chunk->GetValue(12, row).IsNull()) {
                snapshot.nats_subject = chunk->GetValue(12, row).GetValue<string>();
            }
            if (!chunk->GetValue(13, row).IsNull()) {
                for (auto &entry : ListValue::GetChildren(chunk->GetValue(13, row))) {
                    snapshot.json_fields.push_back(entry.GetValue<string>());
                }
            }
            if (!chunk->GetValue(14, row).IsNull()) {
                snapshot.proto_file = chunk->GetValue(14, row).GetValue<string>();
            }
            if (!chunk->GetValue(15, row).IsNull()) {
                snapshot.proto_message = chunk->GetValue(15, row).GetValue<string>();
            }
            if (!chunk->GetValue(16, row).IsNull()) {
                for (auto &entry : ListValue::GetChildren(chunk->GetValue(16, row))) {
                    snapshot.proto_fields.push_back(entry.GetValue<string>());
                }
            }
            snapshot.running = chunk->GetValue(17, row).GetValue<bool>();
            snapshot.stop_requested = chunk->GetValue(18, row).GetValue<bool>();
            snapshot.stopped = chunk->GetValue(19, row).GetValue<bool>();
            snapshot.failed = chunk->GetValue(20, row).GetValue<bool>();
            snapshot.paused = chunk->GetValue(21, row).GetValue<bool>();
            snapshot.pause_requested = chunk->GetValue(22, row).GetValue<bool>();
            snapshot.last_committed_seq = chunk->GetValue(23, row).GetValue<uint64_t>();
            snapshot.last_delivered_seq = chunk->GetValue(24, row).GetValue<uint64_t>();
            snapshot.rows_inserted = chunk->GetValue(25, row).GetValue<uint64_t>();
            snapshot.batches_committed = chunk->GetValue(26, row).GetValue<uint64_t>();
            snapshot.fetches_completed = chunk->GetValue(27, row).GetValue<uint64_t>();
            snapshot.last_batch_rows = chunk->GetValue(28, row).GetValue<uint64_t>();
            snapshot.sequence_lag = chunk->GetValue(29, row).GetValue<uint64_t>();
            if (!chunk->GetValue(30, row).IsNull()) {
                snapshot.last_start_time = chunk->GetValue(30, row).GetValue<timestamp_t>();
            }
            if (!chunk->GetValue(31, row).IsNull()) {
                snapshot.last_fetch_time = chunk->GetValue(31, row).GetValue<timestamp_t>();
            }
            if (!chunk->GetValue(32, row).IsNull()) {
                snapshot.last_ack_time = chunk->GetValue(32, row).GetValue<timestamp_t>();
            }
            if (!chunk->GetValue(33, row).IsNull()) {
                snapshot.last_commit_time = chunk->GetValue(33, row).GetValue<timestamp_t>();
            }
            if (!chunk->GetValue(34, row).IsNull()) {
                snapshot.last_error_time = chunk->GetValue(34, row).GetValue<timestamp_t>();
            }
            if (!chunk->GetValue(35, row).IsNull()) {
                snapshot.last_error = chunk->GetValue(35, row).GetValue<string>();
            }
            if (!chunk->GetValue(36, row).IsNull()) {
                snapshot.credentials_file = chunk->GetValue(36, row).GetValue<string>();
            }
            if (!chunk->GetValue(37, row).IsNull()) {
                snapshot.tls_ca_file = chunk->GetValue(37, row).GetValue<string>();
            }
            if (!chunk->GetValue(38, row).IsNull()) {
                snapshot.tls_cert_file = chunk->GetValue(38, row).GetValue<string>();
            }
            if (!chunk->GetValue(39, row).IsNull()) {
                snapshot.tls_key_file = chunk->GetValue(39, row).GetValue<string>();
            }
            if (!chunk->GetValue(40, row).IsNull()) {
                snapshot.tls_server_name = chunk->GetValue(40, row).GetValue<string>();
            }
            snapshot.tls_skip_verify = chunk->GetValue(41, row).GetValue<bool>();
            snapshots.push_back(std::move(snapshot));
        }
    }
    return snapshots;
}

static bool LoadCheckpoint(Connection &conn, const NatsIngestConfig &config, uint64_t &resume_seq,
                           NatsIngestProgress &progress) {
    std::ostringstream sql;
    sql << "SELECT last_committed_seq, last_delivered_seq, rows_inserted, batches_committed "
        << "FROM " << NATS_INGEST_CHECKPOINT_TABLE << " WHERE stream_name = "
        << SqlStringLiteral(config.stream_name) << " AND durable_name = " << SqlStringLiteral(config.durable_name);

    auto result = conn.Query(sql.str());
    if (result->HasError()) {
        throw std::runtime_error("Failed to load ingest checkpoint: " + result->GetError());
    }

    auto chunk = result->Fetch();
    if (!chunk || chunk->size() == 0) {
        resume_seq = config.start_seq;
        return false;
    }

    progress.last_committed_seq = chunk->GetValue(0, 0).GetValue<uint64_t>();
    progress.last_delivered_seq = chunk->GetValue(1, 0).GetValue<uint64_t>();
    progress.rows_inserted = chunk->GetValue(2, 0).GetValue<uint64_t>();
    progress.batches_committed = chunk->GetValue(3, 0).GetValue<uint64_t>();
    progress.checkpoint_seq = progress.last_committed_seq;
    resume_seq = progress.last_committed_seq + 1;
    return true;
}

static void UpsertCheckpoint(Connection &conn, const NatsIngestJobState &job, uint64_t committed_seq,
                             uint64_t delivered_seq, uint64_t rows_inserted, uint64_t batches_committed) {
    std::ostringstream sql;
    sql << "INSERT INTO " << NATS_INGEST_CHECKPOINT_TABLE
        << " (stream_name, durable_name, job_name, last_committed_seq, last_delivered_seq, rows_inserted, "
           "batches_committed, updated_at) VALUES ("
        << SqlStringLiteral(job.config.stream_name) << ", "
        << SqlStringLiteral(job.config.durable_name) << ", "
        << SqlStringLiteral(job.config.job_name) << ", "
        << committed_seq << ", "
        << delivered_seq << ", "
        << rows_inserted << ", "
        << batches_committed << ", CURRENT_TIMESTAMP"
        << ") ON CONFLICT(stream_name, durable_name) DO UPDATE SET "
        << "job_name = excluded.job_name, "
        << "last_committed_seq = excluded.last_committed_seq, "
        << "last_delivered_seq = excluded.last_delivered_seq, "
        << "rows_inserted = excluded.rows_inserted, "
        << "batches_committed = excluded.batches_committed, "
        << "updated_at = excluded.updated_at";
    ExecuteOrThrow(conn, sql.str(), "Failed to update ingest checkpoint");
}

static bool SubjectIsUnderStreamPattern(const string &subject, const char *stream_pattern) {
    if (stream_pattern == nullptr || subject.empty()) {
        return false;
    }
    string pattern(stream_pattern);
    if (pattern == ">") {
        return true;
    }
    if (pattern == subject) {
        return true;
    }
    if (pattern.size() >= 2 && pattern.substr(pattern.size() - 2) == ".>") {
        string prefix = pattern.substr(0, pattern.size() - 1);
        return subject.rfind(prefix, 0) == 0;
    }
    if (pattern.size() >= 2 && pattern.substr(pattern.size() - 2) == ".*") {
        string prefix = pattern.substr(0, pattern.size() - 1);
        return subject.rfind(prefix, 0) == 0 && subject.find('.', prefix.size()) == string::npos;
    }
    return false;
}

static bool CanUseServerSubjectFilter(const string &subject_filter, const jsStreamInfo *stream_info) {
    if (subject_filter.empty() || stream_info == nullptr || stream_info->Config == nullptr) {
        return false;
    }
    if (subject_filter.find('.') == string::npos && subject_filter.find('>') == string::npos &&
        subject_filter.find('*') == string::npos) {
        return false;
    }
    for (int i = 0; i < stream_info->Config->SubjectsLen; i++) {
        if (SubjectIsUnderStreamPattern(subject_filter, stream_info->Config->Subjects[i])) {
            return true;
        }
    }
    return false;
}

static void DisconnectJetStream(natsConnection **conn, jsCtx **js) {
    if (js && *js != nullptr) {
        jsCtx_Destroy(*js);
        *js = nullptr;
    }
    if (conn && *conn != nullptr) {
        natsConnection_Destroy(*conn);
        *conn = nullptr;
    }
}

static void ImportProtoSchema(const string &proto_file, const string &proto_message,
                              shared_ptr<DiskSourceTree> &source_tree, shared_ptr<ProtobufErrorCollector> &error_collector,
                              shared_ptr<Importer> &importer, const Descriptor *&descriptor) {
    source_tree = make_shared_ptr<DiskSourceTree>();

    std::filesystem::path proto_path(proto_file);
    string proto_dir = proto_path.parent_path().string();
    string proto_filename = proto_path.filename().string();

    if (proto_dir.empty()) {
        proto_dir = ".";
    }

    source_tree->MapPath("", proto_dir);

    error_collector = make_shared_ptr<ProtobufErrorCollector>();
    importer = make_shared_ptr<Importer>(source_tree.get(), error_collector.get());

    const FileDescriptor *file_desc = importer->Import(proto_filename);
    if (!file_desc) {
        string error_msg = "Failed to import protobuf schema file: " + proto_file;
        if (error_collector->HasErrors()) {
            error_msg += "\n" + error_collector->GetErrors();
        }
        throw std::runtime_error(error_msg);
    }

    descriptor = file_desc->FindMessageTypeByName(proto_message);
    if (!descriptor) {
        throw std::runtime_error("Message type '" + proto_message + "' not found in " + proto_file);
    }
}

static void ResolveProtoFieldPaths(const Descriptor *descriptor, const vector<string> &proto_fields,
                                   vector<vector<const FieldDescriptor *>> &resolved_paths) {
    resolved_paths.clear();
    for (const auto &field_path : proto_fields) {
        resolved_paths.push_back(ResolveProtobufFieldPath(descriptor, field_path));
    }
}

static NatsIngestConfig ParseStartConfig(TableFunctionBindInput &input) {
    NatsIngestConfig config;
    bool has_job_name = false;
    bool has_stream_name = false;
    bool has_target_table = false;
    bool has_durable_name = false;
    string subject_legacy;
    bool create_target_table = false;

    for (auto &kv : input.named_parameters) {
        if (kv.first == "job_name") {
            config.job_name = StringValue::Get(kv.second);
            has_job_name = true;
        } else if (kv.first == "stream_name") {
            config.stream_name = StringValue::Get(kv.second);
            has_stream_name = true;
        } else if (kv.first == "target_table") {
            config.target_table = StringValue::Get(kv.second);
            has_target_table = true;
        } else if (kv.first == "durable_name") {
            config.durable_name = StringValue::Get(kv.second);
            has_durable_name = true;
        } else if (ParseNatsConnectionParameter(config.connection, kv.first, kv.second)) {
        } else if (kv.first == "start_seq") {
            config.start_seq = UBigIntValue::Get(kv.second);
        } else if (kv.first == "batch_size") {
            config.batch_size = UBigIntValue::Get(kv.second);
        } else if (kv.first == "poll_ms") {
            config.poll_ms = BigIntValue::Get(kv.second);
        } else if (kv.first == "fetch_timeout_ms") {
            config.fetch_timeout_ms = BigIntValue::Get(kv.second);
        } else if (kv.first == "subject") {
            subject_legacy = StringValue::Get(kv.second);
        } else if (kv.first == "subject_contains") {
            config.subject_contains = StringValue::Get(kv.second);
        } else if (kv.first == "nats_subject") {
            config.nats_subject = StringValue::Get(kv.second);
        } else if (kv.first == "json_extract") {
            auto list_children = ListValue::GetChildren(kv.second);
            for (auto &child : list_children) {
                config.json_fields.push_back(StringValue::Get(child));
            }
        } else if (kv.first == "proto_file") {
            config.proto_file = StringValue::Get(kv.second);
        } else if (kv.first == "proto_message") {
            config.proto_message = StringValue::Get(kv.second);
        } else if (kv.first == "proto_extract") {
            auto list_children = ListValue::GetChildren(kv.second);
            for (auto &child : list_children) {
                config.proto_fields.push_back(StringValue::Get(child));
            }
        } else if (kv.first == "create_target_table") {
            create_target_table = BooleanValue::Get(kv.second);
        }
    }

    if (!subject_legacy.empty()) {
        if (!config.subject_contains.empty()) {
            throw std::runtime_error("Cannot use both subject and subject_contains parameters");
        }
        config.subject_contains = subject_legacy;
    }

    if (!has_job_name) {
        throw std::runtime_error("job_name parameter is required");
    }
    if (!has_stream_name) {
        throw std::runtime_error("stream_name parameter is required");
    }
    if (!has_target_table) {
        throw std::runtime_error("target_table parameter is required");
    }
    if (!has_durable_name) {
        throw std::runtime_error("durable_name parameter is required");
    }

    if (config.batch_size == 0 || config.batch_size > 65536) {
        throw std::runtime_error("batch_size must be between 1 and 65536");
    }
    if (config.poll_ms < 1) {
        throw std::runtime_error("poll_ms must be at least 1");
    }
    if (config.fetch_timeout_ms < 1) {
        throw std::runtime_error("fetch_timeout_ms must be at least 1");
    }
    if (!config.json_fields.empty() && !config.proto_fields.empty()) {
        throw std::runtime_error("Cannot use both json_extract and proto_extract parameters");
    }

    if (!config.proto_fields.empty()) {
        if (config.proto_file.empty()) {
            throw std::runtime_error("proto_file parameter is required when using proto_extract");
        }
        if (config.proto_message.empty()) {
            throw std::runtime_error("proto_message parameter is required when using proto_extract");
        }
    }

    config.create_target_table = create_target_table;
    ValidateNatsConnectionConfig(config.connection);

    return config;
}

static NatsIngestSnapshot SnapshotJob(const shared_ptr<NatsIngestJobState> &job) {
    NatsIngestSnapshot snapshot;
    lock_guard<std::mutex> guard(job->job_mutex);
    snapshot.job_name = job->config.job_name;
    snapshot.stream_name = job->config.stream_name;
    snapshot.target_table = job->config.target_table;
    snapshot.durable_name = job->config.durable_name;
    snapshot.nats_url = job->config.connection.url;
    snapshot.credentials_file = job->config.connection.credentials_file;
    snapshot.tls_ca_file = job->config.connection.tls_ca_file;
    snapshot.tls_cert_file = job->config.connection.tls_cert_file;
    snapshot.tls_key_file = job->config.connection.tls_key_file;
    snapshot.tls_server_name = job->config.connection.tls_server_name;
    snapshot.tls_skip_verify = job->config.connection.tls_skip_verify;
    snapshot.start_seq = job->config.start_seq;
    snapshot.batch_size = job->config.batch_size;
    snapshot.poll_ms = job->config.poll_ms;
    snapshot.fetch_timeout_ms = job->config.fetch_timeout_ms;
    snapshot.resume_from_checkpoint = job->config.resume_from_checkpoint;
    snapshot.create_target_table = job->config.create_target_table;
    snapshot.subject_contains = job->config.subject_contains;
    snapshot.nats_subject = job->config.nats_subject;
    snapshot.json_fields = job->config.json_fields;
    snapshot.proto_file = job->config.proto_file;
    snapshot.proto_message = job->config.proto_message;
    snapshot.proto_fields = job->config.proto_fields;
    snapshot.running = job->progress.running;
    snapshot.stop_requested = job->progress.stop_requested;
    snapshot.stopped = job->progress.stopped;
    snapshot.failed = job->progress.failed;
    snapshot.paused = job->progress.paused;
    snapshot.pause_requested = job->progress.pause_requested;
    snapshot.last_committed_seq = job->progress.last_committed_seq;
    snapshot.last_delivered_seq = job->progress.last_delivered_seq;
    snapshot.rows_inserted = job->progress.rows_inserted;
    snapshot.batches_committed = job->progress.batches_committed;
    snapshot.fetches_completed = job->progress.fetches_completed;
    snapshot.last_batch_rows = job->progress.last_batch_rows;
    snapshot.sequence_lag = snapshot.last_delivered_seq > snapshot.last_committed_seq
                                ? snapshot.last_delivered_seq - snapshot.last_committed_seq
                                : 0;
    snapshot.last_start_time = job->progress.last_start_time;
    snapshot.last_fetch_time = job->progress.last_fetch_time;
    snapshot.last_ack_time = job->progress.last_ack_time;
    snapshot.last_commit_time = job->progress.last_commit_time;
    snapshot.last_error_time = job->progress.last_error_time;
    snapshot.last_error = job->progress.last_error;
    return snapshot;
}

static NatsIngestConfig SnapshotToConfig(const NatsIngestSnapshot &snapshot) {
    NatsIngestConfig config;
    config.job_name = snapshot.job_name;
    config.stream_name = snapshot.stream_name;
    config.target_table = snapshot.target_table;
    config.durable_name = snapshot.durable_name;
    config.connection.url = snapshot.nats_url;
    config.connection.credentials_file = snapshot.credentials_file;
    config.connection.tls_ca_file = snapshot.tls_ca_file;
    config.connection.tls_cert_file = snapshot.tls_cert_file;
    config.connection.tls_key_file = snapshot.tls_key_file;
    config.connection.tls_server_name = snapshot.tls_server_name;
    config.connection.tls_skip_verify = snapshot.tls_skip_verify;
    config.start_seq = snapshot.start_seq;
    config.batch_size = snapshot.batch_size;
    config.poll_ms = snapshot.poll_ms;
    config.fetch_timeout_ms = snapshot.fetch_timeout_ms;
    config.resume_from_checkpoint = snapshot.resume_from_checkpoint;
    config.create_target_table = snapshot.create_target_table;
    config.subject_contains = snapshot.subject_contains;
    config.nats_subject = snapshot.nats_subject;
    config.json_fields = snapshot.json_fields;
    config.proto_file = snapshot.proto_file;
    config.proto_message = snapshot.proto_message;
    config.proto_fields = snapshot.proto_fields;
    return config;
}

static void RestoreJobProgress(const shared_ptr<NatsIngestJobState> &job, const NatsIngestSnapshot &snapshot) {
    lock_guard<std::mutex> guard(job->job_mutex);
    job->progress.pause_requested = snapshot.pause_requested;
    job->progress.paused = snapshot.paused;
    job->progress.last_committed_seq = std::max(job->progress.last_committed_seq, snapshot.last_committed_seq);
    job->progress.last_delivered_seq = std::max(job->progress.last_delivered_seq, snapshot.last_delivered_seq);
    job->progress.rows_inserted = std::max(job->progress.rows_inserted, snapshot.rows_inserted);
    job->progress.batches_committed = std::max(job->progress.batches_committed, snapshot.batches_committed);
    job->progress.fetches_completed = std::max(job->progress.fetches_completed, snapshot.fetches_completed);
    job->progress.last_batch_rows = snapshot.last_batch_rows;
    job->progress.checkpoint_seq = std::max(job->progress.checkpoint_seq, snapshot.last_committed_seq);
    job->progress.last_start_time = snapshot.last_start_time;
    job->progress.last_fetch_time = snapshot.last_fetch_time;
    job->progress.last_ack_time = snapshot.last_ack_time;
    job->progress.last_commit_time = snapshot.last_commit_time;
    job->progress.last_error_time = snapshot.last_error_time;
    job->progress.last_error = snapshot.last_error;
}

static void FillSnapshotColumns(DataChunk &output, idx_t row, const NatsIngestSnapshot &snapshot) {
    output.SetValue(0, row, Value(snapshot.job_name));
    output.SetValue(1, row, Value(snapshot.stream_name));
    output.SetValue(2, row, Value(snapshot.target_table));
    output.SetValue(3, row, Value(snapshot.durable_name));
    output.SetValue(4, row, Value::BOOLEAN(snapshot.running));
    output.SetValue(5, row, Value::BOOLEAN(snapshot.stop_requested));
    output.SetValue(6, row, Value::BOOLEAN(snapshot.stopped));
    output.SetValue(7, row, Value::BOOLEAN(snapshot.failed));
    output.SetValue(8, row, Value::BOOLEAN(snapshot.paused));
    output.SetValue(9, row, Value::BOOLEAN(snapshot.pause_requested));
    output.SetValue(10, row, Value::UBIGINT(snapshot.last_committed_seq));
    output.SetValue(11, row, Value::UBIGINT(snapshot.last_delivered_seq));
    output.SetValue(12, row, Value::UBIGINT(snapshot.rows_inserted));
    output.SetValue(13, row, Value::UBIGINT(snapshot.batches_committed));
    output.SetValue(14, row, Value::UBIGINT(snapshot.fetches_completed));
    output.SetValue(15, row, Value::UBIGINT(snapshot.last_batch_rows));
    output.SetValue(16, row, Value::UBIGINT(snapshot.sequence_lag));
    if (snapshot.last_start_time.value == 0) {
        FlatVector::SetNull(output.data[17], row, true);
    } else {
        output.SetValue(17, row, Value::TIMESTAMP(snapshot.last_start_time));
    }
    if (snapshot.last_fetch_time.value == 0) {
        FlatVector::SetNull(output.data[18], row, true);
    } else {
        output.SetValue(18, row, Value::TIMESTAMP(snapshot.last_fetch_time));
    }
    if (snapshot.last_ack_time.value == 0) {
        FlatVector::SetNull(output.data[19], row, true);
    } else {
        output.SetValue(19, row, Value::TIMESTAMP(snapshot.last_ack_time));
    }
    if (snapshot.last_commit_time.value == 0) {
        FlatVector::SetNull(output.data[20], row, true);
    } else {
        output.SetValue(20, row, Value::TIMESTAMP(snapshot.last_commit_time));
    }
    if (snapshot.last_error_time.value == 0) {
        FlatVector::SetNull(output.data[21], row, true);
    } else {
        output.SetValue(21, row, Value::TIMESTAMP(snapshot.last_error_time));
    }
    if (snapshot.last_error.empty()) {
        FlatVector::SetNull(output.data[22], row, true);
    } else {
        output.SetValue(22, row, Value(snapshot.last_error));
    }
}

static void AddSnapshotColumns(vector<LogicalType> &return_types, vector<string> &names) {
    names.emplace_back("job_name");
    return_types.emplace_back(LogicalType(LogicalTypeId::VARCHAR));
    names.emplace_back("stream_name");
    return_types.emplace_back(LogicalType(LogicalTypeId::VARCHAR));
    names.emplace_back("target_table");
    return_types.emplace_back(LogicalType(LogicalTypeId::VARCHAR));
    names.emplace_back("durable_name");
    return_types.emplace_back(LogicalType(LogicalTypeId::VARCHAR));
    names.emplace_back("running");
    return_types.emplace_back(LogicalType(LogicalTypeId::BOOLEAN));
    names.emplace_back("stop_requested");
    return_types.emplace_back(LogicalType(LogicalTypeId::BOOLEAN));
    names.emplace_back("stopped");
    return_types.emplace_back(LogicalType(LogicalTypeId::BOOLEAN));
    names.emplace_back("failed");
    return_types.emplace_back(LogicalType(LogicalTypeId::BOOLEAN));
    names.emplace_back("paused");
    return_types.emplace_back(LogicalType(LogicalTypeId::BOOLEAN));
    names.emplace_back("pause_requested");
    return_types.emplace_back(LogicalType(LogicalTypeId::BOOLEAN));
    names.emplace_back("last_committed_seq");
    return_types.emplace_back(LogicalType(LogicalTypeId::UBIGINT));
    names.emplace_back("last_delivered_seq");
    return_types.emplace_back(LogicalType(LogicalTypeId::UBIGINT));
    names.emplace_back("rows_inserted");
    return_types.emplace_back(LogicalType(LogicalTypeId::UBIGINT));
    names.emplace_back("batches_committed");
    return_types.emplace_back(LogicalType(LogicalTypeId::UBIGINT));
    names.emplace_back("fetches_completed");
    return_types.emplace_back(LogicalType(LogicalTypeId::UBIGINT));
    names.emplace_back("last_batch_rows");
    return_types.emplace_back(LogicalType(LogicalTypeId::UBIGINT));
    names.emplace_back("sequence_lag");
    return_types.emplace_back(LogicalType(LogicalTypeId::UBIGINT));
    names.emplace_back("last_start_time");
    return_types.emplace_back(LogicalType(LogicalTypeId::TIMESTAMP));
    names.emplace_back("last_fetch_time");
    return_types.emplace_back(LogicalType(LogicalTypeId::TIMESTAMP));
    names.emplace_back("last_ack_time");
    return_types.emplace_back(LogicalType(LogicalTypeId::TIMESTAMP));
    names.emplace_back("last_commit_time");
    return_types.emplace_back(LogicalType(LogicalTypeId::TIMESTAMP));
    names.emplace_back("last_error_time");
    return_types.emplace_back(LogicalType(LogicalTypeId::TIMESTAMP));
    names.emplace_back("last_error");
    return_types.emplace_back(LogicalType(LogicalTypeId::VARCHAR));
}

static void InitializeJobStateFromConfig(const shared_ptr<NatsIngestJobState> &job, ClientContext &context) {
    job->db = context.db;
}

static void InitializeJobStateFromDatabase(const shared_ptr<NatsIngestJobState> &job, DatabaseInstance &db) {
    Connection db_connection(db);
    job->db = db_connection.context->db;
}

static void InitializeIngestResources(const shared_ptr<NatsIngestJobState> &job) {
    auto &config = job->config;
    Connection db_connection(*job->db);
    EnsureCheckpointTable(db_connection);

    if (!config.proto_fields.empty() && config.proto_field_paths.empty()) {
        shared_ptr<DiskSourceTree> source_tree;
        shared_ptr<ProtobufErrorCollector> error_collector;
        shared_ptr<Importer> importer;
        const Descriptor *descriptor = nullptr;
        ImportProtoSchema(config.proto_file, config.proto_message, source_tree, error_collector, importer, descriptor);
        ResolveProtoFieldPaths(descriptor, config.proto_fields, config.proto_field_paths);
    }

    natsConnection *conn = nullptr;
    jsCtx *js = nullptr;
    ConnectNats(config.connection, &conn, 20);
    auto jetstream_status = natsConnection_JetStream(&js, conn, nullptr);
    if (jetstream_status != NATS_OK) {
        natsConnection_Destroy(conn);
        throw std::runtime_error(string("Failed to create JetStream context: ") + natsStatus_GetText(jetstream_status));
    }

    jsOptions stream_info_opts;
    jsOptions_Init(&stream_info_opts);
    bool can_use_deleted_details = config.subject_contains.empty() && config.nats_subject.empty();
    stream_info_opts.Stream.Info.DeletedDetails = can_use_deleted_details;
    if (!config.nats_subject.empty()) {
        stream_info_opts.Stream.Info.SubjectsFilter = config.nats_subject.c_str();
    }
    jsStreamInfo *stream_info = nullptr;
    natsStatus s = js_GetStreamInfo(&stream_info, js, config.stream_name.c_str(), &stream_info_opts, nullptr);
    if (s != NATS_OK) {
        DisconnectJetStream(&conn, &js);
        throw std::runtime_error(std::string("Failed to get stream info for '") + config.stream_name + "': " +
                                 natsStatus_GetText(s));
    }

    if (!config.nats_subject.empty() && !CanUseServerSubjectFilter(config.nats_subject, stream_info)) {
        jsStreamInfo_Destroy(stream_info);
        DisconnectJetStream(&conn, &js);
        throw std::runtime_error("nats_subject '" + config.nats_subject + "' is not covered by stream '" +
                                 config.stream_name + "' subject configuration");
    }

    string subscription_subject = config.nats_subject.empty() ? ">" : config.nats_subject;
    uint64_t effective_start_seq = config.start_seq;

    jsSubOptions sub_opts;
    jsSubOptions_Init(&sub_opts);
    sub_opts.Stream = config.stream_name.c_str();
    sub_opts.Config.DeliverPolicy = js_DeliverByStartSequence;
    sub_opts.Config.AckPolicy = js_AckExplicit;
    sub_opts.Config.ReplayPolicy = js_ReplayInstant;
    sub_opts.Config.InactiveThreshold = 60LL * 1000LL * 1000LL * 1000LL;

    if (config.resume_from_checkpoint) {
        uint64_t resume_seq = config.start_seq;
        {
            lock_guard<std::mutex> guard(job->job_mutex);
            LoadCheckpoint(db_connection, config, resume_seq, job->progress);
            job->progress.checkpoint_seq = job->progress.last_committed_seq;
        }
        effective_start_seq = std::max(config.start_seq, resume_seq);
    } else {
        lock_guard<std::mutex> guard(job->job_mutex);
        job->progress.checkpoint_seq = 0;
    }
    sub_opts.Config.OptStartSeq = effective_start_seq;

    jsErrCode js_err = static_cast<jsErrCode>(0);
    natsSubscription *sub = nullptr;
    s = js_PullSubscribe(&sub, js, subscription_subject.c_str(), nullptr, nullptr, &sub_opts, &js_err);
    if (s != NATS_OK) {
        jsStreamInfo_Destroy(stream_info);
        DisconnectJetStream(&conn, &js);
        throw std::runtime_error(std::string("Failed to create JetStream pull subscription for '") +
                                 config.stream_name + "': " + natsStatus_GetText(s));
    }
    jsStreamInfo_Destroy(stream_info);

    lock_guard<std::mutex> guard(job->job_mutex);
    job->conn = conn;
    job->js = js;
    job->sub = sub;
}

static shared_ptr<NatsIngestJobState> LaunchIngestJob(const NatsIngestConfig &config, ClientContext *context,
                                                      DatabaseInstance *db, const NatsIngestSnapshot *restore_snapshot = nullptr) {
    auto job = NatsIngestManager::Get().CreateJob(config);
    try {
        if (context) {
            InitializeJobStateFromConfig(job, *context);
        } else if (db) {
            InitializeJobStateFromDatabase(job, *db);
        } else {
            throw std::runtime_error("Missing database context for ingest job launch");
        }
        InitializeIngestResources(job);
        if (restore_snapshot != nullptr) {
            RestoreJobProgress(job, *restore_snapshot);
        }
        {
            Connection db_connection(*job->db);
            EnsureRegistryTable(db_connection);
            PersistRegistry(db_connection, job);
        }
        job->worker = std::thread([job]() { RunIngestWorker(job); });
        return job;
    } catch (...) {
        NatsIngestManager::Get().RemoveJob(job->config.job_name);
        throw;
    }
}

static void RehydrateActiveIngestJobs(DatabaseInstance &db) {
    Connection db_connection(db);
    EnsureRegistryTable(db_connection);
    auto snapshots = LoadRegistrySnapshots(db_connection);
    for (const auto &snapshot : snapshots) {
        if (!snapshot.running || snapshot.stopped || snapshot.failed) {
            continue;
        }
        if (NatsIngestManager::Get().GetJob(snapshot.job_name)) {
            continue;
        }
        auto config = SnapshotToConfig(snapshot);
        try {
            LaunchIngestJob(config, nullptr, &db, &snapshot);
        } catch (const std::exception &ex) {
            NatsIngestSnapshot failed_snapshot = snapshot;
            failed_snapshot.running = false;
            failed_snapshot.stopped = true;
            failed_snapshot.failed = true;
            failed_snapshot.last_error_time = Timestamp::GetCurrentTimestamp();
            failed_snapshot.last_error = string("Failed to rehydrate ingest job: ") + ex.what();
            try {
                UpsertRegistry(db_connection, failed_snapshot);
            } catch (...) {
            }
        }
    }
}

static string QuoteIdentifier(const string &identifier) {
    string result = "\"";
    for (char ch : identifier) {
        if (ch == '"') {
            result += "\"\"";
        } else {
            result += ch;
        }
    }
    result += "\"";
    return result;
}

static string ProtobufFieldDescriptorToSQLType(const FieldDescriptor *field) {
    if (field == nullptr || field->is_repeated()) {
        return "VARCHAR";
    }
    switch (field->type()) {
    case FieldDescriptor::TYPE_STRING:
        return "VARCHAR";
    case FieldDescriptor::TYPE_BYTES:
        return "BLOB";
    case FieldDescriptor::TYPE_INT32:
    case FieldDescriptor::TYPE_SINT32:
    case FieldDescriptor::TYPE_SFIXED32:
        return "INTEGER";
    case FieldDescriptor::TYPE_INT64:
    case FieldDescriptor::TYPE_SINT64:
    case FieldDescriptor::TYPE_SFIXED64:
        return "BIGINT";
    case FieldDescriptor::TYPE_UINT32:
    case FieldDescriptor::TYPE_FIXED32:
        return "UINTEGER";
    case FieldDescriptor::TYPE_UINT64:
    case FieldDescriptor::TYPE_FIXED64:
        return "UBIGINT";
    case FieldDescriptor::TYPE_FLOAT:
        return "FLOAT";
    case FieldDescriptor::TYPE_DOUBLE:
        return "DOUBLE";
    case FieldDescriptor::TYPE_BOOL:
        return "BOOLEAN";
    case FieldDescriptor::TYPE_ENUM:
    case FieldDescriptor::TYPE_MESSAGE:
    default:
        return "VARCHAR";
    }
}

struct NatsIngestColumnDef {
    string name;
    string sql_type;
};

static vector<NatsIngestColumnDef> BuildTargetTableColumns(const NatsIngestConfig &config) {
    vector<NatsIngestColumnDef> columns;
    columns.push_back({"stream_name", "VARCHAR"});
    columns.push_back({"subject", "VARCHAR"});
    columns.push_back({"sequence", "UBIGINT"});
    columns.push_back({"ts", "TIMESTAMP"});
    columns.push_back({"payload", config.json_fields.empty() ? "BLOB" : "VARCHAR"});

    for (const auto &field_name : config.json_fields) {
        columns.push_back({field_name, "VARCHAR"});
    }

    for (idx_t i = 0; i < config.proto_fields.size(); i++) {
        string column_name = config.proto_fields[i];
        std::replace(column_name.begin(), column_name.end(), '.', '_');
        string sql_type = "VARCHAR";
        if (i < config.proto_field_paths.size() && !config.proto_field_paths[i].empty()) {
            sql_type = ProtobufFieldDescriptorToSQLType(config.proto_field_paths[i].back());
        }
        columns.push_back({column_name, sql_type});
    }

    return columns;
}

static string BuildTargetTableCreateSql(const string &target_table, const NatsIngestConfig &config) {
    auto qualified_name = QualifiedName::Parse(target_table);
    if (!qualified_name.catalog.empty()) {
        throw std::runtime_error("auto-create target table does not support catalog-qualified names");
    }

    std::ostringstream sql;
    sql << "CREATE TABLE IF NOT EXISTS ";
    if (!qualified_name.schema.empty()) {
        sql << QuoteIdentifier(qualified_name.schema) << ".";
    }
    sql << QuoteIdentifier(qualified_name.name) << " (";

    auto columns = BuildTargetTableColumns(config);
    for (idx_t i = 0; i < columns.size(); i++) {
        if (i > 0) {
            sql << ", ";
        }
        sql << QuoteIdentifier(columns[i].name) << " " << columns[i].sql_type;
    }
    sql << ")";
    return sql.str();
}

static void EnsureTargetTable(Connection &conn, NatsIngestConfig &config) {
    try {
        (void)ResolveTargetTable(*conn.context, config.target_table);
        return;
    } catch (const std::exception &) {
    }

    if (!config.create_target_table) {
        throw std::runtime_error("Target table '" + config.target_table + "' does not exist");
    }

    ExecuteOrThrow(conn, BuildTargetTableCreateSql(config.target_table, config), "Failed to create ingest target table");
}

static TableCatalogEntry &ResolveTargetTable(ClientContext &context, const string &target_table) {
    auto qualified_name = QualifiedName::Parse(target_table);
    auto &entry = Catalog::GetEntry(context, CatalogType::TABLE_ENTRY, qualified_name.catalog, qualified_name.schema,
                                    qualified_name.name);
    return entry.Cast<TableCatalogEntry>();
}

static void AppendJsonFields(DataChunk &chunk, idx_t row_idx, const NatsIngestConfig &config,
                             const NatsPayloadView &payload, vector<Value> &values, bool reuse_values,
                             bool direct_write) {
    if (direct_write) {
        DecodeJsonFieldsToChunk(chunk, row_idx, 5, payload, config.json_fields);
        return;
    }
    vector<Value> allocated_values;
    auto &decode_values = reuse_values ? values : allocated_values;
    DecodeJsonFields(payload, config.json_fields, decode_values);
    for (idx_t i = 0; i < decode_values.size(); i++) {
        chunk.SetValue(5 + i, row_idx, decode_values[i]);
    }
}

static void AppendProtoFields(DataChunk &chunk, idx_t row_idx, const NatsIngestConfig &config, Message *proto_msg,
                              const NatsPayloadView &payload) {
    if (!proto_msg || !DecodeProtobufPayload(*proto_msg, payload)) {
        for (idx_t i = 0; i < config.proto_fields.size(); i++) {
            chunk.SetValue(5 + i, row_idx, Value());
        }
        return;
    }

    idx_t col_idx = 5;
    for (const auto &field_path : config.proto_field_paths) {
        chunk.SetValue(col_idx++, row_idx, ExtractProtobufValue(proto_msg, field_path));
    }
}

static void AppendMessageRow(DataChunk &chunk, idx_t row_idx, const NatsIngestConfig &config,
                             const NatsMessageEnvelope &envelope,
                             Message *proto_msg, vector<Value> &json_values, bool reuse_json_values,
                             bool direct_json_write) {
    const char *subject = envelope.Subject();
    uint64_t stream_seq = envelope.Sequence();
    int64_t timestamp_ns = envelope.TimestampNs();
    const char *msg_data = envelope.Data();
    int data_len = envelope.DataLength();
    NatsPayloadView payload {msg_data, static_cast<idx_t>(data_len)};
    bool payload_as_varchar = !config.json_fields.empty();

    chunk.SetValue(0, row_idx, Value(config.stream_name));
    chunk.SetValue(1, row_idx, Value(subject ? subject : ""));
    chunk.SetValue(2, row_idx, Value::UBIGINT(stream_seq));
    if (timestamp_ns > 0) {
        chunk.SetValue(3, row_idx, Value::TIMESTAMP(timestamp_t(timestamp_ns / 1000)));
    } else {
        chunk.SetValue(3, row_idx, Value());
    }

    if (payload_as_varchar) {
        chunk.SetValue(4, row_idx, Value(string(msg_data ? msg_data : "", data_len)));
    } else {
        auto blob_ptr = data_len > 0 && msg_data ? const_data_ptr_cast(msg_data) : nullptr;
        chunk.SetValue(4, row_idx, Value::BLOB(blob_ptr, data_len));
    }

    if (!config.json_fields.empty()) {
        AppendJsonFields(chunk, row_idx, config, payload, json_values, reuse_json_values, direct_json_write);
    } else if (!config.proto_fields.empty()) {
        AppendProtoFields(chunk, row_idx, config, proto_msg, payload);
    }
}

struct NatsIngestTiming {
    bool enabled = false;
    uint64_t fetch_ns = 0;
    uint64_t row_ns = 0;
    uint64_t append_ns = 0;
    uint64_t flush_ns = 0;
    uint64_t checkpoint_ns = 0;
    uint64_t commit_ns = 0;
    uint64_t persist_ns = 0;
    uint64_t batches = 0;

    void Report(const string &job_name, uint64_t rows) const {
        if (!enabled) {
            return;
        }
        auto milliseconds = [](uint64_t nanoseconds) {
            return static_cast<double>(nanoseconds) / 1000000.0;
        };
        std::fprintf(stderr,
                     "NATS_INGEST_PROFILE job=%s rows=%llu batches=%llu fetch_ms=%.3f row_ms=%.3f append_ms=%.3f "
                     "flush_ms=%.3f checkpoint_ms=%.3f commit_ms=%.3f persist_ms=%.3f\n",
                     job_name.c_str(), static_cast<unsigned long long>(rows),
                     static_cast<unsigned long long>(batches), milliseconds(fetch_ns), milliseconds(row_ns),
                     milliseconds(append_ns), milliseconds(flush_ns), milliseconds(checkpoint_ns),
                     milliseconds(commit_ns), milliseconds(persist_ns));
        std::fflush(stderr);
    }
};

static void RunIngestWorker(const shared_ptr<NatsIngestJobState> &job) {
    try {
        auto &config = job->config;
        const char *profile_env = std::getenv("NATS_INGEST_PROFILE");
        NatsIngestTiming timing;
        timing.enabled = profile_env && string(profile_env) != "0";
        uint64_t registry_persist_interval = 8;
        const char *registry_interval_env = std::getenv("NATS_INGEST_REGISTRY_INTERVAL");
        if (registry_interval_env != nullptr) {
            char *end = nullptr;
            auto parsed_interval = std::strtoull(registry_interval_env, &end, 10);
            if (end != registry_interval_env && *end == '\0' && parsed_interval > 0) {
                registry_persist_interval = parsed_interval;
            }
        }
        Connection db_connection(*job->db);
        const char *fail_after_commit_env = std::getenv("NATS_INGEST_FAIL_AFTER_COMMIT");
        bool inject_fail_after_commit = fail_after_commit_env != nullptr && string(fail_after_commit_env) != "0";
        bool injected_fail_after_commit = false;

        shared_ptr<DiskSourceTree> source_tree;
        shared_ptr<ProtobufErrorCollector> error_collector;
        shared_ptr<Importer> importer;
        const Descriptor *descriptor = nullptr;
        unique_ptr<Message> proto_template;
        shared_ptr<DynamicMessageFactory> proto_factory;

        if (!config.proto_fields.empty()) {
            ImportProtoSchema(config.proto_file, config.proto_message, source_tree, error_collector, importer, descriptor);
            if (config.proto_field_paths.empty()) {
                ResolveProtoFieldPaths(descriptor, config.proto_fields, config.proto_field_paths);
            }
            proto_factory = make_shared_ptr<DynamicMessageFactory>();
            proto_template.reset(proto_factory->GetPrototype(descriptor)->New());
        }

        {
            lock_guard<std::mutex> guard(job->job_mutex);
            job->progress.running = true;
            job->progress.stop_requested = false;
            job->progress.stopped = false;
            job->progress.failed = false;
            job->progress.last_error.clear();
            job->progress.last_start_time = Timestamp::GetCurrentTimestamp();
        }
        job->cv.notify_all();
        EnsureRegistryTable(db_connection);
        PersistRegistry(db_connection, job);
        EnsureActiveTransaction(db_connection);
        MarkTransactionWrite(db_connection);
        EnsureTargetTable(db_connection, config);

        auto &table = ResolveTargetTable(*db_connection.context, config.target_table);
        std::unique_ptr<InternalAppender> appender = make_uniq<InternalAppender>(*db_connection.context, table);

        DataChunk write_chunk;
        write_chunk.Initialize(Allocator::Get(*db_connection.context), appender->GetActiveTypes(), STANDARD_VECTOR_SIZE);
        idx_t write_row = 0;
        vector<Value> json_values;
        json_values.reserve(config.json_fields.size());
        const char *json_buffer_mode = std::getenv("NATS_INGEST_JSON_BUFFER_MODE");
        bool reuse_json_values = !json_buffer_mode || string(json_buffer_mode) != "allocate";
        const char *json_write_mode = std::getenv("NATS_INGEST_JSON_WRITE_MODE");
        bool direct_json_write = json_write_mode && string(json_write_mode) == "direct";

        natsSubscription *sub = nullptr;
        {
            lock_guard<std::mutex> guard(job->job_mutex);
            sub = job->sub;
        }
        if (!sub) {
            throw std::runtime_error("Ingest worker did not initialize a JetStream subscription");
        }
        NatsJetStreamBatchSource message_source(sub);

        while (true) {
            bool pause_requested = false;
            {
                lock_guard<std::mutex> guard(job->job_mutex);
                pause_requested = job->progress.pause_requested && !job->progress.stop_requested &&
                                  !job->progress.failed;
            }
            if (pause_requested) {
                bool updated_pause_state = false;
                {
                    lock_guard<std::mutex> guard(job->job_mutex);
                    if (!job->progress.paused) {
                        job->progress.paused = true;
                        updated_pause_state = true;
                    }
                }
                if (updated_pause_state) {
                    job->cv.notify_all();
                    try {
                        PersistRegistry(db_connection, job);
                    } catch (...) {
                    }
                }

                std::unique_lock<std::mutex> lock(job->job_mutex);
                job->cv.wait(lock, [&]() {
                    return !job->progress.pause_requested || job->progress.stop_requested ||
                           job->progress.failed || db_connection.context->IsInterrupted();
                });
                bool stop_or_failed = job->progress.stop_requested || job->progress.failed ||
                                      db_connection.context->IsInterrupted();
                bool paused_changed = job->progress.paused;
                job->progress.paused = false;
                lock.unlock();
                if (paused_changed) {
                    job->cv.notify_all();
                    try {
                        PersistRegistry(db_connection, job);
                    } catch (...) {
                    }
                }
                if (stop_or_failed) {
                    break;
                }
                continue;
            }

            bool stop_requested = false;
            {
                lock_guard<std::mutex> guard(job->job_mutex);
                stop_requested = job->progress.stop_requested;
            }
            if (stop_requested || db_connection.context->IsInterrupted()) {
                break;
            }

            auto phase_start = std::chrono::steady_clock::now();
            bool fetched = message_source.FetchBatch(config.batch_size, config.fetch_timeout_ms, config.stream_name,
                                                     false);
            timing.fetch_ns += static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - phase_start)
                    .count());
            if (!fetched) {
                if (config.poll_ms > 0) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(config.poll_ms));
                }
                continue;
            }

            {
                lock_guard<std::mutex> guard(job->job_mutex);
                job->progress.fetches_completed++;
                job->progress.last_fetch_time = Timestamp::GetCurrentTimestamp();
            }

            std::vector<natsMsg *> ack_msgs;
            ack_msgs.reserve(static_cast<size_t>(config.batch_size));
            uint64_t batch_last_delivered_seq = 0;
            string batch_stage = "begin";
            natsStatus s = NATS_OK;
            std::unordered_set<uint64_t> batch_seen_sequences;
            batch_seen_sequences.reserve(static_cast<size_t>(config.batch_size));

            try {
                batch_stage = "ensure transaction";
                EnsureActiveTransaction(db_connection);
                MarkTransactionWrite(db_connection);
                idx_t inserted_rows = 0;
                NatsIngestProgress progress_snapshot;
                {
                    lock_guard<std::mutex> guard(job->job_mutex);
                    progress_snapshot = job->progress;
                }

                while (message_source.HasBufferedMessages()) {
                    natsMsg *msg = nullptr;
                    if (!message_source.Next(&msg, config.batch_size, config.fetch_timeout_ms, config.stream_name)) {
                        break;
                    }
                    if (!msg) {
                        continue;
                    }

                    ack_msgs.push_back(msg);

                    NatsMessageEnvelope envelope(msg);
                    uint64_t msg_seq = envelope.Sequence();
                    batch_last_delivered_seq = std::max(batch_last_delivered_seq, msg_seq);
                    if (msg_seq != 0) {
                        if (msg_seq <= progress_snapshot.last_committed_seq) {
                            continue;
                        }
                        if (!batch_seen_sequences.insert(msg_seq).second) {
                            continue;
                        }
                    }

                    const char *subject = envelope.Subject();
                    if (!config.subject_contains.empty() &&
                        (subject == nullptr || string(subject).find(config.subject_contains) == string::npos)) {
                        lock_guard<std::mutex> guard(job->job_mutex);
                        job->progress.last_delivered_seq = std::max(job->progress.last_delivered_seq, msg_seq);
                        continue;
                    }

                    if (!proto_template) {
                        batch_stage = "append row";
                        phase_start = std::chrono::steady_clock::now();
                        AppendMessageRow(write_chunk, write_row, config, envelope, nullptr, json_values,
                                         reuse_json_values, direct_json_write);
                        timing.row_ns += static_cast<uint64_t>(
                            std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - phase_start)
                                .count());
                    } else {
                        std::unique_ptr<Message> row_proto(proto_template->New());
                        batch_stage = "append proto row";
                        phase_start = std::chrono::steady_clock::now();
                        AppendMessageRow(write_chunk, write_row, config, envelope, row_proto.get(), json_values,
                                         reuse_json_values, direct_json_write);
                        timing.row_ns += static_cast<uint64_t>(
                            std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - phase_start)
                                .count());
                    }
                    write_row++;
                    write_chunk.SetCardinality(write_row);
                    inserted_rows++;

                    if (write_row == write_chunk.GetCapacity()) {
                        batch_stage = "append chunk";
                        try {
                            phase_start = std::chrono::steady_clock::now();
                            appender->AppendDataChunk(write_chunk);
                            timing.append_ns += static_cast<uint64_t>(
                                std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - phase_start)
                                    .count());
                        } catch (const std::exception &ex) {
                            throw std::runtime_error(string("Failed to append ingest chunk: ") + ex.what());
                        }
                        write_chunk.Reset();
                        write_row = 0;
                    }
                }

                {
                    lock_guard<std::mutex> guard(job->job_mutex);
                    job->progress.last_delivered_seq = std::max(job->progress.last_delivered_seq, batch_last_delivered_seq);
                }

                if (inserted_rows == 0) {
                    batch_stage = "rollback empty batch";
                    try {
                        db_connection.Rollback();
                    } catch (const std::exception &ex) {
                        throw std::runtime_error(string("Failed to rollback empty ingest transaction: ") + ex.what());
                    }
                    for (auto *msg : ack_msgs) {
                        if (!msg) {
                            continue;
                        }
                        jsErrCode ack_err = static_cast<jsErrCode>(0);
                        s = natsMsg_AckSync(msg, nullptr, &ack_err);
                        if (s != NATS_OK) {
                            natsMsg_Destroy(msg);
                            throw std::runtime_error(std::string("Failed to ack JetStream message from '") +
                                                     config.stream_name + "': " + natsStatus_GetText(s));
                        }
                        natsMsg_Destroy(msg);
                    }
                    continue;
                }

                if (write_row > 0) {
                    batch_stage = "append final chunk";
                    try {
                        phase_start = std::chrono::steady_clock::now();
                        appender->AppendDataChunk(write_chunk);
                        timing.append_ns += static_cast<uint64_t>(
                            std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - phase_start)
                                .count());
                    } catch (const std::exception &ex) {
                        throw std::runtime_error(string("Failed to append final ingest chunk: ") + ex.what());
                    }
                    write_chunk.Reset();
                    write_row = 0;
                }

                if (inserted_rows > 0) {
                    batch_stage = "flush appender";
                    try {
                        phase_start = std::chrono::steady_clock::now();
                        appender->Flush();
                        timing.flush_ns += static_cast<uint64_t>(
                            std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - phase_start)
                                .count());
                    } catch (const std::exception &ex) {
                        throw std::runtime_error(string("Failed to flush ingest appender: ") + ex.what());
                    }
                }

                uint64_t committed_rows = progress_snapshot.rows_inserted + inserted_rows;
                uint64_t committed_batches = progress_snapshot.batches_committed + 1;
                uint64_t committed_fetches = progress_snapshot.fetches_completed;
                uint64_t committed_seq = batch_last_delivered_seq;
                if (committed_seq == 0) {
                    committed_seq = progress_snapshot.last_committed_seq;
                }
                if (batch_last_delivered_seq == 0) {
                    batch_last_delivered_seq = committed_seq;
                }

                try {
                    batch_stage = "write checkpoint";
                    phase_start = std::chrono::steady_clock::now();
                    UpsertCheckpoint(db_connection, *job, committed_seq, batch_last_delivered_seq, committed_rows,
                                     committed_batches);
                    timing.checkpoint_ns += static_cast<uint64_t>(
                        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - phase_start)
                            .count());
                } catch (const std::exception &ex) {
                    throw std::runtime_error(string("Failed to write ingest checkpoint: ") + ex.what());
                }
                try {
                    batch_stage = "commit transaction";
                    phase_start = std::chrono::steady_clock::now();
                    db_connection.Commit();
                    timing.commit_ns += static_cast<uint64_t>(
                        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - phase_start)
                            .count());
                } catch (const std::exception &ex) {
                    throw std::runtime_error(string("Failed to commit ingest transaction: ") + ex.what());
                }

                {
                    lock_guard<std::mutex> guard(job->job_mutex);
                    job->progress.rows_inserted = committed_rows;
                    job->progress.batches_committed = committed_batches;
                    job->progress.fetches_completed = committed_fetches;
                    job->progress.last_batch_rows = inserted_rows;
                    job->progress.last_committed_seq = committed_seq;
                    job->progress.last_delivered_seq = batch_last_delivered_seq;
                    job->progress.checkpoint_seq = committed_seq;
                    job->progress.last_commit_time = Timestamp::GetCurrentTimestamp();
                    job->cv.notify_all();
                }
                timing.batches++;
                if (committed_batches % registry_persist_interval == 0) {
                    phase_start = std::chrono::steady_clock::now();
                    PersistRegistry(db_connection, job);
                    timing.persist_ns += static_cast<uint64_t>(
                        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - phase_start)
                            .count());
                }
                timing.Report(config.job_name, committed_rows);

                if (inject_fail_after_commit && !injected_fail_after_commit) {
                    injected_fail_after_commit = true;
                    std::abort();
                }

                for (auto *msg : ack_msgs) {
                    if (!msg) {
                        continue;
                    }
                    jsErrCode ack_err = static_cast<jsErrCode>(0);
                    s = natsMsg_AckSync(msg, nullptr, &ack_err);
                    if (s != NATS_OK) {
                        natsMsg_Destroy(msg);
                        throw std::runtime_error(std::string("Failed to ack JetStream message from '") +
                                                 config.stream_name + "': " + natsStatus_GetText(s));
                    }
                    natsMsg_Destroy(msg);
                }
                {
                    lock_guard<std::mutex> guard(job->job_mutex);
                    job->progress.last_ack_time = Timestamp::GetCurrentTimestamp();
                    job->cv.notify_all();
                }

            } catch (const std::exception &ex) {
                try {
                    db_connection.Rollback();
                } catch (...) {
                }
                for (auto *msg : ack_msgs) {
                    if (msg) {
                        natsMsg_Destroy(msg);
                    }
                }
                throw std::runtime_error("Ingest failed at stage '" + batch_stage + "': " + ex.what());
            } catch (...) {
                try {
                    db_connection.Rollback();
                } catch (...) {
                }
                for (auto *msg : ack_msgs) {
                    if (msg) {
                        natsMsg_Destroy(msg);
                    }
                }
                throw std::runtime_error("Ingest failed at stage '" + batch_stage + "': unknown exception");
            }
        }

        {
            lock_guard<std::mutex> guard(job->job_mutex);
            job->progress.running = false;
            job->progress.stopped = true;
        }
        job->cv.notify_all();
        PersistRegistry(db_connection, job);
    } catch (const std::exception &ex) {
        {
            lock_guard<std::mutex> guard(job->job_mutex);
            job->progress.running = false;
            job->progress.stopped = true;
            job->progress.failed = true;
            job->progress.last_error_time = Timestamp::GetCurrentTimestamp();
            job->progress.last_error = ex.what();
        }
        job->cv.notify_all();
        if (job->db) {
            Connection db_connection(*job->db);
            try {
                EnsureRegistryTable(db_connection);
                PersistRegistry(db_connection, job);
            } catch (...) {
            }
        }
    } catch (...) {
        {
            lock_guard<std::mutex> guard(job->job_mutex);
            job->progress.running = false;
            job->progress.stopped = true;
            job->progress.failed = true;
            job->progress.last_error_time = Timestamp::GetCurrentTimestamp();
            job->progress.last_error = "Unknown ingest worker failure";
        }
        job->cv.notify_all();
        if (job->db) {
            Connection db_connection(*job->db);
            try {
                EnsureRegistryTable(db_connection);
                PersistRegistry(db_connection, job);
            } catch (...) {
            }
        }
    }
}

NatsIngestJobState::NatsIngestJobState(NatsIngestConfig config_p) : config(std::move(config_p)) {
}

NatsIngestJobState::~NatsIngestJobState() {
    {
        lock_guard<std::mutex> guard(job_mutex);
        progress.stop_requested = true;
    }
    cv.notify_all();
    if (worker.joinable() && worker.get_id() != std::this_thread::get_id()) {
        worker.join();
    }
    if (sub != nullptr) {
        natsSubscription_Unsubscribe(sub);
        natsSubscription_Destroy(sub);
        sub = nullptr;
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

NatsIngestManager &NatsIngestManager::Get() {
    static NatsIngestManager instance;
    return instance;
}

shared_ptr<NatsIngestJobState> NatsIngestManager::CreateJob(NatsIngestConfig config) {
    auto job = make_shared_ptr<NatsIngestJobState>(std::move(config));
    lock_guard<std::mutex> guard(mutex_);
    auto [it, inserted] = jobs_.emplace(job->config.job_name, job);
    if (!inserted) {
        throw std::runtime_error("Ingest job '" + job->config.job_name + "' already exists");
    }
    return job;
}

shared_ptr<NatsIngestJobState> NatsIngestManager::GetJob(const string &job_name) {
    lock_guard<std::mutex> guard(mutex_);
    auto it = jobs_.find(job_name);
    if (it == jobs_.end()) {
        return nullptr;
    }
    return it->second;
}

vector<shared_ptr<NatsIngestJobState>> NatsIngestManager::ListJobs() {
    lock_guard<std::mutex> guard(mutex_);
    vector<shared_ptr<NatsIngestJobState>> result;
    result.reserve(jobs_.size());
    for (auto &entry : jobs_) {
        result.push_back(entry.second);
    }
    return result;
}

bool NatsIngestManager::PauseJob(const string &job_name) {
    auto job = GetJob(job_name);
    if (!job) {
        return false;
    }
    {
        lock_guard<std::mutex> guard(job->job_mutex);
        job->progress.pause_requested = true;
    }
    job->cv.notify_all();
    return true;
}

bool NatsIngestManager::ResumeJob(const string &job_name) {
    auto job = GetJob(job_name);
    if (!job) {
        return false;
    }
    {
        lock_guard<std::mutex> guard(job->job_mutex);
        job->progress.pause_requested = false;
    }
    job->cv.notify_all();
    return true;
}

bool NatsIngestManager::StopJob(const string &job_name) {
    auto job = GetJob(job_name);
    if (!job) {
        return false;
    }
    {
        lock_guard<std::mutex> guard(job->job_mutex);
        job->progress.stop_requested = true;
    }
    job->cv.notify_all();
    return true;
}

bool NatsIngestManager::RemoveJob(const string &job_name) {
    lock_guard<std::mutex> guard(mutex_);
    return jobs_.erase(job_name) > 0;
}

static unique_ptr<FunctionData> NatsIngestStartBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names) {
    auto config = ParseStartConfig(input);

    if (!config.proto_fields.empty()) {
        shared_ptr<DiskSourceTree> source_tree;
        shared_ptr<ProtobufErrorCollector> error_collector;
        shared_ptr<Importer> importer;
        const Descriptor *descriptor = nullptr;
        ImportProtoSchema(config.proto_file, config.proto_message, source_tree, error_collector, importer, descriptor);
        ResolveProtoFieldPaths(descriptor, config.proto_fields, config.proto_field_paths);
    }

    AddSnapshotColumns(return_types, names);
    auto bind_data = make_uniq<NatsIngestStartBindData>();
    bind_data->config = std::move(config);
    return bind_data;
}

static unique_ptr<GlobalTableFunctionState> NatsIngestStartInitGlobal(ClientContext &context,
                                                                      TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<NatsIngestStartBindData>();
    auto job = LaunchIngestJob(bind_data.config, &context, nullptr);

    auto state = make_uniq<NatsIngestControlGlobalState>();
    state->jobs.push_back(job);
    return state;
}

static void NatsIngestStartExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &state = data_p.global_state->Cast<NatsIngestControlGlobalState>();
    if (state.done) {
        output.SetCardinality(0);
        return;
    }
    auto job = state.jobs[0];
    uint64_t initial_rows_inserted = 0;
    {
        std::unique_lock<std::mutex> lock(job->job_mutex);
        initial_rows_inserted = job->progress.rows_inserted;
        job->cv.wait_for(lock, std::chrono::seconds(5), [&]() {
            return job->progress.rows_inserted > initial_rows_inserted || job->progress.failed || job->progress.stopped;
        });
    }
    auto snapshot = SnapshotJob(job);
    FillSnapshotColumns(output, 0, snapshot);
    output.SetCardinality(1);
    state.done = true;
}

static unique_ptr<FunctionData> NatsIngestPauseBind(ClientContext &context, TableFunctionBindInput &input,
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
    AddSnapshotColumns(return_types, names);
    auto bind_data = make_uniq<NatsIngestPauseBindData>();
    bind_data->job_name = std::move(job_name);
    return bind_data;
}

static unique_ptr<GlobalTableFunctionState> NatsIngestPauseInitGlobal(ClientContext &context,
                                                                      TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<NatsIngestPauseBindData>();
    auto job = NatsIngestManager::Get().GetJob(bind_data.job_name);
    if (!job) {
        throw std::runtime_error("Ingest job '" + bind_data.job_name + "' does not exist or is not running");
    }
    {
        lock_guard<std::mutex> guard(job->job_mutex);
        if (!job->progress.running || job->progress.stopped || job->progress.failed) {
            throw std::runtime_error("Ingest job '" + bind_data.job_name + "' is not running");
        }
    }
    NatsIngestManager::Get().PauseJob(bind_data.job_name);
    auto state = make_uniq<NatsIngestControlGlobalState>();
    state->jobs.push_back(job);
    return state;
}

static void NatsIngestPauseExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &state = data_p.global_state->Cast<NatsIngestControlGlobalState>();
    if (state.done) {
        output.SetCardinality(0);
        return;
    }
    auto job = state.jobs[0];
    {
        std::unique_lock<std::mutex> lock(job->job_mutex);
        if (!job->cv.wait_for(lock, std::chrono::seconds(30), [&]() {
            return job->progress.paused || job->progress.failed || job->progress.stopped;
        })) {
            throw std::runtime_error("Timed out waiting for ingest job '" + job->config.job_name + "' to pause");
        }
        if (!job->progress.paused && !job->progress.failed && !job->progress.stopped) {
            throw std::runtime_error("Ingest job '" + job->config.job_name + "' did not pause");
        }
    }
    auto snapshot = SnapshotJob(job);
    FillSnapshotColumns(output, 0, snapshot);
    output.SetCardinality(1);
    state.done = true;
}

static unique_ptr<FunctionData> NatsIngestResumeBind(ClientContext &context, TableFunctionBindInput &input,
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
    AddSnapshotColumns(return_types, names);
    auto bind_data = make_uniq<NatsIngestResumeBindData>();
    bind_data->job_name = std::move(job_name);
    return bind_data;
}

static unique_ptr<GlobalTableFunctionState> NatsIngestResumeInitGlobal(ClientContext &context,
                                                                       TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<NatsIngestResumeBindData>();
    auto job = NatsIngestManager::Get().GetJob(bind_data.job_name);
    if (!job) {
        throw std::runtime_error("Ingest job '" + bind_data.job_name + "' does not exist or is not running");
    }
    {
        lock_guard<std::mutex> guard(job->job_mutex);
        if (!job->progress.running || job->progress.stopped || job->progress.failed) {
            throw std::runtime_error("Ingest job '" + bind_data.job_name + "' is not running");
        }
    }
    NatsIngestManager::Get().ResumeJob(bind_data.job_name);
    auto state = make_uniq<NatsIngestControlGlobalState>();
    state->jobs.push_back(job);
    return state;
}

static void NatsIngestResumeExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &state = data_p.global_state->Cast<NatsIngestControlGlobalState>();
    if (state.done) {
        output.SetCardinality(0);
        return;
    }
    auto job = state.jobs[0];
    {
        std::unique_lock<std::mutex> lock(job->job_mutex);
        if (!job->cv.wait_for(lock, std::chrono::seconds(30), [&]() {
            return !job->progress.paused || job->progress.failed || job->progress.stopped;
        })) {
            throw std::runtime_error("Timed out waiting for ingest job '" + job->config.job_name + "' to resume");
        }
        if (job->progress.paused && !job->progress.failed && !job->progress.stopped) {
            throw std::runtime_error("Ingest job '" + job->config.job_name + "' did not resume");
        }
    }
    auto snapshot = SnapshotJob(job);
    FillSnapshotColumns(output, 0, snapshot);
    output.SetCardinality(1);
    state.done = true;
}

static unique_ptr<FunctionData> NatsIngestStopBind(ClientContext &context, TableFunctionBindInput &input,
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
    AddSnapshotColumns(return_types, names);
    auto bind_data = make_uniq<NatsIngestStopBindData>();
    bind_data->job_name = std::move(job_name);
    return bind_data;
}

static unique_ptr<GlobalTableFunctionState> NatsIngestStopInitGlobal(ClientContext &context,
                                                                     TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<NatsIngestStopBindData>();
    auto job = NatsIngestManager::Get().GetJob(bind_data.job_name);
    if (!job) {
        throw std::runtime_error("Ingest job '" + bind_data.job_name + "' does not exist");
    }
    NatsIngestManager::Get().StopJob(bind_data.job_name);
    auto state = make_uniq<NatsIngestControlGlobalState>();
    state->jobs.push_back(job);
    return state;
}

static void NatsIngestStopExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &state = data_p.global_state->Cast<NatsIngestControlGlobalState>();
    if (state.done) {
        output.SetCardinality(0);
        return;
    }
    auto snapshot = SnapshotJob(state.jobs[0]);
    FillSnapshotColumns(output, 0, snapshot);
    output.SetCardinality(1);
    state.done = true;
}

static unique_ptr<FunctionData> NatsIngestStatusBind(ClientContext &context, TableFunctionBindInput &input,
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
    AddSnapshotColumns(return_types, names);
    auto bind_data = make_uniq<NatsIngestStatusBindData>();
    bind_data->job_name = std::move(job_name);
    return bind_data;
}

static unique_ptr<GlobalTableFunctionState> NatsIngestStatusInitGlobal(ClientContext &context,
                                                                       TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<NatsIngestStatusBindData>();
    auto job = NatsIngestManager::Get().GetJob(bind_data.job_name);
    auto state = make_uniq<NatsIngestControlGlobalState>();
    if (job) {
        state->jobs.push_back(job);
    } else {
        Connection db_connection(*context.db);
        EnsureRegistryTable(db_connection);
        NatsIngestSnapshot snapshot;
        if (!LoadRegistrySnapshot(db_connection, bind_data.job_name, snapshot)) {
            throw std::runtime_error("Ingest job '" + bind_data.job_name + "' does not exist");
        }
        state->registry_rows.push_back(snapshot);
    }
    return state;
}

static void NatsIngestStatusExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &state = data_p.global_state->Cast<NatsIngestControlGlobalState>();
    if (state.done) {
        output.SetCardinality(0);
        return;
    }
    NatsIngestSnapshot snapshot = state.jobs.empty() ? state.registry_rows[0] : SnapshotJob(state.jobs[0]);
    FillSnapshotColumns(output, 0, snapshot);
    output.SetCardinality(1);
    state.done = true;
}

static unique_ptr<FunctionData> NatsIngestJobsBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
    AddSnapshotColumns(return_types, names);
    return make_uniq<NatsIngestJobsBindData>();
}

static unique_ptr<GlobalTableFunctionState> NatsIngestJobsInitGlobal(ClientContext &context,
                                                                     TableFunctionInitInput &input) {
    auto state = make_uniq<NatsIngestControlGlobalState>();
    state->jobs = NatsIngestManager::Get().ListJobs();
    Connection db_connection(*context.db);
    EnsureRegistryTable(db_connection);
    state->registry_rows = LoadRegistrySnapshots(db_connection);
    return state;
}

static void NatsIngestJobsExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &state = data_p.global_state->Cast<NatsIngestControlGlobalState>();
    idx_t row = 0;
    while (row < STANDARD_VECTOR_SIZE && state.row_idx < state.jobs.size()) {
        auto snapshot = SnapshotJob(state.jobs[state.row_idx]);
        FillSnapshotColumns(output, row, snapshot);
        row++;
        state.row_idx++;
    }
    while (row < STANDARD_VECTOR_SIZE && state.row_idx < state.jobs.size() + state.registry_rows.size()) {
        idx_t registry_index = state.row_idx - state.jobs.size();
        auto &snapshot = state.registry_rows[registry_index];
        bool already_listed = false;
        for (idx_t i = 0; i < state.jobs.size(); i++) {
            if (state.jobs[i]->config.job_name == snapshot.job_name) {
                already_listed = true;
                break;
            }
        }
        state.row_idx++;
        if (already_listed) {
            continue;
        }
        FillSnapshotColumns(output, row, snapshot);
        row++;
    }
    output.SetCardinality(row);
}

void NatsIngestFunction::Register(ExtensionLoader &loader) {
    TableFunction start_fn("nats_start_ingest", {}, NatsIngestStartExecute, NatsIngestStartBind, NatsIngestStartInitGlobal);
    start_fn.named_parameters["job_name"] = LogicalType(LogicalTypeId::VARCHAR);
    start_fn.named_parameters["stream_name"] = LogicalType(LogicalTypeId::VARCHAR);
    start_fn.named_parameters["target_table"] = LogicalType(LogicalTypeId::VARCHAR);
    start_fn.named_parameters["durable_name"] = LogicalType(LogicalTypeId::VARCHAR);
    RegisterNatsConnectionParameters(start_fn);
    start_fn.named_parameters["start_seq"] = LogicalType(LogicalTypeId::UBIGINT);
    start_fn.named_parameters["batch_size"] = LogicalType(LogicalTypeId::UBIGINT);
    start_fn.named_parameters["poll_ms"] = LogicalType(LogicalTypeId::BIGINT);
    start_fn.named_parameters["fetch_timeout_ms"] = LogicalType(LogicalTypeId::BIGINT);
    start_fn.named_parameters["create_target_table"] = LogicalType(LogicalTypeId::BOOLEAN);
    start_fn.named_parameters["subject"] = LogicalType(LogicalTypeId::VARCHAR);
    start_fn.named_parameters["subject_contains"] = LogicalType(LogicalTypeId::VARCHAR);
    start_fn.named_parameters["nats_subject"] = LogicalType(LogicalTypeId::VARCHAR);
    start_fn.named_parameters["json_extract"] = LogicalType::LIST(LogicalType(LogicalTypeId::VARCHAR));
    start_fn.named_parameters["proto_file"] = LogicalType(LogicalTypeId::VARCHAR);
    start_fn.named_parameters["proto_message"] = LogicalType(LogicalTypeId::VARCHAR);
    start_fn.named_parameters["proto_extract"] = LogicalType::LIST(LogicalType(LogicalTypeId::VARCHAR));
    loader.RegisterFunction(start_fn);

    TableFunction stop_fn("nats_stop_ingest", {}, NatsIngestStopExecute, NatsIngestStopBind, NatsIngestStopInitGlobal);
    stop_fn.named_parameters["job_name"] = LogicalType(LogicalTypeId::VARCHAR);
    loader.RegisterFunction(stop_fn);

    TableFunction pause_fn("nats_pause_ingest", {}, NatsIngestPauseExecute, NatsIngestPauseBind, NatsIngestPauseInitGlobal);
    pause_fn.named_parameters["job_name"] = LogicalType(LogicalTypeId::VARCHAR);
    loader.RegisterFunction(pause_fn);

    TableFunction resume_fn("nats_resume_ingest", {}, NatsIngestResumeExecute, NatsIngestResumeBind,
                            NatsIngestResumeInitGlobal);
    resume_fn.named_parameters["job_name"] = LogicalType(LogicalTypeId::VARCHAR);
    loader.RegisterFunction(resume_fn);

    TableFunction status_fn("nats_ingest_status", {}, NatsIngestStatusExecute, NatsIngestStatusBind,
                            NatsIngestStatusInitGlobal);
    status_fn.named_parameters["job_name"] = LogicalType(LogicalTypeId::VARCHAR);
    loader.RegisterFunction(status_fn);

    TableFunction jobs_fn("nats_ingest_jobs", {}, NatsIngestJobsExecute, NatsIngestJobsBind, NatsIngestJobsInitGlobal);
    loader.RegisterFunction(jobs_fn);

    const char *disable_rehydrate_env = std::getenv("NATS_INGEST_DISABLE_REHYDRATE");
    bool disable_rehydrate = disable_rehydrate_env != nullptr && string(disable_rehydrate_env) != "0";
    if (!disable_rehydrate) {
        RehydrateActiveIngestJobs(loader.GetDatabaseInstance());
    }
}

} // namespace duckdb
