#include "nats_ingest.hpp"
#include "nats_message_decode.hpp"
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
#include "yyjson.hpp"
#include <algorithm>
#include <chrono>
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

using namespace duckdb_yyjson;
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

struct NatsIngestStatusBindData : public TableFunctionData {
    string job_name;
};

struct NatsIngestJobsBindData : public TableFunctionData {
};

struct NatsIngestControlGlobalState : public GlobalTableFunctionState {
    bool done = false;
    idx_t row_idx = 0;
    vector<shared_ptr<NatsIngestJobState>> jobs;

    idx_t MaxThreads() const override {
        return 1;
    }
};

struct NatsIngestSnapshot {
    string job_name;
    string stream_name;
    string target_table;
    string durable_name;
    bool running = false;
    bool stop_requested = false;
    bool stopped = false;
    bool failed = false;
    uint64_t last_committed_seq = 0;
    uint64_t last_delivered_seq = 0;
    uint64_t rows_inserted = 0;
    uint64_t batches_committed = 0;
    uint64_t sequence_lag = 0;
    timestamp_t last_start_time;
    timestamp_t last_commit_time;
    timestamp_t last_error_time;
    string last_error;
};

static constexpr const char *NATS_INGEST_CHECKPOINT_TABLE = "duckdb_nats_ingest_checkpoints";

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

static void ConnectJetStream(const string &nats_url, natsConnection **conn, jsCtx **js) {
    constexpr idx_t MAX_CONNECT_ATTEMPTS = 20;
    natsStatus last_status = NATS_OK;
    for (idx_t attempt = 0; attempt < MAX_CONNECT_ATTEMPTS; attempt++) {
        natsOptions *opts = nullptr;
        natsStatus s = natsOptions_Create(&opts);
        if (s != NATS_OK) {
            throw std::runtime_error(std::string("Failed to create NATS options: ") + natsStatus_GetText(s));
        }

        s = natsOptions_SetTimeout(opts, 5000);
        if (s != NATS_OK) {
            natsOptions_Destroy(opts);
            throw std::runtime_error(std::string("Failed to set NATS timeout: ") + natsStatus_GetText(s));
        }

        s = natsOptions_SetURL(opts, nats_url.c_str());
        if (s != NATS_OK) {
            natsOptions_Destroy(opts);
            throw std::runtime_error(std::string("Failed to set NATS URL: ") + natsStatus_GetText(s));
        }

        s = natsConnection_Connect(conn, opts);
        natsOptions_Destroy(opts);
        if (s == NATS_OK) {
            s = natsConnection_JetStream(js, *conn, nullptr);
            if (s == NATS_OK) {
                return;
            }
            natsConnection_Destroy(*conn);
            *conn = nullptr;
            last_status = s;
        } else {
            last_status = s;
        }

        if (attempt + 1 < MAX_CONNECT_ATTEMPTS) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }

    throw std::runtime_error(std::string("Failed to connect to NATS: ") + natsStatus_GetText(last_status));
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
        } else if (kv.first == "url") {
            config.nats_url = StringValue::Get(kv.second);
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

    return config;
}

static NatsIngestSnapshot SnapshotJob(const shared_ptr<NatsIngestJobState> &job) {
    NatsIngestSnapshot snapshot;
    lock_guard<std::mutex> guard(job->job_mutex);
    snapshot.job_name = job->config.job_name;
    snapshot.stream_name = job->config.stream_name;
    snapshot.target_table = job->config.target_table;
    snapshot.durable_name = job->config.durable_name;
    snapshot.running = job->progress.running;
    snapshot.stop_requested = job->progress.stop_requested;
    snapshot.stopped = job->progress.stopped;
    snapshot.failed = job->progress.failed;
    snapshot.last_committed_seq = job->progress.last_committed_seq;
    snapshot.last_delivered_seq = job->progress.last_delivered_seq;
    snapshot.rows_inserted = job->progress.rows_inserted;
    snapshot.batches_committed = job->progress.batches_committed;
    snapshot.sequence_lag = snapshot.last_delivered_seq > snapshot.last_committed_seq
                                ? snapshot.last_delivered_seq - snapshot.last_committed_seq
                                : 0;
    snapshot.last_start_time = job->progress.last_start_time;
    snapshot.last_commit_time = job->progress.last_commit_time;
    snapshot.last_error_time = job->progress.last_error_time;
    snapshot.last_error = job->progress.last_error;
    return snapshot;
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
    output.SetValue(8, row, Value::UBIGINT(snapshot.last_committed_seq));
    output.SetValue(9, row, Value::UBIGINT(snapshot.last_delivered_seq));
    output.SetValue(10, row, Value::UBIGINT(snapshot.rows_inserted));
    output.SetValue(11, row, Value::UBIGINT(snapshot.batches_committed));
    output.SetValue(12, row, Value::UBIGINT(snapshot.sequence_lag));
    if (snapshot.last_start_time.value == 0) {
        FlatVector::SetNull(output.data[13], row, true);
    } else {
        output.SetValue(13, row, Value::TIMESTAMP(snapshot.last_start_time));
    }
    if (snapshot.last_commit_time.value == 0) {
        FlatVector::SetNull(output.data[14], row, true);
    } else {
        output.SetValue(14, row, Value::TIMESTAMP(snapshot.last_commit_time));
    }
    if (snapshot.last_error_time.value == 0) {
        FlatVector::SetNull(output.data[15], row, true);
    } else {
        output.SetValue(15, row, Value::TIMESTAMP(snapshot.last_error_time));
    }
    if (snapshot.last_error.empty()) {
        FlatVector::SetNull(output.data[16], row, true);
    } else {
        output.SetValue(16, row, Value(snapshot.last_error));
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
    names.emplace_back("last_committed_seq");
    return_types.emplace_back(LogicalType(LogicalTypeId::UBIGINT));
    names.emplace_back("last_delivered_seq");
    return_types.emplace_back(LogicalType(LogicalTypeId::UBIGINT));
    names.emplace_back("rows_inserted");
    return_types.emplace_back(LogicalType(LogicalTypeId::UBIGINT));
    names.emplace_back("batches_committed");
    return_types.emplace_back(LogicalType(LogicalTypeId::UBIGINT));
    names.emplace_back("sequence_lag");
    return_types.emplace_back(LogicalType(LogicalTypeId::UBIGINT));
    names.emplace_back("last_start_time");
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

static void InitializeIngestResources(const shared_ptr<NatsIngestJobState> &job) {
    auto &config = job->config;
    Connection db_connection(*job->db);
    EnsureCheckpointTable(db_connection);

    natsConnection *conn = nullptr;
    jsCtx *js = nullptr;
    ConnectJetStream(config.nats_url, &conn, &js);

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

static TableCatalogEntry &ResolveTargetTable(ClientContext &context, const string &target_table) {
    auto qualified_name = QualifiedName::Parse(target_table);
    auto &entry = Catalog::GetEntry(context, CatalogType::TABLE_ENTRY, qualified_name.catalog, qualified_name.schema,
                                    qualified_name.name);
    return entry.Cast<TableCatalogEntry>();
}

static uint64_t GetMessageSequence(natsMsg *msg) {
    uint64_t stream_seq = natsMsg_GetSequence(msg);
    if (stream_seq != 0) {
        return stream_seq;
    }

    jsMsgMetaData *meta = nullptr;
    if (natsMsg_GetMetaData(&meta, msg) == NATS_OK && meta != nullptr) {
        stream_seq = meta->Sequence.Stream;
        jsMsgMetaData_Destroy(meta);
    }
    return stream_seq;
}

static void AppendJsonFields(DataChunk &chunk, idx_t row_idx, const NatsIngestConfig &config, const char *msg_data,
                             int data_len) {
    yyjson_doc *doc = yyjson_read(msg_data, data_len, 0);
    if (!doc) {
        for (idx_t i = 0; i < config.json_fields.size(); i++) {
            chunk.SetValue(5 + i, row_idx, Value());
        }
        return;
    }

    yyjson_val *root = yyjson_doc_get_root(doc);
    idx_t col_idx = 5;
    for (const auto &field_name : config.json_fields) {
        yyjson_val *field_val = yyjson_obj_get(root, field_name.c_str());
        if (!field_val) {
            chunk.SetValue(col_idx++, row_idx, Value());
        } else if (yyjson_is_str(field_val)) {
            chunk.SetValue(col_idx++, row_idx, Value(yyjson_get_str(field_val)));
        } else if (yyjson_is_num(field_val)) {
            chunk.SetValue(col_idx++, row_idx, Value(yyjson_get_num(field_val)));
        } else if (yyjson_is_bool(field_val)) {
            chunk.SetValue(col_idx++, row_idx, Value::BOOLEAN(yyjson_get_bool(field_val)));
        } else if (yyjson_is_null(field_val)) {
            chunk.SetValue(col_idx++, row_idx, Value());
        } else {
            char *json_str = yyjson_val_write(field_val, 0, nullptr);
            if (json_str) {
                chunk.SetValue(col_idx++, row_idx, Value(json_str));
                free(json_str);
            } else {
                chunk.SetValue(col_idx++, row_idx, Value());
            }
        }
    }

    yyjson_doc_free(doc);
}

static void AppendProtoFields(DataChunk &chunk, idx_t row_idx, const NatsIngestConfig &config, Message *proto_msg,
                              const char *msg_data, int data_len) {
    if (!proto_msg || !proto_msg->ParseFromArray(msg_data, data_len)) {
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

static void AppendMessageRow(DataChunk &chunk, idx_t row_idx, const NatsIngestConfig &config, natsMsg *msg,
                             Message *proto_msg) {
    const char *subject = natsMsg_GetSubject(msg);
    uint64_t stream_seq = GetMessageSequence(msg);
    int64_t timestamp_ns = 0;

    if (timestamp_ns == 0) {
        jsMsgMetaData *meta = nullptr;
        if (natsMsg_GetMetaData(&meta, msg) == NATS_OK && meta != nullptr) {
            timestamp_ns = meta->Timestamp;
            jsMsgMetaData_Destroy(meta);
        }
    }
    if (timestamp_ns == 0) {
        timestamp_ns = natsMsg_GetTime(msg);
    }

    const char *msg_data = natsMsg_GetData(msg);
    int data_len = natsMsg_GetDataLength(msg);
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
        AppendJsonFields(chunk, row_idx, config, msg_data, data_len);
    } else if (!config.proto_fields.empty()) {
        AppendProtoFields(chunk, row_idx, config, proto_msg, msg_data, data_len);
    }
}

static void RunIngestWorker(const shared_ptr<NatsIngestJobState> &job) {
    try {
        auto &config = job->config;
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

        EnsureActiveTransaction(db_connection);
        MarkTransactionWrite(db_connection);
        auto &table = ResolveTargetTable(*db_connection.context, config.target_table);
        std::unique_ptr<InternalAppender> appender = make_uniq<InternalAppender>(*db_connection.context, table);

        DataChunk write_chunk;
        write_chunk.Initialize(Allocator::Get(*db_connection.context), appender->GetActiveTypes(), STANDARD_VECTOR_SIZE);
        idx_t write_row = 0;

        natsSubscription *sub = nullptr;
        {
            lock_guard<std::mutex> guard(job->job_mutex);
            sub = job->sub;
        }
        if (!sub) {
            throw std::runtime_error("Ingest worker did not initialize a JetStream subscription");
        }

        while (true) {
            bool stop_requested = false;
            {
                lock_guard<std::mutex> guard(job->job_mutex);
                stop_requested = job->progress.stop_requested;
            }
            if (stop_requested || db_connection.context->IsInterrupted()) {
                break;
            }

            natsMsgList fetched_msgs {nullptr, 0};
            jsFetchRequest request;
            natsStatus s = jsFetchRequest_Init(&request);
            if (s != NATS_OK) {
                throw std::runtime_error(std::string("Failed to initialize JetStream fetch request: ") +
                                         natsStatus_GetText(s));
            }
            request.Batch = static_cast<int>(config.batch_size);
            request.Expires = config.fetch_timeout_ms * 1000LL * 1000LL;
            request.NoWait = false;

            s = natsSubscription_FetchRequest(&fetched_msgs, sub, &request);
            if (s == NATS_TIMEOUT || s == NATS_NOT_FOUND) {
                natsMsgList_Destroy(&fetched_msgs);
                if (config.poll_ms > 0) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(config.poll_ms));
                }
                continue;
            }
            if (s != NATS_OK) {
                natsMsgList_Destroy(&fetched_msgs);
                throw std::runtime_error(std::string("Failed to fetch JetStream message batch from '") +
                                         config.stream_name + "': " + natsStatus_GetText(s));
            }
            if (fetched_msgs.Count == 0) {
                natsMsgList_Destroy(&fetched_msgs);
                if (config.poll_ms > 0) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(config.poll_ms));
                }
                continue;
            }

            std::vector<natsMsg *> ack_msgs;
            ack_msgs.reserve(fetched_msgs.Count);
            uint64_t batch_last_delivered_seq = 0;
            string batch_stage = "begin";
            std::unordered_set<uint64_t> batch_seen_sequences;
            batch_seen_sequences.reserve(static_cast<size_t>(fetched_msgs.Count));

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

                for (int i = 0; i < fetched_msgs.Count; i++) {
                    natsMsg *msg = fetched_msgs.Msgs[i];
                    fetched_msgs.Msgs[i] = nullptr;
                    if (!msg) {
                        continue;
                    }

                    ack_msgs.push_back(msg);

                    uint64_t msg_seq = GetMessageSequence(msg);
                    batch_last_delivered_seq = std::max(batch_last_delivered_seq, msg_seq);
                    if (msg_seq != 0) {
                        if (msg_seq <= progress_snapshot.last_committed_seq) {
                            continue;
                        }
                        if (!batch_seen_sequences.insert(msg_seq).second) {
                            continue;
                        }
                    }

                    const char *subject = natsMsg_GetSubject(msg);
                    if (!config.subject_contains.empty() &&
                        (subject == nullptr || string(subject).find(config.subject_contains) == string::npos)) {
                        lock_guard<std::mutex> guard(job->job_mutex);
                        job->progress.last_delivered_seq = std::max(job->progress.last_delivered_seq, msg_seq);
                        continue;
                    }

                    if (!proto_template) {
                        batch_stage = "append row";
                        AppendMessageRow(write_chunk, write_row, config, msg, nullptr);
                    } else {
                        std::unique_ptr<Message> row_proto(proto_template->New());
                        batch_stage = "append proto row";
                        AppendMessageRow(write_chunk, write_row, config, msg, row_proto.get());
                    }
                    write_row++;
                    write_chunk.SetCardinality(write_row);
                    inserted_rows++;

                    if (write_row == write_chunk.GetCapacity()) {
                        batch_stage = "append chunk";
                        try {
                            appender->AppendDataChunk(write_chunk);
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
                    natsMsgList_Destroy(&fetched_msgs);
                    continue;
                }

                if (write_row > 0) {
                    batch_stage = "append final chunk";
                    try {
                        appender->AppendDataChunk(write_chunk);
                    } catch (const std::exception &ex) {
                        throw std::runtime_error(string("Failed to append final ingest chunk: ") + ex.what());
                    }
                    write_chunk.Reset();
                    write_row = 0;
                }

                if (inserted_rows > 0) {
                    batch_stage = "flush appender";
                    try {
                        appender->Flush();
                    } catch (const std::exception &ex) {
                        throw std::runtime_error(string("Failed to flush ingest appender: ") + ex.what());
                    }
                }

                uint64_t committed_rows = progress_snapshot.rows_inserted + inserted_rows;
                uint64_t committed_batches = progress_snapshot.batches_committed + 1;
                uint64_t committed_seq = batch_last_delivered_seq;
                if (committed_seq == 0) {
                    committed_seq = progress_snapshot.last_committed_seq;
                }
                if (batch_last_delivered_seq == 0) {
                    batch_last_delivered_seq = committed_seq;
                }

                try {
                    batch_stage = "commit transaction";
                    db_connection.Commit();
                } catch (const std::exception &ex) {
                    throw std::runtime_error(string("Failed to commit ingest transaction: ") + ex.what());
                }
                try {
                    batch_stage = "write checkpoint";
                    UpsertCheckpoint(db_connection, *job, committed_seq, batch_last_delivered_seq, committed_rows,
                                     committed_batches);
                } catch (const std::exception &ex) {
                    throw std::runtime_error(string("Failed to write ingest checkpoint: ") + ex.what());
                }

                {
                    lock_guard<std::mutex> guard(job->job_mutex);
                    job->progress.rows_inserted = committed_rows;
                    job->progress.batches_committed = committed_batches;
                    job->progress.last_committed_seq = committed_seq;
                    job->progress.last_delivered_seq = batch_last_delivered_seq;
                    job->progress.checkpoint_seq = committed_seq;
                    job->progress.last_commit_time = Timestamp::GetCurrentTimestamp();
                    job->cv.notify_all();
                }

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
                natsMsgList_Destroy(&fetched_msgs);
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
                natsMsgList_Destroy(&fetched_msgs);
                throw std::runtime_error("Ingest failed at stage '" + batch_stage + "': unknown exception");
            }

            natsMsgList_Destroy(&fetched_msgs);
        }

        {
            lock_guard<std::mutex> guard(job->job_mutex);
            job->progress.running = false;
            job->progress.stopped = true;
        }
        job->cv.notify_all();
    } catch (const std::exception &ex) {
        lock_guard<std::mutex> guard(job->job_mutex);
        job->progress.running = false;
        job->progress.stopped = true;
        job->progress.failed = true;
        job->progress.last_error_time = Timestamp::GetCurrentTimestamp();
        job->progress.last_error = ex.what();
        job->cv.notify_all();
    } catch (...) {
        lock_guard<std::mutex> guard(job->job_mutex);
        job->progress.running = false;
        job->progress.stopped = true;
        job->progress.failed = true;
        job->progress.last_error_time = Timestamp::GetCurrentTimestamp();
        job->progress.last_error = "Unknown ingest worker failure";
        job->cv.notify_all();
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
    auto job = NatsIngestManager::Get().CreateJob(bind_data.config);
    InitializeJobStateFromConfig(job, context);
    InitializeIngestResources(job);
    job->worker = std::thread([job]() { RunIngestWorker(job); });

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
    if (!job) {
        throw std::runtime_error("Ingest job '" + bind_data.job_name + "' does not exist");
    }
    auto state = make_uniq<NatsIngestControlGlobalState>();
    state->jobs.push_back(job);
    return state;
}

static void NatsIngestStatusExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
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

static unique_ptr<FunctionData> NatsIngestJobsBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
    AddSnapshotColumns(return_types, names);
    return make_uniq<NatsIngestJobsBindData>();
}

static unique_ptr<GlobalTableFunctionState> NatsIngestJobsInitGlobal(ClientContext &context,
                                                                     TableFunctionInitInput &input) {
    auto state = make_uniq<NatsIngestControlGlobalState>();
    state->jobs = NatsIngestManager::Get().ListJobs();
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
    output.SetCardinality(row);
}

void NatsIngestFunction::Register(ExtensionLoader &loader) {
    TableFunction start_fn("nats_start_ingest", {}, NatsIngestStartExecute, NatsIngestStartBind, NatsIngestStartInitGlobal);
    start_fn.named_parameters["job_name"] = LogicalType(LogicalTypeId::VARCHAR);
    start_fn.named_parameters["stream_name"] = LogicalType(LogicalTypeId::VARCHAR);
    start_fn.named_parameters["target_table"] = LogicalType(LogicalTypeId::VARCHAR);
    start_fn.named_parameters["durable_name"] = LogicalType(LogicalTypeId::VARCHAR);
    start_fn.named_parameters["url"] = LogicalType(LogicalTypeId::VARCHAR);
    start_fn.named_parameters["start_seq"] = LogicalType(LogicalTypeId::UBIGINT);
    start_fn.named_parameters["batch_size"] = LogicalType(LogicalTypeId::UBIGINT);
    start_fn.named_parameters["poll_ms"] = LogicalType(LogicalTypeId::BIGINT);
    start_fn.named_parameters["fetch_timeout_ms"] = LogicalType(LogicalTypeId::BIGINT);
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

    TableFunction status_fn("nats_ingest_status", {}, NatsIngestStatusExecute, NatsIngestStatusBind,
                            NatsIngestStatusInitGlobal);
    status_fn.named_parameters["job_name"] = LogicalType(LogicalTypeId::VARCHAR);
    loader.RegisterFunction(status_fn);

    TableFunction jobs_fn("nats_ingest_jobs", {}, NatsIngestJobsExecute, NatsIngestJobsBind, NatsIngestJobsInitGlobal);
    loader.RegisterFunction(jobs_fn);
}

} // namespace duckdb
