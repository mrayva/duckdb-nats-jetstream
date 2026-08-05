#include "nats_kv.hpp"
#include "nats_connection.hpp"
#include "nats_duckdb_compat.hpp"

#include "duckdb/function/copy_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parsed_data/copy_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include <nats/nats.h>

namespace duckdb {

namespace {

// Owns the connection/JetStream/KV-store handles for one call and releases them
// (in reverse order) however the call exits, success or exception.
struct NatsKvHandle {
    natsConnection *conn = nullptr;
    jsCtx *js = nullptr;
    kvStore *kv = nullptr;

    ~NatsKvHandle() {
        if (kv != nullptr) {
            kvStore_Destroy(kv);
        }
        if (js != nullptr) {
            jsCtx_Destroy(js);
        }
        if (conn != nullptr) {
            natsConnection_Destroy(conn);
        }
    }
};

void OpenKvStore(const NatsConnectionConfig &connection, const string &bucket, NatsKvHandle &handle) {
    ConnectJetStream(connection, &handle.conn, &handle.js);
    auto status = js_KeyValue(&handle.kv, handle.js, bucket.c_str());
    if (status != NATS_OK) {
        throw std::runtime_error("Failed to open KV bucket '" + bucket + "': " + natsStatus_GetText(status));
    }
}

void SetEntryTimestampNs(DataChunk &output, idx_t col_idx, idx_t row_idx, int64_t timestamp_ns) {
    if (timestamp_ns <= 0) {
        FlatVector::SetNull(output.data[col_idx], row_idx, true);
        return;
    }
    output.SetValue(col_idx, row_idx, Value::TIMESTAMP(timestamp_t(timestamp_ns / 1000)));
}

string KvOperationName(kvOperation op) {
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

constexpr int64_t NATS_KV_LIST_TIMEOUT_MS = 5000;

NatsConnectionConfig ParseKvConnectionParameters(const named_parameter_map_t &named_parameters) {
    NatsConnectionConfig connection;
    for (auto &kv : named_parameters) {
        ParseNatsConnectionParameter(connection, string(kv.first), kv.second);
    }
    ValidateNatsConnectionConfig(connection);
    return connection;
}

struct NatsKvOneRowGlobalState : public GlobalTableFunctionState {
    bool done = false;

    idx_t MaxThreads() const override {
        return 1;
    }
};

unique_ptr<GlobalTableFunctionState> NatsKvOneRowInitGlobal(ClientContext &, TableFunctionInitInput &) {
    return make_uniq<NatsKvOneRowGlobalState>();
}

// --------------------------------------------------------------------------
// nats_kv_get(bucket, key)
// --------------------------------------------------------------------------

struct NatsKvGetBindData : public TableFunctionData {
    string bucket;
    string key;
    NatsConnectionConfig connection;
};

unique_ptr<FunctionData> NatsKvGetBind(ClientContext &, TableFunctionBindInput &input,
                                       vector<LogicalType> &return_types, vector<string> &names) {
    auto result = make_uniq<NatsKvGetBindData>();
    result->bucket = input.inputs[0].GetValue<string>();
    result->key = input.inputs[1].GetValue<string>();
    result->connection = ParseKvConnectionParameters(input.named_parameters);

    names = {"bucket", "key", "value", "revision", "created", "operation"};
    return_types = {LogicalType(LogicalTypeId::VARCHAR), LogicalType(LogicalTypeId::VARCHAR),
                    LogicalType(LogicalTypeId::BLOB),    LogicalType(LogicalTypeId::UBIGINT),
                    LogicalType(LogicalTypeId::TIMESTAMP), LogicalType(LogicalTypeId::VARCHAR)};
    return result;
}

void NatsKvGetExecute(ClientContext &, TableFunctionInput &data_p, DataChunk &output) {
    auto &bind_data = data_p.bind_data->Cast<NatsKvGetBindData>();
    auto &state = data_p.global_state->Cast<NatsKvOneRowGlobalState>();
    if (state.done) {
        output.SetCardinality(0);
        return;
    }
    state.done = true;

    NatsKvHandle handle;
    OpenKvStore(bind_data.connection, bind_data.bucket, handle);

    kvEntry *entry = nullptr;
    auto status = kvStore_Get(&entry, handle.kv, bind_data.key.c_str());
    if (status == NATS_NOT_FOUND) {
        output.SetCardinality(0);
        return;
    }
    if (status != NATS_OK) {
        throw std::runtime_error("Failed to get key '" + bind_data.key + "' from bucket '" + bind_data.bucket +
                                 "': " + natsStatus_GetText(status));
    }

    output.SetValue(0, 0, Value(bind_data.bucket));
    output.SetValue(1, 0, Value(bind_data.key));
    output.SetValue(2, 0, Value::BLOB(const_data_ptr_cast(kvEntry_Value(entry)),
                                      static_cast<idx_t>(kvEntry_ValueLen(entry))));
    output.SetValue(3, 0, Value::UBIGINT(kvEntry_Revision(entry)));
    SetEntryTimestampNs(output, 4, 0, kvEntry_Created(entry));
    output.SetValue(5, 0, Value(KvOperationName(kvEntry_Operation(entry))));
    kvEntry_Destroy(entry);

    output.SetCardinality(1);
}

// --------------------------------------------------------------------------
// Shared row storage for nats_kv_scan / nats_kv_history: both fetch a bounded
// set of entries once (in InitGlobal) and paginate the in-memory result across
// Execute calls, so the NATS connection doesn't need to stay open for the
// duration of the scan.
// --------------------------------------------------------------------------

struct NatsKvRow {
    string key;
    string value;
    uint64_t revision = 0;
    int64_t created_ns = 0;
    kvOperation operation = kvOp_Unknown;
};

struct NatsKvRowsGlobalState : public GlobalTableFunctionState {
    vector<NatsKvRow> rows;
    idx_t next_index = 0;

    idx_t MaxThreads() const override {
        return 1;
    }
};

void FillNatsKvRowsOutput(DataChunk &output, NatsKvRowsGlobalState &state, const string &bucket) {
    idx_t row = 0;
    while (row < STANDARD_VECTOR_SIZE && state.next_index < state.rows.size()) {
        auto &entry = state.rows[state.next_index];
        output.SetValue(0, row, Value(bucket));
        output.SetValue(1, row, Value(entry.key));
        output.SetValue(2, row, Value::BLOB(const_data_ptr_cast(entry.value.data()), entry.value.size()));
        output.SetValue(3, row, Value::UBIGINT(entry.revision));
        SetEntryTimestampNs(output, 4, row, entry.created_ns);
        output.SetValue(5, row, Value(KvOperationName(entry.operation)));
        row++;
        state.next_index++;
    }
    output.SetCardinality(row);
}

// --------------------------------------------------------------------------
// nats_kv_scan(bucket, key_filter := '*')
// --------------------------------------------------------------------------

struct NatsKvScanBindData : public TableFunctionData {
    string bucket;
    string key_filter;
    NatsConnectionConfig connection;
};

unique_ptr<FunctionData> NatsKvScanBind(ClientContext &, TableFunctionBindInput &input,
                                        vector<LogicalType> &return_types, vector<string> &names) {
    auto result = make_uniq<NatsKvScanBindData>();
    result->bucket = input.inputs[0].GetValue<string>();

    for (auto &kv : input.named_parameters) {
        if (kv.first == "key_filter") {
            result->key_filter = StringValue::Get(kv.second);
        } else {
            ParseNatsConnectionParameter(result->connection, string(kv.first), kv.second);
        }
    }
    ValidateNatsConnectionConfig(result->connection);

    names = {"bucket", "key", "value", "revision", "created", "operation"};
    return_types = {LogicalType(LogicalTypeId::VARCHAR), LogicalType(LogicalTypeId::VARCHAR),
                    LogicalType(LogicalTypeId::BLOB),    LogicalType(LogicalTypeId::UBIGINT),
                    LogicalType(LogicalTypeId::TIMESTAMP), LogicalType(LogicalTypeId::VARCHAR)};
    return result;
}

unique_ptr<GlobalTableFunctionState> NatsKvScanInitGlobal(ClientContext &, TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<NatsKvScanBindData>();
    auto state = make_uniq<NatsKvRowsGlobalState>();

    NatsKvHandle handle;
    OpenKvStore(bind_data.connection, bind_data.bucket, handle);

    // A filter that matches no keys (e.g. a NATS subject-wildcard pattern that
    // doesn't correspond to how these keys are actually structured) can otherwise
    // block indefinitely: kvStore_Keys/KeysWithFilters wait on a server-side
    // watcher, and without a bounded Timeout there is no default cutoff.
    kvWatchOptions opts;
    kvWatchOptions_Init(&opts);
    opts.Timeout = NATS_KV_LIST_TIMEOUT_MS;

    kvKeysList keys_list{};
    natsStatus status;
    if (bind_data.key_filter.empty()) {
        status = kvStore_Keys(&keys_list, handle.kv, &opts);
    } else {
        const char *filters[1] = {bind_data.key_filter.c_str()};
        status = kvStore_KeysWithFilters(&keys_list, handle.kv, filters, 1, &opts);
    }
    if (status != NATS_OK && status != NATS_NOT_FOUND && status != NATS_TIMEOUT) {
        throw std::runtime_error("Failed to list keys in bucket '" + bind_data.bucket +
                                 "': " + natsStatus_GetText(status));
    }

    vector<string> keys;
    if (status == NATS_OK) {
        keys.reserve(static_cast<size_t>(keys_list.Count));
        for (int i = 0; i < keys_list.Count; i++) {
            keys.emplace_back(keys_list.Keys[i]);
        }
        kvKeysList_Destroy(&keys_list);
    }

    state->rows.reserve(keys.size());
    for (auto &key : keys) {
        kvEntry *entry = nullptr;
        auto get_status = kvStore_Get(&entry, handle.kv, key.c_str());
        if (get_status == NATS_NOT_FOUND) {
            // Deleted between the key listing and this fetch; skip it rather than error.
            continue;
        }
        if (get_status != NATS_OK) {
            throw std::runtime_error("Failed to get key '" + key + "' from bucket '" + bind_data.bucket +
                                     "': " + natsStatus_GetText(get_status));
        }
        NatsKvRow row;
        row.key = key;
        row.value.assign(static_cast<const char *>(kvEntry_Value(entry)),
                         static_cast<size_t>(kvEntry_ValueLen(entry)));
        row.revision = kvEntry_Revision(entry);
        row.created_ns = kvEntry_Created(entry);
        row.operation = kvEntry_Operation(entry);
        kvEntry_Destroy(entry);
        state->rows.push_back(std::move(row));
    }

    return std::move(state);
}

void NatsKvScanExecute(ClientContext &, TableFunctionInput &data_p, DataChunk &output) {
    auto &bind_data = data_p.bind_data->Cast<NatsKvScanBindData>();
    auto &state = data_p.global_state->Cast<NatsKvRowsGlobalState>();
    FillNatsKvRowsOutput(output, state, bind_data.bucket);
}

// --------------------------------------------------------------------------
// nats_kv_history(bucket, key)
// --------------------------------------------------------------------------

struct NatsKvHistoryBindData : public TableFunctionData {
    string bucket;
    string key;
    NatsConnectionConfig connection;
};

unique_ptr<FunctionData> NatsKvHistoryBind(ClientContext &, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names) {
    auto result = make_uniq<NatsKvHistoryBindData>();
    result->bucket = input.inputs[0].GetValue<string>();
    result->key = input.inputs[1].GetValue<string>();
    result->connection = ParseKvConnectionParameters(input.named_parameters);

    names = {"bucket", "key", "value", "revision", "created", "operation"};
    return_types = {LogicalType(LogicalTypeId::VARCHAR), LogicalType(LogicalTypeId::VARCHAR),
                    LogicalType(LogicalTypeId::BLOB),    LogicalType(LogicalTypeId::UBIGINT),
                    LogicalType(LogicalTypeId::TIMESTAMP), LogicalType(LogicalTypeId::VARCHAR)};
    return result;
}

unique_ptr<GlobalTableFunctionState> NatsKvHistoryInitGlobal(ClientContext &, TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<NatsKvHistoryBindData>();
    auto state = make_uniq<NatsKvRowsGlobalState>();

    NatsKvHandle handle;
    OpenKvStore(bind_data.connection, bind_data.bucket, handle);

    kvWatchOptions opts;
    kvWatchOptions_Init(&opts);
    opts.Timeout = NATS_KV_LIST_TIMEOUT_MS;

    kvEntryList entries_list{};
    auto status = kvStore_History(&entries_list, handle.kv, bind_data.key.c_str(), &opts);
    if (status == NATS_NOT_FOUND || status == NATS_TIMEOUT) {
        return std::move(state);
    }
    if (status != NATS_OK) {
        throw std::runtime_error("Failed to get history for key '" + bind_data.key + "' in bucket '" +
                                 bind_data.bucket + "': " + natsStatus_GetText(status));
    }

    state->rows.reserve(static_cast<size_t>(entries_list.Count));
    for (int i = 0; i < entries_list.Count; i++) {
        auto *entry = entries_list.Entries[i];
        NatsKvRow row;
        row.key = bind_data.key;
        row.value.assign(static_cast<const char *>(kvEntry_Value(entry)),
                         static_cast<size_t>(kvEntry_ValueLen(entry)));
        row.revision = kvEntry_Revision(entry);
        row.created_ns = kvEntry_Created(entry);
        row.operation = kvEntry_Operation(entry);
        state->rows.push_back(std::move(row));
    }
    // Destroys the list array and every contained kvEntry.
    kvEntryList_Destroy(&entries_list);

    return std::move(state);
}

void NatsKvHistoryExecute(ClientContext &, TableFunctionInput &data_p, DataChunk &output) {
    auto &bind_data = data_p.bind_data->Cast<NatsKvHistoryBindData>();
    auto &state = data_p.global_state->Cast<NatsKvRowsGlobalState>();
    FillNatsKvRowsOutput(output, state, bind_data.bucket);
}

// --------------------------------------------------------------------------
// nats_kv_put(bucket, key, value) / nats_kv_create(bucket, key, value)
// --------------------------------------------------------------------------

struct NatsKvWriteBindData : public TableFunctionData {
    string bucket;
    string key;
    string value;
    NatsConnectionConfig connection;
};

unique_ptr<FunctionData> NatsKvWriteBind(ClientContext &, TableFunctionBindInput &input,
                                         vector<LogicalType> &return_types, vector<string> &names) {
    auto result = make_uniq<NatsKvWriteBindData>();
    result->bucket = input.inputs[0].GetValue<string>();
    result->key = input.inputs[1].GetValue<string>();
    result->value = input.inputs[2].GetValue<string>();
    result->connection = ParseKvConnectionParameters(input.named_parameters);

    names = {"bucket", "key", "revision"};
    return_types = {LogicalType(LogicalTypeId::VARCHAR), LogicalType(LogicalTypeId::VARCHAR),
                    LogicalType(LogicalTypeId::UBIGINT)};
    return result;
}

void NatsKvPutExecute(ClientContext &, TableFunctionInput &data_p, DataChunk &output) {
    auto &bind_data = data_p.bind_data->Cast<NatsKvWriteBindData>();
    auto &state = data_p.global_state->Cast<NatsKvOneRowGlobalState>();
    if (state.done) {
        output.SetCardinality(0);
        return;
    }
    state.done = true;

    NatsKvHandle handle;
    OpenKvStore(bind_data.connection, bind_data.bucket, handle);

    uint64_t revision = 0;
    auto status = kvStore_PutString(&revision, handle.kv, bind_data.key.c_str(), bind_data.value.c_str());
    if (status != NATS_OK) {
        throw std::runtime_error("Failed to put key '" + bind_data.key + "' in bucket '" + bind_data.bucket +
                                 "': " + natsStatus_GetText(status));
    }

    output.SetValue(0, 0, Value(bind_data.bucket));
    output.SetValue(1, 0, Value(bind_data.key));
    output.SetValue(2, 0, Value::UBIGINT(revision));
    output.SetCardinality(1);
}

void NatsKvCreateExecute(ClientContext &, TableFunctionInput &data_p, DataChunk &output) {
    auto &bind_data = data_p.bind_data->Cast<NatsKvWriteBindData>();
    auto &state = data_p.global_state->Cast<NatsKvOneRowGlobalState>();
    if (state.done) {
        output.SetCardinality(0);
        return;
    }
    state.done = true;

    NatsKvHandle handle;
    OpenKvStore(bind_data.connection, bind_data.bucket, handle);

    uint64_t revision = 0;
    auto status = kvStore_CreateString(&revision, handle.kv, bind_data.key.c_str(), bind_data.value.c_str());
    if (status != NATS_OK) {
        throw std::runtime_error("Failed to create key '" + bind_data.key + "' in bucket '" + bind_data.bucket +
                                 "' (it may already exist; use nats_kv_put or nats_kv_update instead): " +
                                 natsStatus_GetText(status));
    }

    output.SetValue(0, 0, Value(bind_data.bucket));
    output.SetValue(1, 0, Value(bind_data.key));
    output.SetValue(2, 0, Value::UBIGINT(revision));
    output.SetCardinality(1);
}

// --------------------------------------------------------------------------
// nats_kv_update(bucket, key, value, last_revision)
// --------------------------------------------------------------------------

struct NatsKvUpdateBindData : public TableFunctionData {
    string bucket;
    string key;
    string value;
    uint64_t last_revision = 0;
    NatsConnectionConfig connection;
};

unique_ptr<FunctionData> NatsKvUpdateBind(ClientContext &, TableFunctionBindInput &input,
                                          vector<LogicalType> &return_types, vector<string> &names) {
    auto result = make_uniq<NatsKvUpdateBindData>();
    result->bucket = input.inputs[0].GetValue<string>();
    result->key = input.inputs[1].GetValue<string>();
    result->value = input.inputs[2].GetValue<string>();
    auto last_revision = input.inputs[3].GetValue<int64_t>();
    if (last_revision < 0) {
        throw std::runtime_error("last_revision must not be negative");
    }
    result->last_revision = static_cast<uint64_t>(last_revision);
    result->connection = ParseKvConnectionParameters(input.named_parameters);

    names = {"bucket", "key", "revision"};
    return_types = {LogicalType(LogicalTypeId::VARCHAR), LogicalType(LogicalTypeId::VARCHAR),
                    LogicalType(LogicalTypeId::UBIGINT)};
    return result;
}

void NatsKvUpdateExecute(ClientContext &, TableFunctionInput &data_p, DataChunk &output) {
    auto &bind_data = data_p.bind_data->Cast<NatsKvUpdateBindData>();
    auto &state = data_p.global_state->Cast<NatsKvOneRowGlobalState>();
    if (state.done) {
        output.SetCardinality(0);
        return;
    }
    state.done = true;

    NatsKvHandle handle;
    OpenKvStore(bind_data.connection, bind_data.bucket, handle);

    uint64_t revision = 0;
    auto status = kvStore_UpdateString(&revision, handle.kv, bind_data.key.c_str(), bind_data.value.c_str(),
                                       bind_data.last_revision);
    if (status != NATS_OK) {
        throw std::runtime_error("Failed to update key '" + bind_data.key + "' in bucket '" + bind_data.bucket +
                                 "' at expected revision " + std::to_string(bind_data.last_revision) +
                                 " (it may have changed; re-read with nats_kv_get and retry): " +
                                 natsStatus_GetText(status));
    }

    output.SetValue(0, 0, Value(bind_data.bucket));
    output.SetValue(1, 0, Value(bind_data.key));
    output.SetValue(2, 0, Value::UBIGINT(revision));
    output.SetCardinality(1);
}

// --------------------------------------------------------------------------
// nats_kv_delete(bucket, key, purge := false)
// --------------------------------------------------------------------------

struct NatsKvDeleteBindData : public TableFunctionData {
    string bucket;
    string key;
    bool purge = false;
    NatsConnectionConfig connection;
};

unique_ptr<FunctionData> NatsKvDeleteBind(ClientContext &, TableFunctionBindInput &input,
                                          vector<LogicalType> &return_types, vector<string> &names) {
    auto result = make_uniq<NatsKvDeleteBindData>();
    result->bucket = input.inputs[0].GetValue<string>();
    result->key = input.inputs[1].GetValue<string>();

    for (auto &kv : input.named_parameters) {
        if (kv.first == "purge") {
            result->purge = BooleanValue::Get(kv.second);
        } else {
            ParseNatsConnectionParameter(result->connection, string(kv.first), kv.second);
        }
    }
    ValidateNatsConnectionConfig(result->connection);

    names = {"bucket", "key", "deleted"};
    return_types = {LogicalType(LogicalTypeId::VARCHAR), LogicalType(LogicalTypeId::VARCHAR),
                    LogicalType(LogicalTypeId::BOOLEAN)};
    return result;
}

void NatsKvDeleteExecute(ClientContext &, TableFunctionInput &data_p, DataChunk &output) {
    auto &bind_data = data_p.bind_data->Cast<NatsKvDeleteBindData>();
    auto &state = data_p.global_state->Cast<NatsKvOneRowGlobalState>();
    if (state.done) {
        output.SetCardinality(0);
        return;
    }
    state.done = true;

    NatsKvHandle handle;
    OpenKvStore(bind_data.connection, bind_data.bucket, handle);

    natsStatus status;
    if (bind_data.purge) {
        status = kvStore_Purge(handle.kv, bind_data.key.c_str(), nullptr);
    } else {
        status = kvStore_Delete(handle.kv, bind_data.key.c_str());
    }
    if (status != NATS_OK) {
        throw std::runtime_error("Failed to delete key '" + bind_data.key + "' from bucket '" + bind_data.bucket +
                                 "': " + natsStatus_GetText(status));
    }

    output.SetValue(0, 0, Value(bind_data.bucket));
    output.SetValue(1, 0, Value(bind_data.key));
    output.SetValue(2, 0, Value::BOOLEAN(true));
    output.SetCardinality(1);
}

// --------------------------------------------------------------------------
// nats_kv_status(bucket)
// --------------------------------------------------------------------------

struct NatsKvStatusBindData : public TableFunctionData {
    string bucket;
    NatsConnectionConfig connection;
};

unique_ptr<FunctionData> NatsKvStatusBind(ClientContext &, TableFunctionBindInput &input,
                                          vector<LogicalType> &return_types, vector<string> &names) {
    auto result = make_uniq<NatsKvStatusBindData>();
    result->bucket = input.inputs[0].GetValue<string>();
    result->connection = ParseKvConnectionParameters(input.named_parameters);

    names = {"bucket", "values", "history", "ttl_seconds", "replicas", "bytes"};
    return_types = {LogicalType(LogicalTypeId::VARCHAR), LogicalType(LogicalTypeId::UBIGINT),
                    LogicalType(LogicalTypeId::BIGINT),  LogicalType(LogicalTypeId::BIGINT),
                    LogicalType(LogicalTypeId::BIGINT),  LogicalType(LogicalTypeId::UBIGINT)};
    return result;
}

void NatsKvStatusExecute(ClientContext &, TableFunctionInput &data_p, DataChunk &output) {
    auto &bind_data = data_p.bind_data->Cast<NatsKvStatusBindData>();
    auto &state = data_p.global_state->Cast<NatsKvOneRowGlobalState>();
    if (state.done) {
        output.SetCardinality(0);
        return;
    }
    state.done = true;

    NatsKvHandle handle;
    OpenKvStore(bind_data.connection, bind_data.bucket, handle);

    kvStatus *status_info = nullptr;
    auto status = kvStore_Status(&status_info, handle.kv);
    if (status != NATS_OK) {
        throw std::runtime_error("Failed to get status for bucket '" + bind_data.bucket +
                                 "': " + natsStatus_GetText(status));
    }

    output.SetValue(0, 0, Value(bind_data.bucket));
    output.SetValue(1, 0, Value::UBIGINT(kvStatus_Values(status_info)));
    output.SetValue(2, 0, Value::BIGINT(kvStatus_History(status_info)));
    // kvStatus_TTL is reported in nanoseconds, matching kvConfig.TTL's unit on the write side.
    output.SetValue(3, 0, Value::BIGINT(kvStatus_TTL(status_info) / 1000000000LL));
    output.SetValue(4, 0, Value::BIGINT(kvStatus_Replicas(status_info)));
    output.SetValue(5, 0, Value::UBIGINT(kvStatus_Bytes(status_info)));
    kvStatus_Destroy(status_info);

    output.SetCardinality(1);
}

// --------------------------------------------------------------------------
// nats_kv_create_bucket(bucket, history := 1, ttl_seconds := 0, max_bytes := -1, replicas := 1)
// --------------------------------------------------------------------------

struct NatsKvCreateBucketBindData : public TableFunctionData {
    string bucket;
    uint8_t history = 1;
    int64_t ttl_seconds = 0;
    int64_t max_bytes = -1;
    int replicas = 1;
    NatsConnectionConfig connection;
};

unique_ptr<FunctionData> NatsKvCreateBucketBind(ClientContext &, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
    auto result = make_uniq<NatsKvCreateBucketBindData>();
    result->bucket = input.inputs[0].GetValue<string>();

    for (auto &kv : input.named_parameters) {
        if (kv.first == "history") {
            auto history = BigIntValue::Get(kv.second);
            if (history < 1 || history > 64) {
                throw std::runtime_error("history must be between 1 and 64");
            }
            result->history = static_cast<uint8_t>(history);
        } else if (kv.first == "ttl_seconds") {
            result->ttl_seconds = BigIntValue::Get(kv.second);
        } else if (kv.first == "max_bytes") {
            result->max_bytes = BigIntValue::Get(kv.second);
        } else if (kv.first == "replicas") {
            auto replicas = BigIntValue::Get(kv.second);
            if (replicas < 1) {
                throw std::runtime_error("replicas must be at least 1");
            }
            result->replicas = static_cast<int>(replicas);
        } else {
            ParseNatsConnectionParameter(result->connection, string(kv.first), kv.second);
        }
    }
    ValidateNatsConnectionConfig(result->connection);

    names = {"bucket", "created"};
    return_types = {LogicalType(LogicalTypeId::VARCHAR), LogicalType(LogicalTypeId::BOOLEAN)};
    return result;
}

void NatsKvCreateBucketExecute(ClientContext &, TableFunctionInput &data_p, DataChunk &output) {
    auto &bind_data = data_p.bind_data->Cast<NatsKvCreateBucketBindData>();
    auto &state = data_p.global_state->Cast<NatsKvOneRowGlobalState>();
    if (state.done) {
        output.SetCardinality(0);
        return;
    }
    state.done = true;

    natsConnection *conn = nullptr;
    jsCtx *js = nullptr;
    kvStore *kv = nullptr;
    try {
        ConnectJetStream(bind_data.connection, &conn, &js);

        kvConfig cfg;
        kvConfig_Init(&cfg);
        cfg.Bucket = bind_data.bucket.c_str();
        cfg.History = bind_data.history;
        cfg.TTL = bind_data.ttl_seconds * 1000000000LL;
        cfg.MaxBytes = bind_data.max_bytes;
        cfg.Replicas = bind_data.replicas;

        auto status = js_CreateKeyValue(&kv, js, &cfg);
        if (status != NATS_OK) {
            throw std::runtime_error("Failed to create KV bucket '" + bind_data.bucket +
                                     "': " + natsStatus_GetText(status));
        }
        kvStore_Destroy(kv);
        jsCtx_Destroy(js);
        natsConnection_Destroy(conn);
    } catch (...) {
        if (kv != nullptr) {
            kvStore_Destroy(kv);
        }
        if (js != nullptr) {
            jsCtx_Destroy(js);
        }
        if (conn != nullptr) {
            natsConnection_Destroy(conn);
        }
        throw;
    }

    output.SetValue(0, 0, Value(bind_data.bucket));
    output.SetValue(1, 0, Value::BOOLEAN(true));
    output.SetCardinality(1);
}

// --------------------------------------------------------------------------
// nats_kv_delete_bucket(bucket)
// --------------------------------------------------------------------------

struct NatsKvDeleteBucketBindData : public TableFunctionData {
    string bucket;
    NatsConnectionConfig connection;
};

unique_ptr<FunctionData> NatsKvDeleteBucketBind(ClientContext &, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
    auto result = make_uniq<NatsKvDeleteBucketBindData>();
    result->bucket = input.inputs[0].GetValue<string>();
    result->connection = ParseKvConnectionParameters(input.named_parameters);

    names = {"bucket", "deleted"};
    return_types = {LogicalType(LogicalTypeId::VARCHAR), LogicalType(LogicalTypeId::BOOLEAN)};
    return result;
}

void NatsKvDeleteBucketExecute(ClientContext &, TableFunctionInput &data_p, DataChunk &output) {
    auto &bind_data = data_p.bind_data->Cast<NatsKvDeleteBucketBindData>();
    auto &state = data_p.global_state->Cast<NatsKvOneRowGlobalState>();
    if (state.done) {
        output.SetCardinality(0);
        return;
    }
    state.done = true;

    natsConnection *conn = nullptr;
    jsCtx *js = nullptr;
    try {
        ConnectJetStream(bind_data.connection, &conn, &js);
        auto status = js_DeleteKeyValue(js, bind_data.bucket.c_str());
        if (status != NATS_OK) {
            throw std::runtime_error("Failed to delete KV bucket '" + bind_data.bucket +
                                     "': " + natsStatus_GetText(status));
        }
        jsCtx_Destroy(js);
        natsConnection_Destroy(conn);
    } catch (...) {
        if (js != nullptr) {
            jsCtx_Destroy(js);
        }
        if (conn != nullptr) {
            natsConnection_Destroy(conn);
        }
        throw;
    }

    output.SetValue(0, 0, Value(bind_data.bucket));
    output.SetValue(1, 0, Value::BOOLEAN(true));
    output.SetCardinality(1);
}

// --------------------------------------------------------------------------
// COPY tbl TO 'bucket' (FORMAT nats_kv, key_column := ..., value_column := ...)
// Bulk-populates an existing bucket from a query; does not create the bucket
// (use nats_kv_create_bucket first).
// --------------------------------------------------------------------------

idx_t FindKvColumnIndex(const vector<string> &names, const string &column_name) {
    for (idx_t i = 0; i < names.size(); i++) {
        if (StringUtil::CIEquals(names[i], column_name)) {
            return i;
        }
    }
    return DConstants::INVALID_INDEX;
}

struct NatsKvCopyToBindData : public TableFunctionData {
    string bucket;
    string key_column = "key";
    string value_column = "value";
    idx_t key_idx = DConstants::INVALID_INDEX;
    idx_t value_idx = DConstants::INVALID_INDEX;
    NatsConnectionConfig connection;
};

void NatsKvCopyListOptions(ClientContext &, CopyOptionsInput &input) {
    auto &copy_options = input.options;
    copy_options["url"] = CopyOption(LogicalType::VARCHAR, CopyOptionMode::READ_WRITE);
    copy_options["credentials_file"] = CopyOption(LogicalType::VARCHAR, CopyOptionMode::READ_WRITE);
    copy_options["tls_ca_file"] = CopyOption(LogicalType::VARCHAR, CopyOptionMode::READ_WRITE);
    copy_options["tls_cert_file"] = CopyOption(LogicalType::VARCHAR, CopyOptionMode::READ_WRITE);
    copy_options["tls_key_file"] = CopyOption(LogicalType::VARCHAR, CopyOptionMode::READ_WRITE);
    copy_options["tls_server_name"] = CopyOption(LogicalType::VARCHAR, CopyOptionMode::READ_WRITE);
    copy_options["tls_skip_verify"] = CopyOption(LogicalType::BOOLEAN, CopyOptionMode::READ_WRITE);
    copy_options["key_column"] = CopyOption(LogicalType::VARCHAR, CopyOptionMode::READ_WRITE);
    copy_options["value_column"] = CopyOption(LogicalType::VARCHAR, CopyOptionMode::READ_WRITE);
}

unique_ptr<FunctionData> NatsKvCopyToBind(ClientContext &, CopyFunctionBindInput &input,
#ifdef NATS_DUCKDB_IDENTIFIER_API
                                          const vector<Identifier> &identifiers,
#else
                                          const vector<string> &identifiers,
#endif
                                          const vector<LogicalType> &sql_types) {
    vector<string> names;
    names.reserve(identifiers.size());
    for (const auto &identifier : identifiers) {
        names.push_back(string(identifier));
    }

    auto result = make_uniq<NatsKvCopyToBindData>();
    if (input.info.file_path.empty()) {
        throw BinderException("COPY TO FORMAT nats_kv requires a bucket name in the file path");
    }
    result->bucket = input.info.file_path;

    for (const auto &option : input.info.options) {
        auto name = string(option.first);
        if (option.second.empty()) {
            continue;
        }
        if (StringUtil::CIEquals(name, "key_column")) {
            result->key_column = StringValue::Get(option.second.front());
        } else if (StringUtil::CIEquals(name, "value_column")) {
            result->value_column = StringValue::Get(option.second.front());
        } else {
            ParseNatsConnectionParameter(result->connection, name, option.second.front());
        }
    }
    ValidateNatsConnectionConfig(result->connection);

    result->key_idx = FindKvColumnIndex(names, result->key_column);
    if (result->key_idx == DConstants::INVALID_INDEX) {
        throw BinderException("COPY TO FORMAT nats_kv requires a source column named \"%s\" (key_column)",
                              result->key_column);
    }
    if (sql_types[result->key_idx].id() != LogicalTypeId::VARCHAR) {
        throw BinderException("COPY TO FORMAT nats_kv requires key column \"%s\" to be VARCHAR",
                              result->key_column);
    }

    result->value_idx = FindKvColumnIndex(names, result->value_column);
    if (result->value_idx == DConstants::INVALID_INDEX) {
        throw BinderException("COPY TO FORMAT nats_kv requires a source column named \"%s\" (value_column)",
                              result->value_column);
    }
    auto value_type = sql_types[result->value_idx].id();
    if (value_type != LogicalTypeId::VARCHAR && value_type != LogicalTypeId::BLOB) {
        throw BinderException("COPY TO FORMAT nats_kv requires value column \"%s\" to be VARCHAR or BLOB",
                              result->value_column);
    }

    return std::move(result);
}

struct NatsKvCopyToGlobalState : public GlobalFunctionData {
    NatsKvHandle handle;
    mutex lock;
    idx_t rows_written = 0;
};

struct NatsKvCopyToLocalState : public LocalFunctionData {
};

unique_ptr<LocalFunctionData> NatsKvCopyToInitializeLocal(ExecutionContext &, FunctionData &) {
    return make_uniq<NatsKvCopyToLocalState>();
}

unique_ptr<GlobalFunctionData> NatsKvCopyToInitializeGlobal(ClientContext &, FunctionData &bind_data,
                                                            const string &) {
    auto &bdata = bind_data.Cast<NatsKvCopyToBindData>();
    auto result = make_uniq<NatsKvCopyToGlobalState>();
    OpenKvStore(bdata.connection, bdata.bucket, result->handle);
    return std::move(result);
}

void NatsKvCopyToSink(ExecutionContext &, FunctionData &bind_data, GlobalFunctionData &gstate, LocalFunctionData &,
                      DataChunk &input) {
    auto &bdata = bind_data.Cast<NatsKvCopyToBindData>();
    auto &state = gstate.Cast<NatsKvCopyToGlobalState>();
    if (input.size() == 0) {
        return;
    }

    lock_guard<mutex> guard(state.lock);

    UnifiedVectorFormat key_format;
    UnifiedVectorFormat value_format;
    input.data[bdata.key_idx].ToUnifiedFormat(input.size(), key_format);
    input.data[bdata.value_idx].ToUnifiedFormat(input.size(), value_format);
    auto keys = UnifiedVectorFormat::GetData<string_t>(key_format);
    auto values = UnifiedVectorFormat::GetData<string_t>(value_format);

    for (idx_t row_idx = 0; row_idx < input.size(); row_idx++) {
        idx_t key_row = key_format.sel->get_index(row_idx);
        if (!key_format.validity.RowIsValid(key_row)) {
            throw BinderException("COPY TO FORMAT nats_kv does not support NULL key values");
        }
        idx_t value_row = value_format.sel->get_index(row_idx);
        if (!value_format.validity.RowIsValid(value_row)) {
            throw BinderException("COPY TO FORMAT nats_kv does not support NULL value values");
        }

        auto &key = keys[key_row];
        auto &value = values[value_row];
        string key_str(key.GetData(), key.GetSize());
        auto status = kvStore_Put(nullptr, state.handle.kv, key_str.c_str(), value.GetData(),
                                  static_cast<int>(value.GetSize()));
        if (status != NATS_OK) {
            throw std::runtime_error("Failed to put key '" + key_str + "' in bucket '" + bdata.bucket +
                                     "': " + natsStatus_GetText(status));
        }

        state.rows_written++;
    }
}

void NatsKvCopyToCombine(ExecutionContext &, FunctionData &, GlobalFunctionData &, LocalFunctionData &) {
}

void NatsKvCopyToFinalize(ClientContext &, FunctionData &, GlobalFunctionData &gstate) {
    auto &state = gstate.Cast<NatsKvCopyToGlobalState>();
    lock_guard<mutex> guard(state.lock);
    if (state.handle.conn == nullptr) {
        return;
    }
    auto status = natsConnection_FlushTimeout(state.handle.conn, 5000);
    if (status != NATS_OK) {
        throw std::runtime_error(string("Failed to flush NATS connection: ") + natsStatus_GetText(status));
    }
}

} // namespace

void NatsKvFunction::Register(ExtensionLoader &loader) {
    TableFunction get_fn("nats_kv_get",
                         {LogicalType(LogicalTypeId::VARCHAR), LogicalType(LogicalTypeId::VARCHAR)},
                         NatsKvGetExecute, NatsKvGetBind, NatsKvOneRowInitGlobal);
    RegisterNatsConnectionParameters(get_fn);
    loader.RegisterFunction(get_fn);

    TableFunction scan_fn("nats_kv_scan", {LogicalType(LogicalTypeId::VARCHAR)}, NatsKvScanExecute,
                          NatsKvScanBind, NatsKvScanInitGlobal);
    RegisterNatsConnectionParameters(scan_fn);
    scan_fn.named_parameters["key_filter"] = LogicalType(LogicalTypeId::VARCHAR);
    loader.RegisterFunction(scan_fn);

    TableFunction history_fn("nats_kv_history",
                             {LogicalType(LogicalTypeId::VARCHAR), LogicalType(LogicalTypeId::VARCHAR)},
                             NatsKvHistoryExecute, NatsKvHistoryBind, NatsKvHistoryInitGlobal);
    RegisterNatsConnectionParameters(history_fn);
    loader.RegisterFunction(history_fn);

    TableFunction put_fn("nats_kv_put",
                         {LogicalType(LogicalTypeId::VARCHAR), LogicalType(LogicalTypeId::VARCHAR),
                          LogicalType(LogicalTypeId::VARCHAR)},
                         NatsKvPutExecute, NatsKvWriteBind, NatsKvOneRowInitGlobal);
    RegisterNatsConnectionParameters(put_fn);
    loader.RegisterFunction(put_fn);

    TableFunction create_fn("nats_kv_create",
                            {LogicalType(LogicalTypeId::VARCHAR), LogicalType(LogicalTypeId::VARCHAR),
                             LogicalType(LogicalTypeId::VARCHAR)},
                            NatsKvCreateExecute, NatsKvWriteBind, NatsKvOneRowInitGlobal);
    RegisterNatsConnectionParameters(create_fn);
    loader.RegisterFunction(create_fn);

    TableFunction update_fn("nats_kv_update",
                            {LogicalType(LogicalTypeId::VARCHAR), LogicalType(LogicalTypeId::VARCHAR),
                             LogicalType(LogicalTypeId::VARCHAR), LogicalType(LogicalTypeId::BIGINT)},
                            NatsKvUpdateExecute, NatsKvUpdateBind, NatsKvOneRowInitGlobal);
    RegisterNatsConnectionParameters(update_fn);
    loader.RegisterFunction(update_fn);

    TableFunction delete_fn("nats_kv_delete",
                            {LogicalType(LogicalTypeId::VARCHAR), LogicalType(LogicalTypeId::VARCHAR)},
                            NatsKvDeleteExecute, NatsKvDeleteBind, NatsKvOneRowInitGlobal);
    RegisterNatsConnectionParameters(delete_fn);
    delete_fn.named_parameters["purge"] = LogicalType(LogicalTypeId::BOOLEAN);
    loader.RegisterFunction(delete_fn);

    TableFunction status_fn("nats_kv_status", {LogicalType(LogicalTypeId::VARCHAR)}, NatsKvStatusExecute,
                            NatsKvStatusBind, NatsKvOneRowInitGlobal);
    RegisterNatsConnectionParameters(status_fn);
    loader.RegisterFunction(status_fn);

    TableFunction create_bucket_fn("nats_kv_create_bucket", {LogicalType(LogicalTypeId::VARCHAR)},
                                   NatsKvCreateBucketExecute, NatsKvCreateBucketBind, NatsKvOneRowInitGlobal);
    RegisterNatsConnectionParameters(create_bucket_fn);
    create_bucket_fn.named_parameters["history"] = LogicalType(LogicalTypeId::BIGINT);
    create_bucket_fn.named_parameters["ttl_seconds"] = LogicalType(LogicalTypeId::BIGINT);
    create_bucket_fn.named_parameters["max_bytes"] = LogicalType(LogicalTypeId::BIGINT);
    create_bucket_fn.named_parameters["replicas"] = LogicalType(LogicalTypeId::BIGINT);
    loader.RegisterFunction(create_bucket_fn);

    TableFunction delete_bucket_fn("nats_kv_delete_bucket", {LogicalType(LogicalTypeId::VARCHAR)},
                                   NatsKvDeleteBucketExecute, NatsKvDeleteBucketBind, NatsKvOneRowInitGlobal);
    RegisterNatsConnectionParameters(delete_bucket_fn);
    loader.RegisterFunction(delete_bucket_fn);

    CopyFunction copy_function("nats_kv");
    copy_function.copy_options = NatsKvCopyListOptions;
    copy_function.copy_to_bind = NatsKvCopyToBind;
    copy_function.copy_to_initialize_local = NatsKvCopyToInitializeLocal;
    copy_function.copy_to_initialize_global = NatsKvCopyToInitializeGlobal;
    copy_function.copy_to_sink = NatsKvCopyToSink;
    copy_function.copy_to_combine = NatsKvCopyToCombine;
    copy_function.copy_to_finalize = NatsKvCopyToFinalize;
    copy_function.execution_mode = [](bool, bool) {
        return CopyFunctionExecutionMode::REGULAR_COPY_TO_FILE;
    };
    copy_function.extension = "nats_js";
    loader.RegisterFunction(copy_function);
}

} // namespace duckdb
