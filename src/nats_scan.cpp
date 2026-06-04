#include "nats_scan.hpp"
#include "nats_message_decode.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/copy_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/vector.hpp"
#include "yyjson.hpp"
#include <nats/nats.h>
#include <google/protobuf/compiler/importer.h>
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/descriptor.h>
#include <filesystem>
#include <algorithm>
#include <optional>
#include <cstring>

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
    // Protobuf 3.21.x and earlier use AddError with std::string
    // Protobuf 3.22.x and later use RecordError with absl::string_view
#if GOOGLE_PROTOBUF_VERSION >= 3022000
    void RecordError(absl::string_view filename, int line, int column, absl::string_view message) override {
        errors.push_back(string(filename) + ":" + std::to_string(line) + ":" + std::to_string(column) + ": " + string(message));
    }
#else
    void AddError(const std::string& filename, int line, int column, const std::string& message) override {
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

struct NatsScanBindData : public TableFunctionData {
    string stream_name;
    string subject_contains;
    string nats_subject;
    string nats_url;
    uint64_t start_seq;
    uint64_t end_seq;
    int64_t start_time;  // nanoseconds since epoch, 0 = not set
    int64_t end_time;    // nanoseconds since epoch, 0 = not set
    vector<string> json_fields;
    string proto_file;
    string proto_message;
    vector<string> proto_fields;
    vector<vector<const FieldDescriptor*>> proto_field_paths;
    uint64_t batch_size;
    int64_t fetch_timeout_ms;

    // Must outlive the query — proto_descriptor is owned by the importer's pool
    shared_ptr<DiskSourceTree> proto_source_tree;
    shared_ptr<ProtobufErrorCollector> proto_error_collector;
    shared_ptr<Importer> proto_importer;
    const Descriptor* proto_descriptor = nullptr;

    NatsScanBindData(string stream, string subject_substr, string nats_subj, string url, uint64_t start, uint64_t end,
                     int64_t start_ts, int64_t end_ts, vector<string> json_flds,
                     string proto_f, string proto_msg, vector<string> proto_flds,
                     vector<vector<const FieldDescriptor*>> proto_paths, uint64_t batch_sz, int64_t fetch_timeout)
        : stream_name(std::move(stream))
        , subject_contains(std::move(subject_substr))
        , nats_subject(std::move(nats_subj))
        , nats_url(std::move(url))
        , start_seq(start)
        , end_seq(end)
        , start_time(start_ts)
        , end_time(end_ts)
        , json_fields(std::move(json_flds))
        , proto_file(std::move(proto_f))
        , proto_message(std::move(proto_msg))
        , proto_fields(std::move(proto_flds))
        , proto_field_paths(std::move(proto_paths))
        , batch_size(batch_sz)
        , fetch_timeout_ms(fetch_timeout) {
    }
};

static const FieldDescriptor *GetFieldDescriptorForPath(const Descriptor *message_desc, const string &field_path);

struct NatsSourceSchema {
    vector<string> names;
    vector<LogicalType> return_types;
};

struct NatsCopyBindData : public TableFunctionData {
    unique_ptr<NatsScanBindData> scan_bind_data;
};

static std::optional<string> GetCopyOptionString(const case_insensitive_map_t<vector<Value>> &options,
                                                 const string &name) {
    auto it = options.find(name);
    if (it == options.end() || it->second.empty()) {
        return std::nullopt;
    }
    return StringValue::Get(it->second.front());
}

static uint64_t GetCopyOptionUBigInt(const case_insensitive_map_t<vector<Value>> &options, const string &name,
                                     uint64_t default_value) {
    auto it = options.find(name);
    if (it == options.end() || it->second.empty()) {
        return default_value;
    }
    return UBigIntValue::Get(it->second.front());
}

static int64_t GetCopyOptionBigInt(const case_insensitive_map_t<vector<Value>> &options, const string &name,
                                   int64_t default_value) {
    auto it = options.find(name);
    if (it == options.end() || it->second.empty()) {
        return default_value;
    }
    return BigIntValue::Get(it->second.front());
}

static vector<string> GetCopyOptionStringList(const case_insensitive_map_t<vector<Value>> &options, const string &name) {
    vector<string> result;
    auto it = options.find(name);
    if (it == options.end()) {
        return result;
    }
    for (auto &child : it->second) {
        result.push_back(StringValue::Get(child));
    }
    return result;
}

static NatsSourceSchema BuildNatsSourceSchema(const vector<string> &json_fields, const string &proto_file,
                                              const string &proto_message, const vector<string> &proto_fields,
                                              const Descriptor *descriptor) {
    NatsSourceSchema schema;
    schema.names.emplace_back("stream");
    schema.return_types.emplace_back(LogicalType(LogicalTypeId::VARCHAR));
    schema.names.emplace_back("subject");
    schema.return_types.emplace_back(LogicalType(LogicalTypeId::VARCHAR));
    schema.names.emplace_back("seq");
    schema.return_types.emplace_back(LogicalType(LogicalTypeId::UBIGINT));
    schema.names.emplace_back("ts_nats");
    schema.return_types.emplace_back(LogicalType(LogicalTypeId::TIMESTAMP));

    schema.names.emplace_back("payload");
    if (!proto_fields.empty() || json_fields.empty()) {
        schema.return_types.emplace_back(LogicalType(LogicalTypeId::BLOB));
    } else {
        schema.return_types.emplace_back(LogicalType(LogicalTypeId::VARCHAR));
    }

    for (const auto &field : json_fields) {
        schema.names.emplace_back(field);
        schema.return_types.emplace_back(LogicalType(LogicalTypeId::VARCHAR));
    }

    for (const auto &field_path : proto_fields) {
        string column_name = field_path;
        std::replace(column_name.begin(), column_name.end(), '.', '_');
        schema.names.emplace_back(column_name);

        const FieldDescriptor *field_desc = GetFieldDescriptorForPath(descriptor, field_path);
        if (field_desc) {
            schema.return_types.emplace_back(ProtobufFieldDescriptorToDuckDBType(field_desc));
        } else {
            schema.return_types.emplace_back(LogicalType(LogicalTypeId::VARCHAR));
        }
    }

    return schema;
}

static vector<string> SplitFieldPath(const string& field_path) {
    vector<string> parts;
    size_t start = 0;
    size_t end = field_path.find('.');
    while (end != string::npos) {
        parts.push_back(field_path.substr(start, end - start));
        start = end + 1;
        end = field_path.find('.', start);
    }
    parts.push_back(field_path.substr(start));
    return parts;
}

static const FieldDescriptor* GetFieldDescriptorForPath(const Descriptor* message_desc, const string& field_path) {
    auto path_parts = SplitFieldPath(field_path);
    const Descriptor* current_desc = message_desc;
    const FieldDescriptor* field = nullptr;

    for (size_t i = 0; i < path_parts.size(); i++) {
        field = current_desc->FindFieldByName(path_parts[i]);
        if (!field) {
            return nullptr;
        }
        if (i < path_parts.size() - 1) {
            if (field->type() != FieldDescriptor::TYPE_MESSAGE) {
                return nullptr;
            }
            current_desc = field->message_type();
        }
    }

    return field;
}

struct NatsScanGlobalState : public GlobalTableFunctionState {
    natsConnection *conn = nullptr;
    jsCtx *js = nullptr;
    jsStreamInfo *stream_info = nullptr;
    natsSubscription *sub = nullptr;
    natsMsgList fetched_msgs {nullptr, 0};
    int fetched_idx = 0;
    uint64_t current_seq = 0;
    uint64_t end_seq = 0;
    uint64_t target_message_count = UINT64_MAX;
    uint64_t emitted_count = 0;
    bool done = false;
    bool post_filter_subject = true;
    vector<column_t> column_ids;

    shared_ptr<DynamicMessageFactory> proto_factory;
    const Message* proto_prototype = nullptr;

    ~NatsScanGlobalState() {
        natsMsgList_Destroy(&fetched_msgs);
        if (sub != nullptr) {
            natsSubscription_Unsubscribe(sub);
            natsSubscription_Destroy(sub);
            sub = nullptr;
        }
        if (stream_info != nullptr) {
            jsStreamInfo_Destroy(stream_info);
            stream_info = nullptr;
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

    idx_t MaxThreads() const override {
        return 1; // Single-threaded for now
    }
};

struct NatsScanLocalState : public LocalTableFunctionState {
};

struct ProjectedFieldColumn {
    idx_t output_idx;
    idx_t field_idx;
};

static unique_ptr<FunctionData> NatsScanBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<string> &names) {
    if (input.inputs.empty()) {
        throw std::runtime_error("nats_scan requires at least one argument: stream_name");
    }

    auto stream_name = input.inputs[0].GetValue<string>();

    string subject_legacy = "";
    string subject_contains = "";
    string nats_subject = "";
    string nats_url = "nats://localhost:4222";
    uint64_t start_seq = 0;
    uint64_t end_seq = UINT64_MAX;
    int64_t start_time = 0;
    int64_t end_time = 0;
    vector<string> json_fields;
    string proto_file = "";
    string proto_message = "";
    vector<string> proto_fields;
    uint64_t batch_size = 4096;
    int64_t fetch_timeout_ms = 1000;

    for (auto &kv : input.named_parameters) {
        if (kv.first == "subject") {
            subject_legacy = StringValue::Get(kv.second);
        } else if (kv.first == "subject_contains") {
            subject_contains = StringValue::Get(kv.second);
        } else if (kv.first == "nats_subject") {
            nats_subject = StringValue::Get(kv.second);
        } else if (kv.first == "url") {
            nats_url = StringValue::Get(kv.second);
        } else if (kv.first == "start_seq") {
            start_seq = UBigIntValue::Get(kv.second);
        } else if (kv.first == "end_seq") {
            end_seq = UBigIntValue::Get(kv.second);
        } else if (kv.first == "start_time") {
            timestamp_t ts = TimestampValue::Get(kv.second);
            start_time = ts.value * 1000; // DuckDB microseconds -> nanoseconds
        } else if (kv.first == "end_time") {
            timestamp_t ts = TimestampValue::Get(kv.second);
            end_time = ts.value * 1000;
        } else if (kv.first == "json_extract") {
            auto list_children = ListValue::GetChildren(kv.second);
            for (auto &child : list_children) {
                json_fields.push_back(StringValue::Get(child));
            }
        } else if (kv.first == "proto_file") {
            proto_file = StringValue::Get(kv.second);
        } else if (kv.first == "proto_message") {
            proto_message = StringValue::Get(kv.second);
        } else if (kv.first == "proto_extract") {
            auto list_children = ListValue::GetChildren(kv.second);
            for (auto &child : list_children) {
                proto_fields.push_back(StringValue::Get(child));
            }
        } else if (kv.first == "batch_size") {
            batch_size = UBigIntValue::Get(kv.second);
        } else if (kv.first == "fetch_timeout_ms") {
            fetch_timeout_ms = BigIntValue::Get(kv.second);
        }
    }

    if (!subject_legacy.empty()) {
        if (!subject_contains.empty()) {
            throw std::runtime_error("Cannot use both subject and subject_contains parameters");
        }
        subject_contains = subject_legacy;
    }

    if (batch_size == 0 || batch_size > 65536) {
        throw std::runtime_error("batch_size must be between 1 and 65536");
    }
    if (fetch_timeout_ms < 1) {
        throw std::runtime_error("fetch_timeout_ms must be at least 1");
    }

    if ((start_seq > 0 || end_seq != UINT64_MAX) && (start_time > 0 || end_time > 0)) {
        throw std::runtime_error("Cannot mix sequence-based (start_seq/end_seq) and time-based (start_time/end_time) parameters");
    }

    if (!json_fields.empty() && !proto_fields.empty()) {
        throw std::runtime_error("Cannot use both json_extract and proto_extract parameters");
    }

    if (!proto_fields.empty()) {
        if (proto_file.empty()) {
            throw std::runtime_error("proto_file parameter is required when using proto_extract");
        }
        if (proto_message.empty()) {
            throw std::runtime_error("proto_message parameter is required when using proto_extract");
        }
    }

    shared_ptr<DiskSourceTree> source_tree;
    shared_ptr<ProtobufErrorCollector> error_collector;
    shared_ptr<Importer> importer;
    const Descriptor* descriptor = nullptr;
    vector<vector<const FieldDescriptor*>> proto_field_paths;

    if (!proto_fields.empty()) {
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

        const FileDescriptor* file_desc = importer->Import(proto_filename);
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

        // Resolve and validate requested fields once. The descriptor pointers
        // remain valid while bind_data owns the protobuf importer.
        for (const auto &field_path : proto_fields) {
            proto_field_paths.push_back(ResolveProtobufFieldPath(descriptor, field_path));
        }
    }

    auto schema = BuildNatsSourceSchema(json_fields, proto_file, proto_message, proto_fields, descriptor);
    names = schema.names;
    return_types = schema.return_types;

    auto bind_data = make_uniq<NatsScanBindData>(stream_name, subject_contains, nats_subject, nats_url, start_seq, end_seq,
                                                  start_time, end_time, json_fields, proto_file, proto_message, proto_fields,
                                                  std::move(proto_field_paths), batch_size, fetch_timeout_ms);

    if (!proto_fields.empty()) {
        bind_data->proto_source_tree = source_tree;
        bind_data->proto_error_collector = error_collector;
        bind_data->proto_importer = importer;
        bind_data->proto_descriptor = descriptor;
    }

    return bind_data;
}

// Binary search for the first sequence at or after the given timestamp.
static uint64_t ResolveTimestampToSequence(jsCtx *js, const char *stream_name,
                                           int64_t timestamp_ns,
                                           uint64_t first_seq, uint64_t last_seq) {
    natsStatus s;
    uint64_t left = first_seq;
    uint64_t right = last_seq;
    uint64_t result_seq = UINT64_MAX;

    while (left <= right) {
        uint64_t mid = left + (right - left) / 2;

        natsMsg *msg = nullptr;
        jsDirectGetMsgOptions opts;
        memset(&opts, 0, sizeof(opts));
        opts.Sequence = mid;

        s = js_DirectGetMsg(&msg, js, stream_name, nullptr, &opts);

        if (s == NATS_NOT_FOUND) {
            left = mid + 1;
            continue;
        }

        if (s != NATS_OK) {
            throw std::runtime_error(std::string("Failed to fetch message at sequence ") +
                                   std::to_string(mid) + " for timestamp resolution: " + natsStatus_GetText(s));
        }

        int64_t msg_time_ns = natsMsg_GetTime(msg);
        natsMsg_Destroy(msg);

        if (msg_time_ns >= timestamp_ns) {
            result_seq = mid;
            right = mid - 1;
        } else {
            left = mid + 1;
        }
    }

    return result_seq;
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
    if (subject_filter.empty() || stream_info == nullptr) {
        return false;
    }
    // Preserve legacy substring filters like "pm5560-001"; only real NATS subjects
    // under the stream should become server-side filters.
    if (subject_filter.find('.') == string::npos && subject_filter.find('>') == string::npos &&
        subject_filter.find('*') == string::npos) {
        return false;
    }
    if (stream_info->Config == nullptr) {
        return false;
    }
    for (int i = 0; i < stream_info->Config->SubjectsLen; i++) {
        if (SubjectIsUnderStreamPattern(subject_filter, stream_info->Config->Subjects[i])) {
            return true;
        }
    }
    return false;
}

static uint64_t CountDeletedInRange(const jsStreamState &state, uint64_t start_seq, uint64_t end_seq) {
    uint64_t deleted_in_range = 0;
    for (int i = 0; i < state.DeletedLen; i++) {
        uint64_t deleted_seq = state.Deleted[i];
        if (deleted_seq >= start_seq && deleted_seq <= end_seq) {
            deleted_in_range++;
        }
    }
    return deleted_in_range;
}

static uint64_t CountAvailableInSequenceRange(const jsStreamState &state, uint64_t start_seq, uint64_t end_seq) {
    if (state.Msgs == 0 || end_seq < state.FirstSeq || start_seq > state.LastSeq) {
        return 0;
    }

    uint64_t overlap_start = std::max<uint64_t>(start_seq, state.FirstSeq);
    uint64_t overlap_end = std::min<uint64_t>(end_seq, state.LastSeq);
    uint64_t overlap_messages = overlap_end - overlap_start + 1;
    uint64_t deleted_in_range = CountDeletedInRange(state, overlap_start, overlap_end);
    return overlap_messages >= deleted_in_range ? overlap_messages - deleted_in_range : 0;
}

static void ConnectJetStream(const string &nats_url, natsConnection **conn, jsCtx **js) {
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
    if (s != NATS_OK) {
        throw std::runtime_error(std::string("Failed to connect to NATS: ") + natsStatus_GetText(s));
    }

    s = natsConnection_JetStream(js, *conn, nullptr);
    if (s != NATS_OK) {
        throw std::runtime_error(std::string("Failed to create JetStream context: ") + natsStatus_GetText(s));
    }
}

static unique_ptr<GlobalTableFunctionState> NatsScanInitGlobal(ClientContext &context,
                                                                 TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<NatsScanBindData>();
    auto state = make_uniq<NatsScanGlobalState>();

    state->current_seq = bind_data.start_seq > 0 ? bind_data.start_seq : 1;
    state->end_seq = bind_data.end_seq;
    state->done = false;
    state->column_ids = input.column_ids;

    if (!bind_data.proto_fields.empty() && bind_data.proto_descriptor != nullptr) {
        state->proto_factory = make_shared_ptr<DynamicMessageFactory>();
        state->proto_prototype = state->proto_factory->GetPrototype(bind_data.proto_descriptor);
    }

    // Connect to NATS and validate stream at init time (fail-fast on typos / unreachable server)
    ConnectJetStream(bind_data.nats_url, &state->conn, &state->js);

    jsOptions stream_info_opts;
    jsOptions_Init(&stream_info_opts);
    bool can_use_deleted_details = bind_data.subject_contains.empty() && bind_data.nats_subject.empty();
    stream_info_opts.Stream.Info.DeletedDetails = can_use_deleted_details;
    jsOptions *stream_info_opts_ptr = nullptr;
    if (!bind_data.nats_subject.empty()) {
        stream_info_opts.Stream.Info.SubjectsFilter = bind_data.nats_subject.c_str();
    }
    stream_info_opts_ptr = &stream_info_opts;

    natsStatus s = js_GetStreamInfo(&state->stream_info, state->js, bind_data.stream_name.c_str(), stream_info_opts_ptr, nullptr);
    if (s != NATS_OK) {
        throw std::runtime_error(std::string("Failed to get stream info for '") + bind_data.stream_name + "': " + natsStatus_GetText(s));
    }

    if (state->end_seq == UINT64_MAX) {
        state->end_seq = state->stream_info->State.LastSeq;
    }

    // Resolve timestamps to sequence numbers
    if (bind_data.start_time > 0 || bind_data.end_time > 0) {
        if (bind_data.start_time > 0) {
            uint64_t resolved_seq = ResolveTimestampToSequence(
                state->js, bind_data.stream_name.c_str(), bind_data.start_time,
                state->stream_info->State.FirstSeq, state->stream_info->State.LastSeq
            );
            if (resolved_seq == UINT64_MAX) {
                state->done = true;
                return state;
            }
            state->current_seq = resolved_seq;
        }

        if (bind_data.end_time > 0) {
            uint64_t resolved_seq = ResolveTimestampToSequence(
                state->js, bind_data.stream_name.c_str(), bind_data.end_time,
                state->stream_info->State.FirstSeq, state->stream_info->State.LastSeq
            );
            if (resolved_seq != UINT64_MAX) {
                state->end_seq = resolved_seq;
            }
        }
    }

    if (state->current_seq > state->end_seq) {
        state->done = true;
        return state;
    }

    string subscription_subject = ">";
    state->post_filter_subject = true;
    if (!bind_data.nats_subject.empty()) {
        if (!CanUseServerSubjectFilter(bind_data.nats_subject, state->stream_info)) {
            throw std::runtime_error("nats_subject '" + bind_data.nats_subject +
                                   "' is not covered by stream '" + bind_data.stream_name + "' subject configuration");
        }
        subscription_subject = bind_data.nats_subject;
        state->post_filter_subject = false;

        bool full_stream_subject_scan = bind_data.subject_contains.empty() &&
                                        state->current_seq <= state->stream_info->State.FirstSeq &&
                                        state->end_seq >= state->stream_info->State.LastSeq;
        if (full_stream_subject_scan && state->stream_info->State.Subjects != nullptr) {
            state->target_message_count = 0;
            auto subjects = state->stream_info->State.Subjects;
            for (int i = 0; i < subjects->Count; i++) {
                state->target_message_count += subjects->List[i].Msgs;
            }
            if (state->target_message_count == 0) {
                state->done = true;
            }
        }
    } else if (bind_data.subject_contains.empty()) {
        state->target_message_count = CountAvailableInSequenceRange(
            state->stream_info->State, state->current_seq, state->end_seq
        );
        if (state->target_message_count == 0) {
            state->done = true;
        }
    }

    jsSubOptions sub_opts;
    jsSubOptions_Init(&sub_opts);
    sub_opts.Stream = bind_data.stream_name.c_str();
    sub_opts.Config.DeliverPolicy = js_DeliverByStartSequence;
    sub_opts.Config.OptStartSeq = state->current_seq;
    sub_opts.Config.AckPolicy = js_AckNone;
    sub_opts.Config.ReplayPolicy = js_ReplayInstant;
    sub_opts.Config.InactiveThreshold = 60LL * 1000LL * 1000LL * 1000LL; // 60s in ns

    jsErrCode js_err = static_cast<jsErrCode>(0);
    s = js_PullSubscribe(&state->sub, state->js, subscription_subject.c_str(), nullptr, nullptr, &sub_opts, &js_err);
    if (s != NATS_OK) {
        throw std::runtime_error(std::string("Failed to create JetStream pull subscription for '") +
                               bind_data.stream_name + "': " + natsStatus_GetText(s));
    }

    return state;
}

static unique_ptr<LocalTableFunctionState> NatsScanInitLocal(ExecutionContext &context,
                                                               TableFunctionInitInput &input,
                                                               GlobalTableFunctionState *global_state) {
    return make_uniq<NatsScanLocalState>();
}

static void NatsScanExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &bind_data = data_p.bind_data->Cast<NatsScanBindData>();
    auto &global_state = data_p.global_state->Cast<NatsScanGlobalState>();

    if (global_state.done || global_state.current_seq > global_state.end_seq) {
        global_state.done = true;
        output.SetCardinality(0);
        return;
    }

    idx_t count = 0;
    const idx_t max_rows = STANDARD_VECTOR_SIZE;

    bool payload_is_blob = !bind_data.proto_fields.empty() || bind_data.json_fields.empty();
    auto &column_ids = global_state.column_ids;
    bool needs_stream = false;
    bool needs_subject = false;
    bool needs_seq = false;
    bool needs_ts = false;
    bool needs_payload = false;
    bool needs_json = false;
    bool needs_proto = false;
    vector<ProjectedFieldColumn> json_columns;
    vector<ProjectedFieldColumn> proto_columns;
    for (idx_t out_idx = 0; out_idx < column_ids.size(); out_idx++) {
        auto col_id = column_ids[out_idx];
        if (col_id == 0) {
            needs_stream = true;
        } else if (col_id == 1) {
            needs_subject = true;
        } else if (col_id == 2) {
            needs_seq = true;
        } else if (col_id == 3) {
            needs_ts = true;
        } else if (col_id == 4) {
            needs_payload = true;
        } else if (col_id >= 5 && col_id < 5 + bind_data.json_fields.size()) {
            needs_json = true;
            json_columns.push_back({out_idx, static_cast<idx_t>(col_id - 5)});
        } else if (col_id >= 5 + bind_data.json_fields.size()) {
            needs_proto = true;
            proto_columns.push_back({out_idx, static_cast<idx_t>(col_id - 5)});
        }
    }
    bool needs_subject_for_filter = !bind_data.subject_contains.empty();
    bool needs_message_subject = needs_subject || needs_subject_for_filter;
    bool needs_message_payload = needs_payload || needs_json || needs_proto;

    unique_ptr<Message> proto_msg;
    if (needs_proto && !bind_data.proto_fields.empty() && global_state.proto_prototype != nullptr) {
        proto_msg.reset(global_state.proto_prototype->New());
    }

    while (count < max_rows && global_state.current_seq <= global_state.end_seq) {
        if (context.interrupted) {
            break;
        }

        if (global_state.fetched_idx >= global_state.fetched_msgs.Count) {
            natsMsgList_Destroy(&global_state.fetched_msgs);
            global_state.fetched_msgs = {nullptr, 0};
            global_state.fetched_idx = 0;

            uint64_t remaining = global_state.end_seq >= global_state.current_seq ?
                                 global_state.end_seq - global_state.current_seq + 1 : 0;
            int batch = static_cast<int>(std::min<uint64_t>(bind_data.batch_size, remaining));
            if (batch <= 0) {
                global_state.done = true;
                break;
            }

            jsFetchRequest request;
            natsStatus s = jsFetchRequest_Init(&request);
            if (s != NATS_OK) {
                throw std::runtime_error(std::string("Failed to initialize JetStream fetch request: ") +
                                       natsStatus_GetText(s));
            }
            request.Batch = batch;
            request.Expires = bind_data.fetch_timeout_ms * 1000LL * 1000LL;
            request.NoWait = true;

            s = natsSubscription_FetchRequest(&global_state.fetched_msgs, global_state.sub, &request);
            if (s == NATS_TIMEOUT || s == NATS_NOT_FOUND) {
                global_state.done = true;
                break;
            }
            if (s != NATS_OK) {
                throw std::runtime_error(std::string("Failed to fetch JetStream message batch from '") +
                                       bind_data.stream_name + "': " + natsStatus_GetText(s));
            }
            if (global_state.fetched_msgs.Count == 0) {
                global_state.done = true;
                break;
            }
        }

        natsMsg *msg = global_state.fetched_msgs.Msgs[global_state.fetched_idx];
        global_state.fetched_msgs.Msgs[global_state.fetched_idx] = nullptr;
        global_state.fetched_idx++;
        if (msg == nullptr) {
            continue;
        }

        uint64_t stream_seq = natsMsg_GetSequence(msg);
        int64_t timestamp_ns = 0;
        if (needs_ts || stream_seq == 0) {
            jsMsgMetaData *meta = nullptr;
            natsStatus meta_status = natsMsg_GetMetaData(&meta, msg);
            if (meta_status == NATS_OK) {
                stream_seq = meta->Sequence.Stream;
                timestamp_ns = meta->Timestamp;
            } else if (needs_ts) {
                timestamp_ns = natsMsg_GetTime(msg);
            }
            if (meta != nullptr) {
                jsMsgMetaData_Destroy(meta);
            }
        }
        if (stream_seq == 0) {
            natsMsg_Destroy(msg);
            throw std::runtime_error("Failed to read JetStream metadata for fetched message");
        }
        global_state.current_seq = stream_seq + 1;

        if (stream_seq > global_state.end_seq) {
            natsMsg_Destroy(msg);
            global_state.done = true;
            break;
        }

        const char *subject = nullptr;
        if (needs_message_subject) {
            subject = natsMsg_GetSubject(msg);
        }

        if (needs_subject_for_filter &&
            string(subject).find(bind_data.subject_contains) == string::npos) {
            natsMsg_Destroy(msg);
            continue;
        }

        int64_t timestamp_us = timestamp_ns / 1000; // nanos -> micros
        const char *msg_data = nullptr;
        int data_len = 0;
        if (needs_message_payload) {
            msg_data = natsMsg_GetData(msg);
            data_len = natsMsg_GetDataLength(msg);
        }

        for (idx_t out_idx = 0; out_idx < column_ids.size(); out_idx++) {
            auto col_id = column_ids[out_idx];
            switch (col_id) {
                case 0:
                    output.SetValue(out_idx, count, Value(bind_data.stream_name));
                    break;
                case 1:
                    output.SetValue(out_idx, count, Value(subject));
                    break;
                case 2:
                    output.SetValue(out_idx, count, Value::UBIGINT(stream_seq));
                    break;
                case 3:
                    output.SetValue(out_idx, count, Value::TIMESTAMP(timestamp_t(timestamp_us)));
                    break;
                case 4:
                    if (payload_is_blob) {
                        output.SetValue(out_idx, count, Value::BLOB(const_data_ptr_cast(msg_data), data_len));
                    } else {
                        output.SetValue(out_idx, count, Value(string(msg_data, data_len)));
                    }
                    break;
                default:
                    break;
            }
        }

        if (needs_json && !bind_data.json_fields.empty()) {
            yyjson_doc *doc = yyjson_read(msg_data, data_len, 0);

            if (doc) {
                yyjson_val *root = yyjson_doc_get_root(doc);

                for (const auto &json_column : json_columns) {
                    auto &vec = output.data[json_column.output_idx];
                    auto vec_data = FlatVector::GetData<string_t>(vec);
                    yyjson_val *field_val = yyjson_obj_get(root, bind_data.json_fields[json_column.field_idx].c_str());

                    if (field_val) {
                        if (yyjson_is_str(field_val)) {
                            vec_data[count] = StringVector::AddString(vec, yyjson_get_str(field_val));
                        } else if (yyjson_is_num(field_val)) {
                            auto s = std::to_string(yyjson_get_num(field_val));
                            vec_data[count] = StringVector::AddString(vec, s);
                        } else if (yyjson_is_bool(field_val)) {
                            vec_data[count] = StringVector::AddString(vec, yyjson_get_bool(field_val) ? "true" : "false");
                        } else if (yyjson_is_null(field_val)) {
                            FlatVector::SetNull(vec, count, true);
                        } else {
                            char *json_str = yyjson_val_write(field_val, 0, nullptr);
                            if (json_str) {
                                vec_data[count] = StringVector::AddString(vec, json_str);
                                free(json_str);
                            } else {
                                FlatVector::SetNull(vec, count, true);
                            }
                        }
                    } else {
                        FlatVector::SetNull(vec, count, true);
                    }
                }

                yyjson_doc_free(doc);
            } else {
                for (const auto &json_column : json_columns) {
                    FlatVector::SetNull(output.data[json_column.output_idx], count, true);
                }
            }
        }

        if (needs_proto && proto_msg) {
            proto_msg->Clear();
            bool parse_success = proto_msg->ParseFromArray(msg_data, data_len);

            if (parse_success) {
                for (const auto &proto_column : proto_columns) {
                    Value field_value = ExtractProtobufValue(proto_msg.get(), bind_data.proto_field_paths[proto_column.field_idx]);
                    output.SetValue(proto_column.output_idx, count, field_value);
                }
            } else {
                for (const auto &proto_column : proto_columns) {
                    FlatVector::SetNull(output.data[proto_column.output_idx], count, true);
                }
            }
        }

        natsMsg_Destroy(msg);

        count++;
        global_state.emitted_count++;
        if (global_state.emitted_count >= global_state.target_message_count) {
            global_state.done = true;
            break;
        }
    }

    if (global_state.current_seq > global_state.end_seq) {
        global_state.done = true;
    }

    output.SetCardinality(count);
}

struct NatsStreamStatsBindData : public TableFunctionData {
    string stream_name;
    string nats_url;

    NatsStreamStatsBindData(string stream, string url)
        : stream_name(std::move(stream)), nats_url(std::move(url)) {
    }
};

struct NatsStreamRangeStatsBindData : public TableFunctionData {
    string stream_name;
    string nats_url;
    uint64_t start_seq;
    uint64_t end_seq;

    NatsStreamRangeStatsBindData(string stream, string url, uint64_t start, uint64_t end)
        : stream_name(std::move(stream)), nats_url(std::move(url)), start_seq(start), end_seq(end) {
    }
};

struct NatsStreamStatsGlobalState : public GlobalTableFunctionState {
    bool done = false;

    idx_t MaxThreads() const override {
        return 1;
    }
};

static unique_ptr<FunctionData> NatsStreamStatsBind(ClientContext &context, TableFunctionBindInput &input,
                                                     vector<LogicalType> &return_types, vector<string> &names) {
    if (input.inputs.empty()) {
        throw std::runtime_error("nats_stream_stats requires one argument: stream_name");
    }

    auto stream_name = input.inputs[0].GetValue<string>();
    string nats_url = "nats://localhost:4222";

    for (auto &kv : input.named_parameters) {
        if (kv.first == "url") {
            nats_url = StringValue::Get(kv.second);
        }
    }

    names.emplace_back("stream");
    return_types.emplace_back(LogicalType(LogicalTypeId::VARCHAR));
    names.emplace_back("messages");
    return_types.emplace_back(LogicalType(LogicalTypeId::UBIGINT));
    names.emplace_back("bytes");
    return_types.emplace_back(LogicalType(LogicalTypeId::UBIGINT));
    names.emplace_back("first_seq");
    return_types.emplace_back(LogicalType(LogicalTypeId::UBIGINT));
    names.emplace_back("last_seq");
    return_types.emplace_back(LogicalType(LogicalTypeId::UBIGINT));
    names.emplace_back("first_time");
    return_types.emplace_back(LogicalType(LogicalTypeId::TIMESTAMP));
    names.emplace_back("last_time");
    return_types.emplace_back(LogicalType(LogicalTypeId::TIMESTAMP));
    names.emplace_back("deleted_count");
    return_types.emplace_back(LogicalType(LogicalTypeId::UBIGINT));
    names.emplace_back("consumer_count");
    return_types.emplace_back(LogicalType(LogicalTypeId::BIGINT));
    names.emplace_back("subject_count");
    return_types.emplace_back(LogicalType(LogicalTypeId::BIGINT));

    return make_uniq<NatsStreamStatsBindData>(stream_name, nats_url);
}

static unique_ptr<FunctionData> NatsStreamRangeStatsBind(ClientContext &context, TableFunctionBindInput &input,
                                                          vector<LogicalType> &return_types, vector<string> &names) {
    if (input.inputs.empty()) {
        throw std::runtime_error("nats_stream_range_stats requires one argument: stream_name");
    }

    auto stream_name = input.inputs[0].GetValue<string>();
    string nats_url = "nats://localhost:4222";
    uint64_t start_seq = 0;
    uint64_t end_seq = 0;
    bool has_start_seq = false;
    bool has_end_seq = false;

    for (auto &kv : input.named_parameters) {
        if (kv.first == "url") {
            nats_url = StringValue::Get(kv.second);
        } else if (kv.first == "start_seq") {
            start_seq = UBigIntValue::Get(kv.second);
            has_start_seq = true;
        } else if (kv.first == "end_seq") {
            end_seq = UBigIntValue::Get(kv.second);
            has_end_seq = true;
        }
    }

    if (!has_start_seq || !has_end_seq) {
        throw std::runtime_error("nats_stream_range_stats requires start_seq and end_seq parameters");
    }
    if (start_seq == 0) {
        throw std::runtime_error("start_seq must be greater than zero");
    }
    if (end_seq < start_seq) {
        throw std::runtime_error("end_seq must be greater than or equal to start_seq");
    }

    names.emplace_back("stream");
    return_types.emplace_back(LogicalType(LogicalTypeId::VARCHAR));
    names.emplace_back("requested_start_seq");
    return_types.emplace_back(LogicalType(LogicalTypeId::UBIGINT));
    names.emplace_back("requested_end_seq");
    return_types.emplace_back(LogicalType(LogicalTypeId::UBIGINT));
    names.emplace_back("first_seq");
    return_types.emplace_back(LogicalType(LogicalTypeId::UBIGINT));
    names.emplace_back("last_seq");
    return_types.emplace_back(LogicalType(LogicalTypeId::UBIGINT));
    names.emplace_back("overlap_start_seq");
    return_types.emplace_back(LogicalType(LogicalTypeId::UBIGINT));
    names.emplace_back("overlap_end_seq");
    return_types.emplace_back(LogicalType(LogicalTypeId::UBIGINT));
    names.emplace_back("requested_messages");
    return_types.emplace_back(LogicalType(LogicalTypeId::UBIGINT));
    names.emplace_back("overlap_messages");
    return_types.emplace_back(LogicalType(LogicalTypeId::UBIGINT));
    names.emplace_back("deleted_in_range");
    return_types.emplace_back(LogicalType(LogicalTypeId::UBIGINT));
    names.emplace_back("available_messages");
    return_types.emplace_back(LogicalType(LogicalTypeId::UBIGINT));
    names.emplace_back("has_gaps");
    return_types.emplace_back(LogicalType(LogicalTypeId::BOOLEAN));
    names.emplace_back("starts_before_first");
    return_types.emplace_back(LogicalType(LogicalTypeId::BOOLEAN));
    names.emplace_back("ends_after_last");
    return_types.emplace_back(LogicalType(LogicalTypeId::BOOLEAN));

    return make_uniq<NatsStreamRangeStatsBindData>(stream_name, nats_url, start_seq, end_seq);
}

static unique_ptr<GlobalTableFunctionState> NatsStreamStatsInitGlobal(ClientContext &context,
                                                                       TableFunctionInitInput &input) {
    return make_uniq<NatsStreamStatsGlobalState>();
}

static void SetTimestampNs(DataChunk &output, idx_t col_idx, idx_t row_idx, int64_t timestamp_ns) {
    if (timestamp_ns <= 0) {
        FlatVector::SetNull(output.data[col_idx], row_idx, true);
        return;
    }
    output.SetValue(col_idx, row_idx, Value::TIMESTAMP(timestamp_t(timestamp_ns / 1000)));
}

static void NatsStreamStatsExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &bind_data = data_p.bind_data->Cast<NatsStreamStatsBindData>();
    auto &global_state = data_p.global_state->Cast<NatsStreamStatsGlobalState>();

    if (global_state.done) {
        output.SetCardinality(0);
        return;
    }

    natsConnection *conn = nullptr;
    jsCtx *js = nullptr;
    jsStreamInfo *stream_info = nullptr;

    try {
        ConnectJetStream(bind_data.nats_url, &conn, &js);

        natsStatus s = js_GetStreamInfo(&stream_info, js, bind_data.stream_name.c_str(), nullptr, nullptr);
        if (s != NATS_OK) {
            throw std::runtime_error(std::string("Failed to get stream info for '") + bind_data.stream_name +
                                   "': " + natsStatus_GetText(s));
        }

        const auto &state = stream_info->State;
        output.SetValue(0, 0, Value(bind_data.stream_name));
        output.SetValue(1, 0, Value::UBIGINT(state.Msgs));
        output.SetValue(2, 0, Value::UBIGINT(state.Bytes));
        output.SetValue(3, 0, Value::UBIGINT(state.FirstSeq));
        output.SetValue(4, 0, Value::UBIGINT(state.LastSeq));
        SetTimestampNs(output, 5, 0, state.FirstTime);
        SetTimestampNs(output, 6, 0, state.LastTime);
        output.SetValue(7, 0, Value::UBIGINT(state.NumDeleted));
        output.SetValue(8, 0, Value::BIGINT(state.Consumers));
        output.SetValue(9, 0, Value::BIGINT(state.NumSubjects));

        jsStreamInfo_Destroy(stream_info);
        jsCtx_Destroy(js);
        natsConnection_Destroy(conn);

        global_state.done = true;
        output.SetCardinality(1);
    } catch (...) {
        if (stream_info != nullptr) {
            jsStreamInfo_Destroy(stream_info);
        }
        if (js != nullptr) {
            jsCtx_Destroy(js);
        }
        if (conn != nullptr) {
            natsConnection_Destroy(conn);
        }
        throw;
    }
}

static unique_ptr<GlobalTableFunctionState> NatsStreamRangeStatsInitGlobal(ClientContext &context,
                                                                            TableFunctionInitInput &input) {
    return make_uniq<NatsStreamStatsGlobalState>();
}

static void NatsStreamRangeStatsExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &bind_data = data_p.bind_data->Cast<NatsStreamRangeStatsBindData>();
    auto &global_state = data_p.global_state->Cast<NatsStreamStatsGlobalState>();

    if (global_state.done) {
        output.SetCardinality(0);
        return;
    }

    natsConnection *conn = nullptr;
    jsCtx *js = nullptr;
    jsStreamInfo *stream_info = nullptr;

    try {
        ConnectJetStream(bind_data.nats_url, &conn, &js);

        jsOptions opts;
        jsOptions_Init(&opts);
        opts.Stream.Info.DeletedDetails = true;

        natsStatus s = js_GetStreamInfo(&stream_info, js, bind_data.stream_name.c_str(), &opts, nullptr);
        if (s != NATS_OK) {
            throw std::runtime_error(std::string("Failed to get stream info for '") + bind_data.stream_name +
                                   "': " + natsStatus_GetText(s));
        }

        const auto &state = stream_info->State;
        uint64_t requested_messages = bind_data.end_seq - bind_data.start_seq + 1;
        bool starts_before_first = bind_data.start_seq < state.FirstSeq;
        bool ends_after_last = bind_data.end_seq > state.LastSeq;
        bool has_overlap = state.Msgs > 0 && bind_data.end_seq >= state.FirstSeq && bind_data.start_seq <= state.LastSeq;

        uint64_t overlap_start = 0;
        uint64_t overlap_end = 0;
        uint64_t overlap_messages = 0;
        uint64_t deleted_in_range = 0;
        uint64_t available_messages = 0;

        if (has_overlap) {
            overlap_start = std::max<uint64_t>(bind_data.start_seq, state.FirstSeq);
            overlap_end = std::min<uint64_t>(bind_data.end_seq, state.LastSeq);
            overlap_messages = overlap_end - overlap_start + 1;

            for (int i = 0; i < state.DeletedLen; i++) {
                uint64_t deleted_seq = state.Deleted[i];
                if (deleted_seq >= overlap_start && deleted_seq <= overlap_end) {
                    deleted_in_range++;
                }
            }
            available_messages = overlap_messages - deleted_in_range;
        }

        bool has_gaps = starts_before_first || ends_after_last || deleted_in_range > 0;

        output.SetValue(0, 0, Value(bind_data.stream_name));
        output.SetValue(1, 0, Value::UBIGINT(bind_data.start_seq));
        output.SetValue(2, 0, Value::UBIGINT(bind_data.end_seq));
        output.SetValue(3, 0, Value::UBIGINT(state.FirstSeq));
        output.SetValue(4, 0, Value::UBIGINT(state.LastSeq));
        if (has_overlap) {
            output.SetValue(5, 0, Value::UBIGINT(overlap_start));
            output.SetValue(6, 0, Value::UBIGINT(overlap_end));
        } else {
            FlatVector::SetNull(output.data[5], 0, true);
            FlatVector::SetNull(output.data[6], 0, true);
        }
        output.SetValue(7, 0, Value::UBIGINT(requested_messages));
        output.SetValue(8, 0, Value::UBIGINT(overlap_messages));
        output.SetValue(9, 0, Value::UBIGINT(deleted_in_range));
        output.SetValue(10, 0, Value::UBIGINT(available_messages));
        output.SetValue(11, 0, Value::BOOLEAN(has_gaps));
        output.SetValue(12, 0, Value::BOOLEAN(starts_before_first));
        output.SetValue(13, 0, Value::BOOLEAN(ends_after_last));

        jsStreamInfo_Destroy(stream_info);
        jsCtx_Destroy(js);
        natsConnection_Destroy(conn);

        global_state.done = true;
        output.SetCardinality(1);
    } catch (...) {
        if (stream_info != nullptr) {
            jsStreamInfo_Destroy(stream_info);
        }
        if (js != nullptr) {
            jsCtx_Destroy(js);
        }
        if (conn != nullptr) {
            natsConnection_Destroy(conn);
        }
        throw;
    }
}

static TableFunction CreateNatsScanTableFunction() {
    TableFunction nats_scan("nats_scan", {LogicalType(LogicalTypeId::VARCHAR)}, NatsScanExecute, NatsScanBind,
                            NatsScanInitGlobal, NatsScanInitLocal);
    nats_scan.projection_pushdown = true;

    nats_scan.named_parameters["subject"] = LogicalType(LogicalTypeId::VARCHAR);
    nats_scan.named_parameters["subject_contains"] = LogicalType(LogicalTypeId::VARCHAR);
    nats_scan.named_parameters["nats_subject"] = LogicalType(LogicalTypeId::VARCHAR);
    nats_scan.named_parameters["url"] = LogicalType(LogicalTypeId::VARCHAR);
    nats_scan.named_parameters["start_seq"] = LogicalType(LogicalTypeId::UBIGINT);
    nats_scan.named_parameters["end_seq"] = LogicalType(LogicalTypeId::UBIGINT);
    nats_scan.named_parameters["start_time"] = LogicalType(LogicalTypeId::TIMESTAMP);
    nats_scan.named_parameters["end_time"] = LogicalType(LogicalTypeId::TIMESTAMP);
    nats_scan.named_parameters["json_extract"] = LogicalType::LIST(LogicalType(LogicalTypeId::VARCHAR));
    nats_scan.named_parameters["proto_file"] = LogicalType(LogicalTypeId::VARCHAR);
    nats_scan.named_parameters["proto_message"] = LogicalType(LogicalTypeId::VARCHAR);
    nats_scan.named_parameters["proto_extract"] = LogicalType::LIST(LogicalType(LogicalTypeId::VARCHAR));
    nats_scan.named_parameters["batch_size"] = LogicalType(LogicalTypeId::UBIGINT);
    nats_scan.named_parameters["fetch_timeout_ms"] = LogicalType(LogicalTypeId::BIGINT);

    return nats_scan;
}

static unique_ptr<FunctionData> NatsCopyFromBind(ClientContext &context, CopyFromFunctionBindInput &input,
                                                 vector<string> &expected_names, vector<LogicalType> &expected_types) {
    if (input.info.file_path.empty()) {
        throw std::runtime_error("COPY FROM FORMAT nats_js requires a stream name in the file path");
    }

    auto stream_name = input.info.file_path;
    string subject_legacy;
    auto subject_contains = GetCopyOptionString(input.info.options, "subject_contains").value_or("");
    auto nats_subject = GetCopyOptionString(input.info.options, "nats_subject").value_or("");
    auto nats_url = GetCopyOptionString(input.info.options, "url").value_or("nats://localhost:4222");
    uint64_t start_seq = GetCopyOptionUBigInt(input.info.options, "start_seq", 0);
    uint64_t end_seq = GetCopyOptionUBigInt(input.info.options, "end_seq", UINT64_MAX);
    int64_t start_time = 0;
    int64_t end_time = 0;
    vector<string> json_fields = GetCopyOptionStringList(input.info.options, "json_extract");
    string proto_file = GetCopyOptionString(input.info.options, "proto_file").value_or("");
    string proto_message = GetCopyOptionString(input.info.options, "proto_message").value_or("");
    vector<string> proto_fields = GetCopyOptionStringList(input.info.options, "proto_extract");
    uint64_t batch_size = GetCopyOptionUBigInt(input.info.options, "batch_size", 4096);
    int64_t fetch_timeout_ms = GetCopyOptionBigInt(input.info.options, "fetch_timeout_ms", 1000);

    if (auto subject = GetCopyOptionString(input.info.options, "subject")) {
        subject_legacy = *subject;
    }
    if (!subject_legacy.empty()) {
        if (!subject_contains.empty()) {
            throw std::runtime_error("Cannot use both subject and subject_contains parameters");
        }
        subject_contains = subject_legacy;
    }

    if (auto start_time_opt = GetCopyOptionString(input.info.options, "start_time")) {
        start_time = Timestamp::FromString(*start_time_opt, false).value * 1000;
    }
    if (auto end_time_opt = GetCopyOptionString(input.info.options, "end_time")) {
        end_time = Timestamp::FromString(*end_time_opt, false).value * 1000;
    }

    if (batch_size == 0 || batch_size > 65536) {
        throw std::runtime_error("batch_size must be between 1 and 65536");
    }
    if (fetch_timeout_ms < 1) {
        throw std::runtime_error("fetch_timeout_ms must be at least 1");
    }
    if ((start_seq > 0 || end_seq != UINT64_MAX) && (start_time > 0 || end_time > 0)) {
        throw std::runtime_error("Cannot mix sequence-based (start_seq/end_seq) and time-based (start_time/end_time) parameters");
    }
    if (!json_fields.empty() && !proto_fields.empty()) {
        throw std::runtime_error("Cannot use both json_extract and proto_extract parameters");
    }
    if (!proto_fields.empty()) {
        if (proto_file.empty()) {
            throw std::runtime_error("proto_file parameter is required when using proto_extract");
        }
        if (proto_message.empty()) {
            throw std::runtime_error("proto_message parameter is required when using proto_extract");
        }
    }

    shared_ptr<DiskSourceTree> source_tree;
    shared_ptr<ProtobufErrorCollector> error_collector;
    shared_ptr<Importer> importer;
    const Descriptor *descriptor = nullptr;
    vector<vector<const FieldDescriptor *>> proto_field_paths;

    if (!proto_fields.empty()) {
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

        for (const auto &field_path : proto_fields) {
            proto_field_paths.push_back(ResolveProtobufFieldPath(descriptor, field_path));
        }
    }

    auto schema = BuildNatsSourceSchema(json_fields, proto_file, proto_message, proto_fields, descriptor);
    if (schema.return_types.size() != expected_types.size()) {
        throw std::runtime_error("COPY FROM FORMAT nats_js requires the target table schema to match the nats_scan output schema");
    }
    for (idx_t i = 0; i < schema.return_types.size(); i++) {
        if (schema.return_types[i] != expected_types[i]) {
            throw std::runtime_error("COPY FROM FORMAT nats_js requires the target table schema to match the nats_scan output schema");
        }
    }

    auto bind_data = make_uniq<NatsScanBindData>(stream_name, subject_contains, nats_subject, nats_url, start_seq, end_seq,
                                                 start_time, end_time, json_fields, proto_file, proto_message,
                                                 proto_fields, std::move(proto_field_paths), batch_size, fetch_timeout_ms);

    if (!proto_fields.empty()) {
        bind_data->proto_source_tree = source_tree;
        bind_data->proto_error_collector = error_collector;
        bind_data->proto_importer = importer;
        bind_data->proto_descriptor = descriptor;
    }

    return bind_data;
}

void NatsScanFunction::Register(ExtensionLoader &loader) {
    auto nats_scan = CreateNatsScanTableFunction();
    loader.RegisterFunction(nats_scan);
}

void NatsStreamStatsFunction::Register(ExtensionLoader &loader) {
    for (const auto &function_name : {"nats_stream_stats", "nats_stream_info"}) {
        TableFunction nats_stream_stats(function_name, {LogicalType(LogicalTypeId::VARCHAR)}, NatsStreamStatsExecute,
                                        NatsStreamStatsBind, NatsStreamStatsInitGlobal);
        nats_stream_stats.named_parameters["url"] = LogicalType(LogicalTypeId::VARCHAR);
        loader.RegisterFunction(nats_stream_stats);
    }

    TableFunction nats_stream_range_stats("nats_stream_range_stats", {LogicalType(LogicalTypeId::VARCHAR)},
                                          NatsStreamRangeStatsExecute, NatsStreamRangeStatsBind,
                                          NatsStreamRangeStatsInitGlobal);
    nats_stream_range_stats.named_parameters["url"] = LogicalType(LogicalTypeId::VARCHAR);
    nats_stream_range_stats.named_parameters["start_seq"] = LogicalType(LogicalTypeId::UBIGINT);
    nats_stream_range_stats.named_parameters["end_seq"] = LogicalType(LogicalTypeId::UBIGINT);
    loader.RegisterFunction(nats_stream_range_stats);
}

void NatsCopyFunction::Register(ExtensionLoader &loader) {
    CopyFunction function("nats_js");
    function.copy_from_bind = NatsCopyFromBind;
    function.copy_from_function = CreateNatsScanTableFunction();
    function.extension = "nats_js";
    loader.RegisterFunction(function);
}

} // namespace duckdb
