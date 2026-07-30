#include "nats_scan.hpp"
#include "nats_connection.hpp"
#include "nats_message_decode.hpp"
#include "nats_source.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/copy_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/vector.hpp"
#include "yyjson.hpp"
#include <flatbuffers/flexbuffers.h>
#include <nats/nats.h>
#include <google/protobuf/compiler/importer.h>
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/descriptor.h>
#include <filesystem>
#include <algorithm>
#include <optional>
#include <mutex>
#include <chrono>
#include <thread>
#include <cstring>
#include <charconv>

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
    NatsConnectionConfig connection;
    uint64_t start_seq;
    uint64_t end_seq;
    int64_t start_time;  // nanoseconds since epoch, 0 = not set
    int64_t end_time;    // nanoseconds since epoch, 0 = not set
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
    vector<vector<const FieldDescriptor*>> proto_field_paths;
    uint64_t batch_size;
    int64_t fetch_timeout_ms;

    // Must outlive the query — proto_descriptor is owned by the importer's pool
    shared_ptr<DiskSourceTree> proto_source_tree;
    shared_ptr<ProtobufErrorCollector> proto_error_collector;
    shared_ptr<Importer> proto_importer;
    const Descriptor* proto_descriptor = nullptr;

    NatsScanBindData(string stream, string subject_substr, string nats_subj, NatsConnectionConfig connection_p,
                     uint64_t start, uint64_t end,
                     int64_t start_ts, int64_t end_ts, vector<string> json_flds,
                     vector<string> msgpack_flds,
                     vector<string> cbor_flds,
                     vector<string> flexbuffers_flds,
                     string proto_f, string proto_msg, vector<string> proto_flds,
                     vector<vector<const FieldDescriptor*>> proto_paths, uint64_t batch_sz, int64_t fetch_timeout)
        : stream_name(std::move(stream))
        , subject_contains(std::move(subject_substr))
        , nats_subject(std::move(nats_subj))
        , connection(std::move(connection_p))
        , start_seq(start)
        , end_seq(end)
        , start_time(start_ts)
        , end_time(end_ts)
        , json_fields(std::move(json_flds))
        , msgpack_fields(std::move(msgpack_flds))
        , cbor_fields(std::move(cbor_flds))
        , flexbuffers_fields(std::move(flexbuffers_flds))
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

struct NatsCopyToBindData : public TableFunctionData {
    enum class PayloadFormat { RAW, JSON, MSGPACK, CBOR, FLEXBUFFERS, PROTOBUF };
    string stream_name;
    NatsConnectionConfig connection;
    string subject_column = "subject";
    string payload_column = "payload";
    bool constant_subject = false;
    string subject_value;
    idx_t subject_idx = DConstants::INVALID_INDEX;
    idx_t payload_idx = DConstants::INVALID_INDEX;
    PayloadFormat payload_format = PayloadFormat::RAW;
    vector<LogicalType> column_types;
    vector<idx_t> structured_column_indices;
    vector<string> structured_column_names;
    vector<string> structured_json_keys;
    vector<vector<uint8_t>> structured_binary_keys;
    vector<vector<const FieldDescriptor *>> proto_field_paths;
    shared_ptr<DiskSourceTree> proto_source_tree;
    shared_ptr<ProtobufErrorCollector> proto_error_collector;
    shared_ptr<Importer> proto_importer;
    const Descriptor *proto_descriptor = nullptr;
};

struct NatsCopyToGlobalState : public GlobalFunctionData {
    natsConnection *conn = nullptr;
    mutex lock;
    idx_t rows_written = 0;
};

struct NatsCopyToLocalState : public LocalFunctionData {
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
        if (child.type().id() == LogicalTypeId::LIST) {
            for (auto &list_child : ListValue::GetChildren(child)) {
                result.push_back(StringValue::Get(list_child));
            }
        } else {
            result.push_back(StringValue::Get(child));
        }
    }
    return result;
}

static NatsSourceSchema BuildNatsSourceSchema(const vector<string> &json_fields, const vector<string> &msgpack_fields,
                                              const vector<string> &cbor_fields,
                                              const vector<string> &flexbuffers_fields,
                                              const string &proto_file,
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
    if (!proto_fields.empty() || (json_fields.empty() && msgpack_fields.empty() && cbor_fields.empty() &&
                                  flexbuffers_fields.empty())) {
        schema.return_types.emplace_back(LogicalType(LogicalTypeId::BLOB));
    } else {
        schema.return_types.emplace_back(LogicalType(LogicalTypeId::VARCHAR));
    }

    for (const auto &field : json_fields) {
        schema.names.emplace_back(field);
        schema.return_types.emplace_back(LogicalType(LogicalTypeId::VARCHAR));
    }

    for (const auto &field : msgpack_fields) {
        schema.names.emplace_back(field);
        schema.return_types.emplace_back(LogicalType(LogicalTypeId::VARCHAR));
    }

    for (const auto &field : cbor_fields) {
        schema.names.emplace_back(field);
        schema.return_types.emplace_back(LogicalType(LogicalTypeId::VARCHAR));
    }

    for (const auto &field : flexbuffers_fields) {
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

static string ResolveNatsCopyStreamName(const string &file_path, const char *copy_mode) {
    if (file_path.empty()) {
        throw BinderException("%s FORMAT nats_js requires a stream name in the file path", copy_mode);
    }
    return file_path;
}

static string ResolveNatsCopyUrl(const case_insensitive_map_t<vector<Value>> &options, const char *copy_mode,
                                 const string *default_url) {
    auto url = GetCopyOptionString(options, "url");
    if (url.has_value() && !url->empty()) {
        return *url;
    }
    if (default_url != nullptr) {
        return *default_url;
    }
    throw BinderException("%s FORMAT nats_js requires a \"url\" option", copy_mode);
}

static void ValidateNatsCopyFromSchema(const vector<string> &expected_names, const vector<LogicalType> &expected_types,
                                       const NatsSourceSchema &schema) {
    if (schema.names.size() != expected_names.size() || schema.return_types.size() != expected_types.size()) {
        throw std::runtime_error("COPY FROM FORMAT nats_js requires the target table schema to match the nats_scan output schema");
    }
    for (idx_t i = 0; i < schema.return_types.size(); i++) {
        if (!StringUtil::CIEquals(schema.names[i], expected_names[i]) || schema.return_types[i] != expected_types[i]) {
            throw std::runtime_error("COPY FROM FORMAT nats_js requires the target table schema to match the nats_scan output schema");
        }
    }
}

static idx_t FindColumnIndex(const vector<string> &names, const string &column_name) {
    for (idx_t i = 0; i < names.size(); i++) {
        if (StringUtil::CIEquals(names[i], column_name)) {
            return i;
        }
    }
    return DConstants::INVALID_INDEX;
}

static void NatsCopyListOptions(ClientContext &, CopyOptionsInput &input) {
    auto &copy_options = input.options;
    copy_options["url"] = CopyOption(LogicalType::VARCHAR, CopyOptionMode::READ_WRITE);
    copy_options["credentials_file"] = CopyOption(LogicalType::VARCHAR, CopyOptionMode::READ_WRITE);
    copy_options["tls_ca_file"] = CopyOption(LogicalType::VARCHAR, CopyOptionMode::READ_WRITE);
    copy_options["tls_cert_file"] = CopyOption(LogicalType::VARCHAR, CopyOptionMode::READ_WRITE);
    copy_options["tls_key_file"] = CopyOption(LogicalType::VARCHAR, CopyOptionMode::READ_WRITE);
    copy_options["tls_server_name"] = CopyOption(LogicalType::VARCHAR, CopyOptionMode::READ_WRITE);
    copy_options["tls_skip_verify"] = CopyOption(LogicalType::BOOLEAN, CopyOptionMode::READ_WRITE);
    copy_options["subject"] = CopyOption(LogicalType::VARCHAR, CopyOptionMode::READ_WRITE);
    copy_options["subject_contains"] = CopyOption(LogicalType::VARCHAR, CopyOptionMode::READ_WRITE);
    copy_options["nats_subject"] = CopyOption(LogicalType::VARCHAR, CopyOptionMode::READ_WRITE);
    copy_options["start_seq"] = CopyOption(LogicalType::UBIGINT, CopyOptionMode::READ_WRITE);
    copy_options["end_seq"] = CopyOption(LogicalType::UBIGINT, CopyOptionMode::READ_WRITE);
    copy_options["start_time"] = CopyOption(LogicalType::TIMESTAMP, CopyOptionMode::READ_WRITE);
    copy_options["end_time"] = CopyOption(LogicalType::TIMESTAMP, CopyOptionMode::READ_WRITE);
    copy_options["json_extract"] = CopyOption(LogicalType::ANY, CopyOptionMode::READ_WRITE);
    copy_options["msgpack_extract"] = CopyOption(LogicalType::ANY, CopyOptionMode::READ_WRITE);
    copy_options["cbor_extract"] = CopyOption(LogicalType::ANY, CopyOptionMode::READ_WRITE);
    copy_options["flexbuffers_extract"] = CopyOption(LogicalType::ANY, CopyOptionMode::READ_WRITE);
    copy_options["proto_file"] = CopyOption(LogicalType::VARCHAR, CopyOptionMode::READ_WRITE);
    copy_options["proto_message"] = CopyOption(LogicalType::VARCHAR, CopyOptionMode::READ_WRITE);
    copy_options["proto_extract"] = CopyOption(LogicalType::ANY, CopyOptionMode::READ_WRITE);
    copy_options["batch_size"] = CopyOption(LogicalType::UBIGINT, CopyOptionMode::READ_WRITE);
    copy_options["fetch_timeout_ms"] = CopyOption(LogicalType::BIGINT, CopyOptionMode::READ_WRITE);
    copy_options["subject_column"] = CopyOption(LogicalType::VARCHAR, CopyOptionMode::READ_WRITE);
    copy_options["payload_column"] = CopyOption(LogicalType::VARCHAR, CopyOptionMode::READ_WRITE);
    copy_options["payload_format"] = CopyOption(LogicalType::VARCHAR, CopyOptionMode::READ_WRITE);
    copy_options["payload_columns"] = CopyOption(LogicalType::ANY, CopyOptionMode::READ_WRITE);
    copy_options["proto_fields"] = CopyOption(LogicalType::ANY, CopyOptionMode::READ_WRITE);
}

static void DisconnectNats(natsConnection **conn) {
    if (conn && *conn != nullptr) {
        natsConnection_Destroy(*conn);
        *conn = nullptr;
    }
}

static bool IsCopyStringType(LogicalTypeId type) {
    return type == LogicalTypeId::VARCHAR || type == LogicalTypeId::BLOB;
}

static bool IsCopySignedType(LogicalTypeId type) {
    return type == LogicalTypeId::TINYINT || type == LogicalTypeId::SMALLINT || type == LogicalTypeId::INTEGER ||
           type == LogicalTypeId::BIGINT || type == LogicalTypeId::HUGEINT;
}

static bool IsCopyUnsignedType(LogicalTypeId type) {
    return type == LogicalTypeId::UTINYINT || type == LogicalTypeId::USMALLINT || type == LogicalTypeId::UINTEGER ||
           type == LogicalTypeId::UBIGINT || type == LogicalTypeId::UHUGEINT;
}

static string CopyValueText(const Value &value) {
    if (value.IsNull()) {
        return string();
    }
    if (value.type().id() == LogicalTypeId::VARCHAR) {
        return StringValue::Get(value);
    }
    return value.ToString();
}

static void AppendJsonEscaped(string &output, const string &value) {
    output.push_back('"');
    for (unsigned char ch : value) {
        switch (ch) {
        case '"': output += "\\\""; break;
        case '\\': output += "\\\\"; break;
        case '\b': output += "\\b"; break;
        case '\f': output += "\\f"; break;
        case '\n': output += "\\n"; break;
        case '\r': output += "\\r"; break;
        case '\t': output += "\\t"; break;
        default:
            if (ch < 0x20) {
                char buffer[7];
                snprintf(buffer, sizeof(buffer), "\\u%04x", ch);
                output += buffer;
            } else {
                output.push_back(static_cast<char>(ch));
            }
        }
    }
    output.push_back('"');
}

static void AppendJsonValue(string &output, const Value &value) {
    if (value.IsNull()) {
        output += "null";
        return;
    }
    auto type = value.type().id();
    if (type == LogicalTypeId::LIST || type == LogicalTypeId::ARRAY) {
        output += "[";
        const auto &children = type == LogicalTypeId::LIST ? ListValue::GetChildren(value) : ArrayValue::GetChildren(value);
        for (idx_t i = 0; i < children.size(); i++) {
            if (i) output += ",";
            AppendJsonValue(output, children[i]);
        }
        output += "]";
    } else if (type == LogicalTypeId::STRUCT) {
        output += "{";
        const auto &children = StructValue::GetChildren(value);
        for (idx_t i = 0; i < children.size(); i++) {
            if (i) output += ",";
            AppendJsonEscaped(output, StructType::GetChildName(value.type(), i));
            output += ":";
            AppendJsonValue(output, children[i]);
        }
        output += "}";
    } else if (type == LogicalTypeId::MAP) {
        output += "{";
        const auto &entries = MapValue::GetChildren(value);
        for (idx_t i = 0; i < entries.size(); i++) {
            if (i) output += ",";
            const auto &entry = StructValue::GetChildren(entries[i]);
            AppendJsonEscaped(output, CopyValueText(entry[0]));
            output += ":";
            AppendJsonValue(output, entry[1]);
        }
        output += "}";
    } else if (IsCopyStringType(type)) {
        AppendJsonEscaped(output, CopyValueText(value));
    } else if (type == LogicalTypeId::BOOLEAN || IsCopySignedType(type) || IsCopyUnsignedType(type) ||
               type == LogicalTypeId::FLOAT || type == LogicalTypeId::DOUBLE || type == LogicalTypeId::DECIMAL) {
        output += value.ToString();
    } else {
        AppendJsonEscaped(output, value.ToString());
    }
}

static bool AppendJsonVectorScalar(string &output, const UnifiedVectorFormat &format, idx_t row_idx,
                                   LogicalTypeId type) {
    auto value_idx = format.sel->get_index(row_idx);
    if (!format.validity.RowIsValid(value_idx)) {
        output += "null";
        return true;
    }
    switch (type) {
    case LogicalTypeId::VARCHAR:
    case LogicalTypeId::BLOB: {
        auto value = UnifiedVectorFormat::GetData<string_t>(format)[value_idx];
        AppendJsonEscaped(output, string(value.GetData(), value.GetSize()));
        return true;
    }
    case LogicalTypeId::BOOLEAN:
        output += UnifiedVectorFormat::GetData<bool>(format)[value_idx] ? "true" : "false";
        return true;
    case LogicalTypeId::TINYINT:
        output += std::to_string(UnifiedVectorFormat::GetData<int8_t>(format)[value_idx]);
        return true;
    case LogicalTypeId::SMALLINT:
        output += std::to_string(UnifiedVectorFormat::GetData<int16_t>(format)[value_idx]);
        return true;
    case LogicalTypeId::INTEGER:
        output += std::to_string(UnifiedVectorFormat::GetData<int32_t>(format)[value_idx]);
        return true;
    case LogicalTypeId::BIGINT:
        output += std::to_string(UnifiedVectorFormat::GetData<int64_t>(format)[value_idx]);
        return true;
    case LogicalTypeId::UTINYINT:
        output += std::to_string(UnifiedVectorFormat::GetData<uint8_t>(format)[value_idx]);
        return true;
    case LogicalTypeId::USMALLINT:
        output += std::to_string(UnifiedVectorFormat::GetData<uint16_t>(format)[value_idx]);
        return true;
    case LogicalTypeId::UINTEGER:
        output += std::to_string(UnifiedVectorFormat::GetData<uint32_t>(format)[value_idx]);
        return true;
    case LogicalTypeId::UBIGINT:
        output += std::to_string(UnifiedVectorFormat::GetData<uint64_t>(format)[value_idx]);
        return true;
    case LogicalTypeId::FLOAT:
    case LogicalTypeId::DOUBLE: {
        char buffer[64];
        auto number = type == LogicalTypeId::FLOAT ? UnifiedVectorFormat::GetData<float>(format)[value_idx]
                                                   : UnifiedVectorFormat::GetData<double>(format)[value_idx];
        auto converted = std::to_chars(buffer, buffer + sizeof(buffer), number);
        if (converted.ec != std::errc()) return false;
        output.append(buffer, converted.ptr);
        return true;
    }
    default:
        return false;
    }
}

static void AppendBigEndian(vector<uint8_t> &output, uint64_t value, idx_t width) {
    for (idx_t i = width; i > 0; i--) {
        output.push_back(static_cast<uint8_t>(value >> ((i - 1) * 8)));
    }
}

static void AppendMsgpackString(vector<uint8_t> &output, const string &value) {
    auto size = value.size();
    if (size < 32) {
        output.push_back(static_cast<uint8_t>(0xa0 | size));
    } else if (size <= UINT8_MAX) {
        output.push_back(0xd9);
        output.push_back(static_cast<uint8_t>(size));
    } else if (size <= UINT16_MAX) {
        output.push_back(0xda);
        AppendBigEndian(output, size, 2);
    } else {
        output.push_back(0xdb);
        AppendBigEndian(output, size, 4);
    }
    output.insert(output.end(), value.begin(), value.end());
}

static void AppendCborString(vector<uint8_t> &output, const string &value) {
    auto size = value.size();
    if (size < 24) {
        output.push_back(static_cast<uint8_t>(0x60 | size));
    } else if (size <= UINT8_MAX) {
        output.push_back(0x78);
        output.push_back(static_cast<uint8_t>(size));
    } else if (size <= UINT16_MAX) {
        output.push_back(0x79);
        AppendBigEndian(output, size, 2);
    } else {
        output.push_back(0x7a);
        AppendBigEndian(output, size, 4);
    }
    output.insert(output.end(), value.begin(), value.end());
}

static void AppendStructuredCount(vector<uint8_t> &output, idx_t count, bool cbor, bool map) {
    if (cbor) {
        uint8_t major = map ? 0xa0 : 0x80;
        if (count < 24) {
            output.push_back(static_cast<uint8_t>(major | count));
        } else if (count <= UINT8_MAX) {
            output.push_back(static_cast<uint8_t>((map ? 0xb8 : 0x98)));
            AppendBigEndian(output, count, 1);
        } else if (count <= UINT16_MAX) {
            output.push_back(static_cast<uint8_t>((map ? 0xb9 : 0x99)));
            AppendBigEndian(output, count, 2);
        } else {
            output.push_back(static_cast<uint8_t>((map ? 0xba : 0x9a)));
            AppendBigEndian(output, count, 4);
        }
    } else if (map) {
        if (count < 16) output.push_back(static_cast<uint8_t>(0x80 | count));
        else { output.push_back(0xde); AppendBigEndian(output, count, 2); }
    } else {
        if (count < 16) output.push_back(static_cast<uint8_t>(0x90 | count));
        else { output.push_back(0xdc); AppendBigEndian(output, count, 2); }
    }
}

static bool AppendStructuredVectorScalar(vector<uint8_t> &output, const UnifiedVectorFormat &format, idx_t row_idx,
                                          LogicalTypeId type, bool cbor) {
    auto value_idx = format.sel->get_index(row_idx);
    if (!format.validity.RowIsValid(value_idx)) {
        output.push_back(0xc0);
        return true;
    }
    auto append_signed = [&](int64_t number) {
        if (cbor) {
            if (number >= 0 && number < 24) output.push_back(static_cast<uint8_t>(number));
            else if (number >= 0) { output.push_back(0x1b); AppendBigEndian(output, static_cast<uint64_t>(number), 8); }
            else { output.push_back(0x3b); AppendBigEndian(output, static_cast<uint64_t>(-(number + 1)), 8); }
        } else if (number >= -32 && number < 128) {
            output.push_back(static_cast<uint8_t>(number));
        } else {
            output.push_back(0xd3);
            AppendBigEndian(output, static_cast<uint64_t>(number), 8);
        }
    };
    auto append_unsigned = [&](uint64_t number) {
        if ((cbor || number < 128) && number < 24) output.push_back(static_cast<uint8_t>(number));
        else if (!cbor && number < 128) output.push_back(static_cast<uint8_t>(number));
        else { output.push_back(cbor ? 0x1b : 0xcf); AppendBigEndian(output, number, 8); }
    };
    switch (type) {
    case LogicalTypeId::VARCHAR:
    case LogicalTypeId::BLOB: {
        auto value = UnifiedVectorFormat::GetData<string_t>(format)[value_idx];
        auto text = string(value.GetData(), value.GetSize());
        cbor ? AppendCborString(output, text) : AppendMsgpackString(output, text);
        return true;
    }
    case LogicalTypeId::BOOLEAN:
        output.push_back(cbor ? (UnifiedVectorFormat::GetData<bool>(format)[value_idx] ? 0xf5 : 0xf4)
                              : (UnifiedVectorFormat::GetData<bool>(format)[value_idx] ? 0xc3 : 0xc2));
        return true;
    case LogicalTypeId::TINYINT: append_signed(UnifiedVectorFormat::GetData<int8_t>(format)[value_idx]); return true;
    case LogicalTypeId::SMALLINT: append_signed(UnifiedVectorFormat::GetData<int16_t>(format)[value_idx]); return true;
    case LogicalTypeId::INTEGER: append_signed(UnifiedVectorFormat::GetData<int32_t>(format)[value_idx]); return true;
    case LogicalTypeId::BIGINT: append_signed(UnifiedVectorFormat::GetData<int64_t>(format)[value_idx]); return true;
    case LogicalTypeId::UTINYINT: append_unsigned(UnifiedVectorFormat::GetData<uint8_t>(format)[value_idx]); return true;
    case LogicalTypeId::USMALLINT: append_unsigned(UnifiedVectorFormat::GetData<uint16_t>(format)[value_idx]); return true;
    case LogicalTypeId::UINTEGER: append_unsigned(UnifiedVectorFormat::GetData<uint32_t>(format)[value_idx]); return true;
    case LogicalTypeId::UBIGINT: append_unsigned(UnifiedVectorFormat::GetData<uint64_t>(format)[value_idx]); return true;
    case LogicalTypeId::FLOAT: {
        double number = UnifiedVectorFormat::GetData<float>(format)[value_idx];
        uint64_t bits;
        memcpy(&bits, &number, sizeof(bits));
        output.push_back(cbor ? 0xfb : 0xcb);
        AppendBigEndian(output, bits, 8);
        return true;
    }
    case LogicalTypeId::DOUBLE: {
        auto number = UnifiedVectorFormat::GetData<double>(format)[value_idx];
        uint64_t bits;
        memcpy(&bits, &number, sizeof(bits));
        output.push_back(cbor ? 0xfb : 0xcb);
        AppendBigEndian(output, bits, 8);
        return true;
    }
    default:
        return false;
    }
}

static void AppendStructuredValue(vector<uint8_t> &output, const Value &value, bool cbor) {
    if (value.IsNull()) {
        output.push_back(0xc0);
        return;
    }
    auto type = value.type().id();
    if (type == LogicalTypeId::LIST || type == LogicalTypeId::ARRAY) {
        const auto &children = type == LogicalTypeId::LIST ? ListValue::GetChildren(value) : ArrayValue::GetChildren(value);
        AppendStructuredCount(output, children.size(), cbor, false);
        for (const auto &child : children) AppendStructuredValue(output, child, cbor);
    } else if (type == LogicalTypeId::STRUCT) {
        const auto &children = StructValue::GetChildren(value);
        AppendStructuredCount(output, children.size(), cbor, true);
        for (idx_t i = 0; i < children.size(); i++) {
            auto name = StructType::GetChildName(value.type(), i);
            cbor ? AppendCborString(output, name) : AppendMsgpackString(output, name);
            AppendStructuredValue(output, children[i], cbor);
        }
    } else if (type == LogicalTypeId::MAP) {
        const auto &entries = MapValue::GetChildren(value);
        AppendStructuredCount(output, entries.size(), cbor, true);
        for (const auto &entry : entries) {
            const auto &kv = StructValue::GetChildren(entry);
            auto key = CopyValueText(kv[0]);
            cbor ? AppendCborString(output, key) : AppendMsgpackString(output, key);
            AppendStructuredValue(output, kv[1], cbor);
        }
    } else if (IsCopyStringType(type)) {
        cbor ? AppendCborString(output, CopyValueText(value)) : AppendMsgpackString(output, CopyValueText(value));
    } else if (type == LogicalTypeId::BOOLEAN) {
        output.push_back(cbor ? (value.GetValue<bool>() ? 0xf5 : 0xf4) : (value.GetValue<bool>() ? 0xc3 : 0xc2));
    } else if (IsCopySignedType(type)) {
        auto number = std::stoll(value.ToString());
        if (cbor) {
            if (number >= 0) {
                if (number < 24) output.push_back(static_cast<uint8_t>(number));
                else { output.push_back(0x1b); AppendBigEndian(output, static_cast<uint64_t>(number), 8); }
            } else {
                auto encoded = static_cast<uint64_t>(-(number + 1));
                if (encoded < 24) output.push_back(static_cast<uint8_t>(0x20 | encoded));
                else { output.push_back(0x3b); AppendBigEndian(output, encoded, 8); }
            }
        } else if (number >= -32 && number < 128) {
            output.push_back(static_cast<uint8_t>(number));
        } else {
            output.push_back(0xd3);
            AppendBigEndian(output, static_cast<uint64_t>(number), 8);
        }
    } else if (IsCopyUnsignedType(type)) {
        auto number = std::stoull(value.ToString());
        if (cbor && number < 24) output.push_back(static_cast<uint8_t>(number));
        else if (!cbor && number < 128) output.push_back(static_cast<uint8_t>(number));
        else { output.push_back(cbor ? 0x1b : 0xcf); AppendBigEndian(output, number, 8); }
    } else if (type == LogicalTypeId::FLOAT || type == LogicalTypeId::DOUBLE || type == LogicalTypeId::DECIMAL) {
        double number = std::stod(value.ToString());
        uint64_t bits;
        memcpy(&bits, &number, sizeof(bits));
        output.push_back(cbor ? 0xfb : 0xcb);
        AppendBigEndian(output, bits, 8);
    } else {
        cbor ? AppendCborString(output, value.ToString()) : AppendMsgpackString(output, value.ToString());
    }
}

static void SetProtobufOutputField(google::protobuf::Message &message,
                                   const vector<const FieldDescriptor *> &field_path, const Value &value) {
    if (value.IsNull() || field_path.empty()) {
        return;
    }
    Message *current_message = &message;
    const Reflection *reflection = current_message->GetReflection();
    for (idx_t i = 0; i + 1 < field_path.size(); i++) {
        auto *field = field_path[i];
        if (!field || field->is_repeated() || field->is_map() || field->type() != FieldDescriptor::TYPE_MESSAGE) {
            throw BinderException("COPY TO protobuf fields must use singular message paths");
        }
        current_message = reflection->MutableMessage(current_message, field);
        reflection = current_message->GetReflection();
    }

    auto *field = field_path.back();
    if (!field) {
        throw BinderException("COPY TO protobuf output has an invalid field path");
    }
    if (field->is_map()) {
        if (value.type().id() != LogicalTypeId::MAP) {
            throw BinderException("COPY TO protobuf map field '%s' requires a MAP source column", field->full_name());
        }
        auto *map_key = field->message_type()->map_key();
        auto *map_value = field->message_type()->map_value();
        if (map_value->cpp_type() == FieldDescriptor::CPPTYPE_MESSAGE) {
            throw BinderException("COPY TO protobuf map field '%s' does not support message values", field->full_name());
        }
        for (const auto &entry : MapValue::GetChildren(value)) {
            const auto &key_value = StructValue::GetChildren(entry);
            auto *map_entry = reflection->AddMessage(&message, field);
            auto *entry_key = field->message_type()->FindFieldByName("key");
            auto *entry_value = field->message_type()->FindFieldByName("value");
            const auto *entry_reflection = map_entry->GetReflection();
            switch (map_key->cpp_type()) {
            case FieldDescriptor::CPPTYPE_INT32: entry_reflection->SetInt32(map_entry, entry_key, std::stoll(key_value[0].ToString())); break;
            case FieldDescriptor::CPPTYPE_INT64: entry_reflection->SetInt64(map_entry, entry_key, std::stoll(key_value[0].ToString())); break;
            case FieldDescriptor::CPPTYPE_UINT32: entry_reflection->SetUInt32(map_entry, entry_key, std::stoull(key_value[0].ToString())); break;
            case FieldDescriptor::CPPTYPE_UINT64: entry_reflection->SetUInt64(map_entry, entry_key, std::stoull(key_value[0].ToString())); break;
            case FieldDescriptor::CPPTYPE_BOOL: entry_reflection->SetBool(map_entry, entry_key, key_value[0].GetValue<bool>()); break;
            case FieldDescriptor::CPPTYPE_STRING: entry_reflection->SetString(map_entry, entry_key, key_value[0].ToString()); break;
            default:
                throw BinderException("COPY TO protobuf map field '%s' has an unsupported key type", field->full_name());
            }
            const auto &map_scalar = key_value[1];
            if (map_scalar.IsNull()) continue;
            switch (map_value->cpp_type()) {
            case FieldDescriptor::CPPTYPE_INT32: entry_reflection->SetInt32(map_entry, entry_value, std::stoll(map_scalar.ToString())); break;
            case FieldDescriptor::CPPTYPE_INT64: entry_reflection->SetInt64(map_entry, entry_value, std::stoll(map_scalar.ToString())); break;
            case FieldDescriptor::CPPTYPE_UINT32: entry_reflection->SetUInt32(map_entry, entry_value, std::stoull(map_scalar.ToString())); break;
            case FieldDescriptor::CPPTYPE_UINT64: entry_reflection->SetUInt64(map_entry, entry_value, std::stoull(map_scalar.ToString())); break;
            case FieldDescriptor::CPPTYPE_BOOL: entry_reflection->SetBool(map_entry, entry_value, map_scalar.GetValue<bool>()); break;
            case FieldDescriptor::CPPTYPE_FLOAT: entry_reflection->SetFloat(map_entry, entry_value, std::stof(map_scalar.ToString())); break;
            case FieldDescriptor::CPPTYPE_DOUBLE: entry_reflection->SetDouble(map_entry, entry_value, std::stod(map_scalar.ToString())); break;
            case FieldDescriptor::CPPTYPE_STRING: entry_reflection->SetString(map_entry, entry_value, map_scalar.ToString()); break;
            case FieldDescriptor::CPPTYPE_ENUM: {
                auto enum_value = entry_value->enum_type()->FindValueByName(map_scalar.ToString());
                if (!enum_value) enum_value = entry_value->enum_type()->FindValueByNumber(std::stoi(map_scalar.ToString()));
                if (!enum_value) {
                    throw BinderException("COPY TO protobuf map field '%s' has an invalid enum value", field->full_name());
                }
                entry_reflection->SetEnum(map_entry, entry_value, enum_value);
                break;
            }
            default:
                throw BinderException("COPY TO protobuf map field '%s' has an unsupported value type", field->full_name());
            }
        }
        return;
    }
    if (field->type() == FieldDescriptor::TYPE_MESSAGE) {
        throw BinderException("COPY TO protobuf output does not support message-valued fields");
    }
    auto write_scalar = [&](const Value &scalar) {
        switch (field->type()) {
        case FieldDescriptor::TYPE_STRING:
            field->is_repeated() ? reflection->AddString(current_message, field, scalar.ToString())
                                  : reflection->SetString(current_message, field, scalar.ToString());
            break;
        case FieldDescriptor::TYPE_BYTES:
            field->is_repeated() ? reflection->AddString(current_message, field, scalar.ToString())
                                  : reflection->SetString(current_message, field, scalar.ToString());
            break;
        case FieldDescriptor::TYPE_INT32:
        case FieldDescriptor::TYPE_SINT32:
        case FieldDescriptor::TYPE_SFIXED32:
            field->is_repeated() ? reflection->AddInt32(current_message, field, std::stoll(scalar.ToString()))
                                  : reflection->SetInt32(current_message, field, std::stoll(scalar.ToString()));
            break;
        case FieldDescriptor::TYPE_INT64:
        case FieldDescriptor::TYPE_SINT64:
        case FieldDescriptor::TYPE_SFIXED64:
            field->is_repeated() ? reflection->AddInt64(current_message, field, std::stoll(scalar.ToString()))
                                  : reflection->SetInt64(current_message, field, std::stoll(scalar.ToString()));
            break;
        case FieldDescriptor::TYPE_UINT32:
        case FieldDescriptor::TYPE_FIXED32:
            field->is_repeated() ? reflection->AddUInt32(current_message, field, std::stoull(scalar.ToString()))
                                  : reflection->SetUInt32(current_message, field, std::stoull(scalar.ToString()));
            break;
        case FieldDescriptor::TYPE_UINT64:
        case FieldDescriptor::TYPE_FIXED64:
            field->is_repeated() ? reflection->AddUInt64(current_message, field, std::stoull(scalar.ToString()))
                                  : reflection->SetUInt64(current_message, field, std::stoull(scalar.ToString()));
            break;
        case FieldDescriptor::TYPE_FLOAT:
            field->is_repeated() ? reflection->AddFloat(current_message, field, std::stof(scalar.ToString()))
                                  : reflection->SetFloat(current_message, field, std::stof(scalar.ToString()));
            break;
        case FieldDescriptor::TYPE_DOUBLE:
            field->is_repeated() ? reflection->AddDouble(current_message, field, std::stod(scalar.ToString()))
                                  : reflection->SetDouble(current_message, field, std::stod(scalar.ToString()));
            break;
        case FieldDescriptor::TYPE_BOOL:
            field->is_repeated() ? reflection->AddBool(current_message, field, scalar.GetValue<bool>())
                                  : reflection->SetBool(current_message, field, scalar.GetValue<bool>());
            break;
        case FieldDescriptor::TYPE_ENUM: {
            auto enum_value = field->enum_type()->FindValueByName(scalar.ToString());
            if (!enum_value) enum_value = field->enum_type()->FindValueByNumber(std::stoi(scalar.ToString()));
            if (!enum_value) {
                throw BinderException("COPY TO protobuf enum value '%s' was not found for field '%s'", scalar.ToString(),
                                      field->full_name());
            }
            field->is_repeated() ? reflection->AddEnum(current_message, field, enum_value)
                                 : reflection->SetEnum(current_message, field, enum_value);
            break;
        }
        default:
            throw BinderException("COPY TO protobuf field '%s' has an unsupported type", field->full_name());
        }
    };
    if (field->is_repeated()) {
        if (value.type().id() != LogicalTypeId::LIST && value.type().id() != LogicalTypeId::ARRAY) {
            throw BinderException("COPY TO repeated protobuf field '%s' requires a LIST or ARRAY source column",
                                  field->full_name());
        }
        const auto &children = value.type().id() == LogicalTypeId::LIST ? ListValue::GetChildren(value)
                                                                          : ArrayValue::GetChildren(value);
        for (const auto &child : children) {
            if (!child.IsNull()) write_scalar(child);
        }
    } else {
        write_scalar(value);
    }
}

static bool SetProtobufOutputVectorScalar(google::protobuf::Message &message,
                                          const vector<const FieldDescriptor *> &field_path,
                                          const UnifiedVectorFormat &format, idx_t row_idx, LogicalTypeId source_type) {
    if (field_path.size() != 1 || field_path[0]->is_repeated() || field_path[0]->is_map() ||
        field_path[0]->type() == FieldDescriptor::TYPE_MESSAGE) {
        return false;
    }
    auto value_idx = format.sel->get_index(row_idx);
    if (!format.validity.RowIsValid(value_idx)) {
        return true;
    }
    auto *field = field_path[0];
    auto *reflection = message.GetReflection();
    auto signed_value = [&](int64_t &result) {
        switch (source_type) {
        case LogicalTypeId::TINYINT: result = UnifiedVectorFormat::GetData<int8_t>(format)[value_idx]; return true;
        case LogicalTypeId::SMALLINT: result = UnifiedVectorFormat::GetData<int16_t>(format)[value_idx]; return true;
        case LogicalTypeId::INTEGER: result = UnifiedVectorFormat::GetData<int32_t>(format)[value_idx]; return true;
        case LogicalTypeId::BIGINT: result = UnifiedVectorFormat::GetData<int64_t>(format)[value_idx]; return true;
        default: return false;
        }
    };
    auto unsigned_value = [&](uint64_t &result) {
        switch (source_type) {
        case LogicalTypeId::UTINYINT: result = UnifiedVectorFormat::GetData<uint8_t>(format)[value_idx]; return true;
        case LogicalTypeId::USMALLINT: result = UnifiedVectorFormat::GetData<uint16_t>(format)[value_idx]; return true;
        case LogicalTypeId::UINTEGER: result = UnifiedVectorFormat::GetData<uint32_t>(format)[value_idx]; return true;
        case LogicalTypeId::UBIGINT: result = UnifiedVectorFormat::GetData<uint64_t>(format)[value_idx]; return true;
        default: return false;
        }
    };
    switch (field->type()) {
    case FieldDescriptor::TYPE_STRING:
    case FieldDescriptor::TYPE_BYTES: {
        if (source_type != LogicalTypeId::VARCHAR && source_type != LogicalTypeId::BLOB) return false;
        auto value = UnifiedVectorFormat::GetData<string_t>(format)[value_idx];
        reflection->SetString(&message, field, string(value.GetData(), value.GetSize()));
        return true;
    }
    case FieldDescriptor::TYPE_BOOL:
        if (source_type != LogicalTypeId::BOOLEAN) return false;
        reflection->SetBool(&message, field, UnifiedVectorFormat::GetData<bool>(format)[value_idx]);
        return true;
    case FieldDescriptor::TYPE_INT32:
    case FieldDescriptor::TYPE_SINT32:
    case FieldDescriptor::TYPE_SFIXED32:
    case FieldDescriptor::TYPE_INT64:
    case FieldDescriptor::TYPE_SINT64:
    case FieldDescriptor::TYPE_SFIXED64: {
        int64_t value;
        if (!signed_value(value)) return false;
        if (field->type() == FieldDescriptor::TYPE_INT32 || field->type() == FieldDescriptor::TYPE_SINT32 ||
            field->type() == FieldDescriptor::TYPE_SFIXED32) reflection->SetInt32(&message, field, value);
        else reflection->SetInt64(&message, field, value);
        return true;
    }
    case FieldDescriptor::TYPE_UINT32:
    case FieldDescriptor::TYPE_FIXED32:
    case FieldDescriptor::TYPE_UINT64:
    case FieldDescriptor::TYPE_FIXED64: {
        uint64_t value;
        if (!unsigned_value(value)) return false;
        if (field->type() == FieldDescriptor::TYPE_UINT32 || field->type() == FieldDescriptor::TYPE_FIXED32)
            reflection->SetUInt32(&message, field, value);
        else reflection->SetUInt64(&message, field, value);
        return true;
    }
    case FieldDescriptor::TYPE_FLOAT:
        if (source_type == LogicalTypeId::FLOAT) reflection->SetFloat(&message, field, UnifiedVectorFormat::GetData<float>(format)[value_idx]);
        else return false;
        return true;
    case FieldDescriptor::TYPE_DOUBLE:
        if (source_type == LogicalTypeId::DOUBLE) reflection->SetDouble(&message, field, UnifiedVectorFormat::GetData<double>(format)[value_idx]);
        else if (source_type == LogicalTypeId::FLOAT) reflection->SetDouble(&message, field, UnifiedVectorFormat::GetData<float>(format)[value_idx]);
        else return false;
        return true;
    default:
        return false;
    }
}

static void EncodeProtobufPayload(const NatsCopyToBindData &bind_data, const DataChunk &input, idx_t row_idx,
                                  const vector<UnifiedVectorFormat> &structured_formats, vector<uint8_t> &output) {
    DynamicMessageFactory factory;
    const auto *prototype = factory.GetPrototype(bind_data.proto_descriptor);
    if (!prototype) {
        throw BinderException("COPY TO could not create protobuf message '%s'", bind_data.proto_descriptor->full_name());
    }
    auto message = unique_ptr<Message>(prototype->New());
    for (idx_t i = 0; i < bind_data.structured_column_indices.size(); i++) {
        auto column = bind_data.structured_column_indices[i];
        if (!SetProtobufOutputVectorScalar(*message, bind_data.proto_field_paths[i], structured_formats[i], row_idx,
                                           bind_data.column_types[column].id())) {
            SetProtobufOutputField(*message, bind_data.proto_field_paths[i], input.GetValue(column, row_idx));
        }
    }
    string serialized;
    if (!message->SerializeToString(&serialized)) {
        throw BinderException("COPY TO failed to serialize protobuf message '%s'", bind_data.proto_descriptor->full_name());
    }
    output.assign(serialized.begin(), serialized.end());
}

static void AppendFlexValue(flexbuffers::Builder &builder, const Value &value);

static void AppendFlexField(flexbuffers::Builder &builder, const string &key, const Value &value) {
    auto key_ptr = key.c_str();
    if (value.IsNull()) {
        builder.Null(key_ptr);
    } else if (value.type().id() == LogicalTypeId::LIST || value.type().id() == LogicalTypeId::ARRAY) {
        builder.Vector(key_ptr, [&]() {
            const auto &children = value.type().id() == LogicalTypeId::LIST ? ListValue::GetChildren(value)
                                                                              : ArrayValue::GetChildren(value);
            for (const auto &child : children) AppendFlexValue(builder, child);
        });
    } else if (value.type().id() == LogicalTypeId::STRUCT || value.type().id() == LogicalTypeId::MAP) {
        builder.Map(key_ptr, [&]() {
            if (value.type().id() == LogicalTypeId::STRUCT) {
                const auto &children = StructValue::GetChildren(value);
                for (idx_t i = 0; i < children.size(); i++) {
                    AppendFlexField(builder, StructType::GetChildName(value.type(), i), children[i]);
                }
            } else {
                for (const auto &entry : MapValue::GetChildren(value)) {
                    const auto &kv = StructValue::GetChildren(entry);
                    AppendFlexField(builder, CopyValueText(kv[0]), kv[1]);
                }
            }
        });
    } else if (value.type().id() == LogicalTypeId::BOOLEAN) {
        builder.Bool(key_ptr, value.GetValue<bool>());
    } else if (IsCopySignedType(value.type().id())) {
        builder.Int(key_ptr, std::stoll(value.ToString()));
    } else if (IsCopyUnsignedType(value.type().id())) {
        builder.UInt(key_ptr, std::stoull(value.ToString()));
    } else if (value.type().id() == LogicalTypeId::FLOAT || value.type().id() == LogicalTypeId::DOUBLE ||
               value.type().id() == LogicalTypeId::DECIMAL) {
        builder.Double(key_ptr, std::stod(value.ToString()));
    } else {
        builder.String(key_ptr, CopyValueText(value));
    }
}

static void AppendFlexValue(flexbuffers::Builder &builder, const Value &value) {
    if (value.IsNull()) {
        builder.Null();
    } else if (value.type().id() == LogicalTypeId::LIST || value.type().id() == LogicalTypeId::ARRAY) {
        builder.Vector([&]() {
            const auto &children = value.type().id() == LogicalTypeId::LIST ? ListValue::GetChildren(value)
                                                                              : ArrayValue::GetChildren(value);
            for (const auto &child : children) AppendFlexValue(builder, child);
        });
    } else if (value.type().id() == LogicalTypeId::STRUCT || value.type().id() == LogicalTypeId::MAP) {
        builder.Map([&]() {
            if (value.type().id() == LogicalTypeId::STRUCT) {
                const auto &children = StructValue::GetChildren(value);
                for (idx_t i = 0; i < children.size(); i++) {
                    AppendFlexField(builder, StructType::GetChildName(value.type(), i), children[i]);
                }
            } else {
                for (const auto &entry : MapValue::GetChildren(value)) {
                    const auto &kv = StructValue::GetChildren(entry);
                    AppendFlexField(builder, CopyValueText(kv[0]), kv[1]);
                }
            }
        });
    } else if (value.type().id() == LogicalTypeId::BOOLEAN) {
        builder.Bool(value.GetValue<bool>());
    } else if (IsCopySignedType(value.type().id())) {
        builder.Int(std::stoll(value.ToString()));
    } else if (IsCopyUnsignedType(value.type().id())) {
        builder.UInt(std::stoull(value.ToString()));
    } else if (value.type().id() == LogicalTypeId::FLOAT || value.type().id() == LogicalTypeId::DOUBLE ||
               value.type().id() == LogicalTypeId::DECIMAL) {
        builder.Double(std::stod(value.ToString()));
    } else {
        builder.String(CopyValueText(value));
    }
}

static bool AppendFlexVectorField(flexbuffers::Builder &builder, const string &key,
                                  const UnifiedVectorFormat &format, idx_t row_idx, LogicalTypeId type) {
    auto value_idx = format.sel->get_index(row_idx);
    auto key_ptr = key.c_str();
    if (!format.validity.RowIsValid(value_idx)) {
        builder.Null(key_ptr);
        return true;
    }
    switch (type) {
    case LogicalTypeId::VARCHAR:
    case LogicalTypeId::BLOB: {
        auto value = UnifiedVectorFormat::GetData<string_t>(format)[value_idx];
        builder.String(key_ptr, string(value.GetData(), value.GetSize()));
        return true;
    }
    case LogicalTypeId::BOOLEAN:
        builder.Bool(key_ptr, UnifiedVectorFormat::GetData<bool>(format)[value_idx]);
        return true;
    case LogicalTypeId::TINYINT: builder.Int(key_ptr, UnifiedVectorFormat::GetData<int8_t>(format)[value_idx]); return true;
    case LogicalTypeId::SMALLINT: builder.Int(key_ptr, UnifiedVectorFormat::GetData<int16_t>(format)[value_idx]); return true;
    case LogicalTypeId::INTEGER: builder.Int(key_ptr, UnifiedVectorFormat::GetData<int32_t>(format)[value_idx]); return true;
    case LogicalTypeId::BIGINT: builder.Int(key_ptr, UnifiedVectorFormat::GetData<int64_t>(format)[value_idx]); return true;
    case LogicalTypeId::UTINYINT: builder.UInt(key_ptr, UnifiedVectorFormat::GetData<uint8_t>(format)[value_idx]); return true;
    case LogicalTypeId::USMALLINT: builder.UInt(key_ptr, UnifiedVectorFormat::GetData<uint16_t>(format)[value_idx]); return true;
    case LogicalTypeId::UINTEGER: builder.UInt(key_ptr, UnifiedVectorFormat::GetData<uint32_t>(format)[value_idx]); return true;
    case LogicalTypeId::UBIGINT: builder.UInt(key_ptr, UnifiedVectorFormat::GetData<uint64_t>(format)[value_idx]); return true;
    case LogicalTypeId::FLOAT: builder.Double(key_ptr, UnifiedVectorFormat::GetData<float>(format)[value_idx]); return true;
    case LogicalTypeId::DOUBLE: builder.Double(key_ptr, UnifiedVectorFormat::GetData<double>(format)[value_idx]); return true;
    default:
        return false;
    }
}

static void EncodeStructuredPayload(const NatsCopyToBindData &bind_data, const DataChunk &input, idx_t row_idx,
                                    const vector<UnifiedVectorFormat> &structured_formats, vector<uint8_t> &output) {
    const auto &types = bind_data.column_types;
    if (bind_data.payload_format == NatsCopyToBindData::PayloadFormat::PROTOBUF) {
        EncodeProtobufPayload(bind_data, input, row_idx, structured_formats, output);
        return;
    }
    if (bind_data.payload_format == NatsCopyToBindData::PayloadFormat::JSON) {
        string json_output = "{";
        for (idx_t i = 0; i < bind_data.structured_column_indices.size(); i++) {
            if (i) json_output += ",";
            json_output += bind_data.structured_json_keys[i];
            json_output += ":";
            auto column = bind_data.structured_column_indices[i];
            if (!AppendJsonVectorScalar(json_output, structured_formats[i], row_idx, types[column].id())) {
                AppendJsonValue(json_output, input.GetValue(column, row_idx));
            }
        }
        json_output += "}";
        output.assign(json_output.begin(), json_output.end());
        return;
    }
    if (bind_data.payload_format == NatsCopyToBindData::PayloadFormat::FLEXBUFFERS) {
        flexbuffers::Builder builder;
        auto start = builder.StartMap();
        for (idx_t i = 0; i < bind_data.structured_column_indices.size(); i++) {
            auto column = bind_data.structured_column_indices[i];
            auto value = input.GetValue(column, row_idx);
            if (!AppendFlexVectorField(builder, bind_data.structured_column_names[i], structured_formats[i], row_idx,
                                       types[column].id())) {
                AppendFlexField(builder, bind_data.structured_column_names[i], value);
            }
        }
        builder.EndMap(start);
        builder.Finish();
        const auto &buffer = builder.GetBuffer();
        output.assign(buffer.begin(), buffer.end());
        return;
    }

    output.clear();
    bool cbor = bind_data.payload_format == NatsCopyToBindData::PayloadFormat::CBOR;
    auto count = bind_data.structured_column_indices.size();
    AppendStructuredCount(output, count, cbor, true);
    for (idx_t i = 0; i < count; i++) {
        const auto &key = bind_data.structured_binary_keys[i];
        output.insert(output.end(), key.begin(), key.end());
        auto column = bind_data.structured_column_indices[i];
        if (!AppendStructuredVectorScalar(output, structured_formats[i], row_idx, types[column].id(), cbor)) {
            AppendStructuredValue(output, input.GetValue(column, row_idx), cbor);
        }
    }
}

static unique_ptr<FunctionData> NatsCopyToBind(ClientContext &context, CopyFunctionBindInput &input,
                                               const vector<string> &names, const vector<LogicalType> &sql_types) {
    auto result = make_uniq<NatsCopyToBindData>();
    result->column_types = sql_types;
    result->connection.url = ResolveNatsCopyUrl(input.info.options, "COPY TO", nullptr);
    for (const auto &option : input.info.options) {
        if (!option.second.empty()) {
            ParseNatsConnectionParameter(result->connection, option.first, option.second.front());
        }
    }
    ValidateNatsConnectionConfig(result->connection);
    result->stream_name = ResolveNatsCopyStreamName(input.info.file_path, "COPY TO");

    auto payload_format = GetCopyOptionString(input.info.options, "payload_format").value_or("raw");
    payload_format = StringUtil::Lower(payload_format);
    if (payload_format == "raw") {
        result->payload_format = NatsCopyToBindData::PayloadFormat::RAW;
    } else if (payload_format == "json") {
        result->payload_format = NatsCopyToBindData::PayloadFormat::JSON;
    } else if (payload_format == "msgpack" || payload_format == "messagepack") {
        result->payload_format = NatsCopyToBindData::PayloadFormat::MSGPACK;
    } else if (payload_format == "cbor") {
        result->payload_format = NatsCopyToBindData::PayloadFormat::CBOR;
    } else if (payload_format == "flexbuffers" || payload_format == "flex") {
        result->payload_format = NatsCopyToBindData::PayloadFormat::FLEXBUFFERS;
    } else if (payload_format == "protobuf" || payload_format == "proto") {
        result->payload_format = NatsCopyToBindData::PayloadFormat::PROTOBUF;
    } else {
        throw BinderException("COPY TO FORMAT nats_js does not support payload_format '%s'", payload_format);
    }

    auto subject = GetCopyOptionString(input.info.options, "subject");
    if (subject.has_value() && !subject->empty()) {
        result->constant_subject = true;
        result->subject_value = *subject;
    } else {
        auto subject_column = GetCopyOptionString(input.info.options, "subject_column");
        if (subject_column.has_value() && !subject_column->empty()) {
            result->subject_column = *subject_column;
        }
    }

    auto payload_column = GetCopyOptionString(input.info.options, "payload_column");
    if (payload_column.has_value() && !payload_column->empty()) {
        result->payload_column = *payload_column;
    }

    if (result->payload_format == NatsCopyToBindData::PayloadFormat::RAW) {
        result->payload_idx = FindColumnIndex(names, result->payload_column);
        if (result->payload_idx == DConstants::INVALID_INDEX) {
            throw BinderException("COPY TO FORMAT nats_js requires a source column named \"%s\"",
                                  result->payload_column);
        }

        auto payload_type = sql_types[result->payload_idx].id();
        if (payload_type != LogicalTypeId::VARCHAR && payload_type != LogicalTypeId::BLOB) {
            throw BinderException("COPY TO FORMAT nats_js requires payload column \"%s\" to be VARCHAR or BLOB",
                                  result->payload_column);
        }
    } else {
        auto configured_columns = GetCopyOptionStringList(input.info.options, "payload_columns");
        if (configured_columns.empty()) {
            auto subject_index = FindColumnIndex(names, result->subject_column);
            auto raw_payload_index = FindColumnIndex(names, result->payload_column);
            for (idx_t i = 0; i < names.size(); i++) {
                if (i == subject_index || i == raw_payload_index) {
                    continue;
                }
                configured_columns.push_back(names[i]);
            }
        }
        if (configured_columns.empty()) {
            throw BinderException("COPY TO structured payloads require at least one payload column");
        }
        for (const auto &column_name : configured_columns) {
            auto index = FindColumnIndex(names, column_name);
            if (index == DConstants::INVALID_INDEX) {
                throw BinderException("COPY TO FORMAT nats_js payload column \"%s\" was not found", column_name);
            }
            result->structured_column_indices.push_back(index);
            result->structured_column_names.push_back(column_name);
        }

        if (result->payload_format == NatsCopyToBindData::PayloadFormat::JSON) {
            for (const auto &column_name : result->structured_column_names) {
                string key;
                AppendJsonEscaped(key, column_name);
                result->structured_json_keys.push_back(std::move(key));
            }
        } else if (result->payload_format == NatsCopyToBindData::PayloadFormat::MSGPACK ||
                   result->payload_format == NatsCopyToBindData::PayloadFormat::CBOR) {
            bool cbor = result->payload_format == NatsCopyToBindData::PayloadFormat::CBOR;
            for (const auto &column_name : result->structured_column_names) {
                vector<uint8_t> key;
                cbor ? AppendCborString(key, column_name) : AppendMsgpackString(key, column_name);
                result->structured_binary_keys.push_back(std::move(key));
            }
        }

        if (result->payload_format == NatsCopyToBindData::PayloadFormat::PROTOBUF) {
            auto proto_file = GetCopyOptionString(input.info.options, "proto_file").value_or("");
            auto proto_message = GetCopyOptionString(input.info.options, "proto_message").value_or("");
            if (proto_file.empty() || proto_message.empty()) {
                throw BinderException("COPY TO protobuf requires proto_file and proto_message");
            }

            auto proto_fields = GetCopyOptionStringList(input.info.options, "proto_fields");
            if (proto_fields.empty()) {
                proto_fields = result->structured_column_names;
            }
            if (proto_fields.size() != result->structured_column_indices.size()) {
                throw BinderException("COPY TO protobuf requires one proto_fields entry per payload column");
            }

            result->proto_source_tree = make_shared_ptr<DiskSourceTree>();
            std::filesystem::path proto_path(proto_file);
            string proto_dir = proto_path.parent_path().string();
            if (proto_dir.empty()) {
                proto_dir = ".";
            }
            result->proto_source_tree->MapPath("", proto_dir);
            result->proto_error_collector = make_shared_ptr<ProtobufErrorCollector>();
            result->proto_importer = make_shared_ptr<Importer>(result->proto_source_tree.get(),
                                                               result->proto_error_collector.get());
            auto file_desc = result->proto_importer->Import(proto_path.filename().string());
            if (!file_desc) {
                string error = "Failed to import protobuf schema file: " + proto_file;
                if (result->proto_error_collector->HasErrors()) {
                    error += "\n" + result->proto_error_collector->GetErrors();
                }
                throw BinderException("%s", error);
            }
            result->proto_descriptor = file_desc->FindMessageTypeByName(proto_message);
            if (!result->proto_descriptor) {
                throw BinderException("Protobuf message type '%s' was not found in %s", proto_message, proto_file);
            }
            for (const auto &field_path : proto_fields) {
                auto resolved_path = ResolveProtobufFieldPath(result->proto_descriptor, field_path);
                if (resolved_path.back()->type() == FieldDescriptor::TYPE_MESSAGE && !resolved_path.back()->is_map()) {
                    throw BinderException("COPY TO protobuf field '%s' must be a scalar or repeated scalar", field_path);
                }
                result->proto_field_paths.push_back(std::move(resolved_path));
            }
        }
    }

    if (!result->constant_subject) {
        result->subject_idx = FindColumnIndex(names, result->subject_column);
        if (result->subject_idx == DConstants::INVALID_INDEX) {
            throw BinderException(
                "COPY TO FORMAT nats_js requires a source column named \"%s\" or a constant subject option",
                result->subject_column);
        }
        if (sql_types[result->subject_idx].id() != LogicalTypeId::VARCHAR) {
            throw BinderException("COPY TO FORMAT nats_js requires subject column \"%s\" to be VARCHAR",
                                  result->subject_column);
        }
    }

    return std::move(result);
}

static unique_ptr<LocalFunctionData> NatsCopyToInitializeLocal(ExecutionContext &, FunctionData &) {
    return make_uniq_base<LocalFunctionData, NatsCopyToLocalState>();
}

static unique_ptr<GlobalFunctionData> NatsCopyToInitializeGlobal(ClientContext &, FunctionData &bind_data,
                                                                 const string &) {
    auto &bdata = bind_data.Cast<NatsCopyToBindData>();
    auto result = make_uniq<NatsCopyToGlobalState>();
    ConnectNats(bdata.connection, &result->conn, 20);
    return std::move(result);
}

static void NatsCopyToSink(ExecutionContext &, FunctionData &bind_data, GlobalFunctionData &gstate,
                           LocalFunctionData &, DataChunk &input) {
    auto &bdata = bind_data.Cast<NatsCopyToBindData>();
    auto &state = gstate.Cast<NatsCopyToGlobalState>();
    if (input.size() == 0) {
        return;
    }

    lock_guard<mutex> guard(state.lock);

    UnifiedVectorFormat raw_payload_format;
    const string_t *payloads = nullptr;
    if (bdata.payload_format == NatsCopyToBindData::PayloadFormat::RAW) {
        input.data[bdata.payload_idx].ToUnifiedFormat(input.size(), raw_payload_format);
        payloads = UnifiedVectorFormat::GetData<string_t>(raw_payload_format);
    }

    UnifiedVectorFormat subject_format;
    const string_t *subjects = nullptr;
    if (!bdata.constant_subject) {
        input.data[bdata.subject_idx].ToUnifiedFormat(input.size(), subject_format);
        subjects = UnifiedVectorFormat::GetData<string_t>(subject_format);
    }

    vector<uint8_t> encoded_payload;
    vector<UnifiedVectorFormat> structured_formats;
    if (bdata.payload_format != NatsCopyToBindData::PayloadFormat::RAW) {
        encoded_payload.reserve(1024);
        structured_formats.resize(bdata.structured_column_indices.size());
        for (idx_t i = 0; i < bdata.structured_column_indices.size(); i++) {
            input.data[bdata.structured_column_indices[i]].ToUnifiedFormat(input.size(), structured_formats[i]);
        }
    }
    for (idx_t row_idx = 0; row_idx < input.size(); row_idx++) {
        idx_t subject_idx = bdata.constant_subject ? 0 : subject_format.sel->get_index(row_idx);
        if (!bdata.constant_subject && !subject_format.validity.RowIsValid(subject_idx)) {
            throw BinderException("COPY TO FORMAT nats_js does not support NULL subject values");
        }
        string subject = bdata.constant_subject ? bdata.subject_value
                                                : string(subjects[subject_idx].GetData(), subjects[subject_idx].GetSize());

        const void *payload_data = nullptr;
        int payload_len = 0;
        if (bdata.payload_format == NatsCopyToBindData::PayloadFormat::RAW) {
            idx_t out_idx = raw_payload_format.sel->get_index(row_idx);
            if (!raw_payload_format.validity.RowIsValid(out_idx)) {
                throw BinderException("COPY TO FORMAT nats_js does not support NULL payload values");
            }
            auto &payload = payloads[out_idx];
            payload_data = payload.GetData();
            payload_len = static_cast<int>(payload.GetSize());
        } else {
            EncodeStructuredPayload(bdata, input, row_idx, structured_formats, encoded_payload);
            payload_data = encoded_payload.data();
            payload_len = static_cast<int>(encoded_payload.size());
        }
        natsStatus s = natsConnection_Publish(state.conn, subject.c_str(), payload_data, payload_len);
        if (s != NATS_OK) {
            throw std::runtime_error(std::string("Failed to publish JetStream message: ") + natsStatus_GetText(s));
        }

        state.rows_written++;
    }
}

static void NatsCopyToCombine(ExecutionContext &, FunctionData &, GlobalFunctionData &, LocalFunctionData &) {
}

static void NatsCopyToFinalize(ClientContext &, FunctionData &, GlobalFunctionData &gstate) {
    auto &state = gstate.Cast<NatsCopyToGlobalState>();
    lock_guard<mutex> guard(state.lock);
    if (state.conn != nullptr) {
        natsStatus s = natsConnection_FlushTimeout(state.conn, 5000);
        if (s != NATS_OK) {
            DisconnectNats(&state.conn);
            throw std::runtime_error(std::string("Failed to flush NATS connection: ") + natsStatus_GetText(s));
        }
    }
    DisconnectNats(&state.conn);
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
    unique_ptr<NatsJetStreamBatchSource> message_source;
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
        message_source.reset();
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
    NatsConnectionConfig connection;
    uint64_t start_seq = 0;
    uint64_t end_seq = UINT64_MAX;
    int64_t start_time = 0;
    int64_t end_time = 0;
    vector<string> json_fields;
    vector<string> msgpack_fields;
    vector<string> cbor_fields;
    vector<string> flexbuffers_fields;
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
        } else if (ParseNatsConnectionParameter(connection, kv.first, kv.second)) {
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
        } else if (kv.first == "msgpack_extract") {
            auto list_children = ListValue::GetChildren(kv.second);
            for (auto &child : list_children) {
                msgpack_fields.push_back(StringValue::Get(child));
            }
        } else if (kv.first == "cbor_extract") {
            auto list_children = ListValue::GetChildren(kv.second);
            for (auto &child : list_children) {
                cbor_fields.push_back(StringValue::Get(child));
            }
        } else if (kv.first == "flexbuffers_extract") {
            auto list_children = ListValue::GetChildren(kv.second);
            for (auto &child : list_children) {
                flexbuffers_fields.push_back(StringValue::Get(child));
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
    if (static_cast<int>(!json_fields.empty()) + static_cast<int>(!msgpack_fields.empty()) +
            static_cast<int>(!cbor_fields.empty()) + static_cast<int>(!flexbuffers_fields.empty()) +
            static_cast<int>(!proto_fields.empty()) > 1) {
        throw std::runtime_error("Cannot combine JSON, MessagePack, CBOR, FlexBuffers, and protobuf extraction parameters");
    }

    if (!proto_fields.empty()) {
        if (proto_file.empty()) {
            throw std::runtime_error("proto_file parameter is required when using proto_extract");
        }
        if (proto_message.empty()) {
            throw std::runtime_error("proto_message parameter is required when using proto_extract");
        }
    }
    ValidateNatsConnectionConfig(connection);

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

    auto schema = BuildNatsSourceSchema(json_fields, msgpack_fields, cbor_fields, flexbuffers_fields, proto_file,
                                        proto_message, proto_fields, descriptor);
    names = schema.names;
    return_types = schema.return_types;

    auto bind_data = make_uniq<NatsScanBindData>(stream_name, subject_contains, nats_subject, std::move(connection), start_seq, end_seq,
        start_time, end_time, json_fields, msgpack_fields, cbor_fields, flexbuffers_fields, proto_file, proto_message, proto_fields,
                                                  std::move(proto_field_paths), batch_size, fetch_timeout_ms);

    if (!proto_fields.empty()) {
        bind_data->proto_source_tree = source_tree;
        bind_data->proto_error_collector = error_collector;
        bind_data->proto_importer = importer;
    bind_data->proto_descriptor = descriptor;
    }

    bind_data->cbor_field_paths = SplitMsgpackFieldPaths(bind_data->cbor_fields);
    bind_data->flexbuffers_field_paths = SplitMsgpackFieldPaths(bind_data->flexbuffers_fields);

    bind_data->msgpack_field_paths = SplitMsgpackFieldPaths(bind_data->msgpack_fields);

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
    ConnectJetStream(bind_data.connection, &state->conn, &state->js);

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

    state->message_source = make_uniq<NatsJetStreamBatchSource>(state->sub);

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

    bool payload_is_blob = !bind_data.proto_fields.empty() ||
                           (bind_data.json_fields.empty() && bind_data.msgpack_fields.empty() && bind_data.cbor_fields.empty() &&
                            bind_data.flexbuffers_fields.empty());
    auto &column_ids = global_state.column_ids;
    bool needs_stream = false;
    bool needs_subject = false;
    bool needs_seq = false;
    bool needs_ts = false;
    bool needs_payload = false;
    bool needs_json = false;
    bool needs_msgpack = false;
    bool needs_cbor = false;
    bool needs_flexbuffers = false;
    bool needs_proto = false;
    vector<ProjectedFieldColumn> json_columns;
    vector<ProjectedFieldColumn> msgpack_columns;
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
        } else if (col_id >= 5 + bind_data.json_fields.size() &&
                   col_id < 5 + bind_data.json_fields.size() + bind_data.msgpack_fields.size()) {
            needs_msgpack = true;
            msgpack_columns.push_back({out_idx, static_cast<idx_t>(col_id - 5 - bind_data.json_fields.size())});
        } else if (col_id >= 5 + bind_data.json_fields.size() + bind_data.msgpack_fields.size() &&
                   col_id < 5 + bind_data.json_fields.size() + bind_data.msgpack_fields.size() + bind_data.cbor_fields.size()) {
            needs_cbor = true;
        } else if (col_id >= 5 + bind_data.json_fields.size() + bind_data.msgpack_fields.size() + bind_data.cbor_fields.size() &&
                   col_id < 5 + bind_data.json_fields.size() + bind_data.msgpack_fields.size() + bind_data.cbor_fields.size() +
                                 bind_data.flexbuffers_fields.size()) {
            needs_flexbuffers = true;
        } else if (col_id >= 5 + bind_data.json_fields.size() + bind_data.msgpack_fields.size() + bind_data.cbor_fields.size() +
                                 bind_data.flexbuffers_fields.size()) {
            needs_proto = true;
            proto_columns.push_back({out_idx, static_cast<idx_t>(col_id - 5 - bind_data.json_fields.size() - bind_data.msgpack_fields.size() -
                                                                  bind_data.cbor_fields.size() - bind_data.flexbuffers_fields.size())});
        }
    }
    vector<idx_t> msgpack_output_columns;
    vector<vector<string>> msgpack_field_paths;
    for (const auto &msgpack_column : msgpack_columns) {
        msgpack_output_columns.push_back(msgpack_column.output_idx);
        msgpack_field_paths.push_back(bind_data.msgpack_field_paths[msgpack_column.field_idx]);
    }
    vector<idx_t> proto_output_columns;
    vector<vector<const FieldDescriptor *>> proto_field_paths;
    for (const auto &proto_column : proto_columns) {
        proto_output_columns.push_back(proto_column.output_idx);
        proto_field_paths.push_back(bind_data.proto_field_paths[proto_column.field_idx]);
    }
    vector<idx_t> cbor_output_columns;
    vector<vector<string>> cbor_field_paths;
    for (idx_t out_idx = 0; out_idx < column_ids.size(); out_idx++) {
        auto col_id = column_ids[out_idx];
        auto cbor_start = 5 + bind_data.json_fields.size() + bind_data.msgpack_fields.size();
        if (col_id >= cbor_start && col_id < cbor_start + bind_data.cbor_fields.size()) {
            cbor_output_columns.push_back(out_idx);
            cbor_field_paths.push_back(bind_data.cbor_field_paths[col_id - cbor_start]);
        }
    }
    vector<idx_t> flexbuffers_output_columns;
    vector<vector<string>> flexbuffers_field_paths;
    for (idx_t out_idx = 0; out_idx < column_ids.size(); out_idx++) {
        auto col_id = column_ids[out_idx];
        auto flex_start = 5 + bind_data.json_fields.size() + bind_data.msgpack_fields.size() + bind_data.cbor_fields.size();
        if (col_id >= flex_start && col_id < flex_start + bind_data.flexbuffers_fields.size()) {
            flexbuffers_output_columns.push_back(out_idx);
            flexbuffers_field_paths.push_back(bind_data.flexbuffers_field_paths[col_id - flex_start]);
        }
    }
    bool needs_subject_for_filter = !bind_data.subject_contains.empty();
    bool needs_message_subject = needs_subject || needs_subject_for_filter;
    bool needs_message_payload = needs_payload || needs_json || needs_msgpack || needs_cbor || needs_flexbuffers || needs_proto;

    unique_ptr<Message> proto_msg;
    if (needs_proto && !bind_data.proto_fields.empty() && global_state.proto_prototype != nullptr) {
        proto_msg.reset(global_state.proto_prototype->New());
    }

    while (count < max_rows && global_state.current_seq <= global_state.end_seq) {
        if (context.interrupted) {
            break;
        }

        uint64_t remaining = global_state.end_seq >= global_state.current_seq ?
                             global_state.end_seq - global_state.current_seq + 1 : 0;
        natsMsg *msg = nullptr;
        if (remaining == 0 ||
            !global_state.message_source->Next(&msg, std::min<uint64_t>(bind_data.batch_size, remaining),
                                               bind_data.fetch_timeout_ms, bind_data.stream_name)) {
            global_state.done = true;
            break;
        }
        if (msg == nullptr) {
            continue;
        }

        NatsMessageEnvelope envelope(msg);
        uint64_t stream_seq = envelope.Sequence();
        int64_t timestamp_ns = needs_ts ? envelope.TimestampNs() : 0;
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
            subject = envelope.Subject();
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
            msg_data = envelope.Data();
            data_len = envelope.DataLength();
        }
        NatsPayloadView payload {msg_data, static_cast<idx_t>(data_len)};

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

        if (needs_msgpack && !bind_data.msgpack_fields.empty()) {
            DecodeMsgpackFieldPathsToChunk(output, count, msgpack_output_columns, payload, msgpack_field_paths);
        }

        if (needs_cbor && !bind_data.cbor_fields.empty()) {
            DecodeCborFieldPathsToChunk(output, count, cbor_output_columns, payload, cbor_field_paths);
        }

        if (needs_flexbuffers && !bind_data.flexbuffers_fields.empty()) {
            DecodeFlexbuffersFieldPathsToChunk(output, count, flexbuffers_output_columns, payload,
                                               flexbuffers_field_paths);
        }

        if (needs_proto && proto_msg) {
            proto_msg->Clear();
            bool parse_success = DecodeProtobufPayload(*proto_msg, payload);

            if (parse_success) {
                DecodeProtobufFieldsToChunk(output, count, proto_output_columns, proto_msg.get(), proto_field_paths);
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
    NatsConnectionConfig connection;

    NatsStreamStatsBindData(string stream, NatsConnectionConfig connection_p)
        : stream_name(std::move(stream)), connection(std::move(connection_p)) {
    }
};

struct NatsStreamRangeStatsBindData : public TableFunctionData {
    string stream_name;
    NatsConnectionConfig connection;
    uint64_t start_seq;
    uint64_t end_seq;

    NatsStreamRangeStatsBindData(string stream, NatsConnectionConfig connection_p, uint64_t start, uint64_t end)
        : stream_name(std::move(stream)), connection(std::move(connection_p)), start_seq(start), end_seq(end) {
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
    NatsConnectionConfig connection;

    for (auto &kv : input.named_parameters) {
        ParseNatsConnectionParameter(connection, kv.first, kv.second);
    }
    ValidateNatsConnectionConfig(connection);

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

    return make_uniq<NatsStreamStatsBindData>(stream_name, std::move(connection));
}

static unique_ptr<FunctionData> NatsStreamRangeStatsBind(ClientContext &context, TableFunctionBindInput &input,
                                                          vector<LogicalType> &return_types, vector<string> &names) {
    if (input.inputs.empty()) {
        throw std::runtime_error("nats_stream_range_stats requires one argument: stream_name");
    }

    auto stream_name = input.inputs[0].GetValue<string>();
    NatsConnectionConfig connection;
    uint64_t start_seq = 0;
    uint64_t end_seq = 0;
    bool has_start_seq = false;
    bool has_end_seq = false;

    for (auto &kv : input.named_parameters) {
        if (kv.first == "start_seq") {
            start_seq = UBigIntValue::Get(kv.second);
            has_start_seq = true;
        } else if (kv.first == "end_seq") {
            end_seq = UBigIntValue::Get(kv.second);
            has_end_seq = true;
        } else {
            ParseNatsConnectionParameter(connection, kv.first, kv.second);
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

    ValidateNatsConnectionConfig(connection);
    return make_uniq<NatsStreamRangeStatsBindData>(stream_name, std::move(connection), start_seq, end_seq);
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
        ConnectJetStream(bind_data.connection, &conn, &js);

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
        ConnectJetStream(bind_data.connection, &conn, &js);

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
    RegisterNatsConnectionParameters(nats_scan);
    nats_scan.named_parameters["start_seq"] = LogicalType(LogicalTypeId::UBIGINT);
    nats_scan.named_parameters["end_seq"] = LogicalType(LogicalTypeId::UBIGINT);
    nats_scan.named_parameters["start_time"] = LogicalType(LogicalTypeId::TIMESTAMP);
    nats_scan.named_parameters["end_time"] = LogicalType(LogicalTypeId::TIMESTAMP);
    nats_scan.named_parameters["json_extract"] = LogicalType::LIST(LogicalType(LogicalTypeId::VARCHAR));
    nats_scan.named_parameters["msgpack_extract"] = LogicalType::LIST(LogicalType(LogicalTypeId::VARCHAR));
    nats_scan.named_parameters["cbor_extract"] = LogicalType::LIST(LogicalType(LogicalTypeId::VARCHAR));
    nats_scan.named_parameters["flexbuffers_extract"] = LogicalType::LIST(LogicalType(LogicalTypeId::VARCHAR));
    nats_scan.named_parameters["proto_file"] = LogicalType(LogicalTypeId::VARCHAR);
    nats_scan.named_parameters["proto_message"] = LogicalType(LogicalTypeId::VARCHAR);
    nats_scan.named_parameters["proto_extract"] = LogicalType::LIST(LogicalType(LogicalTypeId::VARCHAR));
    nats_scan.named_parameters["batch_size"] = LogicalType(LogicalTypeId::UBIGINT);
    nats_scan.named_parameters["fetch_timeout_ms"] = LogicalType(LogicalTypeId::BIGINT);

    return nats_scan;
}

static unique_ptr<FunctionData> NatsCopyFromBind(ClientContext &context, CopyFromFunctionBindInput &input,
                                                 vector<string> &expected_names, vector<LogicalType> &expected_types) {
    auto stream_name = ResolveNatsCopyStreamName(input.info.file_path, "COPY FROM");
    string subject_legacy;
    auto subject_contains = GetCopyOptionString(input.info.options, "subject_contains").value_or("");
    auto nats_subject = GetCopyOptionString(input.info.options, "nats_subject").value_or("");
    static const string DEFAULT_NATS_URL = "nats://localhost:4222";
    NatsConnectionConfig connection;
    connection.url = ResolveNatsCopyUrl(input.info.options, "COPY FROM", &DEFAULT_NATS_URL);
    for (const auto &option : input.info.options) {
        if (!option.second.empty()) {
            ParseNatsConnectionParameter(connection, option.first, option.second.front());
        }
    }
    ValidateNatsConnectionConfig(connection);
    uint64_t start_seq = GetCopyOptionUBigInt(input.info.options, "start_seq", 0);
    uint64_t end_seq = GetCopyOptionUBigInt(input.info.options, "end_seq", UINT64_MAX);
    int64_t start_time = 0;
    int64_t end_time = 0;
    vector<string> json_fields = GetCopyOptionStringList(input.info.options, "json_extract");
    vector<string> msgpack_fields = GetCopyOptionStringList(input.info.options, "msgpack_extract");
    vector<string> cbor_fields = GetCopyOptionStringList(input.info.options, "cbor_extract");
    vector<string> flexbuffers_fields = GetCopyOptionStringList(input.info.options, "flexbuffers_extract");
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
    if (static_cast<int>(!json_fields.empty()) + static_cast<int>(!msgpack_fields.empty()) +
            static_cast<int>(!cbor_fields.empty()) + static_cast<int>(!flexbuffers_fields.empty()) +
            static_cast<int>(!proto_fields.empty()) > 1) {
        throw std::runtime_error("Cannot combine JSON, MessagePack, CBOR, and protobuf extraction parameters");
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

    auto schema = BuildNatsSourceSchema(json_fields, msgpack_fields, cbor_fields, flexbuffers_fields, proto_file,
                                        proto_message, proto_fields, descriptor);
    ValidateNatsCopyFromSchema(expected_names, expected_types, schema);

    auto bind_data = make_uniq<NatsScanBindData>(stream_name, subject_contains, nats_subject, std::move(connection), start_seq, end_seq,
        start_time, end_time, json_fields, msgpack_fields, cbor_fields, flexbuffers_fields, proto_file, proto_message,
                                                 proto_fields, std::move(proto_field_paths), batch_size, fetch_timeout_ms);

    if (!proto_fields.empty()) {
        bind_data->proto_source_tree = source_tree;
        bind_data->proto_error_collector = error_collector;
        bind_data->proto_importer = importer;
        bind_data->proto_descriptor = descriptor;
    }
    bind_data->cbor_field_paths = SplitMsgpackFieldPaths(bind_data->cbor_fields);

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
        RegisterNatsConnectionParameters(nats_stream_stats);
        loader.RegisterFunction(nats_stream_stats);
    }

    TableFunction nats_stream_range_stats("nats_stream_range_stats", {LogicalType(LogicalTypeId::VARCHAR)},
                                          NatsStreamRangeStatsExecute, NatsStreamRangeStatsBind,
                                          NatsStreamRangeStatsInitGlobal);
    RegisterNatsConnectionParameters(nats_stream_range_stats);
    nats_stream_range_stats.named_parameters["start_seq"] = LogicalType(LogicalTypeId::UBIGINT);
    nats_stream_range_stats.named_parameters["end_seq"] = LogicalType(LogicalTypeId::UBIGINT);
    loader.RegisterFunction(nats_stream_range_stats);
}

void NatsCopyFunction::Register(ExtensionLoader &loader) {
    CopyFunction function("nats_js");
    function.copy_options = NatsCopyListOptions;
    function.copy_to_bind = NatsCopyToBind;
    function.copy_to_initialize_local = NatsCopyToInitializeLocal;
    function.copy_to_initialize_global = NatsCopyToInitializeGlobal;
    function.copy_to_sink = NatsCopyToSink;
    function.copy_to_combine = NatsCopyToCombine;
    function.copy_to_finalize = NatsCopyToFinalize;
    function.execution_mode = [](bool, bool) {
        return CopyFunctionExecutionMode::REGULAR_COPY_TO_FILE;
    };
    function.copy_from_bind = NatsCopyFromBind;
    function.copy_from_function = CreateNatsScanTableFunction();
    function.extension = "nats_js";
    loader.RegisterFunction(function);
}

} // namespace duckdb
