#include "nats_message_decode.hpp"

#include "yyjson.hpp"
#include <flatbuffers/flexbuffers.h>

#include <cstring>
#include <cstdio>
#include <iomanip>
#include <sstream>

using namespace duckdb_yyjson;

namespace duckdb {

namespace {

class MsgpackReader {
public:
    MsgpackReader(const char *data, idx_t size) : data(reinterpret_cast<const uint8_t *>(data)), remaining(size) {
    }

    bool Decode(DataChunk &chunk, idx_t row_idx, const vector<idx_t> &output_columns,
                const vector<vector<string>> &field_paths) {
        uint8_t tag;
        return ReadByte(tag) && DecodeMap(tag, 0, chunk, row_idx, output_columns, field_paths);
    }

private:
    bool ReadStringView(uint8_t tag, std::string_view &value) {
        idx_t length;
        if ((tag & 0xe0) == 0xa0) {
            length = tag & 0x1f;
        } else if (tag == 0xd9 || tag == 0xda || tag == 0xdb) {
            uint64_t size;
            if (!ReadUnsigned(tag == 0xd9 ? 1 : (tag == 0xda ? 2 : 4), size)) return false;
            length = static_cast<idx_t>(size);
        } else {
            return false;
        }
        if (length > remaining) return false;
        value = std::string_view(reinterpret_cast<const char *>(data), length);
        data += length;
        remaining -= length;
        return true;
    }

    bool ReadScalarToVector(uint8_t tag, Vector &vector, idx_t row_idx) {
        auto set_string = [&](const char *data_ptr, idx_t length) {
            FlatVector::SetNull(vector, row_idx, false);
            auto vector_data = GetNatsMutableVectorData<string_t>(vector);
            vector_data[row_idx] = StringVector::AddString(vector, data_ptr, length);
        };
        if ((tag & 0xe0) == 0xa0 || tag == 0xd9 || tag == 0xda || tag == 0xdb) {
            std::string_view value;
            if (!ReadStringView(tag, value)) return false;
            set_string(value.data(), value.size());
            return true;
        }
        char buffer[64];
        int length = 0;
        uint64_t number;
        if (tag <= 0x7f) {
            length = snprintf(buffer, sizeof(buffer), "%u", tag);
        } else if (tag >= 0xe0) {
            length = snprintf(buffer, sizeof(buffer), "%d", static_cast<int8_t>(tag));
        } else {
            switch (tag) {
            case 0xc0:
                FlatVector::SetNull(vector, row_idx, true);
                return true;
            case 0xc2:
                set_string("false", 5);
                return true;
            case 0xc3:
                set_string("true", 4);
                return true;
            case 0xcc: case 0xcd: case 0xce: case 0xcf:
                if (!ReadUnsigned(tag == 0xcc ? 1 : (tag == 0xcd ? 2 : (tag == 0xce ? 4 : 8)), number)) return false;
                length = snprintf(buffer, sizeof(buffer), "%llu", static_cast<unsigned long long>(number));
                break;
            case 0xd0: case 0xd1: case 0xd2: case 0xd3: {
                if (!ReadUnsigned(tag == 0xd0 ? 1 : (tag == 0xd1 ? 2 : (tag == 0xd2 ? 4 : 8)), number)) return false;
                int64_t signed_value = tag == 0xd0 ? static_cast<int8_t>(number) :
                                       tag == 0xd1 ? static_cast<int16_t>(number) :
                                       tag == 0xd2 ? static_cast<int32_t>(number) : static_cast<int64_t>(number);
                length = snprintf(buffer, sizeof(buffer), "%lld", static_cast<long long>(signed_value));
                break;
            }
            case 0xca: {
                if (!ReadUnsigned(4, number)) return false;
                uint32_t bits = static_cast<uint32_t>(number);
                float value;
                memcpy(&value, &bits, sizeof(value));
                length = snprintf(buffer, sizeof(buffer), "%f", static_cast<double>(value));
                break;
            }
            case 0xcb: {
                if (!ReadUnsigned(8, number)) return false;
                double value;
                memcpy(&value, &number, sizeof(value));
                length = snprintf(buffer, sizeof(buffer), "%f", value);
                break;
            }
            default:
                return false;
            }
        }
        if (length < 0 || static_cast<size_t>(length) >= sizeof(buffer)) return false;
        set_string(buffer, static_cast<idx_t>(length));
        return true;
    }

    bool DecodeMap(uint8_t tag, idx_t depth, DataChunk &chunk, idx_t row_idx,
                   const vector<idx_t> &output_columns, const vector<vector<string>> &field_paths) {
        uint32_t count;
        if (!((tag & 0xf0) == 0x80 || tag == 0xde || tag == 0xdf) || !ReadMapCount(tag, count)) return false;
        for (uint32_t i = 0; i < count; i++) {
            uint8_t key_tag;
            std::string_view key;
            if (!ReadByte(key_tag) || !ReadStringView(key_tag, key)) return false;
            idx_t matched = DConstants::INVALID_INDEX;
            for (idx_t field_idx = 0; field_idx < field_paths.size(); field_idx++) {
                if (depth < field_paths[field_idx].size() &&
                    key == std::string_view(field_paths[field_idx][depth])) {
                    matched = field_idx;
                    break;
                }
            }
            uint8_t value_tag;
            if (!ReadByte(value_tag)) return false;
            if (matched == DConstants::INVALID_INDEX) {
                if (!SkipValueAfterTag(value_tag)) return false;
            } else if (depth + 1 == field_paths[matched].size()) {
                auto &vector = chunk.data[output_columns[matched]];
                if (!ReadScalarToVector(value_tag, vector, row_idx) && !SkipValueAfterTag(value_tag)) return false;
            } else if ((value_tag & 0xf0) == 0x80 || value_tag == 0xde || value_tag == 0xdf) {
                if (!DecodeMap(value_tag, depth + 1, chunk, row_idx, output_columns, field_paths)) return false;
            } else if (!SkipValueAfterTag(value_tag)) {
                return false;
            }
        }
        return true;
    }

    bool ReadByte(uint8_t &value) {
        if (remaining == 0) {
            return false;
        }
        value = *data++;
        remaining--;
        return true;
    }

    bool ReadUnsigned(idx_t width, uint64_t &value) {
        if (width > remaining || width > sizeof(uint64_t)) {
            return false;
        }
        value = 0;
        for (idx_t i = 0; i < width; i++) {
            value = (value << 8) | *data++;
        }
        remaining -= width;
        return true;
    }

    bool ReadMapCount(uint8_t tag, uint32_t &count) {
        if ((tag & 0xf0) == 0x80) {
            count = tag & 0x0f;
            return true;
        }
        uint64_t value;
        if (!ReadUnsigned(tag == 0xde ? 2 : 4, value)) {
            return false;
        }
        count = static_cast<uint32_t>(value);
        return true;
    }

    bool SkipValue() {
        uint8_t tag;
        if (!ReadByte(tag)) return false;
        return SkipValueAfterTag(tag);
    }

    bool SkipValueAfterTag(uint8_t tag) {
        if (tag <= 0x7f || tag >= 0xe0) return true;
        if ((tag & 0xe0) == 0xa0) {
            return remaining >= (tag & 0x1f) ? (data += (tag & 0x1f), remaining -= (tag & 0x1f), true) : false;
        }
        if ((tag & 0xf0) == 0x90) {
            for (uint32_t i = 0; i < (tag & 0x0f); i++) if (!SkipValue()) return false;
            return true;
        }
        uint32_t count;
        uint64_t number;
        if ((tag & 0xf0) == 0x80 || tag == 0xde || tag == 0xdf) {
            if (!ReadMapCount(tag, count)) return false;
            for (uint32_t i = 0; i < count; i++) if (!SkipValue() || !SkipValue()) return false;
            return true;
        }
        idx_t bytes = 0;
        switch (tag) {
        case 0xc0: case 0xc2: case 0xc3: return true;
        case 0xcc: case 0xd0: bytes = 1; break;
        case 0xcd: case 0xd1: bytes = 2; break;
        case 0xce: case 0xd2: case 0xca: bytes = 4; break;
        case 0xcf: case 0xd3: case 0xcb: bytes = 8; break;
        case 0xd9: case 0xc4: if (!ReadUnsigned(1, number)) return false; bytes = number; break;
        case 0xda: case 0xc5: if (!ReadUnsigned(2, number)) return false; bytes = number; break;
        case 0xdb: case 0xc6: if (!ReadUnsigned(4, number)) return false; bytes = number; break;
        case 0xdc: if (!ReadUnsigned(2, number)) return false; for (uint32_t i = 0; i < number; i++) if (!SkipValue()) return false; return true;
        case 0xdd: if (!ReadUnsigned(4, number)) return false; for (uint32_t i = 0; i < number; i++) if (!SkipValue()) return false; return true;
        default: return false;
        }
        return bytes <= remaining ? (data += bytes, remaining -= bytes, true) : false;
    }

    const uint8_t *data;
    idx_t remaining;
};

} // namespace

void DecodeJsonFields(const NatsPayloadView &payload, const vector<string> &field_names, vector<Value> &values) {
    // Reuse the caller-owned buffer across messages; JSON strings and nested values still allocate as needed.
    values.resize(field_names.size());

    yyjson_doc *doc = yyjson_read(payload.data, payload.size, 0);
    if (!doc) {
        for (auto &value : values) {
            value = Value();
        }
        return;
    }

    yyjson_val *root = yyjson_doc_get_root(doc);
    for (idx_t i = 0; i < field_names.size(); i++) {
        const auto &field_name = field_names[i];
        yyjson_val *field = yyjson_obj_get(root, field_name.c_str());
        if (!field || yyjson_is_null(field)) {
            values[i] = Value();
        } else if (yyjson_is_str(field)) {
            values[i] = Value(yyjson_get_str(field));
        } else if (yyjson_is_num(field)) {
            values[i] = Value(yyjson_get_num(field));
        } else if (yyjson_is_bool(field)) {
            values[i] = Value::BOOLEAN(yyjson_get_bool(field));
        } else {
            char *json_string = yyjson_val_write(field, 0, nullptr);
            if (json_string) {
                values[i] = Value(json_string);
                free(json_string);
            } else {
                values[i] = Value();
            }
        }
    }

    yyjson_doc_free(doc);
}

void DecodeJsonFieldsToChunk(DataChunk &chunk, idx_t row_idx, idx_t first_column, const NatsPayloadView &payload,
                             const vector<string> &field_names) {
    yyjson_doc *doc = yyjson_read(payload.data, payload.size, 0);
    if (!doc) {
        for (idx_t i = 0; i < field_names.size(); i++) {
            FlatVector::SetNull(chunk.data[first_column + i], row_idx, true);
        }
        return;
    }

    yyjson_val *root = yyjson_doc_get_root(doc);
    for (idx_t i = 0; i < field_names.size(); i++) {
        auto &vector = chunk.data[first_column + i];
        auto vector_data = GetNatsMutableVectorData<string_t>(vector);
        yyjson_val *field = yyjson_obj_get(root, field_names[i].c_str());

        if (!field || yyjson_is_null(field)) {
            FlatVector::SetNull(vector, row_idx, true);
        } else if (yyjson_is_str(field)) {
            vector_data[row_idx] = StringVector::AddString(vector, yyjson_get_str(field));
        } else if (yyjson_is_num(field)) {
            auto value = std::to_string(yyjson_get_num(field));
            vector_data[row_idx] = StringVector::AddString(vector, value);
        } else if (yyjson_is_bool(field)) {
            vector_data[row_idx] = StringVector::AddString(vector, yyjson_get_bool(field) ? "true" : "false");
        } else {
            char *json_string = yyjson_val_write(field, 0, nullptr);
            if (json_string) {
                vector_data[row_idx] = StringVector::AddString(vector, json_string);
                free(json_string);
            } else {
                FlatVector::SetNull(vector, row_idx, true);
            }
        }
    }

    yyjson_doc_free(doc);
}

vector<vector<string>> SplitMsgpackFieldPaths(const vector<string> &field_names) {
    vector<vector<string>> result;
    result.reserve(field_names.size());
    for (const auto &field_name : field_names) {
        vector<string> path;
        size_t start = 0;
        while (true) {
            auto end = field_name.find('.', start);
            path.push_back(field_name.substr(start, end == string::npos ? string::npos : end - start));
            if (end == string::npos) break;
            start = end + 1;
        }
        result.push_back(std::move(path));
    }
    return result;
}

void DecodeMsgpackFieldPathsToChunk(DataChunk &chunk, idx_t row_idx, const vector<idx_t> &output_columns,
                                    const NatsPayloadView &payload, const vector<vector<string>> &field_paths) {
    for (auto output_column : output_columns) {
        FlatVector::SetNull(chunk.data[output_column], row_idx, true);
    }
    MsgpackReader reader(payload.data, payload.size);
    reader.Decode(chunk, row_idx, output_columns, field_paths);
}

namespace {

class CborReader {
public:
    CborReader(const char *data_p, idx_t size_p)
        : data(reinterpret_cast<const uint8_t *>(data_p)), remaining(size_p) {
    }

    bool Decode(DataChunk &chunk, idx_t row_idx, const vector<idx_t> &output_columns,
                const vector<vector<string>> &field_paths) {
        uint8_t tag;
        return ReadByte(tag) && DecodeMap(tag, 0, chunk, row_idx, output_columns, field_paths);
    }

private:
    bool ReadByte(uint8_t &value) {
        if (!remaining) return false;
        value = *data++;
        remaining--;
        return true;
    }

    bool ReadUnsigned(idx_t width, uint64_t &value) {
        if (width > remaining || width > sizeof(uint64_t)) return false;
        value = 0;
        for (idx_t i = 0; i < width; i++) value = (value << 8) | *data++;
        remaining -= width;
        return true;
    }

    bool ReadArgument(uint8_t additional, uint64_t &value) {
        if (additional < 24) {
            value = additional;
            return true;
        }
        if (additional == 24) return ReadUnsigned(1, value);
        if (additional == 25) return ReadUnsigned(2, value);
        if (additional == 26) return ReadUnsigned(4, value);
        if (additional == 27) return ReadUnsigned(8, value);
        return false;
    }

    bool ReadText(uint8_t additional, std::string_view &value) {
        uint64_t length;
        if (!ReadArgument(additional, length) || length > remaining) return false;
        value = std::string_view(reinterpret_cast<const char *>(data), static_cast<size_t>(length));
        data += length;
        remaining -= length;
        return true;
    }

    bool SetScalar(uint8_t major, uint8_t additional, Vector &vector, idx_t row_idx) {
        auto set_string = [&](const char *value, idx_t length) {
            FlatVector::SetNull(vector, row_idx, false);
            GetNatsMutableVectorData<string_t>(vector)[row_idx] = StringVector::AddString(vector, value, length);
        };
        if (major == 0 || major == 1) {
            uint64_t value;
            if (!ReadArgument(additional, value)) return false;
            char buffer[64];
            int length = major == 0 ? snprintf(buffer, sizeof(buffer), "%llu", (unsigned long long)value)
                                    : snprintf(buffer, sizeof(buffer), "%lld", (long long)(-1 - value));
            set_string(buffer, static_cast<idx_t>(length));
            return true;
        }
        if (major == 3) {
            std::string_view value;
            if (!ReadText(additional, value)) return false;
            set_string(value.data(), value.size());
            return true;
        }
        if (major == 7 && (additional == 20 || additional == 21)) {
            set_string(additional == 21 ? "true" : "false", additional == 21 ? 4 : 5);
            return true;
        }
        if (major == 7 && additional == 22) {
            FlatVector::SetNull(vector, row_idx, true);
            return true;
        }
        if (major == 7 && (additional == 25 || additional == 26 || additional == 27)) {
            uint64_t bits;
            idx_t width = additional == 25 ? 2 : (additional == 26 ? 4 : 8);
            if (!ReadUnsigned(width, bits)) return false;
            char buffer[64];
            double value = 0;
            if (additional == 26) {
                float f;
                uint32_t raw = static_cast<uint32_t>(bits);
                memcpy(&f, &raw, sizeof(f));
                value = f;
            } else if (additional == 27) {
                memcpy(&value, &bits, sizeof(value));
            }
            auto length = snprintf(buffer, sizeof(buffer), "%f", value);
            set_string(buffer, static_cast<idx_t>(length));
            return true;
        }
        return false;
    }

    bool ReadMapCount(uint8_t additional, uint64_t &count) {
        return ReadArgument(additional, count);
    }

    bool DecodeMap(uint8_t tag, idx_t depth, DataChunk &chunk, idx_t row_idx,
                   const vector<idx_t> &output_columns, const vector<vector<string>> &field_paths) {
        if ((tag >> 5) != 5) return false;
        uint64_t count;
        if (!ReadMapCount(tag & 0x1f, count)) return false;
        for (uint64_t i = 0; i < count; i++) {
            uint8_t key_tag, value_tag;
            std::string_view key;
            if (!ReadByte(key_tag) || (key_tag >> 5) != 3 || !ReadText(key_tag & 0x1f, key) || !ReadByte(value_tag)) {
                return false;
            }
            idx_t matched = DConstants::INVALID_INDEX;
            for (idx_t field = 0; field < field_paths.size(); field++) {
                if (depth < field_paths[field].size() && key == field_paths[field][depth]) {
                    matched = field;
                    break;
                }
            }
            uint8_t major = value_tag >> 5;
            if (matched != DConstants::INVALID_INDEX && depth + 1 == field_paths[matched].size()) {
                auto &vector = chunk.data[output_columns[matched]];
                if (!SetScalar(major, value_tag & 0x1f, vector, row_idx) && !Skip(value_tag)) return false;
            } else if (matched != DConstants::INVALID_INDEX && major == 5) {
                if (!DecodeMap(value_tag, depth + 1, chunk, row_idx, output_columns, field_paths)) return false;
            } else if (!Skip(value_tag)) {
                return false;
            }
        }
        return true;
    }

    bool Skip(uint8_t tag) {
        uint8_t major = tag >> 5;
        uint8_t additional = tag & 0x1f;
        uint64_t count;
        if (major <= 1) return ReadArgument(additional, count);
        if (major == 2 || major == 3) {
            return ReadArgument(additional, count) && count <= remaining && (data += count, remaining -= count, true);
        }
        if (major == 4 || major == 5) {
            if (!ReadArgument(additional, count)) return false;
            uint64_t elements = major == 5 ? count * 2 : count;
            for (uint64_t i = 0; i < elements; i++) {
                uint8_t child;
                if (!ReadByte(child) || !Skip(child)) return false;
            }
            return true;
        }
        if (major == 6) {
            return ReadArgument(additional, count) && ReadByte(additional) && Skip(additional);
        }
        if (additional < 24) return true;
        return additional == 24 ? ReadUnsigned(1, count) : additional == 25 ? ReadUnsigned(2, count) :
               additional == 26 ? ReadUnsigned(4, count) : additional == 27 ? ReadUnsigned(8, count) : false;
    }

    const uint8_t *data;
    idx_t remaining;
};

} // namespace

void DecodeCborFieldPathsToChunk(DataChunk &chunk, idx_t row_idx, const vector<idx_t> &output_columns,
                                 const NatsPayloadView &payload, const vector<vector<string>> &field_paths) {
    for (auto output_column : output_columns) FlatVector::SetNull(chunk.data[output_column], row_idx, true);
    CborReader reader(payload.data, payload.size);
    reader.Decode(chunk, row_idx, output_columns, field_paths);
}

void DecodeFlexbuffersFieldPathsToChunk(DataChunk &chunk, idx_t row_idx, const vector<idx_t> &output_columns,
                                        const NatsPayloadView &payload, const vector<vector<string>> &field_paths) {
    for (auto output_column : output_columns) {
        FlatVector::SetNull(chunk.data[output_column], row_idx, true);
    }
    if (!payload.data || payload.size < 3 || !flexbuffers::VerifyBuffer(reinterpret_cast<const uint8_t *>(payload.data),
                                                                         payload.size)) {
        return;
    }

    auto root = flexbuffers::GetRoot(reinterpret_cast<const uint8_t *>(payload.data), payload.size);
    for (idx_t field_idx = 0; field_idx < field_paths.size(); field_idx++) {
        auto value = root;
        for (const auto &part : field_paths[field_idx]) {
            if (!value.IsMap()) {
                value = flexbuffers::Reference();
                break;
            }
            value = value.AsMap()[part.c_str()];
        }
        auto &vector = chunk.data[output_columns[field_idx]];
        if (value.IsNull()) {
            continue;
        }
        string text;
        if (value.IsString()) {
            auto string_value = value.AsString();
            FlatVector::SetNull(vector, row_idx, false);
                GetNatsMutableVectorData<string_t>(vector)[row_idx] =
                StringVector::AddString(vector, string_value.c_str(), string_value.length());
            continue;
        } else if (value.IsBool()) {
            text = value.AsBool() ? "true" : "false";
        } else if (value.IsInt()) {
            text = std::to_string(value.AsInt64());
        } else if (value.IsUInt()) {
            text = std::to_string(value.AsUInt64());
        } else if (value.IsFloat()) {
            text = std::to_string(value.AsDouble());
        } else {
            continue;
        }
        FlatVector::SetNull(vector, row_idx, false);
        GetNatsMutableVectorData<string_t>(vector)[row_idx] = StringVector::AddString(vector, text);
    }
}

bool DecodeProtobufPayload(google::protobuf::Message &message, const NatsPayloadView &payload) {
    return message.ParseFromArray(payload.data, static_cast<int>(payload.size));
}

static vector<string> SplitFieldPath(const string &field_path) {
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

vector<const google::protobuf::FieldDescriptor *> ResolveProtobufFieldPath(
    const google::protobuf::Descriptor *message_desc, const string &field_path) {
    auto path_parts = SplitFieldPath(field_path);
    const google::protobuf::Descriptor *current_desc = message_desc;
    vector<const google::protobuf::FieldDescriptor *> resolved_path;
    resolved_path.reserve(path_parts.size());

    for (size_t i = 0; i < path_parts.size(); i++) {
        auto *field = current_desc->FindFieldByName(path_parts[i]);
        if (!field) {
            throw std::runtime_error("Field '" + path_parts[i] + "' not found in message type '" +
                                     string(current_desc->name()) + "' (field path: " + field_path + ")");
        }
        resolved_path.push_back(field);
        if (i < path_parts.size() - 1) {
            if (field->type() != google::protobuf::FieldDescriptor::TYPE_MESSAGE) {
                throw std::runtime_error("Field '" + path_parts[i] + "' is not a message type, cannot navigate to '" +
                                         path_parts[i + 1] + "' (field path: " + field_path + ")");
            }
            current_desc = field->message_type();
        }
    }
    return resolved_path;
}

LogicalType ProtobufFieldDescriptorToDuckDBType(const google::protobuf::FieldDescriptor *field) {
    if (field == nullptr || field->is_repeated()) {
        return LogicalType(LogicalTypeId::VARCHAR);
    }
    switch (field->type()) {
    case google::protobuf::FieldDescriptor::TYPE_STRING:
        return LogicalType(LogicalTypeId::VARCHAR);
    case google::protobuf::FieldDescriptor::TYPE_BYTES:
        return LogicalType(LogicalTypeId::BLOB);
    case google::protobuf::FieldDescriptor::TYPE_INT32:
    case google::protobuf::FieldDescriptor::TYPE_SINT32:
    case google::protobuf::FieldDescriptor::TYPE_SFIXED32:
        return LogicalType(LogicalTypeId::INTEGER);
    case google::protobuf::FieldDescriptor::TYPE_INT64:
    case google::protobuf::FieldDescriptor::TYPE_SINT64:
    case google::protobuf::FieldDescriptor::TYPE_SFIXED64:
        return LogicalType(LogicalTypeId::BIGINT);
    case google::protobuf::FieldDescriptor::TYPE_UINT32:
    case google::protobuf::FieldDescriptor::TYPE_FIXED32:
        return LogicalType(LogicalTypeId::UINTEGER);
    case google::protobuf::FieldDescriptor::TYPE_UINT64:
    case google::protobuf::FieldDescriptor::TYPE_FIXED64:
        return LogicalType(LogicalTypeId::UBIGINT);
    case google::protobuf::FieldDescriptor::TYPE_FLOAT:
        return LogicalType(LogicalTypeId::FLOAT);
    case google::protobuf::FieldDescriptor::TYPE_DOUBLE:
        return LogicalType(LogicalTypeId::DOUBLE);
    case google::protobuf::FieldDescriptor::TYPE_BOOL:
        return LogicalType(LogicalTypeId::BOOLEAN);
    case google::protobuf::FieldDescriptor::TYPE_ENUM:
    case google::protobuf::FieldDescriptor::TYPE_MESSAGE:
    default:
        return LogicalType(LogicalTypeId::VARCHAR);
    }
}

Value ExtractProtobufValue(const google::protobuf::Message *message,
                           const vector<const google::protobuf::FieldDescriptor *> &field_path) {
    const google::protobuf::Message *current_message = message;
    const google::protobuf::Reflection *reflection = message->GetReflection();

    for (size_t i = 0; i < field_path.size(); i++) {
        const auto *field = field_path[i];
        if (!field) {
            return Value();
        }

        if (i < field_path.size() - 1) {
            if (field->type() != google::protobuf::FieldDescriptor::TYPE_MESSAGE) {
                return Value();
            }
            if (!reflection->HasField(*current_message, field)) {
                return Value();
            }
            current_message = &reflection->GetMessage(*current_message, field);
            reflection = current_message->GetReflection();
        } else {
            if (field->is_repeated()) {
                int count = reflection->FieldSize(*current_message, field);
                if (count == 0) {
                    return Value("[]");
                }
                string result = "[";
                for (int j = 0; j < count; j++) {
                    if (j > 0) {
                        result += ",";
                    }
                    switch (field->type()) {
                    case google::protobuf::FieldDescriptor::TYPE_STRING:
                        result += "\"" + reflection->GetRepeatedString(*current_message, field, j) + "\"";
                        break;
                    case google::protobuf::FieldDescriptor::TYPE_INT32:
                    case google::protobuf::FieldDescriptor::TYPE_SINT32:
                    case google::protobuf::FieldDescriptor::TYPE_SFIXED32:
                        result += std::to_string(reflection->GetRepeatedInt32(*current_message, field, j));
                        break;
                    case google::protobuf::FieldDescriptor::TYPE_INT64:
                    case google::protobuf::FieldDescriptor::TYPE_SINT64:
                    case google::protobuf::FieldDescriptor::TYPE_SFIXED64:
                        result += std::to_string(reflection->GetRepeatedInt64(*current_message, field, j));
                        break;
                    case google::protobuf::FieldDescriptor::TYPE_UINT32:
                    case google::protobuf::FieldDescriptor::TYPE_FIXED32:
                        result += std::to_string(reflection->GetRepeatedUInt32(*current_message, field, j));
                        break;
                    case google::protobuf::FieldDescriptor::TYPE_UINT64:
                    case google::protobuf::FieldDescriptor::TYPE_FIXED64:
                        result += std::to_string(reflection->GetRepeatedUInt64(*current_message, field, j));
                        break;
                    case google::protobuf::FieldDescriptor::TYPE_FLOAT:
                        result += std::to_string(reflection->GetRepeatedFloat(*current_message, field, j));
                        break;
                    case google::protobuf::FieldDescriptor::TYPE_DOUBLE:
                        result += std::to_string(reflection->GetRepeatedDouble(*current_message, field, j));
                        break;
                    case google::protobuf::FieldDescriptor::TYPE_BOOL:
                        result += reflection->GetRepeatedBool(*current_message, field, j) ? "true" : "false";
                        break;
                    case google::protobuf::FieldDescriptor::TYPE_ENUM:
                        result += "\"" + string(reflection->GetRepeatedEnum(*current_message, field, j)->name()) + "\"";
                        break;
                    default:
                        result += "null";
                        break;
                    }
                }
                result += "]";
                return Value(result);
            }

            switch (field->type()) {
            case google::protobuf::FieldDescriptor::TYPE_STRING:
                return Value(reflection->GetString(*current_message, field));
            case google::protobuf::FieldDescriptor::TYPE_BYTES: {
                string bytes = reflection->GetString(*current_message, field);
                return Value::BLOB(const_data_ptr_cast(bytes.data()), bytes.size());
            }
            case google::protobuf::FieldDescriptor::TYPE_INT32:
            case google::protobuf::FieldDescriptor::TYPE_SINT32:
            case google::protobuf::FieldDescriptor::TYPE_SFIXED32:
                return Value::INTEGER(reflection->GetInt32(*current_message, field));
            case google::protobuf::FieldDescriptor::TYPE_INT64:
            case google::protobuf::FieldDescriptor::TYPE_SINT64:
            case google::protobuf::FieldDescriptor::TYPE_SFIXED64:
                return Value::BIGINT(reflection->GetInt64(*current_message, field));
            case google::protobuf::FieldDescriptor::TYPE_UINT32:
            case google::protobuf::FieldDescriptor::TYPE_FIXED32:
                return Value::UINTEGER(reflection->GetUInt32(*current_message, field));
            case google::protobuf::FieldDescriptor::TYPE_UINT64:
            case google::protobuf::FieldDescriptor::TYPE_FIXED64:
                return Value::UBIGINT(reflection->GetUInt64(*current_message, field));
            case google::protobuf::FieldDescriptor::TYPE_FLOAT:
                return Value::FLOAT(reflection->GetFloat(*current_message, field));
            case google::protobuf::FieldDescriptor::TYPE_DOUBLE:
                return Value::DOUBLE(reflection->GetDouble(*current_message, field));
            case google::protobuf::FieldDescriptor::TYPE_BOOL:
                return Value::BOOLEAN(reflection->GetBool(*current_message, field));
            case google::protobuf::FieldDescriptor::TYPE_ENUM: {
                const auto *enum_val = reflection->GetEnum(*current_message, field);
                return Value(string(enum_val->name()));
            }
            case google::protobuf::FieldDescriptor::TYPE_MESSAGE:
                return Value();
            default:
                return Value();
            }
        }
    }

    return Value();
}

static void SetProtobufNull(Vector &vector, idx_t row_idx) {
    FlatVector::SetNull(vector, row_idx, true);
}

static bool WriteProtobufScalar(Vector &vector, idx_t row_idx,
                                const google::protobuf::Message &message,
                                const google::protobuf::Reflection &reflection,
                                const google::protobuf::FieldDescriptor &field) {
    using Field = google::protobuf::FieldDescriptor;
    if (field.is_repeated() || field.type() == Field::TYPE_MESSAGE) {
        return false;
    }

    FlatVector::SetNull(vector, row_idx, false);
    switch (field.type()) {
    case Field::TYPE_STRING: {
        const auto &value = reflection.GetString(message, &field);
        GetNatsMutableVectorData<string_t>(vector)[row_idx] = StringVector::AddString(vector, value.data(), value.size());
        return true;
    }
    case Field::TYPE_BYTES: {
        const auto &value = reflection.GetString(message, &field);
        GetNatsMutableVectorData<string_t>(vector)[row_idx] = StringVector::AddStringOrBlob(vector, value.data(), value.size());
        return true;
    }
    case Field::TYPE_INT32:
    case Field::TYPE_SINT32:
    case Field::TYPE_SFIXED32:
        GetNatsMutableVectorData<int32_t>(vector)[row_idx] = reflection.GetInt32(message, &field);
        return true;
    case Field::TYPE_INT64:
    case Field::TYPE_SINT64:
    case Field::TYPE_SFIXED64:
        GetNatsMutableVectorData<int64_t>(vector)[row_idx] = reflection.GetInt64(message, &field);
        return true;
    case Field::TYPE_UINT32:
    case Field::TYPE_FIXED32:
        GetNatsMutableVectorData<uint32_t>(vector)[row_idx] = reflection.GetUInt32(message, &field);
        return true;
    case Field::TYPE_UINT64:
    case Field::TYPE_FIXED64:
        GetNatsMutableVectorData<uint64_t>(vector)[row_idx] = reflection.GetUInt64(message, &field);
        return true;
    case Field::TYPE_FLOAT:
        GetNatsMutableVectorData<float>(vector)[row_idx] = reflection.GetFloat(message, &field);
        return true;
    case Field::TYPE_DOUBLE:
        GetNatsMutableVectorData<double>(vector)[row_idx] = reflection.GetDouble(message, &field);
        return true;
    case Field::TYPE_BOOL:
        GetNatsMutableVectorData<bool>(vector)[row_idx] = reflection.GetBool(message, &field);
        return true;
    case Field::TYPE_ENUM: {
        const auto *value = reflection.GetEnum(message, &field);
        if (!value) {
            SetProtobufNull(vector, row_idx);
            return true;
        }
        auto name = value->name();
        GetNatsMutableVectorData<string_t>(vector)[row_idx] = StringVector::AddString(vector, name.data(), name.size());
        return true;
    }
    default:
        SetProtobufNull(vector, row_idx);
        return true;
    }
}

void DecodeProtobufFieldsToChunk(DataChunk &chunk, idx_t row_idx, const vector<idx_t> &output_columns,
                                 const google::protobuf::Message *message,
                                 const vector<vector<const google::protobuf::FieldDescriptor *>> &field_paths) {
    for (idx_t i = 0; i < output_columns.size(); i++) {
        auto &vector = chunk.data[output_columns[i]];
        SetProtobufNull(vector, row_idx);
        if (!message || i >= field_paths.size() || field_paths[i].empty()) {
            continue;
        }

        const google::protobuf::Message *current_message = message;
        const google::protobuf::Reflection *reflection = message->GetReflection();
        bool missing = false;
        for (idx_t path_idx = 0; path_idx + 1 < field_paths[i].size(); path_idx++) {
            const auto *field = field_paths[i][path_idx];
            if (!field || field->is_repeated() || field->type() != google::protobuf::FieldDescriptor::TYPE_MESSAGE ||
                !reflection->HasField(*current_message, field)) {
                missing = true;
                break;
            }
            current_message = &reflection->GetMessage(*current_message, field);
            reflection = current_message->GetReflection();
        }
        if (missing) {
            continue;
        }

        const auto *leaf = field_paths[i].back();
        if (!leaf || !WriteProtobufScalar(vector, row_idx, *current_message, *reflection, *leaf)) {
            chunk.SetValue(output_columns[i], row_idx, ExtractProtobufValue(message, field_paths[i]));
        }
    }
}

} // namespace duckdb
