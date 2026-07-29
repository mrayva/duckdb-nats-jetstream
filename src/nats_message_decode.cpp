#include "nats_message_decode.hpp"

#include "yyjson.hpp"

#include <cstring>
#include <iomanip>
#include <sstream>

using namespace duckdb_yyjson;

namespace duckdb {

namespace {

class MsgpackReader {
public:
    MsgpackReader(const char *data, idx_t size) : data(reinterpret_cast<const uint8_t *>(data)), remaining(size) {
    }

    bool FindField(const vector<string> &path, idx_t path_idx, string &value) {
        uint8_t tag;
        if (!ReadByte(tag)) {
            return false;
        }
        if ((tag & 0xf0) == 0x80 || tag == 0xde || tag == 0xdf) {
            uint32_t count;
            if (!ReadMapCount(tag, count)) {
                return false;
            }
            for (uint32_t i = 0; i < count; i++) {
                string key;
                uint8_t key_tag;
                if (!ReadByte(key_tag) || !ReadString(key_tag, key) || !SkipValueStart()) {
                    return false;
                }
                if (key == path[path_idx]) {
                    if (path_idx + 1 == path.size()) {
                        return ReadScalarValue(value);
                    }
                    return FindField(path, path_idx + 1, value);
                }
                if (!SkipValue()) {
                    return false;
                }
            }
            return false;
        }
        return false;
    }

private:
    bool ReadByte(uint8_t &value) {
        if (remaining == 0) {
            return false;
        }
        value = *data++;
        remaining--;
        return true;
    }

    bool ReadBytes(void *out, idx_t count) {
        if (count > remaining) {
            return false;
        }
        memcpy(out, data, count);
        data += count;
        remaining -= count;
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

    bool ReadString(uint8_t tag, string &value) {
        idx_t length;
        if ((tag & 0xe0) == 0xa0) {
            length = tag & 0x1f;
        } else if (tag == 0xd9 || tag == 0xda || tag == 0xdb) {
            uint64_t size;
            if (!ReadUnsigned(tag == 0xd9 ? 1 : (tag == 0xda ? 2 : 4), size)) {
                return false;
            }
            length = static_cast<idx_t>(size);
        } else {
            return false;
        }
        value.resize(length);
        return ReadBytes(value.data(), length);
    }

    bool SkipValueStart() {
        return true;
    }

    bool ReadScalarValue(string &value) {
        uint8_t tag;
        if (!ReadByte(tag)) {
            return false;
        }
        if ((tag & 0xe0) == 0xa0 || tag == 0xd9 || tag == 0xda || tag == 0xdb) {
            return ReadString(tag, value);
        }
        if (tag <= 0x7f) {
            value = std::to_string(tag);
            return true;
        }
        if (tag >= 0xe0) {
            value = std::to_string(static_cast<int8_t>(tag));
            return true;
        }
        uint64_t number;
        switch (tag) {
        case 0xc0:
            return false;
        case 0xc2:
            value = "false";
            return true;
        case 0xc3:
            value = "true";
            return true;
        case 0xcc:
        case 0xcd:
        case 0xce:
        case 0xcf:
            if (!ReadUnsigned(tag == 0xcc ? 1 : (tag == 0xcd ? 2 : (tag == 0xce ? 4 : 8)), number)) return false;
            value = std::to_string(number);
            return true;
        case 0xd0:
        case 0xd1:
        case 0xd2:
        case 0xd3: {
            if (!ReadUnsigned(tag == 0xd0 ? 1 : (tag == 0xd1 ? 2 : (tag == 0xd2 ? 4 : 8)), number)) return false;
            int64_t signed_value;
            if (tag == 0xd0) signed_value = static_cast<int8_t>(number);
            else if (tag == 0xd1) signed_value = static_cast<int16_t>(number);
            else if (tag == 0xd2) signed_value = static_cast<int32_t>(number);
            else signed_value = static_cast<int64_t>(number);
            value = std::to_string(signed_value);
            return true;
        }
        case 0xca: {
            if (!ReadUnsigned(4, number)) return false;
            uint32_t bits = static_cast<uint32_t>(number);
            float number_value;
            memcpy(&number_value, &bits, sizeof(number_value));
            value = std::to_string(number_value);
            return true;
        }
        case 0xcb: {
            if (!ReadUnsigned(8, number)) return false;
            double number_value;
            memcpy(&number_value, &number, sizeof(number_value));
            value = std::to_string(number_value);
            return true;
        }
        default:
            return false;
        }
    }

    bool SkipValue() {
        uint8_t tag;
        if (!ReadByte(tag)) return false;
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

static vector<string> SplitMsgpackPath(const string &path) {
    vector<string> result;
    size_t start = 0;
    while (true) {
        auto end = path.find('.', start);
        result.push_back(path.substr(start, end == string::npos ? string::npos : end - start));
        if (end == string::npos) return result;
        start = end + 1;
    }
}

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
        auto vector_data = FlatVector::GetData<string_t>(vector);
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

void DecodeMsgpackFieldsToChunk(DataChunk &chunk, idx_t row_idx, idx_t first_column,
                                const NatsPayloadView &payload, const vector<string> &field_names) {
    for (idx_t i = 0; i < field_names.size(); i++) {
        auto &vector = chunk.data[first_column + i];
        string value;
        MsgpackReader reader(payload.data, payload.size);
        auto path = SplitMsgpackPath(field_names[i]);
        if (reader.FindField(path, 0, value)) {
            auto vector_data = FlatVector::GetData<string_t>(vector);
            vector_data[row_idx] = StringVector::AddString(vector, value);
        } else {
            FlatVector::SetNull(vector, row_idx, true);
        }
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

} // namespace duckdb
