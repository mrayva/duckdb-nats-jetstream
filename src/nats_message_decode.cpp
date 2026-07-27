#include "nats_message_decode.hpp"

#include "yyjson.hpp"

using namespace duckdb_yyjson;

namespace duckdb {

vector<Value> DecodeJsonFields(const NatsPayloadView &payload, const vector<string> &field_names) {
    vector<Value> values;
    values.reserve(field_names.size());

    yyjson_doc *doc = yyjson_read(payload.data, payload.size, 0);
    if (!doc) {
        values.resize(field_names.size());
        return values;
    }

    yyjson_val *root = yyjson_doc_get_root(doc);
    for (const auto &field_name : field_names) {
        yyjson_val *field = yyjson_obj_get(root, field_name.c_str());
        if (!field || yyjson_is_null(field)) {
            values.emplace_back(Value());
        } else if (yyjson_is_str(field)) {
            values.emplace_back(Value(yyjson_get_str(field)));
        } else if (yyjson_is_num(field)) {
            values.emplace_back(Value(yyjson_get_num(field)));
        } else if (yyjson_is_bool(field)) {
            values.emplace_back(Value::BOOLEAN(yyjson_get_bool(field)));
        } else {
            char *json_string = yyjson_val_write(field, 0, nullptr);
            if (json_string) {
                values.emplace_back(Value(json_string));
                free(json_string);
            } else {
                values.emplace_back(Value());
            }
        }
    }

    yyjson_doc_free(doc);
    return values;
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
