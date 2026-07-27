#pragma once

#include "duckdb.hpp"
#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>

namespace duckdb {

struct NatsPayloadView {
    const char *data = nullptr;
    idx_t size = 0;
};

void DecodeJsonFields(const NatsPayloadView &payload, const vector<string> &field_names, vector<Value> &values);

bool DecodeProtobufPayload(google::protobuf::Message &message, const NatsPayloadView &payload);

vector<const google::protobuf::FieldDescriptor *> ResolveProtobufFieldPath(
    const google::protobuf::Descriptor *message_desc, const string &field_path);

LogicalType ProtobufFieldDescriptorToDuckDBType(const google::protobuf::FieldDescriptor *field);

Value ExtractProtobufValue(const google::protobuf::Message *message,
                           const vector<const google::protobuf::FieldDescriptor *> &field_path);

} // namespace duckdb
