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

void DecodeJsonFieldsToChunk(DataChunk &chunk, idx_t row_idx, idx_t first_column, const NatsPayloadView &payload,
                             const vector<string> &field_names);

vector<vector<string>> SplitMsgpackFieldPaths(const vector<string> &field_names);

void DecodeMsgpackFieldPathsToChunk(DataChunk &chunk, idx_t row_idx, const vector<idx_t> &output_columns,
                                    const NatsPayloadView &payload,
                                    const vector<vector<string>> &field_paths);

bool DecodeProtobufPayload(google::protobuf::Message &message, const NatsPayloadView &payload);

vector<const google::protobuf::FieldDescriptor *> ResolveProtobufFieldPath(
    const google::protobuf::Descriptor *message_desc, const string &field_path);

LogicalType ProtobufFieldDescriptorToDuckDBType(const google::protobuf::FieldDescriptor *field);

Value ExtractProtobufValue(const google::protobuf::Message *message,
                           const vector<const google::protobuf::FieldDescriptor *> &field_path);

void DecodeProtobufFieldsToChunk(DataChunk &chunk, idx_t row_idx, const vector<idx_t> &output_columns,
                                 const google::protobuf::Message *message,
                                 const vector<vector<const google::protobuf::FieldDescriptor *>> &field_paths);

} // namespace duckdb
