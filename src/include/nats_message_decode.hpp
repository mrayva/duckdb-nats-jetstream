#pragma once

#include "duckdb.hpp"
#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>

namespace duckdb {

vector<const google::protobuf::FieldDescriptor *> ResolveProtobufFieldPath(
    const google::protobuf::Descriptor *message_desc, const string &field_path);

Value ExtractProtobufValue(const google::protobuf::Message *message,
                           const vector<const google::protobuf::FieldDescriptor *> &field_path);

} // namespace duckdb
