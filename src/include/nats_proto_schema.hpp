#pragma once

#include "duckdb.hpp"
#include <google/protobuf/compiler/importer.h>
#include <google/protobuf/descriptor.h>

namespace duckdb {

struct NatsProtobufSchema {
    shared_ptr<google::protobuf::compiler::DiskSourceTree> source_tree;
    shared_ptr<google::protobuf::compiler::Importer> importer;
    const google::protobuf::Descriptor *descriptor = nullptr;
};

// Load a schema once per process while retaining the importer that owns its descriptors.
shared_ptr<NatsProtobufSchema> GetNatsProtobufSchema(const string &proto_file, const string &proto_message);

} // namespace duckdb
