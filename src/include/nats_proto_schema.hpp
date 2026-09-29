#pragma once

#include "duckdb.hpp"
#include <google/protobuf/compiler/importer.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>

namespace duckdb {

struct NatsProtobufSchema {
    shared_ptr<google::protobuf::compiler::DiskSourceTree> source_tree;
    shared_ptr<google::protobuf::compiler::Importer> importer;
    shared_ptr<google::protobuf::DescriptorPool> descriptor_pool;
    const google::protobuf::Descriptor *descriptor = nullptr;
};

// Load a schema once per process while retaining the importer that owns its descriptors.
shared_ptr<NatsProtobufSchema> GetNatsProtobufSchema(const string &proto_file, const string &proto_message);
shared_ptr<NatsProtobufSchema> GetNatsProtobufDescriptorSetSchema(const string &descriptor_set_file,
                                                                  const string &proto_message);

} // namespace duckdb
