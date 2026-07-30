#include "nats_proto_schema.hpp"

#include <filesystem>
#include <mutex>
#include <sstream>
#include <unordered_map>

// Windows defines GetMessage as a macro (GetMessageA/GetMessageW).
#ifdef GetMessage
#undef GetMessage
#endif

using namespace google::protobuf;
using namespace google::protobuf::compiler;

namespace duckdb {

namespace {

class SchemaErrorCollector : public MultiFileErrorCollector {
public:
#if GOOGLE_PROTOBUF_VERSION >= 3022000
    void RecordError(absl::string_view filename, int line, int column, absl::string_view message) override {
        errors += string(filename) + ":" + std::to_string(line) + ":" + std::to_string(column) + ": " + string(message) + "\n";
    }
#else
    void AddError(const std::string &filename, int line, int column, const std::string &message) override {
        errors += filename + ":" + std::to_string(line) + ":" + std::to_string(column) + ": " + message + "\n";
    }
#endif

    string errors;
};

struct CacheEntry {
    shared_ptr<NatsProtobufSchema> schema;
    std::filesystem::file_time_type modified;
};

std::mutex schema_cache_mutex;
unordered_map<string, CacheEntry> schema_cache;

string CanonicalSchemaPath(const string &proto_file) {
    std::error_code error;
    auto path = std::filesystem::weakly_canonical(std::filesystem::path(proto_file), error);
    if (error) {
        path = std::filesystem::absolute(std::filesystem::path(proto_file), error);
    }
    return path.string();
}

} // namespace

shared_ptr<NatsProtobufSchema> GetNatsProtobufSchema(const string &proto_file, const string &proto_message) {
    auto canonical_path = CanonicalSchemaPath(proto_file);
    std::error_code error;
    auto modified = std::filesystem::last_write_time(canonical_path, error);
    if (error) {
        throw std::runtime_error("Failed to stat protobuf schema file: " + proto_file);
    }
    auto key = canonical_path + "\n" + proto_message;

    lock_guard<std::mutex> guard(schema_cache_mutex);
    auto cached = schema_cache.find(key);
    if (cached != schema_cache.end() && cached->second.modified == modified) {
        return cached->second.schema;
    }

    auto source_tree = make_shared_ptr<DiskSourceTree>();
    std::filesystem::path schema_path(canonical_path);
    source_tree->MapPath("", schema_path.parent_path().string());
    auto error_collector = make_shared_ptr<SchemaErrorCollector>();
    auto importer = make_shared_ptr<Importer>(source_tree.get(), error_collector.get());
    auto file_desc = importer->Import(schema_path.filename().string());
    if (!file_desc) {
        string message = "Failed to import protobuf schema file: " + proto_file;
        if (!error_collector->errors.empty()) {
            message += "\n" + error_collector->errors;
        }
        throw std::runtime_error(message);
    }
    auto descriptor = file_desc->FindMessageTypeByName(proto_message);
    if (!descriptor) {
        throw std::runtime_error("Message type '" + proto_message + "' not found in " + proto_file);
    }

    auto schema = make_shared_ptr<NatsProtobufSchema>();
    schema->source_tree = std::move(source_tree);
    schema->importer = std::move(importer);
    schema->descriptor = descriptor;
    schema_cache[key] = {schema, modified};
    return schema;
}

} // namespace duckdb
