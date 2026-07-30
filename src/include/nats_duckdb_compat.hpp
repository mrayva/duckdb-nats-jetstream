#pragma once

#include "duckdb/parser/qualified_name.hpp"

namespace duckdb {

#ifdef NATS_DUCKDB_IDENTIFIER_API
using NatsQualifiedNamePart = Identifier;

inline const NatsQualifiedNamePart &NatsQualifiedCatalog(const QualifiedName &name) {
    return name.Catalog();
}

inline const NatsQualifiedNamePart &NatsQualifiedSchema(const QualifiedName &name) {
    return name.Schema();
}

inline const NatsQualifiedNamePart &NatsQualifiedTable(const QualifiedName &name) {
    return name.Name();
}
#else
using NatsQualifiedNamePart = string;

inline const NatsQualifiedNamePart &NatsQualifiedCatalog(const QualifiedName &name) {
    return name.catalog;
}

inline const NatsQualifiedNamePart &NatsQualifiedSchema(const QualifiedName &name) {
    return name.schema;
}

inline const NatsQualifiedNamePart &NatsQualifiedTable(const QualifiedName &name) {
    return name.name;
}
#endif

} // namespace duckdb
