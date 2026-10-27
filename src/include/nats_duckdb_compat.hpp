#pragma once

#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/qualified_name.hpp"
#ifdef NATS_DUCKDB_IDENTIFIER_API
#include "duckdb/common/identifier.hpp"
#endif

namespace duckdb {

// DuckDB 2.0 removed TableFunction::named_parameters; optional named arguments are
// declared as typed kwargs on the function signature instead.
#ifdef NATS_DUCKDB_TYPED_KWARGS
inline void NatsAddNamedParameter(TableFunction &function, const char *name, const LogicalType &type) {
    auto &signature = function.GetSignature();
    auto add = [&](TypedKwargs &options) { options.Add(Identifier(name), type); };
    if (signature.GetTypedKwargs()) {
        signature.ExtendTypedKwargs(add);
    } else {
        signature.WithTypedKwargs(Identifier("options"), add);
    }
}
#else
inline void NatsAddNamedParameter(TableFunction &function, const char *name, const LogicalType &type) {
    function.named_parameters[name] = type;
}
#endif

#ifdef NATS_DUCKDB_IDENTIFIER_API
using NatsBindColumnNames = vector<Identifier>;
using NatsCopyFromColumnNames = vector<Identifier>;
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
using NatsBindColumnNames = vector<string>;
using NatsCopyFromColumnNames = vector<string>;
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

// CopyInfo::options switched from case_insensitive_map_t<vector<Value>> (keyed by
// plain string) to identifier_map_t<vector<Value>> (keyed by Identifier, whose
// string constructor is explicit) on the same DuckDB revision that introduced the
// Identifier-based catalog API above.
#ifdef NATS_DUCKDB_IDENTIFIER_API
using NatsCopyOptionsMap = identifier_map_t<vector<Value>>;

inline NatsCopyOptionsMap::const_iterator NatsFindCopyOption(const NatsCopyOptionsMap &options, const string &name) {
    return options.find(Identifier(name));
}
#else
using NatsCopyOptionsMap = case_insensitive_map_t<vector<Value>>;

inline NatsCopyOptionsMap::const_iterator NatsFindCopyOption(const NatsCopyOptionsMap &options, const string &name) {
    return options.find(name);
}
#endif

} // namespace duckdb
