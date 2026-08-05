#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {

class ExtensionLoader;

class NatsKvFunction {
public:
    static void Register(ExtensionLoader &loader);
};

} // namespace duckdb
