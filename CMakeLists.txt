cmake_minimum_required(VERSION 3.5...3.29)

# Set extension name
set(TARGET_NAME nats_js)

# Project configuration
set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})

# Require C++17 for std::filesystem and other modern features
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include_directories(src/include)

# Extension sources
set(EXTENSION_SOURCES src/nats_scan.cpp src/nats_js_extension.cpp src/nats_connection.cpp src/nats_ingest.cpp src/nats_message_decode.cpp src/nats_proto_schema.cpp src/nats_source.cpp src/nats_subscribe.cpp)

# Build static and loadable extensions using DuckDB's build functions
build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Find dependencies via vcpkg
find_package(OpenSSL REQUIRED)
find_package(unofficial-sodium CONFIG REQUIRED)
find_package(cnats CONFIG REQUIRED)
find_package(Protobuf CONFIG REQUIRED)
find_package(flatbuffers CONFIG REQUIRED)

# Link dependencies - use plain signature to match DuckDB's build functions
# cnats exports 'cnats::nats_static' target for static library
target_link_libraries(${EXTENSION_NAME} cnats::nats_static protobuf::libprotobuf protobuf::libprotoc flatbuffers::flatbuffers)
target_link_libraries(${LOADABLE_EXTENSION_NAME} cnats::nats_static protobuf::libprotobuf protobuf::libprotoc flatbuffers::flatbuffers)

# DuckDB 1.5.5/main changed several internal C++ extension APIs. Detect those
# headers from the host DuckDB source so the extension remains buildable with
# the repository's older pinned checkout as well.
set(NATS_DUCKDB_API_DEFINITIONS)
if(DEFINED DUCKDB_SOURCE_DIR)
    set(NATS_DUCKDB_SOURCE_ROOT "${DUCKDB_SOURCE_DIR}")
else()
    set(NATS_DUCKDB_SOURCE_ROOT "${CMAKE_SOURCE_DIR}")
endif()
if(EXISTS "${NATS_DUCKDB_SOURCE_ROOT}/src/include/duckdb/parser/qualified_name.hpp")
    file(READ "${NATS_DUCKDB_SOURCE_ROOT}/src/include/duckdb/parser/qualified_name.hpp" NATS_QUALIFIED_NAME_HEADER)
    string(FIND "${NATS_QUALIFIED_NAME_HEADER}" "const Identifier &Catalog()" NATS_HAS_IDENTIFIER_NAMES)
    if(NATS_HAS_IDENTIFIER_NAMES GREATER -1)
        list(APPEND NATS_DUCKDB_API_DEFINITIONS NATS_DUCKDB_IDENTIFIER_API)
    endif()

    file(READ "${NATS_DUCKDB_SOURCE_ROOT}/src/include/duckdb/common/types/vector.hpp" NATS_VECTOR_HEADER)
    if(EXISTS "${NATS_DUCKDB_SOURCE_ROOT}/src/include/duckdb/common/vector/flat_vector.hpp")
        file(READ "${NATS_DUCKDB_SOURCE_ROOT}/src/include/duckdb/common/vector/flat_vector.hpp" NATS_VECTOR_HEADER)
    endif()
    string(FIND "${NATS_VECTOR_HEADER}" "GetDataMutable" NATS_HAS_MUTABLE_VECTOR_DATA)
    if(NATS_HAS_MUTABLE_VECTOR_DATA GREATER -1)
        list(APPEND NATS_DUCKDB_API_DEFINITIONS NATS_DUCKDB_MUTABLE_VECTOR_API)
    endif()
endif()
target_compile_definitions(${EXTENSION_NAME} PRIVATE ${NATS_DUCKDB_API_DEFINITIONS})
target_compile_definitions(${LOADABLE_EXTENSION_NAME} PRIVATE ${NATS_DUCKDB_API_DEFINITIONS})

# Install static library
install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
