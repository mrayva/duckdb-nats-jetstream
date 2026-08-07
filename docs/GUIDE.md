# DuckDB NATS JetStream Extension - Full Guide

A DuckDB extension that enables SQL queries over NATS JetStream message streams and KV buckets. It supports sequence- and timestamp-based range scans over JetStream streams with JSON, MessagePack, CBOR, FlexBuffers, and Protocol Buffers payload extraction; durable-consumer ingest and core NATS live subscriptions that batch messages into DuckDB tables; and point CRUD, bulk access, and live watch jobs over JetStream KV buckets.

## Installation

### From DuckDB Community Extensions (Recommended)

Once published to the DuckDB Community Extensions repository, installation is simple:

```sql
INSTALL nats_js FROM community;
LOAD nats_js;
```

This method automatically downloads the pre-built extension binary for your platform. No compilation required, and no separate NATS C client, Protocol Buffers, or libsodium/OpenSSL install is needed — the CI-built binary links its native dependencies statically (vcpkg's default triplet builds `cnats`, `protobuf`, `flatbuffers`, and their own transitive dependencies as static libraries bundled into the single `.duckdb_extension` file).

### Building from Source

For development or if you need to build from source:

**Prerequisites:**
- DuckDB v1.5.5 (the pinned `duckdb/` submodule; DuckDB 1.5.x/main "tip" APIs are also supported, see `src/include/nats_duckdb_compat.hpp`)
- CMake 3.15 or later
- C++17 compatible compiler
- [vcpkg](https://vcpkg.io) (recommended — see below) or system installs of the NATS C client (cnats), Protocol Buffers, and FlatBuffers

**Build steps:**

Clone the repository and initialize submodules (this also pulls the vendored `vcpkg/` submodule):

```bash
git clone https://github.com/brannn/duckdb-nats-jetstream
cd duckdb-nats-jetstream
git submodule update --init --recursive
```

**Recommended: build via vcpkg.** This repo vendors vcpkg as a submodule, already
pinned to the commit its `vcpkg.json` manifest requires, and ships a prebuilt
`vcpkg/vcpkg` binary — no bootstrap step needed. The extension's actual native
dependency is just `cnats` (`vcpkg.json`), but `cnats` itself pulls in
`libsodium`, `openssl`, and `protobuf-c` transitively (for NATS TLS and
NKey/JWT auth), plus `protobuf` and `flatbuffers` are direct dependencies of
this extension. vcpkg builds all of them from source into a local,
user-writable prefix — no system package manager or root access required,
as long as a compiler toolchain (perl, autoconf/automake/libtool for the
autotools-based `libsodium` port, etc.) is already on `PATH`.

```bash
export VCPKG_TOOLCHAIN_PATH="$(pwd)/vcpkg/scripts/buildsystems/vcpkg.cmake"
make debug    # or: make release
```

The first build compiles OpenSSL, libsodium, Protobuf, FlatBuffers, and cnats
from source alongside DuckDB itself, so expect it to take significantly
longer than an incremental build. This is also the path CI uses
(`.github/workflows/MainDistributionPipeline.yml`), so it's the most
reliable way to reproduce a CI build locally.

**Alternative: system-installed libraries.** If you'd rather not use vcpkg,
you need `cnats`, `protobuf`, and `flatbuffers` discoverable via CMake's
`find_package(... CONFIG REQUIRED)`, which most distro packages do not
provide out of the box (Homebrew's `cnats`/`protobuf` formulas are closer,
but package availability varies by platform and version). Without
`VCPKG_TOOLCHAIN_PATH` set, `make debug`/`make release` falls back to this
path:

```bash
# macOS
brew install cnats protobuf

# Ubuntu/Debian (package availability varies; may not satisfy CMake CONFIG lookups)
sudo apt-get install libnats-dev libprotobuf-dev protobuf-compiler
```

```bash
make build
```

The compiled extension will be located at `build/release/extension/nats_js/nats_js.duckdb_extension`.

**Loading the extension:**

```sql
LOAD 'build/release/extension/nats_js/nats_js.duckdb_extension';
```

Note: When loading unsigned extensions, you may need to start DuckDB with the `-unsigned` flag:

```bash
duckdb -unsigned
```

### Runtime Requirements

A running NATS server with JetStream enabled is required for operation. The extension connects to `nats://localhost:4222` by default, which can be overridden using the `url` parameter.

## Query Capabilities

The extension provides the `nats_scan` table function, which returns messages from a JetStream stream. Each row represents a single message with columns for stream name, subject, sequence number, timestamp, and payload.

### Basic Message Retrieval

Query all messages from a stream:

```sql
SELECT stream, subject, seq, ts_nats, payload
FROM nats_scan('telemetry');
```

```
┌───────────┬──────────────────────────────────┬────────┬─────────────────────────┬──────────────────────────────────────┐
│  stream   │             subject              │  seq   │        ts_nats          │               payload                │
│  varchar  │             varchar              │ uint64 │       timestamp         │                blob                  │
├───────────┼──────────────────────────────────┼────────┼─────────────────────────┼──────────────────────────────────────┤
│ telemetry │ telemetry.dc1.power.pm5560-001   │      1 │ 2025-11-01 09:00:00     │ \x7B\x22\x64\x65\x76\x69\x63\x65...  │
│ telemetry │ telemetry.dc1.power.pm5560-002   │      2 │ 2025-11-01 09:00:01     │ \x7B\x22\x64\x65\x76\x69\x63\x65...  │
│ telemetry │ telemetry.dc1.power.pm5560-003   │      3 │ 2025-11-01 09:00:02     │ \x7B\x22\x64\x65\x76\x69\x63\x65...  │
└───────────┴──────────────────────────────────┴────────┴─────────────────────────┴──────────────────────────────────────┘
```

The function returns five base columns: `stream` (VARCHAR), `subject` (VARCHAR), `seq` (UBIGINT), `ts_nats` (TIMESTAMP), and `payload` (BLOB by default, VARCHAR when using `json_extract`).

### Sequence Range Queries

Retrieve messages within a specific sequence number range:

```sql
SELECT seq, ts_nats, payload
FROM nats_scan('telemetry', start_seq := 1000, end_seq := 2000);
```

Sequence numbers are inclusive on both ends. The `start_seq` parameter defaults to 1, and `end_seq` defaults to the last message in the stream.

### Timestamp Range Queries

Query messages within a time range:

```sql
SELECT seq, ts_nats, subject, payload
FROM nats_scan('telemetry',
    start_time := '2025-11-01 09:00:00'::TIMESTAMP,
    end_time := '2025-11-01 17:00:00'::TIMESTAMP
);
```

The extension uses binary search to resolve timestamps to sequence numbers, providing O(log n) lookup performance. Timestamp parameters cannot be mixed with sequence parameters in the same query.

### Subject Filtering

Filter messages by NATS subject pattern on the server:

```sql
SELECT seq, subject, payload
FROM nats_scan('telemetry', nats_subject := 'telemetry.dc1.power.pm5560-001');
```

Use `nats_subject` for exact or wildcard NATS subject filters that can be pushed down to JetStream. Use `subject_contains` for client-side substring matching on message subjects. The legacy `subject` parameter is still accepted as a substring-compatible alias.

### Combined Queries

Combine multiple query parameters:

```sql
SELECT seq, ts_nats, subject, payload
FROM nats_scan('telemetry',
    subject_contains := 'pm5560',
    start_time := '2025-11-01 09:00:00'::TIMESTAMP,
    end_time := '2025-11-01 17:00:00'::TIMESTAMP
);
```

## JSON Processing

The extension can extract fields from JSON payloads and expose them as additional columns. This feature is useful for IoT telemetry, application logs, and other structured message data.

### Extracting JSON Fields

Use the `json_extract` parameter to specify fields to extract:

```sql
SELECT device_id, kw, voltage, pf
FROM nats_scan('telemetry',
    start_seq := 1,
    end_seq := 100,
    json_extract := ['device_id', 'kw', 'voltage', 'pf']
);
```

```
┌──────────────┬─────────┬─────────┬─────────┐
│  device_id   │   kw    │ voltage │   pf    │
│   varchar    │ varchar │ varchar │ varchar │
├──────────────┼─────────┼─────────┼─────────┤
│ pm5560-001   │ 42.5    │ 480.2   │ 0.95    │
│ pm5560-002   │ 38.2    │ 479.8   │ 0.92    │
│ pm5560-003   │ 51.7    │ 481.1   │ 0.97    │
│ pm5560-004   │ 45.3    │ 480.5   │ 0.94    │
└──────────────┴─────────┴─────────┴─────────┘
```

This example extracts power monitoring data from a hypothetical IoT sensor stream. Each extracted field becomes a VARCHAR column in the result set.

### Type Handling

The JSON extraction handles multiple data types. String values are returned directly, numeric values are converted to strings, boolean values become "true" or "false", and null values produce SQL NULL. Complex types like objects and arrays are serialized as JSON strings.

### Analytics with Extracted Fields

Combine JSON extraction with DuckDB's analytical capabilities:

```sql
SELECT
    device_id,
    AVG(CAST(kw AS DOUBLE)) as avg_power,
    MAX(CAST(kw AS DOUBLE)) as peak_power,
    COUNT(*) as reading_count
FROM nats_scan('telemetry',
    start_time := '2025-11-01 00:00:00'::TIMESTAMP,
    end_time := '2025-11-01 23:59:59'::TIMESTAMP,
    json_extract := ['device_id', 'kw']
)
GROUP BY device_id
ORDER BY avg_power DESC;
```

```
┌──────────────┬───────────┬────────────┬───────────────┐
│  device_id   │ avg_power │ peak_power │ reading_count │
│   varchar    │  double   │   double   │     int64     │
├──────────────┼───────────┼────────────┼───────────────┤
│ pm5560-003   │     51.24 │      58.90 │          1440 │
│ pm5560-004   │     45.67 │      52.30 │          1440 │
│ pm5560-001   │     42.18 │      49.10 │          1440 │
│ pm5560-002   │     38.92 │      44.50 │          1440 │
└──────────────┴───────────┴────────────┴───────────────┘
```

This query analyzes power consumption across multiple devices, demonstrating how the extension integrates with DuckDB's aggregation and sorting functions.

## Protocol Buffers Processing

The extension supports extracting fields from Protocol Buffers (protobuf) encoded messages. This feature is essential for systems using protobuf for efficient binary serialization, common in IoT telemetry, microservices, and high-throughput data pipelines.

### Extracting Protobuf Fields

Use the `proto_extract` parameter along with `proto_file` and `proto_message` to specify fields to extract:

```sql
SELECT device_id, timestamp, location_zone, metrics_kw, online
FROM nats_scan('telemetry',
    proto_file := 'schemas/telemetry.proto',
    proto_message := 'Telemetry',
    proto_extract := ['device_id', 'timestamp', 'location.zone', 'metrics.kw', 'online']
);
```

```
┌──────────────┬─────────────────────┬───────────────┬────────────┬─────────┐
│  device_id   │      timestamp      │ location_zone │ metrics_kw │ online  │
│   varchar    │       int64         │    varchar    │   double   │ boolean │
├──────────────┼─────────────────────┼───────────────┼────────────┼─────────┤
│ pm5560-001   │ 1730455200000000000 │ zone-a        │      42.50 │ true    │
│ pm5560-002   │ 1730455201000000000 │ zone-a        │      38.20 │ true    │
│ pm5560-003   │ 1730455202000000000 │ zone-b        │      51.70 │ true    │
│ pm5560-004   │ 1730455203000000000 │ zone-b        │      45.30 │ false   │
└──────────────┴─────────────────────┴───────────────┴────────────┴─────────┘
```

This example extracts fields from a protobuf-encoded telemetry stream. The `proto_file` parameter specifies the path to the .proto schema file, `proto_message` specifies the message type name, and `proto_extract` lists the fields to extract.

### Nested Message Fields

Protobuf nested messages are accessed using dot notation in field paths. The extension automatically navigates through nested message structures:

```sql
SELECT device_id, location_zone, location_rack, metrics_kw, metrics_voltage
FROM nats_scan('telemetry',
    proto_file := 'schemas/telemetry.proto',
    proto_message := 'Telemetry',
    proto_extract := [
        'device_id',
        'location.zone',
        'location.rack',
        'metrics.kw',
        'metrics.voltage'
    ]
);
```

For a schema with nested Location and Metrics messages, the extension extracts `location.zone` from the Location message and `metrics.kw` from the Metrics message. Column names use underscores instead of dots (`location_zone`, `metrics_kw`) for natural SQL syntax.

### Type Mapping

The extension maps protobuf types to appropriate DuckDB types:

| Protobuf Type | DuckDB Type |
|---------------|-------------|
| `string` | VARCHAR |
| `bytes` | BLOB |
| `int32`, `sint32`, `sfixed32` | INTEGER |
| `int64`, `sint64`, `sfixed64` | BIGINT |
| `uint32`, `fixed32` | UINTEGER |
| `uint64`, `fixed64` | UBIGINT |
| `float` | FLOAT |
| `double` | DOUBLE |
| `bool` | BOOLEAN |
| `enum` | VARCHAR (enum name) |

This type mapping enables direct use of numeric fields in calculations without type casting:

```sql
SELECT
    device_id,
    AVG(metrics_kw) as avg_power,
    MAX(metrics_voltage) as peak_voltage,
    COUNT(*) as reading_count
FROM nats_scan('telemetry',
    proto_file := 'schemas/telemetry.proto',
    proto_message := 'Telemetry',
    proto_extract := ['device_id', 'metrics.kw', 'metrics.voltage']
)
GROUP BY device_id;
```

```
┌──────────────┬───────────┬──────────────┬───────────────┐
│  device_id   │ avg_power │ peak_voltage │ reading_count │
│   varchar    │  double   │    double    │     int64     │
├──────────────┼───────────┼──────────────┼───────────────┤
│ pm5560-003   │     51.24 │       482.30 │          1440 │
│ pm5560-004   │     45.67 │       481.80 │          1440 │
│ pm5560-001   │     42.18 │       480.90 │          1440 │
│ pm5560-002   │     38.92 │       479.50 │          1440 │
└──────────────┴───────────┴──────────────┴───────────────┘
```

### Analytics with Protobuf Data

Combine protobuf extraction with DuckDB's analytical capabilities for complex queries:

```sql
SELECT
    location_zone,
    COUNT(*) as message_count,
    ROUND(AVG(metrics_kw), 2) as avg_kw,
    ROUND(MIN(metrics_kw), 2) as min_kw,
    ROUND(MAX(metrics_kw), 2) as max_kw
FROM nats_scan('telemetry',
    proto_file := 'schemas/telemetry.proto',
    proto_message := 'Telemetry',
    proto_extract := ['location.zone', 'metrics.kw'],
    start_time := '2025-11-01 00:00:00'::TIMESTAMP,
    end_time := '2025-11-01 23:59:59'::TIMESTAMP
)
GROUP BY location_zone
ORDER BY avg_kw DESC;
```

This query analyzes power consumption by data center zone, demonstrating how protobuf extraction integrates with time-based queries and aggregations.

### Filtering on Protobuf Fields

Filter results using extracted protobuf fields:

```sql
SELECT device_id, location_zone, metrics_kw, metrics_voltage, online
FROM nats_scan('telemetry',
    proto_file := 'schemas/telemetry.proto',
    proto_message := 'Telemetry',
    proto_extract := ['device_id', 'location.zone', 'metrics.kw', 'metrics.voltage', 'online']
)
WHERE location_zone = 'dc1'
  AND metrics_kw > 5.0
  AND online = true;
```

The extension extracts and decodes protobuf fields, then DuckDB applies the WHERE clause filters efficiently.

### Payload Column Behavior

The `payload` column type depends on the extraction parameters used:
- **BLOB** (default): When no extraction parameters are specified, or when using `proto_extract`
- **VARCHAR**: When using `json_extract` for JSON messages

This prevents UTF-8 validation errors on binary data while keeping JSON messages readable:

```sql
SELECT seq, typeof(payload) as payload_type, octet_length(payload) as payload_size
FROM nats_scan('telemetry',
    proto_file := 'schemas/telemetry.proto',
    proto_message := 'Telemetry',
    proto_extract := ['device_id']
)
LIMIT 5;
```

## Implementation Details

Understanding the extension's implementation approach helps explain its performance characteristics and operational behavior.

### Batched Pull Consumers For Message Retrieval

`nats_scan` retrieves the bulk message range through an ephemeral JetStream
pull consumer (`js_PullSubscribe`), fetching messages in batches
(`jsFetchRequest`, sized by `batch_size` and bounded by `fetch_timeout_ms`)
rather than issuing one request per row. This amortizes round-trip overhead
across large scans and is the primary retrieval path for both sequence-range
and subject-filtered queries.

JetStream's Direct Get API (`js_DirectGetMsg`) is used narrowly for resolving
a single message by sequence number without the overhead of standing up a
consumer — specifically as the building block for the binary search
described below, not as the general scan mechanism.

### Binary Search for Timestamp Resolution

When queries specify timestamp ranges using `start_time` or `end_time` parameters, the extension must resolve these timestamps to sequence numbers. The implementation uses binary search over the stream's sequence range to find the first message at or after the target timestamp.

The binary search algorithm uses Direct Get to fetch the single message at the midpoint of the current search range, compares its timestamp to the target, and narrows the range accordingly. This provides O(log n) performance for timestamp resolution, where n is the number of messages in the stream. For a stream with one million messages, timestamp resolution requires approximately 20 message fetches.

After resolving timestamps to sequences, the extension switches to the batched pull-consumer path described above to retrieve messages in the resolved sequence range. Subject filtering, when specified, is applied during message iteration rather than during timestamp resolution.

### Resource Management

The extension manages NATS connections and JetStream contexts using RAII patterns. Connections are established during the table function's initialization phase and cleaned up automatically when the query completes. Connection timeouts are set to 5 seconds to prevent indefinite blocking on unreachable servers. On transient disconnects mid-scan, `nats_scan`'s batch fetch retries automatically rather than failing the query outright.

The extension uses the NATS C client library (cnats) for all NATS protocol operations and yyjson for JSON parsing. Both libraries are production-tested and provide the necessary performance for analytical workloads.

### Execution Model

`nats_scan` executes as a single-threaded table function: each query establishes one connection to the NATS server and pulls message batches sequentially. Messages are returned to DuckDB in batches of up to `STANDARD_VECTOR_SIZE` rows (2048 by default), allowing DuckDB to process results incrementally. Ingest and live-subscribe jobs (`nats_start_ingest`, `nats_start_subscribe`, `nats_start_kv_watch`) run as separate background worker threads independent of query execution — see their sections below.

### Performance Behavior

The implementation uses a small set of fast paths to keep common scans predictable:

- `nats_scan` fetches messages in batches instead of issuing one request per row.
- Bounded scans without client-side subject filtering use JetStream deleted sequence
  metadata to stop after the available messages in the requested range have been emitted.
- Full-stream scans with `nats_subject` use JetStream subject counts to stop at the
  end of the matching subject set without waiting on missing messages.
- `nats_stream_stats` and `nats_stream_range_stats` are metadata-only helpers for
  resume bounds and deleted/gapped range checks.
- `subject_contains` remains a client-side filter, so it does not benefit from the
  same completion shortcuts as server-side subject pushdown.

Ingest jobs persist checkpoints in `duckdb_nats_ingest_checkpoints`, keyed by
`(stream_name, durable_name)`. A restarted job with the same durable resumes from
the last committed sequence rather than replaying from the original start.
By default `nats_start_ingest` expects the destination table to already exist;
pass `create_target_table := true` to have the extension create the target
table before ingest begins, using the standard ingest column layout.

### Ingest Validation

Use `scripts/run-ingest-harness.sh` for deterministic correctness checks. It
supports `HARNESS_MODE=resume` and `HARNESS_MODE=redelivery`.

Use `scripts/benchmark-ingest.sh` for timing and throughput measurements. It
prints CSV and runs both modes by default so you can diff checkpoint/resume
performance over time.

Use `scripts/run-ingest-pause-resume-harness.sh` to validate the control plane:
it exercises `nats_pause_ingest`, `nats_resume_ingest`, and the richer status
fields such as `paused`, `pause_requested`, `fetches_completed`, and
`last_batch_rows`.

For a SQL-native write path, the extension also exposes `nats_js` `COPY FROM`
and `COPY TO` formats. `COPY FROM` reuses the same source schema as
`nats_scan`, and is intended for `COPY target FROM 'stream_name' (FORMAT
nats_js, url '...')` when the target table matches the scan output layout.
`COPY TO` publishes rows into JetStream using a source query that provides
`subject` and `payload` columns, or a constant `subject` option plus a
`payload` column. The regression tests for those paths are
[test/sql/test_copy_from.sql](/home/mrayva/duckdb-nats-jetstream/test/sql/test_copy_from.sql),
[test/sql/test_copy_from_errors.sql](/home/mrayva/duckdb-nats-jetstream/test/sql/test_copy_from_errors.sql),
[test/sql/test_copy_to.sql](/home/mrayva/duckdb-nats-jetstream/test/sql/test_copy_to.sql),
and [test/sql/test_copy_to_errors.sql](/home/mrayva/duckdb-nats-jetstream/test/sql/test_copy_to_errors.sql);
the round-trip regression is
[test/sql/test_copy_roundtrip.sql](/home/mrayva/duckdb-nats-jetstream/test/sql/test_copy_roundtrip.sql);
the end-to-end smoke tests are [scripts/run-copy-harness.sh](/home/mrayva/duckdb-nats-jetstream/scripts/run-copy-harness.sh)
and [scripts/run-copy-to-harness.sh](/home/mrayva/duckdb-nats-jetstream/scripts/run-copy-to-harness.sh);
the export benchmark is [scripts/benchmark-copy-to.sh](/home/mrayva/duckdb-nats-jetstream/scripts/benchmark-copy-to.sh).

Both scripts rely on the seeded `ingest_resume` and `ingest_redelivery` streams
created by `scripts/setup-streams.sh`, so keep the same DuckDB binary and local
NATS instance when comparing runs.

## API Reference

### `nats_scan`

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `stream_name` | VARCHAR | Yes | - | Name of the JetStream stream to query |
| `url` | VARCHAR | No | `nats://localhost:4222` | NATS server URL |
| `nats_subject` | VARCHAR | No | - | Server-side NATS subject filter |
| `subject_contains` | VARCHAR | No | - | Client-side subject substring filter |
| `subject` | VARCHAR | No | - | Legacy alias for `subject_contains` |
| `start_seq` | UBIGINT | No | 1 | Starting sequence number (inclusive) |
| `end_seq` | UBIGINT | No | Last message | Ending sequence number (inclusive) |
| `start_time` | TIMESTAMP | No | - | Starting timestamp (inclusive) |
| `end_time` | TIMESTAMP | No | - | Ending timestamp (inclusive) |
| `batch_size` | UBIGINT | No | - | Pull-consumer fetch batch size |
| `fetch_timeout_ms` | BIGINT | No | 1000 | Max wait per batch fetch, in milliseconds |
| `json_extract` | LIST(VARCHAR) | No | - | JSON field paths to extract (dot notation for nested fields) |
| `msgpack_extract` | LIST(VARCHAR) | No | - | MessagePack map field paths to extract |
| `cbor_extract` | LIST(VARCHAR) | No | - | CBOR map field paths to extract |
| `flexbuffers_extract` | LIST(VARCHAR) | No | - | FlexBuffers map field paths to extract |
| `proto_file` | VARCHAR | No | - | Path to .proto schema file |
| `proto_message` | VARCHAR | No | - | Protobuf message type name |
| `proto_extract` | LIST(VARCHAR) | No | - | Protobuf field paths to extract (dot notation for nested fields) |
| `credentials_file` | VARCHAR | No | - | Path to a NATS `.creds` file (JWT + NKey seed) |
| `tls_ca_file` | VARCHAR | No | - | Path to a TLS CA certificate |
| `tls_cert_file` | VARCHAR | No | - | Path to a TLS client certificate (requires `tls_key_file`) |
| `tls_key_file` | VARCHAR | No | - | Path to a TLS client private key (requires `tls_cert_file`) |
| `tls_server_name` | VARCHAR | No | - | Expected TLS server hostname |
| `tls_skip_verify` | BOOLEAN | No | false | Disable server certificate verification (local testing only) |

### Parameter Constraints

Sequence-based parameters (`start_seq`, `end_seq`) cannot be combined with timestamp-based parameters (`start_time`, `end_time`) in the same query. The extension will return an error if both parameter types are specified.

The extraction parameters (`json_extract`, `msgpack_extract`, `cbor_extract`, `flexbuffers_extract`, `proto_extract`) are mutually exclusive — use exactly one, matching your payload's encoding.

When using `proto_extract`, both `proto_file` and `proto_message` parameters are required. The `proto_file` parameter specifies the path to the .proto schema file, and `proto_message` specifies the message type name within that file.

Extracted fields are appended as additional columns after the five base columns (`stream`, `subject`, `seq`, `ts_nats`, `payload`). Column names for nested protobuf fields use underscores instead of dots (e.g., `location.zone` becomes `location_zone`).

### JetStream Ingest (`nats_start_ingest` and friends)

`nats_start_ingest` launches a background job that batch-inserts JetStream
messages into a DuckDB table via a durable pull consumer, with checkpointing
and exactly-once delivery across restarts. It accepts the same connection,
subject-filter, and payload-extraction parameters as `nats_scan` (`url`,
`credentials_file`, `tls_*`, `nats_subject`, `subject_contains`,
`json_extract`/`msgpack_extract`/`cbor_extract`/`flexbuffers_extract`/
`proto_extract` + `proto_file`/`proto_message`), plus:

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `job_name` | VARCHAR | Yes | - | Unique name identifying this ingest job |
| `stream_name` | VARCHAR | Yes | - | JetStream stream to consume |
| `target_table` | VARCHAR | Yes | - | Destination DuckDB table |
| `durable_name` | VARCHAR | No | - | Durable consumer name (enables checkpointed resume) |
| `create_target_table` | BOOLEAN | No | false | Create `target_table` if it doesn't exist |
| `start_seq` | UBIGINT | No | 1 | Starting sequence for a first run (ignored on resume) |
| `batch_size` | UBIGINT | No | - | Pull-consumer fetch batch size |
| `fetch_timeout_ms` | BIGINT | No | - | Max wait per batch fetch, in milliseconds |
| `poll_ms` | BIGINT | No | - | Worker poll interval between fetch attempts |

Control and status functions: `nats_stop_ingest(job_name := ...)`,
`nats_pause_ingest(...)`, `nats_resume_ingest(...)`, `nats_remove_ingest(...)`
(stops and forgets the job, freeing the name for reuse), `nats_ingest_status(...)`,
and `nats_ingest_jobs()` (lists all jobs). Ingest jobs also claim a
fencing-token-backed ownership lease keyed by `(stream_name, durable_name)` so
a second process can't write concurrently against the same durable consumer.

### Core NATS Live Subscribe (`nats_start_subscribe` and friends)

`nats_start_subscribe` batches messages from a core NATS subject (no
JetStream replay, no durable resume) into a target table. It accepts the same
connection and payload-extraction parameters as `nats_scan`, plus:

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `job_name` | VARCHAR | Yes | - | Unique name identifying this subscribe job |
| `target_table` | VARCHAR | Yes | - | Destination DuckDB table |
| `subject` | VARCHAR | Yes | - | Core NATS subject to subscribe to |
| `queue_group` | VARCHAR | No | - | NATS queue group for load-balanced delivery |
| `create_target_table` | BOOLEAN | No | false | Create `target_table` if it doesn't exist |
| `batch_size` | UBIGINT | No | - | Rows batched per insert |
| `poll_ms` | BIGINT | No | - | Worker poll interval |
| `subject_column` / `payload_column` | VARCHAR | No | - | Override default column names for subject/payload |
| `pending_message_limit` | BIGINT | No | 65536 | Client-side pending queue limit (messages); `-1` for unbounded |
| `pending_bytes_limit` | BIGINT | No | 67108864 | Client-side pending queue limit (bytes); `-1` for unbounded |

Control and status functions: `nats_stop_subscribe(job_name := ...)`,
`nats_pause_subscribe(...)`, `nats_resume_subscribe(...)`,
`nats_remove_subscribe(...)`, `nats_subscribe_status(...)` (reports
`connected`, `reconnecting`, `messages_delivered`, `messages_dropped`, and
pending-queue depth), and `nats_subscribe_jobs()`. Core NATS delivery is
lossy under sustained overload — use JetStream ingest when no-loss delivery
is required.

### JetStream KV Store

Point operations, each accepting the standard connection parameters
(`url`, `credentials_file`, `tls_*`):

| Function | Positional Args | Notes |
|----------|------------------|-------|
| `nats_kv_get(bucket, key)` | bucket VARCHAR, key VARCHAR | Returns `bucket, key, value, revision, created, operation` |
| `nats_kv_put(bucket, key, value)` | + value VARCHAR | Unconditional write; returns `bucket, key, revision` |
| `nats_kv_create(bucket, key, value)` | + value VARCHAR | Fails if the key already exists |
| `nats_kv_update(bucket, key, value, revision)` | + expected revision BIGINT | Optimistic-concurrency write; fails if the key moved past `revision` |
| `nats_kv_delete(bucket, key)` | + `purge := true/false` named param | Returns `bucket, key, deleted`; `purge` removes history instead of tombstoning |
| `nats_kv_status(bucket)` | bucket VARCHAR | Returns `bucket, values, history, ttl_seconds, replicas, bytes` |
| `nats_kv_create_bucket(bucket)` | + `history`, `ttl_seconds`, `max_bytes`, `replicas` named params | Returns `bucket, created` |
| `nats_kv_delete_bucket(bucket)` | bucket VARCHAR | Returns `bucket, deleted` |

Bulk access:

| Function | Args | Notes |
|----------|------|-------|
| `nats_kv_scan(bucket, key_filter := ..., since_revision := ...)` | bucket VARCHAR | Returns all matching keys; `since_revision` enables incremental re-scans |
| `nats_kv_history(bucket, key)` | bucket VARCHAR, key VARCHAR | Full revision history for one key |

`COPY <table> TO '<bucket>' (FORMAT nats_kv, key_column '...', value_column '...')`
publishes rows as KV entries, accepting the same connection options plus
`key_column`/`value_column` to select the source columns.

### Live KV Watch (`nats_start_kv_watch` and friends)

`nats_start_kv_watch` batches KV bucket change notifications into a target
table, the same job-lifecycle pattern as `nats_start_subscribe`:

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `job_name` | VARCHAR | Yes | - | Unique name identifying this watch job |
| `target_table` | VARCHAR | Yes | - | Destination DuckDB table |
| `bucket` | VARCHAR | Yes | - | KV bucket to watch |
| `key_filter` | VARCHAR | No | - | Restrict the watch to matching keys |
| `batch_size` | UBIGINT | No | - | Rows batched per insert |
| `poll_ms` | BIGINT | No | - | Worker poll interval |
| `updates_only` | BOOLEAN | No | false | Skip the initial full-bucket snapshot, deliver only subsequent changes |
| `ignore_deletes` | BOOLEAN | No | false | Don't emit rows for delete/purge operations |
| `create_target_table` | BOOLEAN | No | false | Create `target_table` if it doesn't exist |
| `key_column` / `value_column` | VARCHAR | No | - | Override default output column names |

Control and status functions: `nats_stop_kv_watch(job_name := ...)`,
`nats_pause_kv_watch(...)`, `nats_resume_kv_watch(...)`,
`nats_remove_kv_watch(...)`, `nats_kv_watch_status(...)`, and
`nats_kv_watch_jobs()`.

## Roadmap

### Current Capabilities

- Bounded historical queries via batched JetStream pull consumers, plus Direct-Get-backed binary search for timestamp resolution
- Sequence-based range queries (`start_seq`, `end_seq`) and timestamp-based range queries (`start_time`, `end_time`)
- Server-side subject filtering (`nats_subject`, including wildcards) and client-side substring filtering (`subject_contains`)
- Stream metadata helpers (`nats_stream_stats`/`nats_stream_info`, `nats_stream_range_stats`) for resume bounds and gap checks without a full scan
- Payload extraction for JSON, MessagePack, CBOR, FlexBuffers, and Protocol Buffers (runtime .proto schema parsing, primitive types, nested message navigation with dot notation, automatic DuckDB type mapping), with process-wide caching of parsed protobuf schemas
- `COPY FROM`/`COPY TO ... (FORMAT nats_js)` for reading into and publishing from tables, including structured (JSON/MessagePack/CBOR/FlexBuffers/protobuf) `COPY TO` payload serialization
- Durable-consumer JetStream ingest (`nats_start_ingest`) with checkpointed resume, exactly-once delivery across redelivery/restart, pause/resume control, and a fencing-token-backed ownership lease for cross-process safety
- Core NATS live subscriptions (`nats_start_subscribe`) with bounded backpressure, automatic reconnect handling, and pause/resume control (live-only — no JetStream replay or durable resume)
- JetStream KV store: point CRUD (`nats_kv_get`/`put`/`create`/`update`/`delete`), optimistic-concurrency updates, bulk scan/history, bucket lifecycle management, and `COPY TO ... (FORMAT nats_kv)`
- Live KV watch jobs (`nats_start_kv_watch`) batching bucket change notifications into a target table
- File-based authentication and TLS (`credentials_file`, `tls_ca_file`, `tls_cert_file`, `tls_key_file`, `tls_server_name`, `tls_skip_verify`) across all connection-taking functions
- Multi-platform builds: Linux, macOS, Windows, WebAssembly

### Planned Features

#### Advanced Protocol Buffers
- **Repeated fields** - Array support with proper DuckDB LIST type mapping (currently serialized as JSON arrays)
- **Map fields** - Key-value map support with DuckDB MAP type
- **Oneof fields** - Union type handling
- **Any types** - Dynamic type resolution
- **Import resolution** - Support for .proto files with imports
- **Well-known types** - Native support for Timestamp, Duration, Struct, etc.

#### Additional Data Formats
- **Apache Avro** - Schema registry integration and binary encoding support

#### Performance Enhancements
- **Parallel scanning** - Multi-threaded message retrieval for multi-subject streams
- **Vectorized decoding** - SIMD optimizations for JSON/protobuf parsing
- **Connection pooling** - Reduce connection overhead for repeated queries

#### Configuration & Usability
- **Connection profiles** - Named, reusable connection configurations
- **Stream discovery** - Automatic stream and subject enumeration (current stats functions require a known stream name)

See [CHANGELOG.md](../CHANGELOG.md) for what shipped in each release.

## Development

Build the extension:

```bash
make build
```

Run tests:

```bash
make test
```

Clean build artifacts:

```bash
make clean
```

See [CHANGELOG.md](../CHANGELOG.md) for release history and the Roadmap
section above for planned work. The `scripts/` directory has deterministic
regression harnesses and benchmarks for ingest, subscribe, KV, copy, and
reconnect behavior — see the harness/benchmark references throughout this
guide and [README.md](../README.md) for which one covers a given code path.

## License

This project is licensed under the MIT License. See the LICENSE file for details.
