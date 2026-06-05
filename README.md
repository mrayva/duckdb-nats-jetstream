# DuckDB NATS JetStream Extension

[![CI](https://github.com/brannn/duckdb-nats-jetstream/actions/workflows/MainDistributionPipeline.yml/badge.svg)](https://github.com/brannn/duckdb-nats-jetstream/actions/workflows/MainDistributionPipeline.yml)
[![Version](https://img.shields.io/badge/Version-v0.2.1-orange)](https://github.com/brannn/duckdb-nats-jetstream/releases/tag/v0.2.1)
[![DuckDB Version](https://img.shields.io/badge/DuckDB-v1.5.3-blue)](https://github.com/duckdb/duckdb/releases/tag/v1.5.3)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Platform](https://img.shields.io/badge/Platform-Linux%20%7C%20macOS%20%7C%20Windows%20%7C%20WebAssembly-lightgrey)](https://github.com/brannn/duckdb-nats-jetstream/actions)

Query NATS JetStream message streams directly with SQL. Timestamp-based range queries, subject filtering, and native type support for JSON and Protocol Buffers payloads.

---

## JSON Payload Extraction

<img src="images/demo-json.png" alt="JSON Demo" width="800"/>

## Protocol Buffers with Native Types

<img src="images/demo-protobuf.png" alt="Protobuf Demo" width="800"/>

---

## Quick Start

```sql
-- Install from DuckDB Community Extensions
INSTALL nats_js FROM community;
LOAD nats_js;

-- Query messages with timestamp range and subject filtering
SELECT seq, ts_nats, subject, device_id, kw
FROM nats_scan(
    'telemetry',
    url := 'nats://nats.messaging.svc.cluster.local:4222',
    start_time := '2025-11-01 09:00:00'::TIMESTAMP,
    end_time := '2025-11-01 09:05:00'::TIMESTAMP,
    nats_subject := 'telemetry.dc1.power.>',
    json_extract := ['device_id', 'kw']
)
ORDER BY seq;
```

---

## Key Features

- **Timestamp-based queries** - Binary search through message streams by time range
- **Subject filtering** - Filter messages by NATS subject patterns
- **Batched JetStream pulls** - Fetch messages in batches for large scans instead of one request per row
- **JSON extraction** - Extract JSON fields as columns
- **Protocol Buffers** - Native type support (VARCHAR, DOUBLE, BOOLEAN, INTEGER, etc.)
- **Nested fields** - Access nested protobuf fields with dot notation
- **Sequence ranges** - Query by message sequence numbers
- **Multi-platform** - Linux, macOS, Windows, WebAssembly

---

## Large Stream Scans

Use `nats_subject` for server-side JetStream subject filtering. Use
`subject_contains` for client-side substring filtering. The older `subject`
parameter remains as a substring-compatible alias.

```sql
SELECT seq, subject, device_id, kw
FROM nats_scan(
    'telemetry',
    url := 'nats://localhost:4222',
    nats_subject := 'telemetry.dc1.power.>',
    json_extract := ['device_id', 'kw']
);
```

For whole-stream counts and resume bounds, use `nats_stream_stats` instead of
scanning messages:

```sql
SELECT messages, first_seq, last_seq, last_seq + 1 AS next_start_seq
FROM nats_stream_stats('telemetry', url := 'nats://localhost:4222');
```

`nats_stream_info` is an alias for the same table function.

Use `nats_stream_range_stats` to check whether a sequence range is fully
available before scanning it:

```sql
SELECT available_messages, deleted_in_range, has_gaps
FROM nats_stream_range_stats(
    'telemetry',
    url := 'nats://localhost:4222',
    start_seq := 100000,
    end_seq := 200000
);
```

`nats_scan` uses batched pull consumers by default. For large streams, keep `batch_size`
large enough to avoid per-message fetch overhead:

```sql
SELECT count(*)
FROM nats_scan(
    'telemetry',
    url := 'nats://localhost:4222',
    batch_size := 4096
);
```

For incremental reads, persist the highest processed `seq` and resume from the next
sequence. If `end_seq` is omitted, the scan reads through the stream's latest sequence
observed when the query starts:

```sql
SELECT *
FROM nats_scan(
    'telemetry',
    url := 'nats://localhost:4222',
    start_seq := 1234568,
    batch_size := 4096
);
```

`fetch_timeout_ms` controls how long a batch fetch may wait. The default is `1000`.

## Performance Behavior

The scan path is tuned for the common analytical cases:

- `nats_scan` uses batched pull requests, so large reads avoid one fetch per message.
- Bounded scans without client-side subject filtering use JetStream deleted sequence
  metadata to stop once all available messages in the requested range have been emitted.
- Full-stream scans with `nats_subject` use JetStream subject counts to avoid waiting
  for missing messages at the end of the stream.
- `nats_stream_stats` and `nats_stream_range_stats` provide cheap metadata-only checks
  for resume bounds and deleted or gapped ranges before scanning.
- Scans with `subject_contains` still use client-side filtering, so they remain
  fetch-driven rather than count-driven.

Run the local benchmark script to capture basic regression timings for stats,
range stats, full scans, subject pushdown, JSON extraction, and protobuf
extraction:

```bash
DUCKDB_BIN=/home/mrayva/.duckdb/cli/1.5.3/duckdb \
NATS_CLI=/home/mrayva/nats \
./scripts/benchmark-local.sh
```

Set `PREPARE_FIXTURES=1` to reset local JetStream streams and regenerate the
standard test fixtures before timing.

For a deterministic ingest smoke test, use the dedicated harness:

```bash
./scripts/run-ingest-harness.sh
```

It resets the local streams, starts a bounded ingest job, verifies the inserted
row count, and stops the job cleanly. The harness also supports
`HARNESS_MODE=redelivery` for a second deterministic checkpoint/resume pass.
Use `scripts/run-ingest-pause-resume-harness.sh` to exercise the pause/resume
control path and the richer ingest status fields.

JetStream also registers a `nats_js` `COPY FROM` format that reuses the same
source path. Use it when you want to load a table with the standard scan
schema via `COPY target FROM 'ingest_resume' (FORMAT nats_js, url '...')`.
Regression coverage for the format lives in `test/sql/test_copy_from.sql`,
`test/sql/test_copy_from_errors.sql`, `test/sql/test_copy_roundtrip.sql`, and
`scripts/run-copy-harness.sh`.

JetStream also registers a `nats_js` `COPY TO` format for publishing rows back
into a stream. Use it when your source query provides `subject` and `payload`
columns, or a constant `subject` option plus a `payload` column. Regression
coverage for the export path lives in `test/sql/test_copy_to.sql`,
`test/sql/test_copy_to_errors.sql`, `test/sql/test_copy_roundtrip.sql`, and
`scripts/run-copy-to-harness.sh`.

Ingest jobs also persist a checkpoint keyed by `stream_name` and `durable_name`,
so restarting with the same durable resumes from the last committed sequence.
If you want DuckDB to create the destination table for you, pass
`create_target_table := true` to `nats_start_ingest(...)`.

For timing and throughput checks, use:

```bash
./scripts/benchmark-ingest.sh
```

The benchmark prints CSV and runs both ingest modes by default. Use
`HARNESS_MODE=resume` or `HARNESS_MODE=redelivery` to run one mode at a time.

---

## Documentation

- **[Examples](docs/EXAMPLES.md)** - Practical examples for common use cases
- **[Full Guide](docs/GUIDE.md)** - Complete documentation including installation, query patterns, and Protocol Buffers integration
- Performance considerations
- API reference

---

## License

MIT License - see [LICENSE](LICENSE) file for details.
