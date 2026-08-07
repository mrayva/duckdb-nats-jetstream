# DuckDB NATS JetStream Extension

[![CI](https://github.com/brannn/duckdb-nats-jetstream/actions/workflows/MainDistributionPipeline.yml/badge.svg)](https://github.com/brannn/duckdb-nats-jetstream/actions/workflows/MainDistributionPipeline.yml)
[![Version](https://img.shields.io/badge/Version-v0.2.2-orange)](https://github.com/brannn/duckdb-nats-jetstream/releases/tag/v0.2.2)
[![DuckDB Version](https://img.shields.io/badge/DuckDB-v1.5.5-blue)](https://github.com/duckdb/duckdb/releases/tag/v1.5.5)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Platform](https://img.shields.io/badge/Platform-Linux%20%7C%20macOS%20%7C%20Windows%20%7C%20WebAssembly-lightgrey)](https://github.com/brannn/duckdb-nats-jetstream/actions)

Query NATS JetStream message streams directly with SQL. Timestamp-based range queries, subject filtering, and extraction support for JSON, MessagePack, CBOR, FlexBuffers, and Protocol Buffers payloads.

---

## JSON Payload Extraction

<img src="images/demo-json.png" alt="JSON Demo" width="800"/>

## Protocol Buffers with Native Types

<img src="images/demo-protobuf.png" alt="Protobuf Demo" width="800"/>

## MessagePack Payload Extraction

MessagePack object fields can be projected directly without converting the payload
through JSON. Use dot notation for nested map fields:

```sql
SELECT device_id, "metrics.kw"
FROM nats_scan(
    'telemetry',
    nats_subject := 'telemetry.msgpack',
    msgpack_extract := ['device_id', 'metrics.kw']
);
```

CBOR and schema-less FlexBuffers payloads support the same projected field extraction API:

```sql
SELECT device_id, "metrics.kw"
FROM nats_scan(
    'telemetry',
    cbor_extract := ['device_id', 'metrics.kw']
);
```

Use `flexbuffers_extract := [...]` for FlexBuffers payloads. FlexBuffers builds require the
FlatBuffers runtime dependency; the local subscription regression harness also requires `flatc`.

## Structured COPY TO

`COPY TO ... (FORMAT nats_js)` publishes raw `payload` values by default. To serialize selected
source columns as one structured object per message, set `payload_format` to `json`, `msgpack`,
`cbor`, or `flexbuffers` and provide `payload_columns`. Protobuf output is schema-driven:

```sql
COPY readings
TO 'telemetry'
(FORMAT nats_js,
 url 'nats://localhost:4222',
 payload_format 'json',
 payload_columns ['device_id', 'kw', 'online']);
```

The subject still comes from `subject` unless a constant `subject` option is supplied. The
structured COPY TO regression is covered by `scripts/run-copy-structured-harness.sh`. Protobuf
output uses `payload_format 'protobuf'`, `proto_file`, `proto_message`, and optional
`proto_fields` to map source columns to scalar or nested singular fields.

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
- **MessagePack extraction** - Extract MessagePack map fields as VARCHAR columns
- **CBOR extraction** - Extract CBOR map fields as VARCHAR columns
- **FlexBuffers extraction** - Extract schema-less FlexBuffers map fields as VARCHAR columns
- **Protocol Buffers** - Native type support (VARCHAR, DOUBLE, BOOLEAN, INTEGER, etc.)
- **Nested fields** - Access nested protobuf fields with dot notation
- **Sequence ranges** - Query by message sequence numbers
- **JetStream KV store** - Point CRUD, bulk scan/history, and live watch jobs for JetStream KV buckets
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
DUCKDB_BIN="$HOME/duckdb" \
NATS_CLI="$HOME/nats" \
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
The harnesses and benchmarks auto-resolve a clean host `libduckdb.so` when one
is available. Set `DUCKDB_HOST_LIB` if you want to override that selection.

Use `scripts/benchmark-subscribe.sh` to measure live core NATS subscription
throughput and batch latency against the seeded `live.subscribe` subject.

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
For throughput checks, use `scripts/benchmark-copy-to.sh`.

Ingest jobs also persist a checkpoint keyed by `stream_name` and `durable_name`,
so restarting with the same durable resumes from the last committed sequence.
If you want DuckDB to create the destination table for you, pass
`create_target_table := true` to `nats_start_ingest(...)`.
New databases store checkpoints in the append-only
`duckdb_nats_ingest_checkpoint_log` table, avoiding a keyed SQL upsert on every
commit batch. The existing `duckdb_nats_ingest_checkpoints` name remains
available as a latest-row view; databases created with the older table schema
continue to use that table for backward compatibility.
The log is compacted transactionally every 1,000 committed batches by default,
retaining only the latest row for each stream/durable pair. Set
`NATS_INGEST_CHECKPOINT_COMPACTION_INTERVAL=0` to disable compaction or use a
smaller positive value for tighter storage bounds.

Core NATS live subscriptions are available through `nats_start_subscribe(...)`,
`nats_pause_subscribe(...)`, `nats_resume_subscribe(...)`, and
`nats_stop_subscribe(...)`. They are live-only, batch-insert jobs without
JetStream replay or durable resume.
The client-side pending queue is bounded by default to 65,536 messages and
64 MiB. Override these with `pending_message_limit` and `pending_bytes_limit`.
`nats_subscribe_status(...)` reports current and peak pending values plus
`messages_delivered` and `messages_dropped`. Core NATS is lossy under overload,
including while a job is paused; use JetStream ingest when replay and no-loss
delivery are required.
Use `scripts/run-subscribe-pause-resume-harness.sh` to exercise the control
path deterministically against local NATS.
Use `scripts/run-subscribe-backpressure-harness.sh` to verify bounded pending
queues and dropped-message reporting under overload.
NATS connections use automatic reconnect with a bounded retry window. JetStream
ingest retries transient pull failures and resumes from its durable consumer and
DuckDB checkpoint; core subscriptions explicitly reattach after reconnect.
Status functions expose `connected`, `reconnecting`, `reconnect_count`, and
`last_reconnect_time`. Run `scripts/run-reconnect-harness.sh` for an isolated
server restart regression test.
Use `scripts/run-ingest-exactly-once-harness.sh` to force a post-commit
redelivery across a server restart and verify that the target row count stays
exactly once; `duplicates_skipped` reports messages discarded by checkpoint or
same-batch sequence deduplication.
Ingest jobs also claim a lease keyed by stream and durable name. A fencing token
is refreshed and checked in the same transaction as each target commit, so a
second DuckDB process cannot write concurrently with an active owner. Run
`scripts/run-ingest-ownership-harness.sh` to exercise cross-process contention.

## JetStream KV Store

Point CRUD and bulk access to JetStream KV buckets are available alongside the
stream-scanning APIs:

```sql
-- Point operations
SELECT * FROM nats_kv_put('config', 'feature.enabled', 'true');
SELECT * FROM nats_kv_get('config', 'feature.enabled');
SELECT * FROM nats_kv_delete('config', 'feature.enabled');

-- Bulk scan and history
SELECT bucket, key, value, revision, created, operation
FROM nats_kv_scan('config', key_filter := 'feature.*');

SELECT * FROM nats_kv_history('config', 'feature.enabled');
```

`nats_kv_create` and `nats_kv_update` provide optimistic-concurrency writes
(`nats_kv_update` takes an expected `revision` and fails if the key has moved
on). `nats_kv_scan` supports incremental reads via `since_revision`.
`nats_kv_create_bucket` / `nats_kv_delete_bucket` manage bucket lifecycle, and
`nats_kv_status` reports bucket metadata (`values`, `history`, `ttl_seconds`,
`replicas`, `bytes`). A `COPY TO ... (FORMAT nats_kv)` writer publishes rows
into a bucket using `key_column`/`value_column` options.

For live change notification instead of point-in-time reads, use the KV watch
job API: `nats_start_kv_watch(...)`, `nats_kv_watch_status(...)`,
`nats_pause_kv_watch(...)`, `nats_resume_kv_watch(...)`, and
`nats_stop_kv_watch(...)`. It batches KV updates into a target table the same
way `nats_start_subscribe` batches core NATS messages, with `updates_only` and
`ignore_deletes` options to control which KV operations are captured. See
[docs/GUIDE.md](docs/GUIDE.md) for the full parameter reference and
[docs/EXAMPLES.md](docs/EXAMPLES.md) for worked examples.

## Authentication And TLS

All scan, stream stats, COPY, ingest, and live-subscribe connection APIs accept
the same file-based security options:

```sql
SELECT *
FROM nats_scan(
    'telemetry',
    url := 'nats://nats.example.com:4222',
    credentials_file := '/run/secrets/nats.creds',
    tls_ca_file := '/run/secrets/ca.pem',
    tls_cert_file := '/run/secrets/client.pem',
    tls_key_file := '/run/secrets/client-key.pem',
    tls_server_name := 'nats.example.com'
);
```

`credentials_file` uses the standard NATS credentials format containing a user
JWT and NKey seed. The extension stores only file paths when persisting ingest
jobs, not credential or private-key contents. `tls_cert_file` and
`tls_key_file` must be supplied together. `tls_skip_verify := true` is available
for local testing only and disables server certificate verification.

For timing and throughput checks, use:

```bash
./scripts/benchmark-ingest.sh
```

To compare the current JSON extraction path against direct `DataChunk` vector writes:

```bash
./scripts/benchmark-json-buffer.sh
```

Set `NATS_INGEST_PROFILE=1` to emit cumulative worker phase timings. Registry progress is persisted every eight
committed batches by default; set `NATS_INGEST_REGISTRY_INTERVAL=1` when per-batch registry durability is required.
For transactional recovery testing, use `scripts/run-ingest-inflight-recovery-harness.sh`; it crashes after
two rows are appended but before commit, then verifies replay and exactly-once final insertion.
Ingest uses a bounded one-batch JetStream prefetch queue to overlap transport fetches with DuckDB processing.
The transport window defaults to `4 * batch_size`; set `NATS_INGEST_TRANSPORT_BATCH_MULTIPLIER=1` for the
one-batch baseline.

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
