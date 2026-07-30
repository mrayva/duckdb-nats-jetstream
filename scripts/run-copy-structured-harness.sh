#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT_DIR/scripts/duckdb_host_lib.sh"

TIP_ROOT="${TIP_ROOT:-/tmp/nats-js-tip-build5}"
DUCKDB_BIN="${DUCKDB_BIN:-$TIP_ROOT/duckdb}"
DUCKDB_LIB="${DUCKDB_LIB:-$(duckdb_host_lib_resolve)}"
EXTENSION_PATH="${EXTENSION_PATH:-$TIP_ROOT/extension/nats_js/nats_js.duckdb_extension}"
NATS_URL="${NATS_URL:-nats://127.0.0.1:4222}"
NATS_CLI="${NATS_CLI:-$HOME/nats}"

if [ ! -x "$DUCKDB_BIN" ]; then
  echo "DuckDB binary not found: $DUCKDB_BIN" >&2
  exit 1
fi
if [ ! -f "$EXTENSION_PATH" ]; then
  echo "Extension not found: $EXTENSION_PATH" >&2
  exit 1
fi
if [ ! -x "$NATS_CLI" ]; then
  NATS_CLI="$(command -v nats || true)"
fi
if [ -z "$NATS_CLI" ]; then
  echo "NATS CLI not found. Set NATS_CLI=/path/to/nats." >&2
  exit 1
fi

"$NATS_CLI" server check connection --server "$NATS_URL"
NATS_URL="$NATS_URL" NATS_CLI="$NATS_CLI" RESET_STREAMS=1 "$ROOT_DIR/scripts/setup-streams.sh" >/dev/null

db_file="$(mktemp /tmp/nats_copy_structured.XXXXXX.duckdb)"
log_file="$(mktemp /tmp/nats_copy_structured.XXXXXX.log)"
rm -f "$db_file"
trap 'rc=$?; rm -f "$log_file" "$db_file"; exit $rc' EXIT

if ! DUCKDB_LIB="$DUCKDB_LIB" python3 "$ROOT_DIR/scripts/duckdb_session.py" \
    --duckdb-bin "$DUCKDB_BIN" --db-file "$db_file" <<SQL >"$log_file" 2>&1
SEND
LOAD '${EXTENSION_PATH}';
CREATE TABLE structured_source(
    subject VARCHAR,
    device_id VARCHAR,
    kw DOUBLE,
    online BOOLEAN,
    reading_count INTEGER,
    tags VARCHAR[]
);
INSERT INTO structured_source VALUES
    ('copy_roundtrip.json', 'json-device', 42.5, true, 7, NULL),
    ('copy_roundtrip.msgpack', 'msgpack-device', 43.5, false, 8, NULL),
    ('copy_roundtrip.cbor', 'cbor-device', 44.5, true, 9, NULL),
    ('copy_roundtrip.flex', 'flex-device', 45.5, false, 10, NULL),
    ('copy_roundtrip.proto', 'proto-device', 46.5, true, 11, ['p1', 'p2']);
COPY (SELECT * FROM structured_source WHERE subject = 'copy_roundtrip.json')
TO 'copy_roundtrip'
(FORMAT nats_js, url '${NATS_URL}', payload_format 'json', payload_columns ['device_id', 'kw', 'online', 'reading_count']);
COPY (SELECT * FROM structured_source WHERE subject = 'copy_roundtrip.msgpack')
TO 'copy_roundtrip'
(FORMAT nats_js, url '${NATS_URL}', payload_format 'msgpack', payload_columns ['device_id', 'kw', 'online', 'reading_count']);
COPY (SELECT * FROM structured_source WHERE subject = 'copy_roundtrip.cbor')
TO 'copy_roundtrip'
(FORMAT nats_js, url '${NATS_URL}', payload_format 'cbor', payload_columns ['device_id', 'kw', 'online', 'reading_count']);
COPY (SELECT * FROM structured_source WHERE subject = 'copy_roundtrip.flex')
TO 'copy_roundtrip'
(FORMAT nats_js, url '${NATS_URL}', payload_format 'flexbuffers', payload_columns ['device_id', 'kw', 'online', 'reading_count']);
COPY (SELECT * FROM structured_source WHERE subject = 'copy_roundtrip.proto')
TO 'copy_roundtrip'
(FORMAT nats_js,
 url '${NATS_URL}',
 payload_format 'protobuf',
 proto_file '${ROOT_DIR}/test/proto/telemetry.proto',
 proto_message 'Telemetry',
 payload_columns ['device_id', 'kw', 'online', 'tags'],
 proto_fields ['device_id', 'metrics.kw', 'online', 'tags']);
SELECT 'rows=' || COUNT(*) FROM nats_scan('copy_roundtrip', url := '${NATS_URL}');
SELECT 'json=' || device_id || '/' || kw || '/' || online || '/' || reading_count
FROM nats_scan('copy_roundtrip', url := '${NATS_URL}', nats_subject := 'copy_roundtrip.json', json_extract := ['device_id', 'kw', 'online', 'reading_count']);
SELECT 'msgpack=' || device_id || '/' || kw || '/' || online || '/' || reading_count
FROM nats_scan('copy_roundtrip', url := '${NATS_URL}', nats_subject := 'copy_roundtrip.msgpack', msgpack_extract := ['device_id', 'kw', 'online', 'reading_count']);
SELECT 'cbor=' || device_id || '/' || kw || '/' || online || '/' || reading_count
FROM nats_scan('copy_roundtrip', url := '${NATS_URL}', nats_subject := 'copy_roundtrip.cbor', cbor_extract := ['device_id', 'kw', 'online', 'reading_count']);
SELECT 'flex=' || device_id || '/' || kw || '/' || online || '/' || reading_count
FROM nats_scan('copy_roundtrip', url := '${NATS_URL}', nats_subject := 'copy_roundtrip.flex', flexbuffers_extract := ['device_id', 'kw', 'online', 'reading_count']);
SELECT 'proto=' || device_id || '/' || metrics_kw || '/' || online
FROM nats_scan('copy_roundtrip', url := '${NATS_URL}', nats_subject := 'copy_roundtrip.proto',
    proto_file := '${ROOT_DIR}/test/proto/telemetry.proto', proto_message := 'Telemetry',
    proto_extract := ['device_id', 'metrics.kw', 'online']);
SELECT 'proto_tags=' || CASE WHEN tags LIKE '%p1%' AND tags LIKE '%p2%' THEN 'ok' ELSE 'bad' END
FROM nats_scan('copy_roundtrip', url := '${NATS_URL}', nats_subject := 'copy_roundtrip.proto',
    proto_file := '${ROOT_DIR}/test/proto/telemetry.proto', proto_message := 'Telemetry',
    proto_extract := ['tags']);
CREATE TABLE nested_source(
    subject VARCHAR,
    tags VARCHAR[],
    metrics STRUCT(kw DOUBLE, online BOOLEAN),
    attrs MAP(VARCHAR, INTEGER)
);
INSERT INTO nested_source VALUES
    ('copy_roundtrip.nested', ['a', 'b'], struct_pack(kw := 12.5, online := true), MAP {'x': 1, 'y': 2});
COPY nested_source
TO 'copy_roundtrip'
(FORMAT nats_js, url '${NATS_URL}', payload_format 'json', payload_columns ['tags', 'metrics', 'attrs']);
SELECT 'nested=' || "metrics.kw" || '/' || "metrics.online" || '/' || "attrs.x"
FROM nats_scan('copy_roundtrip', url := '${NATS_URL}', nats_subject := 'copy_roundtrip.nested',
    json_extract := ['metrics.kw', 'metrics.online', 'attrs.x']);
END
EXPECT rows=5 20
EXPECT json=json-device/42.500000/true/7.000000 20
EXPECT msgpack=msgpack-device/43.500000/false/8 20
EXPECT cbor=cbor-device/44.500000/true/9 20
EXPECT flex=flex-device/45.500000/false/10 20
EXPECT proto=proto-device/46.5/true 20
EXPECT proto_tags=ok 20
EXPECT nested=12.500000/true/1 20
QUIT
SQL
then
  cat "$log_file" >&2
  exit 1
fi

echo "structured COPY TO harness passed"
