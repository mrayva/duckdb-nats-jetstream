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
    reading_count INTEGER
);
INSERT INTO structured_source VALUES
    ('copy_roundtrip.json', 'json-device', 42.5, true, 7),
    ('copy_roundtrip.msgpack', 'msgpack-device', 43.5, false, 8),
    ('copy_roundtrip.cbor', 'cbor-device', 44.5, true, 9),
    ('copy_roundtrip.flex', 'flex-device', 45.5, false, 10);
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
SELECT 'rows=' || COUNT(*) FROM nats_scan('copy_roundtrip', url := '${NATS_URL}');
SELECT 'json=' || device_id || '/' || kw || '/' || online || '/' || reading_count
FROM nats_scan('copy_roundtrip', url := '${NATS_URL}', nats_subject := 'copy_roundtrip.json', json_extract := ['device_id', 'kw', 'online', 'reading_count']);
SELECT 'msgpack=' || device_id || '/' || kw || '/' || online || '/' || reading_count
FROM nats_scan('copy_roundtrip', url := '${NATS_URL}', nats_subject := 'copy_roundtrip.msgpack', msgpack_extract := ['device_id', 'kw', 'online', 'reading_count']);
SELECT 'cbor=' || device_id || '/' || kw || '/' || online || '/' || reading_count
FROM nats_scan('copy_roundtrip', url := '${NATS_URL}', nats_subject := 'copy_roundtrip.cbor', cbor_extract := ['device_id', 'kw', 'online', 'reading_count']);
SELECT 'flex=' || device_id || '/' || kw || '/' || online || '/' || reading_count
FROM nats_scan('copy_roundtrip', url := '${NATS_URL}', nats_subject := 'copy_roundtrip.flex', flexbuffers_extract := ['device_id', 'kw', 'online', 'reading_count']);
END
EXPECT rows=4 20
EXPECT json=json-device/42.5/true/7 20
EXPECT msgpack=msgpack-device/43.5/false/8 20
EXPECT cbor=cbor-device/44.5/true/9 20
EXPECT flex=flex-device/45.5/false/10 20
QUIT
SQL
then
  cat "$log_file" >&2
  exit 1
fi

echo "structured COPY TO harness passed"
