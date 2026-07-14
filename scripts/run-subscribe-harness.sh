#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
TIP_ROOT="${TIP_ROOT:-/tmp/duckdb-tip-clean}"
DUCKDB_BIN="${DUCKDB_BIN:-${SUBSCRIBE_DUCKDB_BIN:-$TIP_ROOT/build/release/duckdb}}"
DUCKDB_LIB="${DUCKDB_LIB:-${SUBSCRIBE_DUCKDB_LIB:-$TIP_ROOT/build/release/src/libduckdb.so}}"
EXTENSION_PATH="${EXTENSION_PATH:-${SUBSCRIBE_EXTENSION_PATH:-$TIP_ROOT/build/nats_js-tip/extension/nats_js/nats_js.duckdb_extension}}"
NATS_URL="${NATS_URL:-nats://127.0.0.1:4222}"
NATS_CLI="${NATS_CLI:-$HOME/nats}"
PYTHON_BIN="${PYTHON_BIN:-$ROOT_DIR/.venv/bin/python}"

if [ ! -x "$DUCKDB_BIN" ]; then
  echo "DuckDB binary not found: $DUCKDB_BIN" >&2
  echo "Build first with: make release" >&2
  exit 1
fi

if [ ! -f "$EXTENSION_PATH" ]; then
  echo "Extension not found: $EXTENSION_PATH" >&2
  echo "Build tip extension first or set SUBSCRIBE_EXTENSION_PATH." >&2
  exit 1
fi

if [ ! -x "$NATS_CLI" ]; then
  NATS_CLI="$(command -v nats || true)"
fi

if [ -z "$NATS_CLI" ]; then
  echo "NATS CLI not found. Set NATS_CLI=/path/to/nats." >&2
  exit 1
fi

if [ ! -x "$PYTHON_BIN" ]; then
  PYTHON_BIN="$(command -v python3)"
fi

echo "Checking NATS connection at $NATS_URL"
"$NATS_CLI" server check connection --server "$NATS_URL"

db_file="$(mktemp /tmp/nats_subscribe_harness.XXXXXX.duckdb)"
log_file="$(mktemp /tmp/nats_subscribe_harness.XXXXXX.log)"
rm -f "$db_file"
trap 'rc=$?; rm -f "$log_file" "$db_file"; exit $rc' EXIT

(
  sleep 1
  echo "Publishing live subscribe batch to live.subscribe"
  "$NATS_CLI" pub --quiet --count 4 --server="$NATS_URL" "live.subscribe" "subscribe-test-{{Count}}" >/dev/null
) &

if ! DUCKDB_LIB="$DUCKDB_LIB" "$PYTHON_BIN" "$ROOT_DIR/scripts/duckdb_session.py" --duckdb-bin "$DUCKDB_BIN" --db-file "$db_file" <<SQL >"$log_file" 2>&1
SEND
LOAD '${EXTENSION_PATH}';
SELECT 'start=' || job_name || '|' || target_table || '|' || subject AS start_result
FROM nats_start_subscribe(
    job_name := 'live_subscribe_probe',
    target_table := 'main.subscribe_out',
    url := '${NATS_URL}',
    subject := 'live.subscribe',
    batch_size := 2,
    poll_ms := 100,
    create_target_table := true,
    subject_column := 'select',
    payload_column := 'payload data'
);
END
EXPECT start=live_subscribe_probe|main.subscribe_out|live.subscribe 10
SLEEP 3
SEND
SELECT 'status=' || rows_inserted || '/' || batches_committed || '/' || running || '/' || failed AS subscribe_status
FROM nats_subscribe_status(job_name := 'live_subscribe_probe');
SELECT 'count=' || COUNT(*) AS inserted_rows FROM main.subscribe_out;
SELECT 'types=' || string_agg(type, '/' ORDER BY cid) AS target_types
FROM pragma_table_info('subscribe_out');
SELECT 'stop=' || job_name || '|' || target_table || '|' || subject AS stop_result
FROM nats_stop_subscribe(job_name := 'live_subscribe_probe');
END
EXPECT status=4/2/true/false 30
EXPECT count=4 10
EXPECT types=VARCHAR/BLOB/TIMESTAMP 10
EXPECT stop=live_subscribe_probe|main.subscribe_out|live.subscribe 10
QUIT
SQL
then
  cat "$log_file" >&2
  exit 1
fi

echo "Subscribe harness passed"
