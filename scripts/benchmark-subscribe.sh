#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
TIP_ROOT="${TIP_ROOT:-/tmp/duckdb-tip-clean}"
DUCKDB_BIN="${DUCKDB_BIN:-${SUBSCRIBE_DUCKDB_BIN:-$TIP_ROOT/build/release/duckdb}}"
DUCKDB_LIB="${DUCKDB_LIB:-${DUCKDB_HOST_LIB:-${SUBSCRIBE_DUCKDB_LIB:-$TIP_ROOT/build/release/src/libduckdb.so}}}"
EXTENSION_PATH="${EXTENSION_PATH:-${SUBSCRIBE_EXTENSION_PATH:-$TIP_ROOT/build/nats_js-tip/extension/nats_js/nats_js.duckdb_extension}}"
NATS_URL="${NATS_URL:-nats://127.0.0.1:4222}"
NATS_CLI="${NATS_CLI:-$HOME/nats}"
PYTHON_BIN="${PYTHON_BIN:-$ROOT_DIR/.venv/bin/python}"

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

if [ ! -x "$PYTHON_BIN" ]; then
  PYTHON_BIN="$(command -v python3)"
fi

echo "Checking NATS connection at $NATS_URL"
"$NATS_CLI" server check connection --server "$NATS_URL"

db_file="$(mktemp /tmp/nats_subscribe_benchmark.XXXXXX.duckdb)"
log_file="$(mktemp /tmp/nats_subscribe_benchmark.XXXXXX.log)"
rm -f "$db_file"
trap 'rc=$?; rm -f "$log_file" "$db_file"; exit $rc' EXIT

(
  sleep 1
  echo "Publishing first live subscribe batch"
  "$NATS_CLI" pub --quiet --count 4 --server="$NATS_URL" "live.subscribe" "benchmark-subscribe-1-{{Count}}" >/dev/null
) &

(
  sleep 6
  echo "Publishing second live subscribe batch"
  "$NATS_CLI" pub --quiet --count 4 --server="$NATS_URL" "live.subscribe" "benchmark-subscribe-2-{{Count}}" >/dev/null
) &

start_ns="$(date +%s%N)"
if ! DUCKDB_LIB="$DUCKDB_LIB" "$PYTHON_BIN" "$ROOT_DIR/scripts/duckdb_session.py" --duckdb-bin "$DUCKDB_BIN" --db-file "$db_file" <<SQL >"$log_file" 2>&1
SEND
LOAD '${EXTENSION_PATH}';
CREATE TABLE subscribe_out(
    subject VARCHAR,
    payload VARCHAR,
    received_at TIMESTAMP
);
SELECT 'start=' || job_name || '|' || target_table || '|' || subject AS start_result
FROM nats_start_subscribe(
    job_name := 'live_subscribe_benchmark',
    target_table := 'subscribe_out',
    url := '${NATS_URL}',
    subject := 'live.subscribe',
    batch_size := 2,
    poll_ms := 100,
    create_target_table := false
);
END
EXPECT start=live_subscribe_benchmark|subscribe_out|live.subscribe 10
SLEEP 3
SEND
SELECT 'status1=' || rows_inserted || '/' || batches_committed AS subscribe_status
FROM nats_subscribe_status(job_name := 'live_subscribe_benchmark');
SELECT 'count1=' || COUNT(*) AS inserted_rows FROM subscribe_out;
END
EXPECT status1=4/2 30
EXPECT count1=4 10
SLEEP 4
SEND
SELECT 'status2=' || rows_inserted || '/' || batches_committed AS subscribe_status
FROM nats_subscribe_status(job_name := 'live_subscribe_benchmark');
SELECT 'count=' || COUNT(*) AS inserted_rows FROM subscribe_out;
SELECT 'stop=' || job_name || '|' || target_table || '|' || subject AS stop_result
FROM nats_stop_subscribe(job_name := 'live_subscribe_benchmark');
END
EXPECT status2=8/4 30
EXPECT count=8 10
EXPECT stop=live_subscribe_benchmark|subscribe_out|live.subscribe 10
QUIT
SQL
then
  cat "$log_file" >&2
  exit 1
fi
end_ns="$(date +%s%N)"
total_ms="$(((end_ns - start_ns) / 1000000))"
rows_per_sec="$(awk -v ms="$total_ms" 'BEGIN { if (ms <= 0) { print "inf"; } else { printf "%.2f", 8000 / (ms / 1000.0); } }')"

if ! grep -Fq "status1=4/2" "$log_file"; then
  echo "Missing expected intermediate subscribe status" >&2
  tail -n 80 "$log_file" >&2
  exit 1
fi

if ! grep -Fq "status2=8/4" "$log_file"; then
  echo "Missing expected final subscribe status" >&2
  tail -n 80 "$log_file" >&2
  exit 1
fi

if ! grep -Fq "count=8" "$log_file"; then
  echo "Missing expected final subscribe row count" >&2
  tail -n 80 "$log_file" >&2
  exit 1
fi

printf 'mode,total_ms,rows_inserted,approx_rows_per_sec,status\n'
printf 'live,%s,%s,%s,%s\n' "$total_ms" "8" "$rows_per_sec" "status2=8/4"

echo "Subscribe benchmark passed"
