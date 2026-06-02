#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
DUCKDB_BIN="${DUCKDB_BIN:-/home/mrayva/.duckdb/cli/1.5.3/duckdb}"
EXTENSION_PATH="${EXTENSION_PATH:-$ROOT_DIR/build/release/extension/nats_js/nats_js.duckdb_extension}"
NATS_URL="${NATS_URL:-nats://127.0.0.1:4222}"
NATS_CLI="${NATS_CLI:-$HOME/nats}"

if [ ! -x "$DUCKDB_BIN" ]; then
  echo "DuckDB binary not found: $DUCKDB_BIN" >&2
  echo "Build first with: make release" >&2
  exit 1
fi

if [ ! -f "$EXTENSION_PATH" ]; then
  echo "Extension not found: $EXTENSION_PATH" >&2
  echo "Build first with: make release" >&2
  exit 1
fi

if [ ! -x "$NATS_CLI" ]; then
  NATS_CLI="$(command -v nats || true)"
fi

if [ -z "$NATS_CLI" ]; then
  echo "NATS CLI not found. Set NATS_CLI=/path/to/nats." >&2
  exit 1
fi

sql_escape() {
  printf "%s" "$1" | sed "s/'/''/g"
}

db_file="$(mktemp /tmp/nats_ingest_rehydrate.XXXXXX.duckdb)"
log_first="$(mktemp /tmp/nats_ingest_rehydrate.first.XXXXXX.log)"
log_second="$(mktemp /tmp/nats_ingest_rehydrate.second.XXXXXX.log)"
rm -f "$db_file"
trap 'rm -f "$log_first" "$log_second" "$db_file"' EXIT

echo "Checking NATS connection at $NATS_URL"
"$NATS_CLI" server check connection --server "$NATS_URL"

echo "Preparing JetStream streams"
NATS_URL="$NATS_URL" NATS_CLI="$NATS_CLI" RESET_STREAMS="${RESET_STREAMS:-1}" "$ROOT_DIR/scripts/setup-streams.sh"

run_duckdb() {
  local sql="$1"
  local log_file="$2"
  set +e
  "$DUCKDB_BIN" -unsigned "$db_file" -c "$sql" >"$log_file" 2>&1
  local duckdb_status=$?
  set -e
  if [ "$duckdb_status" -ne 0 ]; then
    cat "$log_file" >&2
    exit "$duckdb_status"
  fi
}

first_sql=$(cat <<SQL
LOAD 'build/release/extension/nats_js/nats_js.duckdb_extension';
CREATE TABLE ingest_out(
    stream_name VARCHAR,
    subject VARCHAR,
    sequence UBIGINT,
    ts TIMESTAMP,
    payload BLOB
);
SELECT 'start1=' || job_name || '|' || stream_name || '|' || target_table || '|' || durable_name AS start_result
FROM nats_start_ingest(
    job_name := 'ingest_rehydrate',
    stream_name := 'ingest_resume',
    target_table := 'ingest_out',
    durable_name := 'duckdb_ingest_rehydrate',
    url := 'nats://127.0.0.1:4222',
    batch_size := 4,
    poll_ms := 10000,
    fetch_timeout_ms := 100,
    start_seq := 1
);
SQL
)

run_duckdb "$first_sql" "$log_first"

if ! grep -Fq "start1=ingest_rehydrate|ingest_resume|ingest_out|duckdb_ingest_rehydrate" "$log_first"; then
  echo "Missing expected rehydrate start output" >&2
  tail -n 80 "$log_first" >&2
  exit 1
fi

echo "Publishing second ingest batch to ingest_resume"
"$NATS_CLI" pub --jetstream --quiet --count 4 --server="$NATS_URL" "ingest_resume.items" "rehydrate-test-{{Count}}" >/dev/null

second_sql=$(cat <<SQL
LOAD 'build/release/extension/nats_js/nats_js.duckdb_extension';
SELECT 'status1=' || running || '/' || rows_inserted || '/' || last_committed_seq || '/' || failed || '/' || stopped AS ingest_status
FROM nats_ingest_status(job_name := 'ingest_rehydrate');
SELECT SUM(i) FROM range(100000000) t(i);
SELECT 'status2=' || running || '/' || rows_inserted || '/' || last_committed_seq || '/' || failed || '/' || stopped AS ingest_status
FROM nats_ingest_status(job_name := 'ingest_rehydrate');
SELECT 'count=' || COUNT(*) AS inserted_rows FROM ingest_out;
SELECT 'stop=' || job_name || '|' || stream_name || '|' || target_table || '|' || durable_name AS stop_result
FROM nats_stop_ingest(job_name := 'ingest_rehydrate');
SELECT 'stopped=' || stop_requested || '/' || stopped || '/' || failed AS stopped_status
FROM nats_ingest_status(job_name := 'ingest_rehydrate');
SQL
)

run_duckdb "$second_sql" "$log_second"

if ! grep -Fq "status1=true/4/4/false/false" "$log_second"; then
  echo "Missing expected initial rehydrate status" >&2
  tail -n 120 "$log_second" >&2
  exit 1
fi

if ! grep -Fq "status2=true/8/8/false/false" "$log_second"; then
  echo "Missing expected progressed rehydrate status" >&2
  tail -n 120 "$log_second" >&2
  exit 1
fi

if ! grep -Fq "count=8" "$log_second"; then
  echo "Missing expected rehydrate row count" >&2
  tail -n 120 "$log_second" >&2
  exit 1
fi

if ! grep -Fq "stop=ingest_rehydrate|ingest_resume|ingest_out|duckdb_ingest_rehydrate" "$log_second"; then
  echo "Missing expected rehydrate stop output" >&2
  tail -n 120 "$log_second" >&2
  exit 1
fi

if ! grep -Fq "stopped=true/false/false" "$log_second"; then
  echo "Missing expected rehydrate stop status" >&2
  tail -n 120 "$log_second" >&2
  exit 1
fi

echo "Ingest rehydrate harness passed"
