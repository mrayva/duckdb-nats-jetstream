#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
DUCKDB_BIN="${DUCKDB_BIN:-/home/mrayva/.duckdb/cli/1.5.3/duckdb}"
EXTENSION_PATH="${EXTENSION_PATH:-$ROOT_DIR/build/release/extension/nats_js/nats_js.duckdb_extension}"
NATS_URL="${NATS_URL:-nats://localhost:4222}"
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

echo "Checking NATS connection at $NATS_URL"
"$NATS_CLI" server check connection --server "$NATS_URL"

echo "Preparing JetStream streams"
NATS_URL="$NATS_URL" NATS_CLI="$NATS_CLI" RESET_STREAMS="${RESET_STREAMS:-1}" "$ROOT_DIR/scripts/setup-streams.sh"

db_file="$(mktemp /tmp/nats_ingest_harness.XXXXXX.duckdb)"
log_file="$(mktemp /tmp/nats_ingest_harness.XXXXXX.log)"
rm -f "$db_file"
trap 'rm -f "$log_file" "$db_file"' EXIT

sql_start_a=$(cat <<'SQL'
LOAD 'build/release/extension/nats_js/nats_js.duckdb_extension';
CREATE TABLE ingest_out(stream_name VARCHAR, subject VARCHAR, sequence UBIGINT, ts TIMESTAMP, payload BLOB);
SELECT 'start1=' || job_name || '|' || stream_name || '|' || target_table || '|' || durable_name AS start_result
FROM nats_start_ingest(
    job_name := 'ingest_probe3_a',
    stream_name := 'ingest_resume',
    target_table := 'ingest_out',
    durable_name := 'duckdb_ingest_resume',
    url := 'nats://localhost:4222',
    batch_size := 4,
    poll_ms := 10000,
    fetch_timeout_ms := 100,
    start_seq := 1
);
SELECT SUM(i) FROM range(100000000) t(i);
SELECT 'status1=' || rows_inserted || '/' || batches_committed || '/' || last_committed_seq AS ingest_status
FROM nats_ingest_status(job_name := 'ingest_probe3_a');
SELECT 'stop1=' || job_name || '|' || stream_name || '|' || target_table || '|' || durable_name AS stop_result
FROM nats_stop_ingest(job_name := 'ingest_probe3_a');
SQL
)

sql_start_a="${sql_start_a//$'\n'/ }"

set +e
"$DUCKDB_BIN" -unsigned "$db_file" -c "$sql_start_a" >"$log_file" 2>&1
duckdb_status=$?
set -e
if [ "$duckdb_status" -ne 0 ]; then
  cat "$log_file" >&2
  exit "$duckdb_status"
fi

for pattern in \
  "start1=ingest_probe3_a|ingest_resume|ingest_out|duckdb_ingest_resume" \
  "status1=4/1/4" \
  "stop1=ingest_probe3_a|ingest_resume|ingest_out|duckdb_ingest_resume"; do
  if ! grep -Fq "$pattern" "$log_file"; then
    echo "Missing expected ingest output pattern: $pattern" >&2
    tail -n 80 "$log_file" >&2
    exit 1
  fi
done

echo "Publishing second ingest batch to ingest_resume"
"$NATS_CLI" pub --jetstream --quiet --count 4 --server="$NATS_URL" ingest_resume.items "resume-test-{{Count}}" >/dev/null

sql_start_b=$(cat <<'SQL'
LOAD 'build/release/extension/nats_js/nats_js.duckdb_extension';
SELECT 'start2=' || job_name || '|' || stream_name || '|' || target_table || '|' || durable_name AS start_result
FROM nats_start_ingest(
    job_name := 'ingest_probe3_b',
    stream_name := 'ingest_resume',
    target_table := 'ingest_out',
    durable_name := 'duckdb_ingest_resume',
    url := 'nats://localhost:4222',
    batch_size := 4,
    poll_ms := 10000,
    fetch_timeout_ms := 100,
    start_seq := 1
);
SELECT SUM(i) FROM range(100000000) t(i);
SELECT 'status2=' || rows_inserted || '/' || batches_committed || '/' || last_committed_seq AS ingest_status
FROM nats_ingest_status(job_name := 'ingest_probe3_b');
SELECT 'count=' || COUNT(*) AS inserted_rows FROM ingest_out;
SELECT 'stop2=' || job_name || '|' || stream_name || '|' || target_table || '|' || durable_name AS stop_result
FROM nats_stop_ingest(job_name := 'ingest_probe3_b');
SELECT 'stopped=' || stop_requested || '/' || stopped || '/' || failed AS stopped_status
FROM nats_ingest_status(job_name := 'ingest_probe3_b');
SQL
)

sql_start_b="${sql_start_b//$'\n'/ }"

set +e
"$DUCKDB_BIN" -unsigned "$db_file" -c "$sql_start_b" >"$log_file" 2>&1
duckdb_status=$?
set -e
if [ "$duckdb_status" -ne 0 ]; then
  cat "$log_file" >&2
  exit "$duckdb_status"
fi

for pattern in \
  "start2=ingest_probe3_b|ingest_resume|ingest_out|duckdb_ingest_resume" \
  "status2=8/2/8" \
  "count=8" \
  "stop2=ingest_probe3_b|ingest_resume|ingest_out|duckdb_ingest_resume" \
  "stopped=true/false/false"; do
  if ! grep -Fq "$pattern" "$log_file"; then
    echo "Missing expected ingest output pattern: $pattern" >&2
    tail -n 80 "$log_file" >&2
    exit 1
  fi
done

echo "Ingest harness passed"
