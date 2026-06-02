#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
DUCKDB_BIN="${DUCKDB_BIN:-/home/mrayva/.duckdb/cli/1.5.3/duckdb}"
EXTENSION_PATH="${EXTENSION_PATH:-$ROOT_DIR/build/release/extension/nats_js/nats_js.duckdb_extension}"
NATS_URL="${NATS_URL:-nats://localhost:4222}"
NATS_CLI="${NATS_CLI:-$HOME/nats}"
HARNESS_MODE="${HARNESS_MODE:-resume}"

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

case "$HARNESS_MODE" in
  resume)
    stream_name="ingest_resume"
    durable_name="duckdb_ingest_resume"
    start_job_a="ingest_probe3_a"
    start_job_b="ingest_probe3_b"
    publish_second_batch=1
    ;;
  redelivery)
    stream_name="ingest_redelivery"
    durable_name="duckdb_ingest_redelivery"
    start_job_a="ingest_redelivery_a"
    start_job_b="ingest_redelivery_b"
    publish_second_batch=1
    ;;
  *)
    echo "Unknown HARNESS_MODE: $HARNESS_MODE (expected resume or redelivery)" >&2
    exit 1
    ;;
esac

echo "Checking NATS connection at $NATS_URL"
"$NATS_CLI" server check connection --server "$NATS_URL"

echo "Preparing JetStream streams"
NATS_URL="$NATS_URL" NATS_CLI="$NATS_CLI" RESET_STREAMS="${RESET_STREAMS:-1}" "$ROOT_DIR/scripts/setup-streams.sh"

db_file="$(mktemp /tmp/nats_ingest_harness.XXXXXX.duckdb)"
log_file="$(mktemp /tmp/nats_ingest_harness.XXXXXX.log)"
rm -f "$db_file"
trap 'rm -f "$log_file" "$db_file"' EXIT

run_duckdb() {
  local sql="$1"
  set +e
  "$DUCKDB_BIN" -unsigned "$db_file" -c "$sql" >"$log_file" 2>&1
  local duckdb_status=$?
  set -e
  if [ "$duckdb_status" -ne 0 ]; then
    cat "$log_file" >&2
    exit "$duckdb_status"
  fi
}

verify_patterns() {
  shift
  for pattern in "$@"; do
    if ! grep -Fq "$pattern" "$log_file"; then
      echo "Missing expected ingest output pattern: $pattern" >&2
      tail -n 80 "$log_file" >&2
      exit 1
    fi
  done
}

sql_start_a=$(cat <<SQL
LOAD 'build/release/extension/nats_js/nats_js.duckdb_extension';
CREATE TABLE ingest_out(stream_name VARCHAR, subject VARCHAR, sequence UBIGINT, ts TIMESTAMP, payload BLOB);
SELECT 'start1=' || job_name || '|' || stream_name || '|' || target_table || '|' || durable_name AS start_result
FROM nats_start_ingest(
    job_name := '${start_job_a}',
    stream_name := '${stream_name}',
    target_table := 'ingest_out',
    durable_name := '${durable_name}',
    url := 'nats://localhost:4222',
    batch_size := 4,
    poll_ms := 10000,
    fetch_timeout_ms := 100,
    start_seq := 1
);
SELECT SUM(i) FROM range(100000000) t(i);
SELECT 'status1=' || rows_inserted || '/' || batches_committed || '/' || last_committed_seq AS ingest_status
FROM nats_ingest_status(job_name := '${start_job_a}');
SELECT 'stop1=' || job_name || '|' || stream_name || '|' || target_table || '|' || durable_name AS stop_result
FROM nats_stop_ingest(job_name := '${start_job_a}');
SQL
)
run_duckdb "$sql_start_a"
verify_patterns \
  "start1=${start_job_a}|${stream_name}|ingest_out|${durable_name}" \
  "status1=4/1/4" \
  "stop1=${start_job_a}|${stream_name}|ingest_out|${durable_name}"

if [ "$publish_second_batch" -eq 1 ]; then
  echo "Publishing second ingest batch to ${stream_name}"
  "$NATS_CLI" pub --jetstream --quiet --count 4 --server="$NATS_URL" "${stream_name}.items" "resume-test-{{Count}}" >/dev/null
fi

sql_start_b=$(cat <<SQL
LOAD 'build/release/extension/nats_js/nats_js.duckdb_extension';
SELECT 'start2=' || job_name || '|' || stream_name || '|' || target_table || '|' || durable_name AS start_result
FROM nats_start_ingest(
    job_name := '${start_job_b}',
    stream_name := '${stream_name}',
    target_table := 'ingest_out',
    durable_name := '${durable_name}',
    url := 'nats://localhost:4222',
    batch_size := 4,
    poll_ms := 10000,
    fetch_timeout_ms := 100,
    start_seq := 1
);
SELECT SUM(i) FROM range(100000000) t(i);
SELECT 'status2=' || rows_inserted || '/' || batches_committed || '/' || last_committed_seq AS ingest_status
FROM nats_ingest_status(job_name := '${start_job_b}');
SELECT 'count=' || COUNT(*) AS inserted_rows FROM ingest_out;
SELECT 'stop2=' || job_name || '|' || stream_name || '|' || target_table || '|' || durable_name AS stop_result
FROM nats_stop_ingest(job_name := '${start_job_b}');
SELECT 'stopped=' || stop_requested || '/' || stopped || '/' || failed AS stopped_status
FROM nats_ingest_status(job_name := '${start_job_b}');
SQL
)
run_duckdb "$sql_start_b"
verify_patterns \
  "start2=${start_job_b}|${stream_name}|ingest_out|${durable_name}" \
  "status2=8/2/8" \
  "count=8" \
  "stop2=${start_job_b}|${stream_name}|ingest_out|${durable_name}" \
  "stopped=true/false/false"

echo "Ingest harness passed"
