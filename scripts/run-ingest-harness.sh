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

log_file="$(mktemp /tmp/nats_ingest_harness.XXXXXX.log)"
trap 'rm -f "$log_file"' EXIT

sql=$(cat <<'SQL'
LOAD 'build/release/extension/nats_js/nats_js.duckdb_extension';
CREATE TABLE ingest_out(stream_name VARCHAR, subject VARCHAR, sequence UBIGINT, ts TIMESTAMP, payload BLOB);
SELECT job_name || '|' || stream_name || '|' || target_table || '|' || durable_name AS start_result
FROM nats_start_ingest(
    job_name := 'ingest_probe3',
    stream_name := 'stats_gaps',
    target_table := 'ingest_out',
    durable_name := 'duckdb_ingest_probe3',
    url := 'nats://localhost:4222',
    batch_size := 8,
    poll_ms := 10,
    fetch_timeout_ms := 100,
    start_seq := 1
);
SELECT SUM(i) FROM range(100000000) t(i);
SELECT 'status=' || rows_inserted || '/' || batches_committed || '/' || stop_requested || '/' || stopped || '/' || failed AS ingest_status
FROM nats_ingest_status(job_name := 'ingest_probe3');
SELECT 'count=' || COUNT(*) AS inserted_rows FROM ingest_out;
SELECT 'stop=' || job_name || '|' || stream_name || '|' || target_table || '|' || durable_name AS stop_result
FROM nats_stop_ingest(job_name := 'ingest_probe3');
SELECT 'stopped=' || stop_requested || '/' || stopped || '/' || failed AS stopped_status
FROM nats_ingest_status(job_name := 'ingest_probe3');
SQL
)

sql="${sql//$'\n'/ }"

set +e
"$DUCKDB_BIN" -unsigned :memory: -c "$sql" >"$log_file" 2>&1
duckdb_status=$?
set -e
if [ "$duckdb_status" -ne 0 ]; then
  cat "$log_file" >&2
  exit "$duckdb_status"
fi

for pattern in \
  "ingest_probe3|stats_gaps|ingest_out|duckdb_ingest_probe3" \
  "status=8/1/false/false/false" \
  "count=8" \
  "stop=ingest_probe3|stats_gaps|ingest_out|duckdb_ingest_probe3" \
  "stopped=true/false/false"; do
  if ! grep -Fq "$pattern" "$log_file"; then
    echo "Missing expected ingest output pattern: $pattern" >&2
    tail -n 80 "$log_file" >&2
    exit 1
  fi
done

echo "Ingest harness passed"
