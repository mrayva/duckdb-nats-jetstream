#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
TIP_ROOT="${TIP_ROOT:-/tmp/duckdb-tip-clean}"
DUCKDB_BIN="${DUCKDB_BIN:-${TIP_DUCKDB_BIN:-$TIP_ROOT/build/release/duckdb}}"
EXTENSION_PATH="${EXTENSION_PATH:-${TIP_EXTENSION_PATH:-$TIP_ROOT/build/nats_js-tip/extension/nats_js/nats_js.duckdb_extension}}"
NATS_URL="${NATS_URL:-nats://localhost:4222}"
NATS_CLI="${NATS_CLI:-$HOME/nats}"

if [ ! -x "$DUCKDB_BIN" ]; then
  echo "DuckDB binary not found: $DUCKDB_BIN" >&2
  echo "Build first with: make release" >&2
  exit 1
fi

if [ ! -f "$EXTENSION_PATH" ]; then
  echo "Extension not found: $EXTENSION_PATH" >&2
  echo "Build tip extension first or set TIP_EXTENSION_PATH." >&2
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

db_file="$(mktemp /tmp/nats_ingest_autocreate.XXXXXX.duckdb)"
log_file="$(mktemp /tmp/nats_ingest_autocreate.XXXXXX.log)"
rm -f "$db_file"
trap 'rm -f "$log_file" "$db_file"' EXIT

run_duckdb() {
  local sql="$1"
  set +e
  NATS_INGEST_DISABLE_REHYDRATE=1 "$DUCKDB_BIN" -unsigned "$db_file" -c "$sql" >"$log_file" 2>&1
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

sql_start=$(cat <<'SQL'
LOAD '${EXTENSION_PATH}';
SELECT 'start=' || job_name || '|' || stream_name || '|' || target_table || '|' || durable_name AS start_result
FROM nats_start_ingest(
    job_name := 'ingest_autocreate',
    stream_name := 'ingest_resume',
    target_table := 'ingest_out',
    durable_name := 'duckdb_ingest_autocreate',
    url := 'nats://localhost:4222',
    batch_size := 4,
    poll_ms := 10000,
    fetch_timeout_ms := 100,
    start_seq := 1,
    create_target_table := TRUE
);
SELECT SUM(i) FROM range(5000000) t(i);
SELECT 'status=' || rows_inserted || '/' || batches_committed || '/' || last_committed_seq AS ingest_status
FROM nats_ingest_status(job_name := 'ingest_autocreate');
SQL
)

run_duckdb "$sql_start"
verify_patterns \
  "start=ingest_autocreate|ingest_resume|ingest_out|duckdb_ingest_autocreate" \
  "status=4/1/4"

run_duckdb "$(cat <<'SQL'
LOAD '${EXTENSION_PATH}';
SELECT 'stop=' || job_name || '|' || stream_name || '|' || target_table || '|' || durable_name AS stop_result
FROM nats_stop_ingest(job_name := 'ingest_autocreate');
SELECT 'count=' || COUNT(*) AS inserted_rows FROM ingest_out;
SELECT 'checkpoint=' || last_committed_seq AS checkpoint_seq
FROM duckdb_nats_ingest_checkpoints
WHERE stream_name = 'ingest_resume' AND durable_name = 'duckdb_ingest_autocreate';
SQL
)"

verify_patterns \
  "start=ingest_autocreate|ingest_resume|ingest_out|duckdb_ingest_autocreate" \
  "status=4/1/4" \
  "stop=ingest_autocreate|ingest_resume|ingest_out|duckdb_ingest_autocreate" \
  "count=4" \
  "checkpoint=4"

echo "Ingest auto-create harness passed"
