#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
TIP_ROOT="${TIP_ROOT:-/tmp/duckdb-tip-clean}"
DUCKDB_BIN="${DUCKDB_BIN:-${TIP_DUCKDB_BIN:-$TIP_ROOT/build/release/duckdb}}"
DUCKDB_LIB="${DUCKDB_LIB:-${TIP_DUCKDB_LIB:-$TIP_ROOT/build/release/src/libduckdb.so}}"
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

db_file="$(mktemp /tmp/nats_ingest_crash.XXXXXX.duckdb)"
log_first="$(mktemp /tmp/nats_ingest_crash.first.XXXXXX.log)"
log_second="$(mktemp /tmp/nats_ingest_crash.second.XXXXXX.log)"
log_probe="$(mktemp /tmp/nats_ingest_crash.probe.XXXXXX.log)"
rm -f "$db_file"
trap 'rc=$?; rm -f "$log_first" "$log_second" "$log_probe" "$db_file"; exit $rc' EXIT

run_duckdb_once() {
  local sql="$1"
  local log_file="$2"
  local extra_env="${3:-}"
  set +e
  if [ -n "$extra_env" ]; then
    env "$extra_env" NATS_INGEST_DISABLE_REHYDRATE=1 "$DUCKDB_BIN" -unsigned "$db_file" -c "$sql" >"$log_file" 2>&1
  else
    NATS_INGEST_DISABLE_REHYDRATE=1 "$DUCKDB_BIN" -unsigned "$db_file" -c "$sql" >"$log_file" 2>&1
  fi
  local duckdb_status=$?
  set -e
  return "$duckdb_status"
}

if ! DUCKDB_LIB="$DUCKDB_LIB" NATS_INGEST_FAIL_AFTER_COMMIT=1 NATS_INGEST_DISABLE_REHYDRATE=1 python3 "$ROOT_DIR/scripts/duckdb_session.py" --duckdb-bin "$DUCKDB_BIN" --db-file "$db_file" <<SQL >"$log_first" 2>&1
SEND
LOAD '${EXTENSION_PATH}';
CREATE TABLE ingest_out(
    stream_name VARCHAR,
    subject VARCHAR,
    sequence UBIGINT,
    ts TIMESTAMP,
    payload BLOB
);
SELECT 'start1=' || job_name || '|' || stream_name || '|' || target_table || '|' || durable_name AS start_result
FROM nats_start_ingest(
    job_name := 'ingest_crash_a',
    stream_name := 'ingest_resume',
    target_table := 'ingest_out',
    durable_name := 'duckdb_ingest_crash',
    url := 'nats://127.0.0.1:4222',
    batch_size := 4,
    poll_ms := 10000,
    fetch_timeout_ms := 100,
    start_seq := 1
);
END
EXPECT start1=ingest_crash_a|ingest_resume|ingest_out|duckdb_ingest_crash 10
QUIT
SQL
then
  echo "Expected crash harness to fail, but DuckDB exited 0" >&2
  cat "$log_first" >&2
  exit 1
fi

probe_sql=$(cat <<SQL
LOAD '${EXTENSION_PATH}';
SELECT 'count=' || COUNT(*) AS inserted_rows FROM ingest_out;
SELECT 'checkpoint=' || last_committed_seq AS checkpoint_seq
FROM duckdb_nats_ingest_checkpoints
WHERE stream_name = 'ingest_resume' AND durable_name = 'duckdb_ingest_crash';
SQL
)

run_duckdb_once "$probe_sql" "$log_probe"

if ! grep -Fq "count=4" "$log_probe"; then
  echo "Missing expected crash checkpoint row count" >&2
  tail -n 80 "$log_probe" >&2
  exit 1
fi

if ! grep -Fq "checkpoint=4" "$log_probe"; then
  echo "Missing expected persisted checkpoint after crash" >&2
  tail -n 80 "$log_probe" >&2
  exit 1
fi

if ! DUCKDB_LIB="$DUCKDB_LIB" NATS_INGEST_DISABLE_REHYDRATE=1 python3 "$ROOT_DIR/scripts/duckdb_session.py" --duckdb-bin "$DUCKDB_BIN" --db-file "$db_file" <<SQL >"$log_second" 2>&1
SEND
LOAD '${EXTENSION_PATH}';
SELECT 'start2=' || job_name || '|' || stream_name || '|' || target_table || '|' || durable_name AS start_result
FROM nats_start_ingest(
    job_name := 'ingest_crash_b',
    stream_name := 'ingest_resume',
    target_table := 'ingest_out',
    durable_name := 'duckdb_ingest_crash',
    url := 'nats://127.0.0.1:4222',
    batch_size := 4,
    poll_ms := 10000,
    fetch_timeout_ms := 100,
    start_seq := 1
);
END
EXPECT start2=ingest_crash_b|ingest_resume|ingest_out|duckdb_ingest_crash 10
SEND
SELECT 'status2=' || rows_inserted || '/' || batches_committed || '/' || last_committed_seq || '/' || failed || '/' || stop_requested || '/' || stopped AS ingest_status
FROM nats_ingest_status(job_name := 'ingest_crash_b');
END
EXPECT status2=4/1/4/false/false/false 30
SEND
SELECT 'count=' || COUNT(*) AS inserted_rows FROM ingest_out;
SELECT 'stop2=' || job_name || '|' || stream_name || '|' || target_table || '|' || durable_name AS stop_result
FROM nats_stop_ingest(job_name := 'ingest_crash_b');
END
EXPECT count=4 10
EXPECT stop2=ingest_crash_b|ingest_resume|ingest_out|duckdb_ingest_crash 10
QUIT
SQL
then
  cat "$log_second" >&2
  exit 1
fi

echo "Ingest crash harness passed"
