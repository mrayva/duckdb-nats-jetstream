#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
source "$ROOT_DIR/scripts/duckdb_host_lib.sh"
TIP_ROOT="${TIP_ROOT:-/tmp/duckdb-tip-clean}"
DUCKDB_BIN="${DUCKDB_BIN:-${TIP_DUCKDB_BIN:-$TIP_ROOT/build/release/duckdb}}"
DUCKDB_LIB="${DUCKDB_LIB:-$(duckdb_host_lib_resolve)}"
EXTENSION_PATH="${EXTENSION_PATH:-${TIP_EXTENSION_PATH:-$TIP_ROOT/build/nats_js-tip/extension/nats_js/nats_js.duckdb_extension}}"
NATS_URL="${NATS_URL:-nats://localhost:4222}"
NATS_CLI="${NATS_CLI:-$HOME/nats}"
PYTHON_BIN="${PYTHON_BIN:-$ROOT_DIR/.venv/bin/python}"

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

if [ ! -x "$PYTHON_BIN" ]; then
  PYTHON_BIN="$(command -v python3)"
fi

echo "Checking NATS connection at $NATS_URL"
"$NATS_CLI" server check connection --server "$NATS_URL"

echo "Preparing JetStream streams"
NATS_URL="$NATS_URL" NATS_CLI="$NATS_CLI" RESET_STREAMS="${RESET_STREAMS:-1}" "$ROOT_DIR/scripts/setup-streams.sh"

db_file="$(mktemp /tmp/nats_ingest_autocreate.XXXXXX.duckdb)"
log_file="$(mktemp /tmp/nats_ingest_autocreate.XXXXXX.log)"
rm -f "$db_file"
trap 'rc=$?; rm -f "$log_file" "$db_file"; exit $rc' EXIT

if ! DUCKDB_LIB="$DUCKDB_LIB" NATS_INGEST_DISABLE_REHYDRATE=1 "$PYTHON_BIN" "$ROOT_DIR/scripts/duckdb_session.py" --duckdb-bin "$DUCKDB_BIN" --db-file "$db_file" <<SQL >"$log_file" 2>&1
SEND
LOAD '${EXTENSION_PATH}';
SELECT 'start=' || job_name || '|' || stream_name || '|' || target_table || '|' || durable_name AS start_result
FROM nats_start_ingest(
    job_name := 'ingest_autocreate',
    stream_name := 'ingest_resume',
    target_table := 'ingest_out',
    durable_name := 'duckdb_ingest_autocreate',
    url := '${NATS_URL}',
    batch_size := 4,
    poll_ms := 100,
    fetch_timeout_ms := 100,
    start_seq := 1,
    create_target_table := TRUE
);
END
EXPECT start=ingest_autocreate|ingest_resume|ingest_out|duckdb_ingest_autocreate 20
SLEEP 2
SEND
SELECT 'status=' || rows_inserted || '/' || batches_committed || '/' || last_committed_seq AS ingest_status
FROM nats_ingest_status(job_name := 'ingest_autocreate');
END
EXPECT status=4/1/4 20
SEND
SELECT 'stop=' || job_name || '|' || stream_name || '|' || target_table || '|' || durable_name AS stop_result
FROM nats_stop_ingest(job_name := 'ingest_autocreate');
SELECT 'count=' || COUNT(*) AS inserted_rows FROM ingest_out;
SELECT 'checkpoint=' || last_committed_seq AS checkpoint_seq
FROM duckdb_nats_ingest_checkpoints
WHERE stream_name = 'ingest_resume' AND durable_name = 'duckdb_ingest_autocreate';
END
EXPECT stop=ingest_autocreate|ingest_resume|ingest_out|duckdb_ingest_autocreate 10
EXPECT count=4 10
EXPECT checkpoint=4 10
QUIT
SQL
then
  cat "$log_file" >&2
  exit 1
fi

echo "Ingest auto-create harness passed"
