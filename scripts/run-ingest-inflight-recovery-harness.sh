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

if [ ! -x "$DUCKDB_BIN" ] || [ ! -f "$EXTENSION_PATH" ]; then
  echo "DuckDB binary or extension not found" >&2
  exit 1
fi
if [ ! -x "$NATS_CLI" ]; then
  NATS_CLI="$(command -v nats || true)"
fi
if [ -z "$NATS_CLI" ]; then
  echo "NATS CLI not found" >&2
  exit 1
fi

echo "Checking NATS connection at $NATS_URL"
"$NATS_CLI" server check connection --server "$NATS_URL"
NATS_URL="$NATS_URL" NATS_CLI="$NATS_CLI" RESET_STREAMS="${RESET_STREAMS:-1}" "$ROOT_DIR/scripts/setup-streams.sh" >/dev/null

db_file="$(mktemp /tmp/nats_ingest_inflight.XXXXXX.duckdb)"
log_first="$(mktemp /tmp/nats_ingest_inflight.first.XXXXXX.log)"
log_second="$(mktemp /tmp/nats_ingest_inflight.second.XXXXXX.log)"
log_probe="$(mktemp /tmp/nats_ingest_inflight.probe.XXXXXX.log)"
rm -f "$db_file"
trap 'rc=$?; rm -f "$log_first" "$log_second" "$log_probe" "$db_file"; exit $rc' EXIT

if DUCKDB_LIB="$DUCKDB_LIB" NATS_INGEST_FAIL_AFTER_APPEND=2 NATS_INGEST_DISABLE_REHYDRATE=1 \
  python3 "$ROOT_DIR/scripts/duckdb_session.py" --duckdb-bin "$DUCKDB_BIN" --db-file "$db_file" <<SQL >"$log_first" 2>&1
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
    job_name := 'ingest_inflight_a',
    stream_name := 'ingest_resume',
    target_table := 'ingest_out',
    durable_name := 'duckdb_ingest_inflight',
    url := '${NATS_URL}',
    batch_size := 4,
    poll_ms := 100,
    fetch_timeout_ms := 100,
    start_seq := 1
);
END
EXPECT start1=ingest_inflight_a|ingest_resume|ingest_out|duckdb_ingest_inflight 10
SLEEP 2
QUIT
SQL
then
  echo "Expected in-flight failure injection to terminate DuckDB" >&2
  cat "$log_first" >&2
  exit 1
fi

DUCKDB_LIB="$DUCKDB_LIB" NATS_INGEST_DISABLE_REHYDRATE=1 "$DUCKDB_BIN" -unsigned "$db_file" \
  -c "LOAD '${EXTENSION_PATH}'; SELECT 'rows_after_crash=' || COUNT(*) FROM ingest_out; SELECT 'checkpoints_after_crash=' || COALESCE(MAX(last_committed_seq), 0) FROM duckdb_nats_ingest_checkpoints;" \
  >"$log_probe" 2>&1
grep -Fq "rows_after_crash=0" "$log_probe" || { cat "$log_probe" >&2; exit 1; }
grep -Fq "checkpoints_after_crash=0" "$log_probe" || { cat "$log_probe" >&2; exit 1; }

if ! DUCKDB_LIB="$DUCKDB_LIB" NATS_INGEST_DISABLE_REHYDRATE=1 \
  python3 "$ROOT_DIR/scripts/duckdb_session.py" --duckdb-bin "$DUCKDB_BIN" --db-file "$db_file" <<SQL >"$log_second" 2>&1
SEND
LOAD '${EXTENSION_PATH}';
SELECT 'start2=' || job_name || '|' || stream_name || '|' || target_table || '|' || durable_name AS start_result
FROM nats_start_ingest(
    job_name := 'ingest_inflight_b',
    stream_name := 'ingest_resume',
    target_table := 'ingest_out',
    durable_name := 'duckdb_ingest_inflight',
    url := '${NATS_URL}',
    batch_size := 4,
    poll_ms := 100,
    fetch_timeout_ms := 100,
    start_seq := 1
);
END
EXPECT start2=ingest_inflight_b|ingest_resume|ingest_out|duckdb_ingest_inflight 10
POLL status2=4/1/4 20
SELECT 'status2=' || rows_inserted || '/' || batches_committed || '/' || last_committed_seq AS ingest_status
FROM nats_ingest_status(job_name := 'ingest_inflight_b');
SELECT 'rows_after_recovery=' || COUNT(*) FROM ingest_out;
SELECT * FROM nats_stop_ingest(job_name := 'ingest_inflight_b');
END
EXPECT rows_after_recovery=4 10
QUIT
SQL
then
  cat "$log_second" >&2
  exit 1
fi

echo "In-flight ingest recovery harness passed"
