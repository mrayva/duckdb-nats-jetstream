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
if [ ! -x "$PYTHON_BIN" ]; then
  PYTHON_BIN="$(command -v python3)"
fi

echo "Checking NATS connection at $NATS_URL"
"$NATS_CLI" server check connection --server "$NATS_URL"
NATS_URL="$NATS_URL" NATS_CLI="$NATS_CLI" RESET_STREAMS="${RESET_STREAMS:-1}" \
  "$ROOT_DIR/scripts/setup-streams.sh" >/dev/null

run_crash_case() {
  local name="$1"
  local failure_env="$2"
  local expected_before="$3"
  local expected_checkpoint="$4"
  local db_file log_first log_probe log_recovery
  db_file="$(mktemp "/tmp/nats_ingest_crash_${name}.XXXXXX.duckdb")"
  log_first="$(mktemp "/tmp/nats_ingest_crash_${name}.first.XXXXXX.log")"
  log_probe="$(mktemp "/tmp/nats_ingest_crash_${name}.probe.XXXXXX.log")"
  log_recovery="$(mktemp "/tmp/nats_ingest_crash_${name}.recovery.XXXXXX.log")"
  rm -f "$db_file"

  local env_status=0
  set +e
  env "$failure_env" DUCKDB_LIB="$DUCKDB_LIB" NATS_INGEST_DISABLE_REHYDRATE=1 \
    "$PYTHON_BIN" "$ROOT_DIR/scripts/duckdb_session.py" --duckdb-bin "$DUCKDB_BIN" --db-file "$db_file" \
    >"$log_first" 2>&1 <<SQL
SEND
LOAD '${EXTENSION_PATH}';
CREATE TABLE ingest_out(stream_name VARCHAR, subject VARCHAR, sequence UBIGINT, ts TIMESTAMP, payload BLOB);
SELECT 'started=' || job_name AS marker FROM nats_start_ingest(
    job_name := 'crash_${name}', stream_name := 'ingest_resume', target_table := 'ingest_out',
    durable_name := 'duckdb_crash_${name}', url := '${NATS_URL}', batch_size := 4,
    poll_ms := 10000, fetch_timeout_ms := 100, start_seq := 1
);
END
EXPECT started=crash_${name} 10
QUIT
SQL
  env_status=$?
  set -e
  if [ "$env_status" -eq 0 ]; then
    cat "$log_first" >&2
    echo "Expected injected crash at $name, but DuckDB exited 0" >&2
    return 1
  fi

  DUCKDB_LIB="$DUCKDB_LIB" NATS_INGEST_DISABLE_REHYDRATE=1 "$DUCKDB_BIN" -unsigned "$db_file" -c \
    "LOAD '${EXTENSION_PATH}'; SELECT 'count=' || COUNT(*) FROM ingest_out; SELECT 'checkpoint=' || COALESCE(MAX(last_committed_seq), 0) FROM duckdb_nats_ingest_checkpoints WHERE stream_name = 'ingest_resume' AND durable_name = 'duckdb_crash_${name}';" \
    >"$log_probe" 2>&1
  if ! grep -Fq "count=${expected_before}" "$log_probe" || ! grep -Fq "checkpoint=${expected_checkpoint}" "$log_probe"; then
    cat "$log_probe" >&2
    echo "Unexpected durable state at crash point $name" >&2
    return 1
  fi

  # Abrupt process death leaves the persisted ownership lease until its 30s TTL.
  sleep 32

  if ! DUCKDB_LIB="$DUCKDB_LIB" NATS_INGEST_DISABLE_REHYDRATE=1 \
    "$PYTHON_BIN" "$ROOT_DIR/scripts/duckdb_session.py" --duckdb-bin "$DUCKDB_BIN" --db-file "$db_file" \
    >"$log_recovery" 2>&1 <<SQL
SEND
LOAD '${EXTENSION_PATH}';
SELECT 'restarted=' || job_name AS marker FROM nats_start_ingest(
    job_name := 'recovered_${name}', stream_name := 'ingest_resume', target_table := 'ingest_out',
    durable_name := 'duckdb_crash_${name}', url := '${NATS_URL}', batch_size := 4,
    poll_ms := 100, fetch_timeout_ms := 100, start_seq := 1
);
END
EXPECT restarted=recovered_${name} 10
POLL recovered=4/4/false 45
SELECT 'recovered=' || rows_inserted || '/' || last_committed_seq || '/' || failed
FROM nats_ingest_status(job_name := 'recovered_${name}');
END
SEND
SELECT 'count=' || COUNT(*) FROM ingest_out;
SELECT * FROM nats_stop_ingest(job_name := 'recovered_${name}');
END
EXPECT count=4 10
QUIT
SQL
  then
    cat "$log_recovery" >&2
    return 1
  fi

  rm -f "$log_first" "$log_probe" "$log_recovery" "$db_file"
  echo "PASS ingest crash point: $name"
}

run_crash_case after_fetch NATS_INGEST_FAIL_AFTER_FETCH=1 0 0
run_crash_case after_append NATS_INGEST_FAIL_AFTER_APPEND=1 0 0
run_crash_case after_flush NATS_INGEST_FAIL_AFTER_FLUSH=1 0 0
run_crash_case after_commit NATS_INGEST_FAIL_AFTER_COMMIT=1 4 4

echo "Ingest crash-window matrix passed"
