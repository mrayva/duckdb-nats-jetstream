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
if [ ! -x "$NATS_CLI" ]; then NATS_CLI="$(command -v nats || true)"; fi
if [ -z "$NATS_CLI" ]; then echo "NATS CLI not found" >&2; exit 1; fi

"$NATS_CLI" server check connection --server "$NATS_URL"
NATS_URL="$NATS_URL" NATS_CLI="$NATS_CLI" RESET_STREAMS="${RESET_STREAMS:-1}" \
  "$ROOT_DIR/scripts/setup-streams.sh" >/dev/null

db_file="$(mktemp /tmp/nats_ingest_stale_lease.XXXXXX.duckdb)"
owner_log="$(mktemp /tmp/nats_ingest_stale_owner.XXXXXX.log)"
blocked_log="$(mktemp /tmp/nats_ingest_stale_blocked.XXXXXX.log)"
recovery_log="$(mktemp /tmp/nats_ingest_stale_recovery.XXXXXX.log)"
rm -f "$db_file"
owner_pid=""
cleanup() {
  local rc=$?
  if [ -n "$owner_pid" ] && kill -0 "$owner_pid" 2>/dev/null; then kill -KILL "$owner_pid" 2>/dev/null || true; wait "$owner_pid" 2>/dev/null || true; fi
  rm -f "$db_file" "$owner_log" "$blocked_log" "$recovery_log"
  exit "$rc"
}
trap cleanup EXIT

DUCKDB_LIB="$DUCKDB_LIB" "$DUCKDB_BIN" -unsigned "$db_file" -c \
  "LOAD '${EXTENSION_PATH}'; CREATE TABLE ingest_out(stream_name VARCHAR, subject VARCHAR, sequence UBIGINT, ts TIMESTAMP, payload BLOB);" >/dev/null

DUCKDB_LIB="$DUCKDB_LIB" NATS_INGEST_DISABLE_REHYDRATE=1 \
  python3 "$ROOT_DIR/scripts/duckdb_session.py" --duckdb-bin "$DUCKDB_BIN" --db-file "$db_file" \
  >"$owner_log" 2>&1 <<SQL &
SEND
LOAD '${EXTENSION_PATH}';
SELECT * FROM nats_start_ingest(job_name := 'stale_owner', stream_name := 'ingest_resume',
    target_table := 'ingest_out', durable_name := 'duckdb_stale_lease', url := '${NATS_URL}',
    batch_size := 4, poll_ms := 100, fetch_timeout_ms := 100, start_seq := 1);
END
EXPECT stale_owner 10
SLEEP 60
QUIT
SQL
owner_pid=$!

for _ in $(seq 1 100); do
  if grep -q "stale_owner" "$owner_log"; then break; fi
  if ! kill -0 "$owner_pid" 2>/dev/null; then cat "$owner_log" >&2; echo "Owner exited before acquiring lease" >&2; exit 1; fi
  sleep 0.1
done
if ! grep -q "stale_owner" "$owner_log"; then cat "$owner_log" >&2; echo "Owner did not start" >&2; exit 1; fi

# Abrupt death releases DuckDB's file lock but leaves the persisted ownership lease.
kill -KILL "$owner_pid" 2>/dev/null || true
wait "$owner_pid" 2>/dev/null || true
owner_pid=""

set +e
DUCKDB_LIB="$DUCKDB_LIB" NATS_INGEST_DISABLE_REHYDRATE=1 "$DUCKDB_BIN" -unsigned "$db_file" -c \
  "LOAD '${EXTENSION_PATH}'; SELECT * FROM nats_start_ingest(job_name := 'stale_blocked', stream_name := 'ingest_resume', target_table := 'ingest_out', durable_name := 'duckdb_stale_lease', url := '${NATS_URL}', batch_size := 4, poll_ms := 100, fetch_timeout_ms := 100, start_seq := 1);" \
  >"$blocked_log" 2>&1
blocked_status=$?
set -e
if [ "$blocked_status" -eq 0 ] || ! grep -Fq "Ingest lease for stream 'ingest_resume' and durable 'duckdb_stale_lease' is held by another process" "$blocked_log"; then
  cat "$blocked_log" >&2
  echo "Expected stale owner lease to fence immediate takeover" >&2
  exit 1
fi

# Lease TTL is 30 seconds; wait past it, then prove a new process can recover.
sleep 32
if ! DUCKDB_LIB="$DUCKDB_LIB" NATS_INGEST_DISABLE_REHYDRATE=1 \
  python3 "$ROOT_DIR/scripts/duckdb_session.py" --duckdb-bin "$DUCKDB_BIN" --db-file "$db_file" \
  >"$recovery_log" 2>&1 <<SQL
SEND
LOAD '${EXTENSION_PATH}';
SELECT 'recovered=' || job_name FROM nats_start_ingest(job_name := 'stale_recovered',
    stream_name := 'ingest_resume', target_table := 'ingest_out', durable_name := 'duckdb_stale_lease',
    url := '${NATS_URL}', batch_size := 4, poll_ms := 100, fetch_timeout_ms := 100, start_seq := 1);
END
EXPECT recovered=stale_recovered 10
SLEEP 2
SEND
SELECT * FROM nats_stop_ingest(job_name := 'stale_recovered');
END
QUIT
SQL
then
  cat "$recovery_log" >&2
  exit 1
fi

echo "Ingest stale-lease fencing harness passed"
