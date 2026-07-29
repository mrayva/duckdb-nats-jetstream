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

"$NATS_CLI" server check connection --server "$NATS_URL"
NATS_URL="$NATS_URL" NATS_CLI="$NATS_CLI" RESET_STREAMS="${RESET_STREAMS:-1}" \
  "$ROOT_DIR/scripts/setup-streams.sh" >/dev/null

db_file="$(mktemp /tmp/nats_ingest_ownership.XXXXXX.duckdb)"
contender_db_file="$(mktemp /tmp/nats_ingest_ownership.contender.XXXXXX.duckdb)"
log_owner="$(mktemp /tmp/nats_ingest_ownership.owner.XXXXXX.log)"
log_contender="$(mktemp /tmp/nats_ingest_ownership.contender.XXXXXX.log)"
log_probe="$(mktemp /tmp/nats_ingest_ownership.probe.XXXXXX.log)"
rm -f "$db_file" "$contender_db_file"
owner_pid=""

cleanup() {
  local rc=$?
  if [ -n "$owner_pid" ] && kill -0 "$owner_pid" 2>/dev/null; then
    kill -TERM "$owner_pid" 2>/dev/null || true
    wait "$owner_pid" 2>/dev/null || true
  fi
  rm -f "$db_file" "$contender_db_file" "$log_owner" "$log_contender" "$log_probe"
  exit "$rc"
}
trap cleanup EXIT

LD_PRELOAD="$DUCKDB_LIB" DUCKDB_LIB="$DUCKDB_LIB" "$DUCKDB_BIN" -unsigned "$db_file" -c \
  "CREATE TABLE ingest_out(stream_name VARCHAR, subject VARCHAR, sequence UBIGINT, ts TIMESTAMP, payload BLOB);" >/dev/null

LD_PRELOAD="$DUCKDB_LIB" DUCKDB_LIB="$DUCKDB_LIB" NATS_INGEST_DELAY_BEFORE_ACK_MS=10000 \
  NATS_INGEST_DISABLE_REHYDRATE=1 python3 "$ROOT_DIR/scripts/duckdb_session.py" \
  --duckdb-bin "$DUCKDB_BIN" --db-file "$db_file" >"$log_owner" 2>&1 <<SQL &
SEND
LOAD '${EXTENSION_PATH}';
SELECT * FROM nats_start_ingest(
    job_name := 'ownership_owner', stream_name := 'ingest_resume', target_table := 'ingest_out',
    durable_name := 'duckdb_ingest_ownership', url := '${NATS_URL}', batch_size := 4,
    poll_ms := 100, fetch_timeout_ms := 100, start_seq := 1
);
END
SLEEP 6
SEND
SELECT * FROM nats_stop_ingest(job_name := 'ownership_owner');
END
QUIT
SQL
owner_pid=$!

sleep 5
set +e
LD_PRELOAD="$DUCKDB_LIB" DUCKDB_LIB="$DUCKDB_LIB" NATS_INGEST_DISABLE_REHYDRATE=1 \
  "$DUCKDB_BIN" -unsigned "$contender_db_file" -c \
  "LOAD '${EXTENSION_PATH}'; SELECT * FROM nats_start_ingest(job_name := 'ownership_contender', stream_name := 'ingest_resume', target_table := 'ingest_out', durable_name := 'duckdb_ingest_ownership', url := '${NATS_URL}', batch_size := 4, poll_ms := 100, fetch_timeout_ms := 100, start_seq := 1);" \
  >"$log_contender" 2>&1
contender_status=$?
set -e

if [ "$contender_status" -eq 0 ] || ! grep -Fq "Ingest lease for stream 'ingest_resume' and durable 'duckdb_ingest_ownership' is held by another process" "$log_contender"; then
  cat "$log_contender" >&2
  echo "Expected competing ingest job to be rejected by the lease" >&2
  exit 1
fi

wait "$owner_pid"
owner_pid=""

LD_PRELOAD="$DUCKDB_LIB" DUCKDB_LIB="$DUCKDB_LIB" NATS_INGEST_DISABLE_REHYDRATE=1 "$DUCKDB_BIN" -unsigned "$db_file" -c \
  "LOAD '${EXTENSION_PATH}'; SELECT 'rows=' || COUNT(*) FROM ingest_out; SELECT 'leases=' || COUNT(*) FROM duckdb_nats_ingest_leases;" \
  >"$log_probe" 2>&1
grep -Fq "rows=4" "$log_probe" || { cat "$log_probe" >&2; exit 1; }
grep -Fq "leases=1" "$log_probe" || { cat "$log_probe" >&2; exit 1; }

echo "Ingest ownership harness passed"
