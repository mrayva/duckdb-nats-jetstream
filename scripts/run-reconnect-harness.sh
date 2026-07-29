#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
source "$ROOT_DIR/scripts/duckdb_host_lib.sh"

TIP_ROOT="${TIP_ROOT:-/tmp/duckdb-tip-clean}"
DUCKDB_BIN="${DUCKDB_BIN:-${TIP_DUCKDB_BIN:-$TIP_ROOT/build/release/duckdb}}"
DUCKDB_LIB="${DUCKDB_LIB:-$(duckdb_host_lib_resolve)}"
EXTENSION_PATH="${EXTENSION_PATH:-${TIP_EXTENSION_PATH:-$TIP_ROOT/build/nats_js-tip/extension/nats_js/nats_js.duckdb_extension}}"
NATS_CLI="${NATS_CLI:-$HOME/nats}"
NATS_SERVER="${NATS_SERVER:-/home/mrayva/nats-server}"
NATS_PORT="${NATS_PORT:-$((4000 + RANDOM % 1000))}"
NATS_URL="nats://127.0.0.1:${NATS_PORT}"

if [ ! -x "$DUCKDB_BIN" ] || [ ! -f "$EXTENSION_PATH" ] || [ ! -x "$NATS_SERVER" ]; then
  echo "DuckDB, extension, or nats-server binary is missing" >&2
  exit 1
fi

server_dir="$(mktemp -d /tmp/nats_reconnect_server.XXXXXX)"
server_log="$(mktemp /tmp/nats_reconnect_server.XXXXXX.log)"
db_file="$(mktemp /tmp/nats_reconnect.XXXXXX.duckdb)"
log_file="$(mktemp /tmp/nats_reconnect.XXXXXX.log)"
rm -f "$db_file"
server_pid=""

cleanup() {
  local rc=$?
  if [ -n "$server_pid" ] && kill -0 "$server_pid" 2>/dev/null; then
    kill -TERM "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
  fi
  rm -rf "$server_dir" "$server_log" "$db_file" "$log_file"
  exit "$rc"
}
trap cleanup EXIT

start_server() {
  "$NATS_SERVER" -js --addr 127.0.0.1 -p "$NATS_PORT" -sd "$server_dir" >"$server_log" 2>&1 &
  server_pid=$!
  sleep 0.2
  if ! kill -0 "$server_pid" 2>/dev/null; then
    cat "$server_log" >&2
    echo "NATS server exited during startup" >&2
    return 1
  fi
  for _ in $(seq 1 50); do
    if "$NATS_CLI" server check connection --server "$NATS_URL" >/dev/null 2>&1; then
      return
    fi
    sleep 0.1
  done
  cat "$server_log" >&2
  echo "NATS server did not become ready" >&2
  return 1
}

if [ ! -x "$NATS_CLI" ]; then
  NATS_CLI="$(command -v nats || true)"
fi
if [ -z "$NATS_CLI" ]; then
  echo "NATS CLI not found" >&2
  exit 1
fi

start_server
"$NATS_CLI" stream add reconnect_test --subjects reconnect.test --storage file --retention limits \
  --max-msgs=-1 --max-bytes=-1 --max-age=1h --max-msg-size=1048576 --discard old \
  --dupe-window=2m --replicas=1 --server="$NATS_URL" --defaults >/dev/null
"$NATS_CLI" pub reconnect.test before --jetstream --server="$NATS_URL" >/dev/null
"$NATS_CLI" pub reconnect.core before --server="$NATS_URL" >/dev/null

if ! DUCKDB_LIB="$DUCKDB_LIB" NATS_INGEST_DISABLE_REHYDRATE=1 \
  python3 "$ROOT_DIR/scripts/duckdb_session.py" --duckdb-bin "$DUCKDB_BIN" --db-file "$db_file" <<SQL >"$log_file" 2>&1
SEND
LOAD '${EXTENSION_PATH}';
CREATE TABLE ingest_out(stream_name VARCHAR, subject VARCHAR, sequence UBIGINT, ts TIMESTAMP, payload BLOB);
CREATE TABLE subscribe_out(subject VARCHAR, payload VARCHAR, received_at TIMESTAMP);
SELECT * FROM nats_start_ingest(
    job_name := 'reconnect_ingest', stream_name := 'reconnect_test', target_table := 'ingest_out',
    durable_name := 'reconnect_durable', url := '${NATS_URL}', batch_size := 1,
    poll_ms := 100, fetch_timeout_ms := 100, start_seq := 1
);
SELECT * FROM nats_start_subscribe(
    job_name := 'reconnect_subscribe', target_table := 'subscribe_out', subject := 'reconnect.core',
    url := '${NATS_URL}', batch_size := 1, poll_ms := 100
);
END
SLEEP 1
RUN kill -TERM ${server_pid}
SLEEP 2
RUN bash -c '${NATS_SERVER} -js --addr 127.0.0.1 -p ${NATS_PORT} -sd ${server_dir} >${server_log} 2>&1 &'
SLEEP 3
RUN ${NATS_CLI} server check connection --server=${NATS_URL}
RUN ${NATS_CLI} pub reconnect.test after --jetstream --server=${NATS_URL}
RUN ${NATS_CLI} pub reconnect.core after --server=${NATS_URL}
SLEEP 3
SLEEP 1
SEND
SELECT 'ingest_count=' || COUNT(*) FROM ingest_out;
SELECT 'ingest_reconnect=' || (reconnect_count > 0) FROM nats_ingest_status(job_name := 'reconnect_ingest');
SELECT 'subscribe_count=' || COUNT(*) FROM subscribe_out;
SELECT 'subscribe_reconnect=' || (reconnect_count > 0) FROM nats_subscribe_status(job_name := 'reconnect_subscribe');
SELECT * FROM nats_stop_ingest(job_name := 'reconnect_ingest');
SELECT * FROM nats_stop_subscribe(job_name := 'reconnect_subscribe');
END
EXPECT ingest_count=2 10
EXPECT ingest_reconnect=true 10
EXPECT subscribe_count=1 10
EXPECT subscribe_reconnect=true 10
QUIT
SQL
then
  cat "$log_file" >&2
  exit 1
fi

echo "Reconnect harness passed"
