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
NATS_PORT="${NATS_PORT:-$((5000 + RANDOM % 1000))}"
NATS_URL="nats://127.0.0.1:${NATS_PORT}"

if [ ! -x "$DUCKDB_BIN" ] || [ ! -f "$EXTENSION_PATH" ] || [ ! -x "$NATS_SERVER" ]; then
  echo "DuckDB, extension, or nats-server binary is missing" >&2
  exit 1
fi
if [ ! -x "$NATS_CLI" ]; then
  NATS_CLI="$(command -v nats || true)"
fi
if [ -z "$NATS_CLI" ]; then
  echo "NATS CLI not found" >&2
  exit 1
fi

server_dir="$(mktemp -d /tmp/nats_exactly_once_server.XXXXXX)"
server_log="$(mktemp /tmp/nats_exactly_once_server.XXXXXX.log)"
db_file="$(mktemp /tmp/nats_exactly_once.XXXXXX.duckdb)"
log_file="$(mktemp /tmp/nats_exactly_once.XXXXXX.log)"
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

start_server
"$NATS_CLI" stream add exactly_once_test --subjects exactly.once --storage file --retention limits \
  --max-msgs=-1 --max-bytes=-1 --max-age=1h --max-msg-size=1048576 --discard old \
  --dupe-window=2m --replicas=1 --server="$NATS_URL" --defaults >/dev/null
for value in one two three four; do
  "$NATS_CLI" pub exactly.once "$value" --jetstream --server="$NATS_URL" >/dev/null
done

if ! DUCKDB_LIB="$DUCKDB_LIB" NATS_INGEST_ACK_WAIT_MS=1000 NATS_INGEST_DELAY_BEFORE_ACK_MS=6000 NATS_INGEST_DISABLE_REHYDRATE=1 \
  python3 "$ROOT_DIR/scripts/duckdb_session.py" --duckdb-bin "$DUCKDB_BIN" --db-file "$db_file" <<SQL >"$log_file" 2>&1
SEND
LOAD '${EXTENSION_PATH}';
CREATE TABLE ingest_out(stream_name VARCHAR, subject VARCHAR, sequence UBIGINT, ts TIMESTAMP, payload BLOB);
SELECT * FROM nats_start_ingest(
    job_name := 'exactly_once_ingest', stream_name := 'exactly_once_test', target_table := 'ingest_out',
    durable_name := 'exactly_once_durable', url := '${NATS_URL}', batch_size := 4,
    poll_ms := 100, fetch_timeout_ms := 100, start_seq := 1
);
END
SLEEP 1
RUN kill -TERM ${server_pid}
SLEEP 2
RUN bash -c '${NATS_SERVER} -js --addr 127.0.0.1 -p ${NATS_PORT} -sd ${server_dir} >${server_log} 2>&1 &'
SLEEP 6
RUN ${NATS_CLI} server check connection --server=${NATS_URL}
POLL exactly_once=true/true 30
SELECT 'exactly_once=' || (COUNT(*) = 4) || '/' || (MAX(duplicates_skipped) > 0) AS result
FROM ingest_out, (SELECT MAX(duplicates_skipped) AS duplicates_skipped
                  FROM nats_ingest_status(job_name := 'exactly_once_ingest'));
SELECT * FROM nats_stop_ingest(job_name := 'exactly_once_ingest');
END
EXPECT exactly_once=true/true 10
QUIT
SQL
then
  cat "$log_file" >&2
  exit 1
fi

echo "Exactly-once ingest harness passed"
