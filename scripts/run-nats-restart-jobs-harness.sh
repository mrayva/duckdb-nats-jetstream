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
NATS_PORT="${NATS_PORT:-$((6000 + RANDOM % 1000))}"
NATS_URL="nats://127.0.0.1:${NATS_PORT}"

if [ ! -x "$DUCKDB_BIN" ] || [ ! -f "$EXTENSION_PATH" ] || [ ! -x "$NATS_SERVER" ]; then
  echo "DuckDB, extension, or nats-server binary is missing" >&2; exit 1
fi
if [ ! -x "$NATS_CLI" ]; then NATS_CLI="$(command -v nats || true)"; fi
if [ -z "$NATS_CLI" ]; then echo "NATS CLI not found" >&2; exit 1; fi

server_dir="$(mktemp -d /tmp/nats_restart_jobs_server.XXXXXX)"
server_log="$(mktemp /tmp/nats_restart_jobs_server.XXXXXX.log)"
db_file="$(mktemp /tmp/nats_restart_jobs.XXXXXX.duckdb)"
log_file="$(mktemp /tmp/nats_restart_jobs.XXXXXX.log)"
pid_file="${server_dir}/server.pid"
rm -f "$db_file"
server_pid=""
cleanup() {
  local rc=$?
  if [ -f "$pid_file" ]; then server_pid="$(<"$pid_file")"; fi
  if [ -n "$server_pid" ] && kill -0 "$server_pid" 2>/dev/null; then kill -TERM "$server_pid" 2>/dev/null || true; wait "$server_pid" 2>/dev/null || true; fi
  rm -rf "$server_dir" "$server_log" "$db_file" "$log_file"
  exit "$rc"
}
trap cleanup EXIT

start_server() {
  "$NATS_SERVER" -js --addr 127.0.0.1 -p "$NATS_PORT" -sd "$server_dir" >"$server_log" 2>&1 &
  server_pid=$!
  printf '%s\n' "$server_pid" >"$pid_file"
  for _ in $(seq 1 100); do
    if "$NATS_CLI" server check connection --server "$NATS_URL" >/dev/null 2>&1; then return 0; fi
    sleep 0.1
  done
  cat "$server_log" >&2; echo "NATS server did not become ready" >&2; return 1
}

start_server
"$NATS_CLI" stream add restart_ingest --subjects restart.ingest --storage file --retention limits \
  --max-msgs=-1 --max-bytes=-1 --max-age=1h --max-msg-size=1048576 --discard old \
  --dupe-window=2m --replicas=1 --server="$NATS_URL" --defaults >/dev/null
for value in one two; do "$NATS_CLI" pub restart.ingest "$value" --jetstream --server="$NATS_URL" >/dev/null; done

if ! DUCKDB_LIB="$DUCKDB_LIB" NATS_INGEST_DISABLE_REHYDRATE=1 \
  python3 "$ROOT_DIR/scripts/duckdb_session.py" --duckdb-bin "$DUCKDB_BIN" --db-file "$db_file" \
  >"$log_file" 2>&1 <<SQL
SEND
LOAD '${EXTENSION_PATH}';
CREATE TABLE ingest_out(stream_name VARCHAR, subject VARCHAR, sequence UBIGINT, ts TIMESTAMP, payload BLOB);
SELECT 'ingest_started=' || job_name FROM nats_start_ingest(job_name := 'restart_ingest_job',
    stream_name := 'restart_ingest', target_table := 'ingest_out', durable_name := 'restart_ingest_durable',
    url := '${NATS_URL}', batch_size := 2, poll_ms := 100, fetch_timeout_ms := 100, start_seq := 1);
SELECT 'subscribe_started=' || job_name FROM nats_start_subscribe(job_name := 'restart_subscribe_job',
    target_table := 'subscribe_out', url := '${NATS_URL}', subject := 'restart.core', batch_size := 2,
    poll_ms := 100, create_target_table := true);
END
EXPECT ingest_started=restart_ingest_job 10
EXPECT subscribe_started=restart_subscribe_job 10
POLL pre_restart=2 30
SELECT 'pre_restart=' || rows_inserted || '/' || ingest_connected || '/' || subscribe_connected
FROM (SELECT rows_inserted, connected AS ingest_connected FROM nats_ingest_status(job_name := 'restart_ingest_job')),
     (SELECT connected AS subscribe_connected FROM nats_subscribe_status(job_name := 'restart_subscribe_job'));
END
RUN bash -c 'kill -TERM "$(cat ${pid_file})"'
SLEEP 2
RUN bash -c '${NATS_SERVER} -js --addr 127.0.0.1 -p ${NATS_PORT} -sd ${server_dir} >${server_log} 2>&1 & echo $! > ${pid_file}'
SLEEP 3
RUN ${NATS_CLI} server check connection --server=${NATS_URL}
POLL reconnects=true/true/true/true 40
SELECT 'reconnects=' || (i.reconnect_count > 0) || '/' || i.connected || '/' ||
       (s.reconnect_count > 0) || '/' || s.connected
FROM (SELECT reconnect_count, connected FROM nats_ingest_status(job_name := 'restart_ingest_job')) i,
     (SELECT reconnect_count, connected FROM nats_subscribe_status(job_name := 'restart_subscribe_job')) s;
END
RUN ${NATS_CLI} pub restart.core after-restart --server=${NATS_URL}
RUN ${NATS_CLI} pub restart.ingest three --jetstream --server=${NATS_URL}
POLL final_counts=3/1/true/true 40
SELECT 'final_counts=' || i.rows_inserted || '/' || s.rows_inserted || '/' ||
       (i.reconnect_count > 0) || '/' || (s.reconnect_count > 0)
FROM (SELECT rows_inserted, reconnect_count FROM nats_ingest_status(job_name := 'restart_ingest_job')) i,
     (SELECT rows_inserted, reconnect_count FROM nats_subscribe_status(job_name := 'restart_subscribe_job')) s;
END
SEND
SELECT * FROM nats_stop_subscribe(job_name := 'restart_subscribe_job');
SELECT * FROM nats_stop_ingest(job_name := 'restart_ingest_job');
END
EXPECT pre_restart=2 10
EXPECT reconnects=true/true/true/true 10
EXPECT final_counts=3/1/true/true 10
QUIT
SQL
then
  cat "$log_file" >&2; cat "$server_log" >&2; exit 1
fi

echo "NATS restart active-job harness passed"
