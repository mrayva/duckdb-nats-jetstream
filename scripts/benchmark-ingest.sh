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
HARNESS_MODE="${HARNESS_MODE:-resume}"
RUN_BOTH_MODES="${RUN_BOTH_MODES:-1}"

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

case "$HARNESS_MODE" in
  resume)
    stream_name="ingest_resume"
    durable_name="duckdb_ingest_resume"
    start_job_a="ingest_probe3_a"
    start_job_b="ingest_probe3_b"
    ;;
  redelivery)
    stream_name="ingest_redelivery"
    durable_name="duckdb_ingest_redelivery"
    start_job_a="ingest_redelivery_a"
    start_job_b="ingest_redelivery_b"
    ;;
  *)
    echo "Unknown HARNESS_MODE: $HARNESS_MODE (expected resume or redelivery)" >&2
    exit 1
    ;;
esac

sql_escape() {
  printf "%s" "$1" | sed "s/'/''/g"
}

bench_mode() {
  local mode="$1"
  local stream="$2"
  local durable="$3"
  local job_a="$4"
  local db_file
  local log_file
  local start_ns
  local end_ns
  local total_ms
  local result

  db_file="$(mktemp /tmp/nats_ingest_benchmark.XXXXXX.duckdb)"
  log_file="$(mktemp /tmp/nats_ingest_benchmark.XXXXXX.log)"
  rm -f "$db_file"
  trap 'rm -f "$log_file" "$db_file"' RETURN

  local session_plan
  session_plan=$(cat <<SQL
SEND
LOAD '$(sql_escape "$EXTENSION_PATH")';
CREATE TABLE ingest_out(stream_name VARCHAR, subject VARCHAR, sequence UBIGINT, ts TIMESTAMP, payload BLOB);
SELECT 'start1=' || job_name || '|' || stream_name || '|' || target_table || '|' || durable_name AS start_result
FROM nats_start_ingest(
    job_name := '${job_a}',
    stream_name := '${stream}',
    target_table := 'ingest_out',
    durable_name := '${durable}',
    url := '${NATS_URL}',
    batch_size := 4,
    poll_ms := 10000,
    fetch_timeout_ms := 100,
    start_seq := 1
);
END
SQL
)
session_plan+=$'\n'
  session_plan+=$(cat <<SQL
EXPECT start1=${job_a}|${stream}|ingest_out|${durable} 10
POLL status1=4/1/4 10
SELECT 'status1=' || rows_inserted || '/' || batches_committed || '/' || last_committed_seq AS ingest_status
FROM nats_ingest_status(job_name := '${job_a}');
END
RUN env -u LD_PRELOAD ${NATS_CLI} pub --jetstream --quiet --count 4 --server=${NATS_URL} ${stream}.items benchmark-test-{{Count}}
POLL status2=8/2/8 10
SELECT 'status2=' || rows_inserted || '/' || batches_committed || '/' || last_committed_seq AS ingest_status
FROM nats_ingest_status(job_name := '${job_a}');
END
SEND
SELECT 'count=' || COUNT(*) AS inserted_rows FROM ingest_out;
SELECT 'stop2=' || job_name || '|' || stream_name || '|' || target_table || '|' || durable_name AS stop_result
FROM nats_stop_ingest(job_name := '${job_a}');
END
EXPECT count=8 10
EXPECT stop2=${job_a}|${stream}|ingest_out|${durable} 10
QUIT
SQL
)

  start_ns="$(date +%s%N)"
  set +e
  NATS_INGEST_DISABLE_REHYDRATE=1 DUCKDB_LIB="$DUCKDB_LIB" python3 "$ROOT_DIR/scripts/duckdb_session.py" \
    --duckdb-bin "$DUCKDB_BIN" --db-file "$db_file" <<<"$session_plan" >"$log_file" 2>&1
  local duckdb_status=$?
  set -e
  if [ "$duckdb_status" -ne 0 ]; then
    cat "$log_file" >&2
    exit "$duckdb_status"
  fi
  end_ns="$(date +%s%N)"
  total_ms="$(((end_ns - start_ns) / 1000000))"

  if ! grep -Fq "status2=8/2/8" "$log_file"; then
    echo "Missing expected final status for mode $mode" >&2
    tail -n 80 "$log_file" >&2
    exit 1
  fi

  if ! grep -Fq "count=8" "$log_file"; then
    echo "Missing expected final row count for mode $mode" >&2
    tail -n 80 "$log_file" >&2
    exit 1
  fi

  result="$(grep -o 'status2=[0-9/]*' "$log_file" | tail -n 1)"
  rows_per_sec="$(awk -v ms="$total_ms" 'BEGIN { if (ms <= 0) { print "inf"; } else { printf "%.2f", 8000 / (ms / 1000.0); } }')"
  printf '%s,%s,%s,%s,%s,%s\n' "$mode" "$total_ms" "8" "$rows_per_sec" "$result" "single_session"

  rm -f "$log_file" "$db_file"
  trap - RETURN
}

echo "mode,total_ms,rows_inserted,approx_rows_per_sec,status,driver"

echo "Checking NATS connection at $NATS_URL" >&2
"$NATS_CLI" server check connection --server "$NATS_URL" >&2

echo "Preparing JetStream streams" >&2
NATS_URL="$NATS_URL" NATS_CLI="$NATS_CLI" RESET_STREAMS="${RESET_STREAMS:-1}" "$ROOT_DIR/scripts/setup-streams.sh" >&2

if [ "$RUN_BOTH_MODES" = "1" ]; then
  bench_mode "resume" "ingest_resume" "duckdb_ingest_resume" "ingest_probe3_a" "ingest_probe3_b"
  bench_mode "redelivery" "ingest_redelivery" "duckdb_ingest_redelivery" "ingest_redelivery_a" "ingest_redelivery_b"
else
  bench_mode "$HARNESS_MODE" "$stream_name" "$durable_name" "$start_job_a" "$start_job_b"
fi
