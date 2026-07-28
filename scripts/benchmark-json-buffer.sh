#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
source "$ROOT_DIR/scripts/duckdb_host_lib.sh"
TIP_ROOT="${TIP_ROOT:-/tmp/duckdb-tip-clean}"
DUCKDB_BIN="${DUCKDB_BIN:-${TIP_DUCKDB_BIN:-$TIP_ROOT/build/release/duckdb}}"
DUCKDB_LIB="${DUCKDB_LIB:-$(duckdb_host_lib_resolve)}"
EXTENSION_PATH="${EXTENSION_PATH:-${TIP_EXTENSION_PATH:-$TIP_ROOT/build/nats_js-tip/extension/nats_js/nats_js.duckdb_extension}}"
NATS_URL="${NATS_URL:-nats://127.0.0.1:4222}"
NATS_CLI="${NATS_CLI:-$HOME/nats}"
MESSAGE_COUNT="${MESSAGE_COUNT:-2000}"
BATCH_SIZE="${BATCH_SIZE:-128}"
CASE_ID="${CASE_ID:-$(date +%s)_$$}"

if [ ! -x "$DUCKDB_BIN" ] || [ ! -f "$EXTENSION_PATH" ]; then
  echo "Set DUCKDB_BIN and EXTENSION_PATH to a built DuckDB tip extension." >&2
  exit 1
fi

if [ ! -x "$NATS_CLI" ]; then
  NATS_CLI="$(command -v nats || true)"
fi
if [ -z "$NATS_CLI" ]; then
  echo "NATS CLI not found. Set NATS_CLI=/path/to/nats." >&2
  exit 1
fi

sql_escape() {
  printf "%s" "$1" | sed "s/'/''/g"
}

prepare_stream() {
  local stream="$1"
  "$NATS_CLI" stream rm "$stream" --force --server="$NATS_URL" >/dev/null 2>&1 || true
  "$NATS_CLI" stream add "$stream" \
    --subjects "${stream}.>" \
    --storage file \
    --retention limits \
    --max-msgs=-1 \
    --max-bytes=-1 \
    --max-age=7d \
    --discard old \
    --replicas=1 \
    --server="$NATS_URL" \
    --defaults >/dev/null
  "$NATS_CLI" pub --jetstream --quiet --count "$MESSAGE_COUNT" --server="$NATS_URL" \
    "${stream}.items" '{"value":"json-benchmark-{{Count}}","nested":{"ok":true}}' >/dev/null
}

bench_mode() {
  local mode="$1"
  local stream="json_buffer_${CASE_ID}_${mode}"
  local durable="duckdb_json_buffer_${CASE_ID}_${mode}"
  local job="json_buffer_${CASE_ID}_${mode}"
  local db_file log_file start_ns end_ns total_ms result rows_per_sec profile_line
  local fetch_ms row_ms append_ms flush_ms checkpoint_ms commit_ms persist_ms

  prepare_stream "$stream"
  db_file="$(mktemp /tmp/nats_json_buffer.XXXXXX.duckdb)"
  log_file="$(mktemp /tmp/nats_json_buffer.XXXXXX.log)"
  rm -f "$db_file"
  trap 'rm -f "$log_file" "$db_file"; "$NATS_CLI" stream rm "'$stream'" --force --server="'$NATS_URL'" >/dev/null 2>&1 || true' RETURN

  local session_plan
  session_plan=$(cat <<SQL
SEND
LOAD '$(sql_escape "$EXTENSION_PATH")';
CREATE TABLE ingest_out(
    stream_name VARCHAR,
    subject VARCHAR,
    sequence UBIGINT,
    ts TIMESTAMP,
    payload VARCHAR,
    value VARCHAR,
    nested VARCHAR
);
SELECT 'start=' || job_name || '|' || stream_name AS start_result
FROM nats_start_ingest(
    job_name := '$(sql_escape "$job")',
    stream_name := '$(sql_escape "$stream")',
    target_table := 'ingest_out',
    durable_name := '$(sql_escape "$durable")',
    url := '$(sql_escape "$NATS_URL")',
    batch_size := ${BATCH_SIZE},
    poll_ms := 10,
    fetch_timeout_ms := 100,
    start_seq := 1,
    json_extract := ['value', 'nested']
);
END
EXPECT start=${job}|${stream} 10
POLL complete=${MESSAGE_COUNT}/${MESSAGE_COUNT} 60
SELECT 'complete=' || rows_inserted || '/' || last_committed_seq AS ingest_status
FROM nats_ingest_status(job_name := '$(sql_escape "$job")');
END
SEND
SELECT 'count=' || COUNT(*) AS inserted_rows FROM ingest_out;
SELECT 'json_values=' || COUNT(*) AS valid_json_rows
FROM ingest_out
WHERE value LIKE 'json-benchmark-%' AND nested = '{"ok":true}';
SELECT 'stop=' || job_name AS stop_result FROM nats_stop_ingest(job_name := '$(sql_escape "$job")');
END
EXPECT count=${MESSAGE_COUNT} 10
EXPECT json_values=${MESSAGE_COUNT} 10
EXPECT stop=${job} 10
QUIT
SQL
)

  start_ns="$(date +%s%N)"
  set +e
  NATS_INGEST_DISABLE_REHYDRATE=1 NATS_INGEST_PROFILE=1 NATS_INGEST_JSON_BUFFER_MODE=reuse \
    NATS_INGEST_JSON_WRITE_MODE="$mode" \
    LD_PRELOAD="${LD_PRELOAD:-$DUCKDB_LIB}" \
    DUCKDB_LIB="$DUCKDB_LIB" \
    python3 "$ROOT_DIR/scripts/duckdb_session.py" --duckdb-bin "$DUCKDB_BIN" --db-file "$db_file" \
    <<<"$session_plan" >"$log_file" 2>&1
  local status=$?
  set -e
  if [ "$status" -ne 0 ]; then
    cat "$log_file" >&2
    exit "$status"
  fi
  end_ns="$(date +%s%N)"
  total_ms="$(((end_ns - start_ns) / 1000000))"
  result="$(grep -o "complete=${MESSAGE_COUNT}/${MESSAGE_COUNT}" "$log_file" | tail -n 1)"
  profile_line="$(grep 'NATS_INGEST_PROFILE ' "$log_file" | tail -n 1)"
  if [ -z "$profile_line" ]; then
    echo "Missing ingest profile for mode $mode" >&2
    tail -n 80 "$log_file" >&2
    exit 1
  fi
  fetch_ms="$(sed -n 's/.*fetch_ms=\([^ ]*\).*/\1/p' <<<"$profile_line")"
  row_ms="$(sed -n 's/.*row_ms=\([^ ]*\).*/\1/p' <<<"$profile_line")"
  append_ms="$(sed -n 's/.*append_ms=\([^ ]*\).*/\1/p' <<<"$profile_line")"
  flush_ms="$(sed -n 's/.*flush_ms=\([^ ]*\).*/\1/p' <<<"$profile_line")"
  checkpoint_ms="$(sed -n 's/.*checkpoint_ms=\([^ ]*\).*/\1/p' <<<"$profile_line")"
  commit_ms="$(sed -n 's/.*commit_ms=\([^ ]*\).*/\1/p' <<<"$profile_line")"
  persist_ms="$(sed -n 's/.*persist_ms=\([^ ]*\).*/\1/p' <<<"$profile_line")"
  rows_per_sec="$(awk -v ms="$total_ms" -v rows="$MESSAGE_COUNT" 'BEGIN { if (ms <= 0) print "inf"; else printf "%.2f", rows / (ms / 1000.0); }')"
  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' "$mode" "$total_ms" "$MESSAGE_COUNT" "$rows_per_sec" "$result" \
    "$fetch_ms" "$row_ms" "$append_ms" "$flush_ms" "$checkpoint_ms" "$commit_ms" "$persist_ms"

  rm -f "$log_file" "$db_file"
  "$NATS_CLI" stream rm "$stream" --force --server="$NATS_URL" >/dev/null 2>&1 || true
  trap - RETURN
}

echo "Checking NATS connection at $NATS_URL" >&2
"$NATS_CLI" server check connection --server "$NATS_URL" >&2
echo "write_mode,total_ms,rows_inserted,approx_rows_per_sec,status,fetch_ms,row_ms,append_ms,flush_ms,checkpoint_ms,commit_ms,persist_ms"
current_result="$(bench_mode current)"
direct_result="$(bench_mode direct)"
printf '%s\n%s\n' "$current_result" "$direct_result"

current_ms="$(printf '%s\n' "$current_result" | awk -F, '{print $2}')"
direct_ms="$(printf '%s\n' "$direct_result" | awk -F, '{print $2}')"
awk -v current="$current_ms" -v direct="$direct_ms" 'BEGIN {
  if (current <= 0 || direct <= 0) exit 0;
  printf "direct_speedup_vs_current,%.2fx\n", current / direct;
}'
