#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
source "$ROOT_DIR/scripts/duckdb_host_lib.sh"

TIP_ROOT="${TIP_ROOT:-/tmp/duckdb-tip-clean}"
DUCKDB_BIN="${DUCKDB_BIN:-${COPY_DUCKDB_BIN:-$TIP_ROOT/build/release/duckdb}}"
DUCKDB_LIB="${DUCKDB_LIB:-$(duckdb_host_lib_resolve)}"
EXTENSION_PATH="${EXTENSION_PATH:-${COPY_EXTENSION_PATH:-$TIP_ROOT/build/nats_js-tip/extension/nats_js/nats_js.duckdb_extension}}"
NATS_URL="${NATS_URL:-nats://127.0.0.1:4222}"
NATS_CLI="${NATS_CLI:-$HOME/nats}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
RUN_BOTH_VARIANTS="${RUN_BOTH_VARIANTS:-1}"

if [ ! -x "$DUCKDB_BIN" ]; then
  echo "DuckDB binary not found: $DUCKDB_BIN" >&2
  exit 1
fi

if [ ! -f "$EXTENSION_PATH" ]; then
  echo "Extension not found: $EXTENSION_PATH" >&2
  exit 1
fi

if [ ! -x "$NATS_CLI" ]; then
  NATS_CLI="$(command -v nats || true)"
fi

if [ -z "$NATS_CLI" ]; then
  echo "NATS CLI not found. Set NATS_CLI=/path/to/nats." >&2
  exit 1
fi

if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  PYTHON_BIN="$(command -v python3)"
fi

sql_escape() {
  printf "%s" "$1" | sed "s/'/''/g"
}

bench_variant() {
  local variant="$1"
  local stream="$2"
  local table_name="$3"
  local source_sql="$4"
  local copy_options="$5"
  local expected_count="$6"
  local db_file
  local log_file
  local start_ns
  local end_ns
  local elapsed_ms
  local rows_per_sec

  db_file="$(mktemp /tmp/nats_copy_to_benchmark.XXXXXX.duckdb)"
  log_file="$(mktemp /tmp/nats_copy_to_benchmark.XXXXXX.log)"
  rm -f "$db_file"
  trap 'rc=$?; rm -f "$log_file" "$db_file"; exit $rc' RETURN

  # Reuse the configured stream for each case without carrying rows between variants.
  "$NATS_CLI" stream purge "$stream" --force --server "$NATS_URL" >/dev/null 2>&1 || true

  start_ns="$(date +%s%N)"
  if ! DUCKDB_LIB="$DUCKDB_LIB" python3 "$ROOT_DIR/scripts/duckdb_session.py" --duckdb-bin "$DUCKDB_BIN" --db-file "$db_file" <<SQL >"$log_file" 2>&1
SEND
LOAD '$(sql_escape "$EXTENSION_PATH")';
CREATE TABLE ${table_name} AS ${source_sql};
COPY ${table_name}
TO '${stream}'
(FORMAT nats_js, url '${NATS_URL}'${copy_options});
END
QUIT
SQL
  then
    cat "$log_file" >&2
    exit 1
  fi
  end_ns="$(date +%s%N)"
  elapsed_ms="$(((end_ns - start_ns) / 1000000))"
  local stream_count=""
  local deadline=$((SECONDS + 20))
  while [ "$SECONDS" -lt "$deadline" ]; do
    stream_count="$("$NATS_CLI" stream info "$stream" --server "$NATS_URL" 2>/dev/null | awk '/^[[:space:]]+Messages:/ {gsub(",", "", $NF); print $NF; exit}')"
    if [ "$stream_count" = "$expected_count" ]; then
      break
    fi
    sleep 1
  done

  if [ "$stream_count" != "$expected_count" ]; then
    echo "Timed out waiting for stream $stream to reach $expected_count messages (got ${stream_count:-unknown})" >&2
    tail -n 80 "$log_file" >&2
    exit 1
  fi

  rows_per_sec="$(awk -v ms="$elapsed_ms" -v rows="$expected_count" 'BEGIN { if (ms <= 0) { print "inf"; } else { printf "%.2f", rows / (ms / 1000.0); } }')"

  printf '%s,%s,%s,%s\n' "$variant" "$elapsed_ms" "$expected_count" "$rows_per_sec"

  rm -f "$log_file" "$db_file"
  trap - RETURN
}

echo "variant,elapsed_ms,rows,publish_rows_per_sec"

echo "Checking NATS connection at $NATS_URL" >&2
"$NATS_CLI" server check connection --server "$NATS_URL" >&2

echo "Preparing JetStream streams" >&2
NATS_URL="$NATS_URL" NATS_CLI="$NATS_CLI" RESET_STREAMS="${RESET_STREAMS:-1}" "$ROOT_DIR/scripts/setup-streams.sh" >&2

bench_variant \
  "subject_payload" \
  "copy_out" \
  "copy_bench_subject" \
  "SELECT 'copy_out.' || CASE WHEN i % 2 = 0 THEN 'alpha' ELSE 'beta' END AS subject, CASE WHEN i % 2 = 0 THEN 'hello' ELSE 'world' END AS payload FROM range(1000) t(i)" \
  "" \
  "1000"

if [ "$RUN_BOTH_VARIANTS" = "1" ]; then
  bench_variant \
    "constant_subject" \
    "copy_roundtrip_const" \
    "copy_bench_constant" \
    "SELECT CASE WHEN i % 2 = 0 THEN 'gamma' ELSE 'delta' END AS payload FROM range(1000) t(i)" \
    ", subject 'copy_roundtrip_const.constant'" \
    "1000"
fi

structured_source="SELECT 'copy_roundtrip.bench' AS subject, 'device-' || i AS device_id,
    i::DOUBLE AS kw, (i % 2 = 0) AS online FROM range(1000) t(i)"

bench_variant \
  "structured_json" \
  "copy_roundtrip" \
  "copy_bench_json" \
  "$structured_source" \
  ", payload_format 'json', payload_columns ['device_id', 'kw', 'online']" \
  "1000"

bench_variant \
  "structured_msgpack" \
  "copy_roundtrip" \
  "copy_bench_msgpack" \
  "$structured_source" \
  ", payload_format 'msgpack', payload_columns ['device_id', 'kw', 'online']" \
  "1000"

bench_variant \
  "structured_cbor" \
  "copy_roundtrip" \
  "copy_bench_cbor" \
  "$structured_source" \
  ", payload_format 'cbor', payload_columns ['device_id', 'kw', 'online']" \
  "1000"

bench_variant \
  "structured_flexbuffers" \
  "copy_roundtrip" \
  "copy_bench_flexbuffers" \
  "$structured_source" \
  ", payload_format 'flexbuffers', payload_columns ['device_id', 'kw', 'online']" \
  "1000"

bench_variant \
  "structured_protobuf" \
  "copy_roundtrip" \
  "copy_bench_protobuf" \
  "$structured_source" \
  ", payload_format 'protobuf', proto_file '${ROOT_DIR}/test/proto/telemetry.proto', proto_message 'Telemetry', proto_fields ['device_id', 'metrics.kw', 'online'], payload_columns ['device_id', 'kw', 'online']" \
  "1000"
