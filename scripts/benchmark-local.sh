#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DUCKDB_BIN="${DUCKDB_BIN:-$ROOT_DIR/build/release/duckdb}"
EXTENSION_PATH="${EXTENSION_PATH:-$ROOT_DIR/build/release/extension/nats_js/nats_js.duckdb_extension}"
NATS_URL="${NATS_URL:-nats://127.0.0.1:4222}"
NATS_CLI="${NATS_CLI:-$HOME/nats}"
PYTHON_BIN="${PYTHON_BIN:-$ROOT_DIR/.venv/bin/python}"
PREPARE_FIXTURES="${PREPARE_FIXTURES:-0}"
FIXTURE_HOURS="${FIXTURE_HOURS:-2}"
FIXTURE_INTERVAL_SECONDS="${FIXTURE_INTERVAL_SECONDS:-60}"
OUT_FILE="${OUT_FILE:-}"

if [ ! -x "$DUCKDB_BIN" ]; then
  echo "DuckDB binary not found: $DUCKDB_BIN" >&2
  echo "Set DUCKDB_BIN=/path/to/duckdb or build first." >&2
  exit 1
fi

if [ ! -f "$EXTENSION_PATH" ]; then
  echo "Extension not found: $EXTENSION_PATH" >&2
  echo "Set EXTENSION_PATH=/path/to/nats_js.duckdb_extension or build first." >&2
  exit 1
fi

if [ ! -x "$NATS_CLI" ]; then
  NATS_CLI="$(command -v nats || true)"
fi

if [ -z "$NATS_CLI" ]; then
  echo "NATS CLI not found. Set NATS_CLI=/path/to/nats." >&2
  exit 1
fi

if [ ! -x "$PYTHON_BIN" ]; then
  PYTHON_BIN="$(command -v python3)"
fi

sql_escape() {
  printf "%s" "$1" | sed "s/'/''/g"
}

run_query() {
  local name="$1"
  local query="$2"
  local sql_file
  local output
  local start_ns
  local end_ns
  local elapsed_ms

  sql_file="$(mktemp)"
  {
    printf "LOAD '%s';\n" "$(sql_escape "$EXTENSION_PATH")"
    printf "%s\n" "$query"
  } > "$sql_file"

  start_ns="$(date +%s%N)"
  output="$("$DUCKDB_BIN" -unsigned -csv -noheader :memory: < "$sql_file")"
  end_ns="$(date +%s%N)"
  elapsed_ms="$(((end_ns - start_ns) / 1000000))"
  rm -f "$sql_file"

  printf '%s,%s,"%s"\n' "$name" "$elapsed_ms" "$(printf "%s" "$output" | tr '\n' ' ' | sed 's/"/""/g')"
}

if [ "$PREPARE_FIXTURES" = "1" ]; then
  echo "Preparing local JetStream fixtures at $NATS_URL" >&2
  NATS_URL="$NATS_URL" NATS_CLI="$NATS_CLI" RESET_STREAMS=1 "$ROOT_DIR/scripts/setup-streams.sh" >/dev/null
  "$PYTHON_BIN" "$ROOT_DIR/scripts/generate-telemetry.py" \
    --url "$NATS_URL" \
    --hours "$FIXTURE_HOURS" \
    --interval-seconds "$FIXTURE_INTERVAL_SECONDS" >/dev/null
  "$PYTHON_BIN" "$ROOT_DIR/test/proto/generate_protobuf_data.py" >/dev/null
fi

if [ -n "$OUT_FILE" ]; then
  exec > "$OUT_FILE"
fi

echo "benchmark,elapsed_ms,result"

run_query "stream_stats_count" "
SELECT messages
FROM nats_stream_stats('telemetry', url := '$NATS_URL');
"

run_query "stream_range_stats_clean" "
SELECT available_messages
FROM nats_stream_range_stats('telemetry_proto',
    url := '$NATS_URL',
    start_seq := 1,
    end_seq := 500
);
"

run_query "stream_range_stats_deleted_gaps" "
SELECT available_messages || '/' || deleted_in_range || '/' || has_gaps
FROM nats_stream_range_stats('stats_gaps',
    url := '$NATS_URL',
    start_seq := 1,
    end_seq := 10
);
"

run_query "scan_full_count" "
SELECT COUNT(*)
FROM nats_scan('telemetry',
    url := '$NATS_URL',
    batch_size := 4096
);
"

run_query "scan_bounded_range_count" "
SELECT COUNT(*)
FROM nats_scan('telemetry_proto',
    url := '$NATS_URL',
    start_seq := 1,
    end_seq := 500,
    batch_size := 4096
);
"

run_query "scan_bounded_deleted_gaps_count" "
SELECT COUNT(*)
FROM nats_scan('stats_gaps',
    url := '$NATS_URL',
    start_seq := 1,
    end_seq := 10,
    batch_size := 10,
    fetch_timeout_ms := 5000
);
"

run_query "scan_subject_pushdown_count" "
SELECT COUNT(*)
FROM nats_scan('telemetry_proto',
    url := '$NATS_URL',
    nats_subject := 'telemetry_proto.dc1.power.pm5560.*',
    batch_size := 4096
);
"

run_query "scan_json_projected_extract" "
SELECT COUNT(device_id) || '/' || ROUND(AVG(CAST(kw AS DOUBLE)), 3)
FROM nats_scan('telemetry',
    url := '$NATS_URL',
    json_extract := ['device_id', 'kw', 'voltage'],
    batch_size := 4096
);
"

run_query "scan_protobuf_projected_extract" "
SELECT COUNT(device_id) || '/' || ROUND(AVG(metrics_kw), 3)
FROM nats_scan('telemetry_proto',
    url := '$NATS_URL',
    proto_file := '$ROOT_DIR/test/proto/telemetry.proto',
    proto_message := 'Telemetry',
    proto_extract := ['device_id', 'metrics.kw', 'metrics.voltage'],
    batch_size := 4096
);
"
