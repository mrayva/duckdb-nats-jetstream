#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DUCKDB_BIN="${DUCKDB_BIN:-/home/mrayva/.duckdb/cli/1.5.3/duckdb}"
EXTENSION_PATH="${EXTENSION_PATH:-$ROOT_DIR/build/release/extension/nats_js/nats_js.duckdb_extension}"
NATS_URL="${NATS_URL:-nats://127.0.0.1:4222}"
NATS_CLI="${NATS_CLI:-$HOME/nats}"
PYTHON_BIN="${PYTHON_BIN:-$ROOT_DIR/.venv/bin/python}"

if [ ! -x "$DUCKDB_BIN" ]; then
  echo "DuckDB binary not found: $DUCKDB_BIN" >&2
  echo "Build first with: make release" >&2
  exit 1
fi

if [ ! -f "$EXTENSION_PATH" ]; then
  echo "Extension not found: $EXTENSION_PATH" >&2
  echo "Build first with: make release" >&2
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

run_sql_test() {
  local test_file="$1"
  local log_file="$2"
  sed "s#build/release/extension/nats_js/nats_js.duckdb_extension#$EXTENSION_PATH#g" "$ROOT_DIR/$test_file" \
    | "$DUCKDB_BIN" -unsigned :memory: >"$log_file"
}

echo "Checking NATS connection at $NATS_URL"
"$NATS_CLI" server check connection --server "$NATS_URL"

echo "Preparing JetStream streams"
NATS_URL="$NATS_URL" NATS_CLI="$NATS_CLI" RESET_STREAMS="${RESET_STREAMS:-1}" "$ROOT_DIR/scripts/setup-streams.sh"

echo "Generating fixtures"
"$PYTHON_BIN" "$ROOT_DIR/scripts/generate-telemetry.py" --url "$NATS_URL" --hours "${FIXTURE_HOURS:-2}" --interval-seconds "${FIXTURE_INTERVAL_SECONDS:-60}"
"$PYTHON_BIN" "$ROOT_DIR/test/proto/generate_protobuf_data.py"

success_tests=(
  test/sql/test_json_extraction.sql
  test/sql/test_timestamp_queries.sql
  test/sql/test_sequence_ranges.sql
  test/sql/test_subject_filtering.sql
  test/sql/test_protobuf.sql
  test/sql/test_payload_blob.sql
  test/sql/test_stream_stats.sql
  test/sql/test_connection_errors.sql
  test/sql/test_repeated_fields.sql
)

for test_file in "${success_tests[@]}"; do
  echo "RUN $test_file"
  log_file="/tmp/$(basename "$test_file")".log
  run_sql_test "$test_file" "$log_file"
  echo "PASS $test_file"
done

echo "RUN test/sql/test_protobuf_errors.sql (expected errors)"
if run_sql_test "test/sql/test_protobuf_errors.sql" /tmp/test_protobuf_errors.sql.log >/tmp/test_protobuf_errors.sql.log 2>&1; then
  echo "Expected protobuf error suite to fail, but it exited 0" >&2
  exit 1
fi

for pattern in \
  "proto_file parameter is required" \
  "proto_message parameter is required" \
  "Failed to import protobuf schema file" \
  "Message type 'NonExistentMessage' not found" \
  "Field 'nonexis" \
  "Cannot use both json_extract and proto_extract"; do
  if ! grep -q "$pattern" /tmp/test_protobuf_errors.sql.log; then
    echo "Missing expected error pattern: $pattern" >&2
    tail -n 80 /tmp/test_protobuf_errors.sql.log >&2
    exit 1
  fi
done
echo "PASS test/sql/test_protobuf_errors.sql"

echo "RUN scripts/run-ingest-harness.sh"
NATS_URL="$NATS_URL" \
NATS_CLI="$NATS_CLI" \
RESET_STREAMS=1 \
"$ROOT_DIR/scripts/run-ingest-harness.sh"
echo "PASS scripts/run-ingest-harness.sh"

echo "RUN scripts/run-ingest-harness.sh (HARNESS_MODE=redelivery)"
HARNESS_MODE=redelivery \
NATS_URL="$NATS_URL" \
NATS_CLI="$NATS_CLI" \
RESET_STREAMS=1 \
"$ROOT_DIR/scripts/run-ingest-harness.sh"
echo "PASS scripts/run-ingest-harness.sh (HARNESS_MODE=redelivery)"

echo "RUN scripts/run-ingest-crash-harness.sh"
NATS_URL="$NATS_URL" \
NATS_CLI="$NATS_CLI" \
RESET_STREAMS=1 \
"$ROOT_DIR/scripts/run-ingest-crash-harness.sh"
echo "PASS scripts/run-ingest-crash-harness.sh"

echo "RUN scripts/run-ingest-rehydrate-harness.sh"
NATS_URL="$NATS_URL" \
NATS_CLI="$NATS_CLI" \
RESET_STREAMS=1 \
"$ROOT_DIR/scripts/run-ingest-rehydrate-harness.sh"
echo "PASS scripts/run-ingest-rehydrate-harness.sh"

echo "All local integration tests passed"
