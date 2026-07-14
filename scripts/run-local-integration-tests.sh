#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DUCKDB_BIN="${DUCKDB_BIN:-$HOME/.duckdb/cli/1.5.4/duckdb}"
EXTENSION_PATH="${EXTENSION_PATH:-$ROOT_DIR/build/release/extension/nats_js/nats_js.duckdb_extension}"
NATS_URL="${NATS_URL:-nats://127.0.0.1:4222}"
NATS_CLI="${NATS_CLI:-$HOME/nats}"
PYTHON_BIN="${PYTHON_BIN:-$ROOT_DIR/.venv/bin/python}"
TIP_ROOT="${TIP_ROOT:-/tmp/duckdb-tip-clean}"
COPY_DUCKDB_BIN="${COPY_DUCKDB_BIN:-$TIP_ROOT/build/release/duckdb}"
COPY_DUCKDB_LIB="${COPY_DUCKDB_LIB:-$TIP_ROOT/build/release/src/libduckdb.so}"
COPY_EXTENSION_PATH="${COPY_EXTENSION_PATH:-$TIP_ROOT/build/nats_js-tip/extension/nats_js/nats_js.duckdb_extension}"

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

run_copy_sql_test() {
  local test_file="$1"
  local log_file="$2"
  sed "s#build/release/extension/nats_js/nats_js.duckdb_extension#$COPY_EXTENSION_PATH#g" "$ROOT_DIR/$test_file" \
    | "$COPY_DUCKDB_BIN" -unsigned :memory: >"$log_file"
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

echo "RUN test/sql/test_connection_options_errors.sql (expected errors)"
if run_sql_test "test/sql/test_connection_options_errors.sql" /tmp/test_connection_options_errors.sql.log \
    >/tmp/test_connection_options_errors.sql.log 2>&1; then
  echo "Expected connection option error suite to fail, but it exited 0" >&2
  exit 1
fi

for pattern in \
  "tls_cert_file and tls_key_file must be specified together" \
  "credentials_file does not exist"; do
  if ! grep -q "$pattern" /tmp/test_connection_options_errors.sql.log; then
    echo "Missing expected connection option error pattern: $pattern" >&2
    tail -n 80 /tmp/test_connection_options_errors.sql.log >&2
    exit 1
  fi
done
echo "PASS test/sql/test_connection_options_errors.sql"

echo "RUN test/sql/test_copy_from.sql"
log_file="/tmp/test_copy_from.sql.log"
run_copy_sql_test "test/sql/test_copy_from.sql" "$log_file"
echo "PASS test/sql/test_copy_from.sql"

echo "RUN test/sql/test_copy_from_errors.sql (expected errors)"
if run_copy_sql_test "test/sql/test_copy_from_errors.sql" /tmp/test_copy_from_errors.sql.log >/tmp/test_copy_from_errors.sql.log 2>&1; then
  echo "Expected COPY FROM error suite to fail, but it exited 0" >&2
  exit 1
fi

for pattern in \
  "requires the target table schema to match" \
  "Cannot use both subject and subject_contains parameters"; do
  if ! grep -q "$pattern" /tmp/test_copy_from_errors.sql.log; then
    echo "Missing expected COPY error pattern: $pattern" >&2
    tail -n 80 /tmp/test_copy_from_errors.sql.log >&2
    exit 1
  fi
done
echo "PASS test/sql/test_copy_from_errors.sql"

echo "RUN test/sql/test_copy_to.sql"
log_file="/tmp/test_copy_to.sql.log"
run_copy_sql_test "test/sql/test_copy_to.sql" "$log_file"
echo "PASS test/sql/test_copy_to.sql"

echo "RUN test/sql/test_copy_to_errors.sql (expected errors)"
if run_copy_sql_test "test/sql/test_copy_to_errors.sql" /tmp/test_copy_to_errors.sql.log >/tmp/test_copy_to_errors.sql.log 2>&1; then
  echo "Expected COPY TO error suite to fail, but it exited 0" >&2
  exit 1
fi

for pattern in \
  "requires a source column named \"payload\""; do
  if ! grep -q "$pattern" /tmp/test_copy_to_errors.sql.log; then
    echo "Missing expected COPY TO error pattern: $pattern" >&2
    tail -n 80 /tmp/test_copy_to_errors.sql.log >&2
    exit 1
  fi
done
echo "PASS test/sql/test_copy_to_errors.sql"

echo "RUN test/sql/test_copy_roundtrip.sql"
log_file="/tmp/test_copy_roundtrip.sql.log"
run_copy_sql_test "test/sql/test_copy_roundtrip.sql" "$log_file"
echo "PASS test/sql/test_copy_roundtrip.sql"

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

echo "RUN scripts/run-ingest-pause-resume-harness.sh"
NATS_URL="$NATS_URL" \
NATS_CLI="$NATS_CLI" \
RESET_STREAMS=1 \
"$ROOT_DIR/scripts/run-ingest-pause-resume-harness.sh"
echo "PASS scripts/run-ingest-pause-resume-harness.sh"

echo "RUN scripts/run-copy-harness.sh"
DUCKDB_BIN="$COPY_DUCKDB_BIN" \
DUCKDB_LIB="$COPY_DUCKDB_LIB" \
EXTENSION_PATH="$COPY_EXTENSION_PATH" \
NATS_URL="$NATS_URL" \
NATS_CLI="$NATS_CLI" \
RESET_STREAMS=1 \
"$ROOT_DIR/scripts/run-copy-harness.sh"
echo "PASS scripts/run-copy-harness.sh"

echo "RUN scripts/run-copy-to-harness.sh"
DUCKDB_BIN="$COPY_DUCKDB_BIN" \
DUCKDB_LIB="$COPY_DUCKDB_LIB" \
EXTENSION_PATH="$COPY_EXTENSION_PATH" \
NATS_URL="$NATS_URL" \
NATS_CLI="$NATS_CLI" \
RESET_STREAMS=1 \
"$ROOT_DIR/scripts/run-copy-to-harness.sh"
echo "PASS scripts/run-copy-to-harness.sh"

echo "RUN scripts/run-ingest-rehydrate-harness.sh"
NATS_URL="$NATS_URL" \
NATS_CLI="$NATS_CLI" \
RESET_STREAMS=1 \
"$ROOT_DIR/scripts/run-ingest-rehydrate-harness.sh"
echo "PASS scripts/run-ingest-rehydrate-harness.sh"

echo "RUN scripts/run-subscribe-harness.sh"
NATS_URL="$NATS_URL" \
NATS_CLI="$NATS_CLI" \
"$ROOT_DIR/scripts/run-subscribe-harness.sh"
echo "PASS scripts/run-subscribe-harness.sh"

echo "RUN scripts/run-subscribe-pause-resume-harness.sh"
NATS_URL="$NATS_URL" \
NATS_CLI="$NATS_CLI" \
"$ROOT_DIR/scripts/run-subscribe-pause-resume-harness.sh"
echo "PASS scripts/run-subscribe-pause-resume-harness.sh"

echo "RUN scripts/run-subscribe-backpressure-harness.sh"
NATS_URL="$NATS_URL" \
NATS_CLI="$NATS_CLI" \
"$ROOT_DIR/scripts/run-subscribe-backpressure-harness.sh"
echo "PASS scripts/run-subscribe-backpressure-harness.sh"

echo "All local integration tests passed"
