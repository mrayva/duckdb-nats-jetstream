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
PYTHON_BIN="${PYTHON_BIN:-$ROOT_DIR/.venv/bin/python}"
if [ ! -x "$PYTHON_BIN" ]; then PYTHON_BIN="$(command -v python3)"; fi

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

echo "Checking NATS connection at $NATS_URL"
"$NATS_CLI" server check connection --server "$NATS_URL"

echo "Preparing JetStream streams"
NATS_URL="$NATS_URL" NATS_CLI="$NATS_CLI" RESET_STREAMS="${RESET_STREAMS:-1}" "$ROOT_DIR/scripts/setup-streams.sh"

db_file="$(mktemp /tmp/nats_ingest_rehydrate.XXXXXX.duckdb)"
log_first="$(mktemp /tmp/nats_ingest_rehydrate.first.XXXXXX.log)"
log_second="$(mktemp /tmp/nats_ingest_rehydrate.second.XXXXXX.log)"
fixture="$(mktemp /tmp/nats_ingest_rehydrate.XXXXXX.bin)"
descriptor_set="$(mktemp /tmp/nats_ingest_rehydrate.XXXXXX.protoset)"
proto_py_dir="$(mktemp -d /tmp/nats_ingest_rehydrate_proto.XXXXXX)"
rm -f "$db_file" "$fixture" "$descriptor_set"
trap 'rc=$?; if [ "$rc" -eq 0 ]; then rm -f "$log_first" "$log_second" "$db_file"; else echo "Harness logs: $log_first $log_second; database: $db_file" >&2; fi; rm -f "$fixture" "$descriptor_set"; rm -rf "$proto_py_dir"; exit $rc' EXIT

protoc -I "$ROOT_DIR/test/proto" --include_imports --descriptor_set_out="$descriptor_set" \
  "$ROOT_DIR/test/proto/telemetry.proto"
protoc -I "$ROOT_DIR/test/proto" --python_out="$proto_py_dir" "$ROOT_DIR/test/proto/telemetry.proto"
PYTHONPATH="$proto_py_dir" "$PYTHON_BIN" -c \
  "import telemetry_pb2; m=telemetry_pb2.Telemetry(); m.device_id='descriptor-ingest'; m.timestamp=123456789; m.location.zone='test-zone'; m.metrics.kw=12.5; m.online=True; open('$fixture','wb').write(m.SerializeToString())"
for _ in 1 2 3 4; do
  "$NATS_CLI" pub --server "$NATS_URL" --jetstream --force-stdin telemetry_proto.descriptor.ingest <"$fixture" >/dev/null
done

if DUCKDB_LIB="$DUCKDB_LIB" NATS_INGEST_FAIL_AFTER_COMMIT=1 NATS_INGEST_DISABLE_REHYDRATE=1 \
  python3 "$ROOT_DIR/scripts/duckdb_session.py" --duckdb-bin "$DUCKDB_BIN" --db-file "$db_file" <<SQL >"$log_first" 2>&1
SEND
LOAD '${EXTENSION_PATH}';
SELECT 'start1=' || job_name || '|' || stream_name || '|' || target_table || '|' || durable_name AS start_result
FROM nats_start_ingest(
    job_name := 'ingest_rehydrate',
    stream_name := 'telemetry_proto',
    target_table := 'main.ingest_out',
    durable_name := 'duckdb_ingest_rehydrate',
    url := 'nats://127.0.0.1:4222',
    batch_size := 4,
    poll_ms := 10000,
    fetch_timeout_ms := 100,
    start_seq := 1,
    create_target_table := true,
    proto_descriptor_set := '${descriptor_set}',
    proto_message := 'telemetry.Telemetry',
    proto_extract := ['device_id', 'metrics.kw', 'location.zone']
);
END
EXPECT start1=ingest_rehydrate|telemetry_proto|main.ingest_out|duckdb_ingest_rehydrate 10
SLEEP 5
QUIT
SQL
then
  echo "Expected the first rehydrate process to terminate after its committed batch" >&2
  cat "$log_first" >&2
  exit 1
fi

# The simulated crash intentionally leaves the durable-ownership lease behind.
sleep 32

if ! DUCKDB_LIB="$DUCKDB_LIB" python3 "$ROOT_DIR/scripts/duckdb_session.py" --duckdb-bin "$DUCKDB_BIN" --db-file "$db_file" <<SQL >"$log_second" 2>&1
SEND
LOAD '${EXTENSION_PATH}';
SELECT 'status2=' || running || '/' || rows_inserted || '/' || last_committed_seq || '/' || failed || '/' || stopped AS ingest_status
FROM nats_ingest_status(job_name := 'ingest_rehydrate');
END
EXPECT status2=true/4/4/false/false 30
SEND
SELECT 'count=' || COUNT(*) AS inserted_rows FROM ingest_out;
SELECT 'descriptor_path=' || proto_descriptor_set AS persisted_descriptor_path
FROM duckdb_nats_ingest_jobs WHERE job_name = 'ingest_rehydrate';
SELECT 'decoded=' || count(*) || '/' || count(device_id) || '/' || count(metrics_kw) || '/' || count(location_zone) AS decoded_rows
FROM ingest_out;
SELECT 'stop=' || job_name || '|' || stream_name || '|' || target_table || '|' || durable_name AS stop_result
FROM nats_stop_ingest(job_name := 'ingest_rehydrate');
SELECT 'stopped=' || stop_requested || '/' || stopped || '/' || failed AS stopped_status
FROM nats_ingest_status(job_name := 'ingest_rehydrate');
END
EXPECT count=4 10
EXPECT descriptor_path=${descriptor_set} 10
EXPECT decoded=4/4/4/4 10
EXPECT stop=ingest_rehydrate|telemetry_proto|main.ingest_out|duckdb_ingest_rehydrate 10
EXPECT stopped=true/false/false 10
QUIT
SQL
then
  cat "$log_second" >&2
  exit 1
fi

echo "Ingest rehydrate harness passed"
