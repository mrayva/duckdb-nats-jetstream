#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT_DIR/scripts/duckdb_host_lib.sh"
DUCKDB_BIN="${DUCKDB_BIN:-/tmp/duckdb-tip-clean/build/release/duckdb}"
DUCKDB_LIB="${DUCKDB_LIB:-$(duckdb_host_lib_resolve)}"
EXTENSION_PATH="${EXTENSION_PATH:-/tmp/duckdb-tip-clean/build/nats_js-tip/extension/nats_js/nats_js.duckdb_extension}"
NATS_URL="${NATS_URL:-nats://127.0.0.1:4222}"
NATS_CLI="${NATS_CLI:-$HOME/nats}"
PYTHON_BIN="${PYTHON_BIN:-$ROOT_DIR/.venv/bin/python}"

if [ ! -x "$NATS_CLI" ]; then NATS_CLI="$(command -v nats || true)"; fi
if [ -z "$NATS_CLI" ]; then echo "NATS CLI not found" >&2; exit 1; fi
if [ ! -x "$PYTHON_BIN" ]; then PYTHON_BIN="$(command -v python3)"; fi

"$NATS_CLI" server check connection --server "$NATS_URL"

db_file="$(mktemp /tmp/nats_subscribe_protobuf.XXXXXX.duckdb)"
log_file="$(mktemp /tmp/nats_subscribe_protobuf.XXXXXX.log)"
fixture="$(mktemp /tmp/nats_subscribe_protobuf.XXXXXX.bin)"
rm -f "$db_file"
trap 'rc=$?; rm -f "$log_file" "$db_file" "$fixture"; exit $rc' EXIT

PYTHONPATH="$ROOT_DIR/test/proto" "$PYTHON_BIN" -c \
  "import telemetry_pb2; m=telemetry_pb2.Telemetry(); m.device_id='proto-device-1'; m.timestamp=123456789; m.metrics.kw=12.5; m.online=True; open('$fixture','wb').write(m.SerializeToString())"
(
  sleep 10
  for _ in 1 2 3 4 5; do
    "$NATS_CLI" pub --server "$NATS_URL" --force-stdin live.subscribe.protobuf <"$fixture" >/dev/null
    sleep 1
  done
) &
publisher_pid=$!

if ! DUCKDB_LIB="$DUCKDB_LIB" "$PYTHON_BIN" "$ROOT_DIR/scripts/duckdb_session.py" \
    --duckdb-bin "$DUCKDB_BIN" --db-file "$db_file" <<SQL >"$log_file" 2>&1
SEND
LOAD '${EXTENSION_PATH}';
SELECT 'start=' || job_name || '|' || target_table || '|' || subject AS start_result
FROM nats_start_subscribe(
    job_name := 'live_subscribe_protobuf_probe',
    target_table := 'main.subscribe_protobuf_out',
    url := '${NATS_URL}',
    subject := 'live.subscribe.protobuf',
    batch_size := 1,
    poll_ms := 100,
    create_target_table := true,
    proto_file := '${ROOT_DIR}/test/proto/telemetry.proto',
    proto_message := 'Telemetry',
    proto_extract := ['device_id', 'metrics.kw', 'online', 'timestamp']
);
END
EXPECT start=live_subscribe_protobuf_probe|main.subscribe_protobuf_out|live.subscribe.protobuf 10
SLEEP 12
SEND
SELECT 'status=' || (rows_inserted > 0) || '/' || (messages_delivered > 0) || '/' || failed AS subscribe_status
FROM nats_subscribe_status(job_name := 'live_subscribe_protobuf_probe');
SELECT 'row=' || device_id || '/' || "metrics.kw" || '/' || online || '/' || timestamp AS extracted_row
FROM main.subscribe_protobuf_out;
SELECT 'types=' || string_agg(type, '/' ORDER BY cid) AS target_types
FROM pragma_table_info('subscribe_protobuf_out');
SELECT 'stop=' || job_name || '|' || target_table || '|' || subject AS stop_result
FROM nats_stop_subscribe(job_name := 'live_subscribe_protobuf_probe');
END
EXPECT status=true/true/false 10
EXPECT row=proto-device-1/12.5/true/123456789 10
EXPECT types=VARCHAR/BLOB/TIMESTAMP/VARCHAR/DOUBLE/BOOLEAN/BIGINT 10
EXPECT stop=live_subscribe_protobuf_probe|main.subscribe_protobuf_out|live.subscribe.protobuf 10
QUIT
SQL
then
  cat "$log_file" >&2
  exit 1
fi

wait "$publisher_pid"

echo "Subscribe protobuf harness passed"
