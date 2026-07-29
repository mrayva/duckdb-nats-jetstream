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

if [ ! -x "$NATS_CLI" ]; then
  NATS_CLI="$(command -v nats || true)"
fi
if [ -z "$NATS_CLI" ]; then
  echo "NATS CLI not found" >&2
  exit 1
fi
if [ ! -x "$PYTHON_BIN" ]; then
  PYTHON_BIN="$(command -v python3)"
fi

"$NATS_CLI" server check connection --server "$NATS_URL"

db_file="$(mktemp /tmp/nats_subscribe_msgpack.XXXXXX.duckdb)"
log_file="$(mktemp /tmp/nats_subscribe_msgpack.XXXXXX.log)"
fixture="$(mktemp /tmp/nats_subscribe_msgpack.XXXXXX.bin)"
rm -f "$db_file"
trap 'rc=$?; rm -f "$log_file" "$db_file" "$fixture"; exit $rc' EXIT

"$PYTHON_BIN" "$ROOT_DIR/test/msgpack/generate_msgpack_data.py" "$fixture"
(
  sleep 1
  "$NATS_CLI" pub --server "$NATS_URL" --force-stdin live.subscribe.msgpack <"$fixture" >/dev/null
) &

if ! DUCKDB_LIB="$DUCKDB_LIB" "$PYTHON_BIN" "$ROOT_DIR/scripts/duckdb_session.py" \
    --duckdb-bin "$DUCKDB_BIN" --db-file "$db_file" <<SQL >"$log_file" 2>&1
SEND
LOAD '${EXTENSION_PATH}';
SELECT 'start=' || job_name || '|' || target_table || '|' || subject AS start_result
FROM nats_start_subscribe(
    job_name := 'live_subscribe_msgpack_probe',
    target_table := 'main.subscribe_msgpack_out',
    url := '${NATS_URL}',
    subject := 'live.subscribe.msgpack',
    batch_size := 1,
    poll_ms := 100,
    create_target_table := true,
    msgpack_extract := ['device_id', 'metrics.kw', 'online', 'reading_count']
);
END
EXPECT start=live_subscribe_msgpack_probe|main.subscribe_msgpack_out|live.subscribe.msgpack 10
SLEEP 3
SEND
SELECT 'status=' || rows_inserted || '/' || failed AS subscribe_status
FROM nats_subscribe_status(job_name := 'live_subscribe_msgpack_probe');
SELECT 'row=' || device_id || '/' || "metrics.kw" || '/' || online || '/' || reading_count AS extracted_row
FROM main.subscribe_msgpack_out;
SELECT 'types=' || string_agg(type, '/' ORDER BY cid) AS target_types
FROM pragma_table_info('subscribe_msgpack_out');
SELECT 'stop=' || job_name || '|' || target_table || '|' || subject AS stop_result
FROM nats_stop_subscribe(job_name := 'live_subscribe_msgpack_probe');
END
EXPECT status=1/false 10
EXPECT row=msgpack-device-1/42.500000/true/7 10
EXPECT types=VARCHAR/BLOB/TIMESTAMP/VARCHAR/VARCHAR/VARCHAR/VARCHAR 10
EXPECT stop=live_subscribe_msgpack_probe|main.subscribe_msgpack_out|live.subscribe.msgpack 10
QUIT
SQL
then
  cat "$log_file" >&2
  exit 1
fi

echo "Subscribe MessagePack harness passed"
