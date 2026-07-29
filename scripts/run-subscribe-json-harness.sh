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

db_file="$(mktemp /tmp/nats_subscribe_json.XXXXXX.duckdb)"
log_file="$(mktemp /tmp/nats_subscribe_json.XXXXXX.log)"
rm -f "$db_file"
trap 'rc=$?; rm -f "$log_file" "$db_file"; exit $rc' EXIT

(
  sleep 1
  "$NATS_CLI" pub --server "$NATS_URL" live.subscribe.json \
    '{"device_id":"json-device-1","metrics":{"kw":17.25},"online":true}' >/dev/null
) &

if ! DUCKDB_LIB="$DUCKDB_LIB" "$PYTHON_BIN" "$ROOT_DIR/scripts/duckdb_session.py" \
    --duckdb-bin "$DUCKDB_BIN" --db-file "$db_file" <<SQL >"$log_file" 2>&1
SEND
LOAD '${EXTENSION_PATH}';
SELECT 'start=' || job_name || '|' || target_table || '|' || subject AS start_result
FROM nats_start_subscribe(
    job_name := 'live_subscribe_json_probe',
    target_table := 'main.subscribe_json_out',
    url := '${NATS_URL}',
    subject := 'live.subscribe.json',
    batch_size := 1,
    poll_ms := 100,
    create_target_table := true,
    json_extract := ['device_id', 'metrics', 'online']
);
END
EXPECT start=live_subscribe_json_probe|main.subscribe_json_out|live.subscribe.json 10
SLEEP 3
SEND
SELECT 'status=' || rows_inserted || '/' || failed AS subscribe_status
FROM nats_subscribe_status(job_name := 'live_subscribe_json_probe');
SELECT 'row=' || device_id || '/' || metrics || '/' || online AS extracted_row
FROM main.subscribe_json_out;
SELECT 'types=' || string_agg(type, '/' ORDER BY cid) AS target_types
FROM pragma_table_info('subscribe_json_out');
SELECT 'stop=' || job_name || '|' || target_table || '|' || subject AS stop_result
FROM nats_stop_subscribe(job_name := 'live_subscribe_json_probe');
END
EXPECT status=1/false 10
EXPECT row=json-device-1/{"kw":17.25}/true 10
EXPECT types=VARCHAR/BLOB/TIMESTAMP/VARCHAR/VARCHAR/VARCHAR 10
EXPECT stop=live_subscribe_json_probe|main.subscribe_json_out|live.subscribe.json 10
QUIT
SQL
then
  cat "$log_file" >&2
  exit 1
fi

echo "Subscribe JSON harness passed"
