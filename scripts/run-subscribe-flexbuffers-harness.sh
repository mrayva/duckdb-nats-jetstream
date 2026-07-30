#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT_DIR/scripts/duckdb_host_lib.sh"
DUCKDB_BIN="${DUCKDB_BIN:-/tmp/nats-js-tip-build5/duckdb}"
DUCKDB_LIB="${DUCKDB_LIB:-$(duckdb_host_lib_resolve)}"
EXTENSION_PATH="${EXTENSION_PATH:-/tmp/nats-js-tip-build5/extension/nats_js/nats_js.duckdb_extension}"
NATS_URL="${NATS_URL:-nats://127.0.0.1:4222}"
NATS_CLI="${NATS_CLI:-$HOME/nats}"
PYTHON_BIN="${PYTHON_BIN:-$ROOT_DIR/.venv/bin/python}"
FLATC="${FLATC:-$(command -v flatc || true)}"

if [ ! -x "$NATS_CLI" ]; then NATS_CLI="$(command -v nats || true)"; fi
if [ -z "$NATS_CLI" ] || [ ! -x "$PYTHON_BIN" ] || [ -z "$FLATC" ]; then
  echo "NATS CLI, Python, and flatc are required" >&2
  exit 1
fi

db_file="$(mktemp /tmp/nats_subscribe_flexbuffers.XXXXXX.duckdb)"
log_file="$(mktemp /tmp/nats_subscribe_flexbuffers.XXXXXX.log)"
fixture_json="$(mktemp /tmp/nats_subscribe_flexbuffers.XXXXXX.json)"
fixture_bin="${fixture_json%.json}.bin"
rm -f "$db_file"
trap 'rc=$?; rm -f "$log_file" "$db_file" "$fixture_json" "$fixture_bin"; exit $rc' EXIT

printf '%s\n' '{"device_id":"flex-1","metrics":{"kw":42}}' >"$fixture_json"
"$FLATC" --flexbuffers --binary -o "$(dirname "$fixture_json")" "$fixture_json"

(
  sleep 4
  "$NATS_CLI" pub --server "$NATS_URL" --force-stdin live.subscribe.flexbuffers <"$fixture_bin" >/dev/null
) &

if ! LD_PRELOAD="${LD_PRELOAD:-$DUCKDB_LIB}" DUCKDB_LIB="$DUCKDB_LIB" \
    "$PYTHON_BIN" "$ROOT_DIR/scripts/duckdb_session.py" --duckdb-bin "$DUCKDB_BIN" --db-file "$db_file" \
    >"$log_file" 2>&1 <<SQL
SEND
LOAD '${EXTENSION_PATH}';
SELECT job_name FROM nats_start_subscribe(
    job_name := 'live_subscribe_flexbuffers_probe',
    target_table := 'flexbuffers_out',
    url := '${NATS_URL}',
    subject := 'live.subscribe.flexbuffers',
    batch_size := 1,
    poll_ms := 100,
    create_target_table := true,
    flexbuffers_extract := ['device_id', 'metrics.kw']
);
END
SLEEP 6
SEND
SELECT failed, rows_inserted FROM nats_subscribe_status(job_name := 'live_subscribe_flexbuffers_probe');
SELECT device_id || '/' || "metrics.kw" FROM flexbuffers_out;
SELECT job_name FROM nats_stop_subscribe(job_name := 'live_subscribe_flexbuffers_probe');
END
EXPECT false 10
EXPECT flex-1/42 10
QUIT
SQL
then
  cat "$log_file" >&2
  exit 1
fi

echo "Subscribe FlexBuffers harness passed"
