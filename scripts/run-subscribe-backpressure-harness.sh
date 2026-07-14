#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
source "$ROOT_DIR/scripts/duckdb_host_lib.sh"
TIP_ROOT="${TIP_ROOT:-/tmp/duckdb-tip-clean}"
DUCKDB_BIN="${DUCKDB_BIN:-${SUBSCRIBE_DUCKDB_BIN:-$TIP_ROOT/build/release/duckdb}}"
DUCKDB_LIB="${DUCKDB_LIB:-$(duckdb_host_lib_resolve)}"
EXTENSION_PATH="${EXTENSION_PATH:-${SUBSCRIBE_EXTENSION_PATH:-$TIP_ROOT/build/nats_js-tip/extension/nats_js/nats_js.duckdb_extension}}"
NATS_URL="${NATS_URL:-nats://127.0.0.1:4222}"
NATS_CLI="${NATS_CLI:-$HOME/nats}"
PYTHON_BIN="${PYTHON_BIN:-$ROOT_DIR/.venv/bin/python}"

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
if [ ! -x "$PYTHON_BIN" ]; then
  PYTHON_BIN="$(command -v python3)"
fi

echo "Checking NATS connection at $NATS_URL"
"$NATS_CLI" server check connection --server "$NATS_URL"

db_file="$(mktemp /tmp/nats_subscribe_backpressure.XXXXXX.duckdb)"
log_file="$(mktemp /tmp/nats_subscribe_backpressure.XXXXXX.log)"
rm -f "$db_file"
trap 'rc=$?; rm -f "$log_file" "$db_file"; exit $rc' EXIT

(
  sleep 2
  echo "Publishing overload burst to live.backpressure"
  "$NATS_CLI" pub --quiet --count 100 --server="$NATS_URL" "live.backpressure" "overload-{{Count}}" >/dev/null
) &

if ! DUCKDB_LIB="$DUCKDB_LIB" "$PYTHON_BIN" "$ROOT_DIR/scripts/duckdb_session.py" \
  --duckdb-bin "$DUCKDB_BIN" --db-file "$db_file" <<SQL >"$log_file" 2>&1
SEND
LOAD '${EXTENSION_PATH}';
CREATE TABLE subscribe_out(subject VARCHAR, payload BLOB, received_at TIMESTAMP);
SELECT 'start=' || job_name
FROM nats_start_subscribe(
    job_name := 'live_subscribe_backpressure',
    target_table := 'subscribe_out',
    url := '${NATS_URL}',
    subject := 'live.backpressure',
    pending_message_limit := 2,
    pending_bytes_limit := 1024,
    poll_ms := 50
);
END
EXPECT start=live_subscribe_backpressure 10
SLEEP 1
SEND
SELECT 'pause=' || paused
FROM nats_pause_subscribe(job_name := 'live_subscribe_backpressure');
END
EXPECT pause=true 10
SLEEP 4
SEND
SELECT 'bounded=' || (pending_messages <= 2) || '/' || (max_pending_messages <= 2) || '/' ||
       (messages_dropped > 0) || '/' || paused || '/' || failed
FROM nats_subscribe_status(job_name := 'live_subscribe_backpressure');
SELECT 'stop=' || job_name
FROM nats_stop_subscribe(job_name := 'live_subscribe_backpressure');
END
EXPECT bounded=true/true/true/true/false 30
EXPECT stop=live_subscribe_backpressure 10
QUIT
SQL
then
  cat "$log_file" >&2
  exit 1
fi

echo "Subscribe backpressure harness passed"
