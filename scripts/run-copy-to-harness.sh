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

echo "Checking NATS connection at $NATS_URL"
"$NATS_CLI" server check connection --server "$NATS_URL"

echo "Preparing JetStream streams"
NATS_URL="$NATS_URL" NATS_CLI="$NATS_CLI" RESET_STREAMS="${RESET_STREAMS:-1}" "$ROOT_DIR/scripts/setup-streams.sh"

db_file="$(mktemp /tmp/nats_copy_to_harness.XXXXXX.duckdb)"
log_file="$(mktemp /tmp/nats_copy_to_harness.XXXXXX.log)"
rm -f "$db_file"
trap 'rc=$?; rm -f "$log_file" "$db_file"; exit $rc' EXIT

if ! DUCKDB_LIB="$DUCKDB_LIB" python3 "$ROOT_DIR/scripts/duckdb_session.py" --duckdb-bin "$DUCKDB_BIN" --db-file "$db_file" <<SQL >"$log_file" 2>&1
SEND
LOAD '${EXTENSION_PATH}';
CREATE TABLE copy_source(
    subject VARCHAR,
    payload VARCHAR
);
INSERT INTO copy_source VALUES
    ('copy_out.alpha', 'hello'),
    ('copy_out.beta', 'world');
COPY copy_source
TO 'copy_out'
(FORMAT nats_js, url '${NATS_URL}');
SELECT 'count=' || COUNT(*) AS copied_rows
FROM nats_scan('copy_out', url := '${NATS_URL}');
SELECT 'subjects=' || string_agg(subject, ',' ORDER BY subject) AS copied_subjects
FROM nats_scan('copy_out', url := '${NATS_URL}');
SELECT 'payloads=' || string_agg(CAST(payload AS VARCHAR), ',' ORDER BY CAST(payload AS VARCHAR)) AS copied_payloads
FROM nats_scan('copy_out', url := '${NATS_URL}');
END
EXPECT count=2 20
EXPECT subjects=copy_out.alpha,copy_out.beta 20
EXPECT payloads=hello,world 20
QUIT
SQL
then
  cat "$log_file" >&2
  exit 1
fi

echo "COPY TO harness passed"
