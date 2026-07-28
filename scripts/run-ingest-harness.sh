#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
source "$ROOT_DIR/scripts/duckdb_host_lib.sh"
TIP_ROOT="${TIP_ROOT:-/tmp/duckdb-tip-clean}"
DUCKDB_BIN="${DUCKDB_BIN:-${TIP_DUCKDB_BIN:-$TIP_ROOT/build/release/duckdb}}"
DUCKDB_LIB="${DUCKDB_LIB:-$(duckdb_host_lib_resolve)}"
EXTENSION_PATH="${EXTENSION_PATH:-${TIP_EXTENSION_PATH:-$TIP_ROOT/build/nats_js-tip/extension/nats_js/nats_js.duckdb_extension}}"
NATS_URL="${NATS_URL:-nats://localhost:4222}"
NATS_CLI="${NATS_CLI:-$HOME/nats}"
HARNESS_MODE="${HARNESS_MODE:-resume}"

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

case "$HARNESS_MODE" in
  resume)
    stream_name="ingest_resume"
    durable_name="duckdb_ingest_resume"
    start_job_a="ingest_probe3_a"
    ;;
  redelivery)
    stream_name="ingest_redelivery"
    durable_name="duckdb_ingest_redelivery"
    start_job_a="ingest_redelivery_a"
    ;;
  *)
    echo "Unknown HARNESS_MODE: $HARNESS_MODE (expected resume or redelivery)" >&2
    exit 1
    ;;
esac

echo "Checking NATS connection at $NATS_URL"
"$NATS_CLI" server check connection --server "$NATS_URL"

echo "Preparing JetStream streams"
NATS_URL="$NATS_URL" NATS_CLI="$NATS_CLI" RESET_STREAMS="${RESET_STREAMS:-1}" "$ROOT_DIR/scripts/setup-streams.sh"

db_file="$(mktemp /tmp/nats_ingest_harness.XXXXXX.duckdb)"
log_file="$(mktemp /tmp/nats_ingest_harness.XXXXXX.log)"
rm -f "$db_file"
trap 'rc=$?; rm -f "$log_file" "$db_file"; exit $rc' EXIT

if ! DUCKDB_LIB="$DUCKDB_LIB" NATS_INGEST_DISABLE_REHYDRATE=1 NATS_INGEST_CHECKPOINT_COMPACTION_INTERVAL=2 \
  python3 "$ROOT_DIR/scripts/duckdb_session.py" --duckdb-bin "$DUCKDB_BIN" --db-file "$db_file" <<SQL >"$log_file" 2>&1
SEND
LOAD '${EXTENSION_PATH}';
CREATE TABLE ingest_out(
    stream_name VARCHAR,
    subject VARCHAR,
    sequence UBIGINT,
    ts TIMESTAMP,
    payload BLOB
);
SELECT 'start1=' || job_name || '|' || stream_name || '|' || target_table || '|' || durable_name AS start_result
FROM nats_start_ingest(
    job_name := '${start_job_a}',
    stream_name := '${stream_name}',
    target_table := 'ingest_out',
    durable_name := '${durable_name}',
    url := '${NATS_URL}',
    batch_size := 2,
    poll_ms := 10000,
    fetch_timeout_ms := 100,
    start_seq := 1
);
END
EXPECT start1=${start_job_a}|${stream_name}|ingest_out|${durable_name} 30
SEND
SELECT 'status1=' || rows_inserted || '/' || batches_committed || '/' || last_committed_seq AS ingest_status
FROM nats_ingest_status(job_name := '${start_job_a}');
END
EXPECT status1=2/1/2 30
SLEEP 1
SEND
SELECT 'status2=' || rows_inserted || '/' || batches_committed || '/' || last_committed_seq AS ingest_status
FROM nats_ingest_status(job_name := '${start_job_a}');
END
EXPECT status2=4/2/4 30
SEND
SELECT 'stop1=' || job_name || '|' || stream_name || '|' || target_table || '|' || durable_name AS stop_result
FROM nats_stop_ingest(job_name := '${start_job_a}');
SELECT 'count=' || COUNT(*) AS inserted_rows FROM ingest_out;
SELECT 'checkpoint=' || last_committed_seq AS checkpoint_seq
FROM duckdb_nats_ingest_checkpoints
WHERE stream_name = '${stream_name}' AND durable_name = '${durable_name}';
SELECT 'checkpoint_log_rows=' || COUNT(*) AS checkpoint_log_rows
FROM duckdb_nats_ingest_checkpoint_log
WHERE stream_name = '${stream_name}' AND durable_name = '${durable_name}';
END
EXPECT stop1=${start_job_a}|${stream_name}|ingest_out|${durable_name} 10
EXPECT count=4 10
EXPECT checkpoint=4 10
EXPECT checkpoint_log_rows=1 10
SEND
CREATE TABLE ingest_bad_target(
    stream_name INTEGER,
    subject INTEGER,
    sequence INTEGER,
    ts INTEGER,
    payload INTEGER
);
SELECT 'failed_start=' || failed || '/' || stopped AS failed_start
FROM nats_start_ingest(
    job_name := '${start_job_a}_bad_target',
    stream_name := '${stream_name}',
    target_table := 'ingest_bad_target',
    durable_name := '${durable_name}_bad_target',
    url := '${NATS_URL}',
    batch_size := 4,
    poll_ms := 100,
    fetch_timeout_ms := 100,
    start_seq := 1
);
END
EXPECT failed_start=true/true 10
SEND
SELECT 'failed_status=' || failed || '/' || stopped || '/' || (length(last_error) > 0) AS failed_status
FROM nats_ingest_status(job_name := '${start_job_a}_bad_target');
END
EXPECT failed_status=true/true/true 10
QUIT
SQL
then
  cat "$log_file" >&2
  exit 1
fi

echo "Ingest harness passed"
