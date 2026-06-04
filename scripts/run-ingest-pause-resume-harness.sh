#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
DUCKDB_BIN="${DUCKDB_BIN:-/home/mrayva/.duckdb/cli/1.5.3/duckdb}"
DUCKDB_LIB="${DUCKDB_LIB:-$ROOT_DIR/build/release/src/libduckdb.so}"
EXTENSION_PATH="${EXTENSION_PATH:-$ROOT_DIR/build/release/extension/nats_js/nats_js.duckdb_extension}"
NATS_URL="${NATS_URL:-nats://127.0.0.1:4222}"
NATS_CLI="${NATS_CLI:-$HOME/nats}"

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

echo "Checking NATS connection at $NATS_URL"
"$NATS_CLI" server check connection --server "$NATS_URL"

echo "Preparing JetStream streams"
NATS_URL="$NATS_URL" NATS_CLI="$NATS_CLI" RESET_STREAMS="${RESET_STREAMS:-1}" "$ROOT_DIR/scripts/setup-streams.sh"

db_file="$(mktemp /tmp/nats_ingest_pause_resume.XXXXXX.duckdb)"
log_file="$(mktemp /tmp/nats_ingest_pause_resume.XXXXXX.log)"
rm -f "$db_file"
trap 'rc=$?; rm -f "$log_file" "$db_file"; exit $rc' EXIT

(
  sleep 2
  echo "Publishing second ingest batch to ingest_resume"
  "$NATS_CLI" pub --jetstream --quiet --count 4 --server="$NATS_URL" "ingest_resume.items" "pause-resume-test-{{Count}}" >/dev/null
) &

if ! DUCKDB_LIB="$DUCKDB_LIB" NATS_INGEST_DISABLE_REHYDRATE=1 python3 "$ROOT_DIR/scripts/duckdb_session.py" --duckdb-bin "$DUCKDB_BIN" --db-file "$db_file" <<SQL >"$log_file" 2>&1
SEND
LOAD '${EXTENSION_PATH}';
CREATE TABLE ingest_out(
    stream_name VARCHAR,
    subject VARCHAR,
    sequence UBIGINT,
    ts TIMESTAMP,
    payload BLOB
);
SELECT 'start=' || job_name || '|' || stream_name || '|' || target_table || '|' || durable_name AS start_result
FROM nats_start_ingest(
    job_name := 'ingest_pause_resume',
    stream_name := 'ingest_resume',
    target_table := 'ingest_out',
    durable_name := 'duckdb_ingest_pause_resume',
    url := 'nats://127.0.0.1:4222',
    batch_size := 4,
    poll_ms := 100,
    fetch_timeout_ms := 100,
    start_seq := 1
);
END
EXPECT start=ingest_pause_resume|ingest_resume|ingest_out|duckdb_ingest_pause_resume 10
SEND
SELECT 'status1=' || paused || '/' || pause_requested || '/' || rows_inserted || '/' || batches_committed || '/' ||
       last_batch_rows || '/' || last_committed_seq AS ingest_status
FROM nats_ingest_status(job_name := 'ingest_pause_resume');
END
EXPECT status1=false/false/4/1/4/4 30
SEND
SELECT 'pause=' || job_name || '|' || stream_name || '|' || target_table || '|' || durable_name || '|' ||
       paused || '/' || pause_requested AS pause_result
FROM nats_pause_ingest(job_name := 'ingest_pause_resume');
END
EXPECT pause=ingest_pause_resume|ingest_resume|ingest_out|duckdb_ingest_pause_resume|true/true 30
SLEEP 3
SEND
SELECT 'status2=' || paused || '/' || pause_requested || '/' || rows_inserted || '/' || batches_committed || '/' ||
       last_batch_rows || '/' || last_committed_seq AS ingest_status
FROM nats_ingest_status(job_name := 'ingest_pause_resume');
END
EXPECT status2=true/true/4/1/4/4 30
SEND
SELECT 'resume=' || job_name || '|' || stream_name || '|' || target_table || '|' || durable_name || '|' ||
       paused || '/' || pause_requested AS resume_result
FROM nats_resume_ingest(job_name := 'ingest_pause_resume');
END
EXPECT resume=ingest_pause_resume|ingest_resume|ingest_out|duckdb_ingest_pause_resume|false/false 30
SLEEP 3
SEND
SELECT 'status3=' || paused || '/' || pause_requested || '/' || rows_inserted || '/' || batches_committed || '/' ||
       last_batch_rows || '/' || last_committed_seq AS ingest_status
FROM nats_ingest_status(job_name := 'ingest_pause_resume');
SELECT 'count=' || COUNT(*) AS inserted_rows FROM ingest_out;
SELECT 'stop=' || job_name || '|' || stream_name || '|' || target_table || '|' || durable_name AS stop_result
FROM nats_stop_ingest(job_name := 'ingest_pause_resume');
END
EXPECT status3=false/false/8/2/4/8 30
EXPECT count=8 10
EXPECT stop=ingest_pause_resume|ingest_resume|ingest_out|duckdb_ingest_pause_resume 10
QUIT
SQL
then
  cat "$log_file" >&2
  exit 1
fi

echo "Ingest pause/resume harness passed"
