#!/usr/bin/env bash
#
# Quick live smoke test for the core NATS subscribe and JetStream KV paths:
# KV bucket/CRUD/scan, kv watch job (incl. pause/resume), and subscribe job lifecycle against a running NATS server.
# Uses its own bucket/subject/job/table names and cleans them up on exit.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
source "$ROOT_DIR/scripts/duckdb_host_lib.sh"
DUCKDB_BIN="${DUCKDB_BIN:-$ROOT_DIR/build/release/duckdb}"
DUCKDB_LIB="${DUCKDB_LIB:-$(duckdb_host_lib_resolve)}"
EXTENSION_PATH="${EXTENSION_PATH:-$ROOT_DIR/build/release/extension/nats_js/nats_js.duckdb_extension}"
NATS_URL="${NATS_URL:-nats://127.0.0.1:4222}"
NATS_CLI="${NATS_CLI:-$HOME/nats}"
PYTHON_BIN="${PYTHON_BIN:-$ROOT_DIR/.venv/bin/python}"

if [ ! -x "$DUCKDB_BIN" ]; then
  echo "DuckDB binary not found: $DUCKDB_BIN (build with: make release BUILD_SHELL=1)" >&2
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

# The 2.0 shell switches to JSON output when it detects an agent environment.
unset AI_AGENT CLAUDECODE

echo "Checking NATS connection at $NATS_URL"
"$NATS_CLI" server check connection --server "$NATS_URL" >/dev/null

bucket="smoke_kv"
subject="smoke.subscribe"
db_file="$(mktemp /tmp/nats_smoke_subscribe_kv.XXXXXX.duckdb)"
log_file="$(mktemp /tmp/nats_smoke_subscribe_kv.XXXXXX.log)"
rm -f "$db_file"
cleanup() {
  rc=$?
  "$NATS_CLI" kv del "$bucket" --force --server="$NATS_URL" >/dev/null 2>&1 || true
  rm -f "$log_file" "$db_file"
  exit $rc
}
trap cleanup EXIT

# Start from a clean bucket in case an earlier run was interrupted.
"$NATS_CLI" kv del "$bucket" --force --server="$NATS_URL" >/dev/null 2>&1 || true

(
  sleep 2
  "$NATS_CLI" pub --quiet --count 3 --server="$NATS_URL" "$subject" "smoke-{{Count}}" >/dev/null
) &

if ! DUCKDB_LIB="$DUCKDB_LIB" "$PYTHON_BIN" "$ROOT_DIR/scripts/duckdb_session.py" --duckdb-bin "$DUCKDB_BIN" --db-file "$db_file" <<SQL >"$log_file" 2>&1
SEND
LOAD '${EXTENSION_PATH}';
SELECT 'create=' || bucket || '|' || created AS r FROM nats_kv_create_bucket('${bucket}', history := 3, url := '${NATS_URL}');
SELECT 'put1=' || revision AS r FROM nats_kv_put('${bucket}', 'cfg.a', 'v1', url := '${NATS_URL}');
SELECT 'put2=' || revision AS r FROM nats_kv_put('${bucket}', 'cfg.b', 'v2', url := '${NATS_URL}');
SELECT 'get=' || key || '|' || value::VARCHAR || '|' || revision AS r FROM nats_kv_get('${bucket}', 'cfg.a', url := '${NATS_URL}');
SELECT 'update=' || revision AS r FROM nats_kv_update('${bucket}', 'cfg.a', 'v1b', 1, url := '${NATS_URL}');
SELECT 'scan=' || count(*) AS r FROM nats_kv_scan('${bucket}', key_filter := 'cfg.*', url := '${NATS_URL}');
SELECT 'status=' || bucket || '|' || history AS r FROM nats_kv_status('${bucket}', url := '${NATS_URL}');
END
EXPECT create=${bucket}|true 15
EXPECT put1=1 5
EXPECT put2=2 5
EXPECT get=cfg.a|v1|1 5
EXPECT update=3 5
EXPECT scan=2 5
EXPECT status=${bucket}|3 5
SEND
SELECT 'kvwatch_start=' || job_name AS r FROM nats_start_kv_watch(
    job_name := 'smoke_kv_watch',
    target_table := 'main.smoke_kv_changes',
    bucket := '${bucket}',
    url := '${NATS_URL}',
    batch_size := 1,
    poll_ms := 100,
    create_target_table := true
);
SELECT 'sub_start=' || job_name AS r FROM nats_start_subscribe(
    job_name := 'smoke_sub',
    target_table := 'main.smoke_sub_out',
    url := '${NATS_URL}',
    subject := '${subject}',
    batch_size := 1,
    poll_ms := 100,
    create_target_table := true
);
END
EXPECT kvwatch_start=smoke_kv_watch 15
EXPECT sub_start=smoke_sub 15
SLEEP 1
SEND
SELECT 'kv_live_put=' || revision AS r FROM nats_kv_put('${bucket}', 'cfg.c', 'v3', url := '${NATS_URL}');
END
EXPECT kv_live_put=4 5
SLEEP 4
SEND
SELECT 'watch_running=' || running || '/' || failed AS r FROM nats_kv_watch_status(job_name := 'smoke_kv_watch');
SELECT 'watch_rows_ok=' || (count(*) >= 3) AS r FROM main.smoke_kv_changes;
SELECT 'sub_status=' || rows_inserted || '/' || running || '/' || failed AS r FROM nats_subscribe_status(job_name := 'smoke_sub');
SELECT 'sub_rows=' || count(*) AS r FROM main.smoke_sub_out;
END
EXPECT watch_running=true/false 10
EXPECT watch_rows_ok=true 10
EXPECT sub_status=3/true/false 10
EXPECT sub_rows=3 10
SEND
SELECT 'kvwatch_pause=' || job_name AS r FROM nats_pause_kv_watch(job_name := 'smoke_kv_watch');
END
EXPECT kvwatch_pause=smoke_kv_watch 10
SLEEP 1
SEND
CREATE TABLE smoke_mark AS SELECT count(*) AS n FROM main.smoke_kv_changes;
SELECT 'paused_put=' || revision AS r FROM nats_kv_put('${bucket}', 'cfg.d', 'v4', url := '${NATS_URL}');
END
EXPECT paused_put=5 5
SLEEP 2
SEND
SELECT 'paused_hold=' || ((SELECT count(*) FROM main.smoke_kv_changes) = (SELECT n FROM smoke_mark)) || '|' || paused AS r FROM nats_kv_watch_status(job_name := 'smoke_kv_watch');
SELECT 'kvwatch_resume=' || job_name AS r FROM nats_resume_kv_watch(job_name := 'smoke_kv_watch');
END
EXPECT paused_hold=true|true 10
EXPECT kvwatch_resume=smoke_kv_watch 10
SLEEP 2
SEND
SELECT 'resumed_grew=' || ((SELECT count(*) FROM main.smoke_kv_changes) > (SELECT n FROM smoke_mark)) AS r;
END
EXPECT resumed_grew=true 10
SEND
SELECT 'kvwatch_stop=' || job_name AS r FROM nats_stop_kv_watch(job_name := 'smoke_kv_watch');
SELECT 'sub_stop=' || job_name AS r FROM nats_stop_subscribe(job_name := 'smoke_sub');
END
EXPECT kvwatch_stop=smoke_kv_watch 10
EXPECT sub_stop=smoke_sub 10
SLEEP 2
SEND
SELECT 'kvwatch_remove=' || job_name AS r FROM nats_remove_kv_watch(job_name := 'smoke_kv_watch');
SELECT 'sub_remove=' || job_name AS r FROM nats_remove_subscribe(job_name := 'smoke_sub');
SELECT 'delete_key=' || deleted AS r FROM nats_kv_delete('${bucket}', 'cfg.c', url := '${NATS_URL}');
SELECT 'delete_bucket=' || deleted AS r FROM nats_kv_delete_bucket('${bucket}', url := '${NATS_URL}');
END
EXPECT kvwatch_remove=smoke_kv_watch 10
EXPECT sub_remove=smoke_sub 10
EXPECT delete_key=true 10
EXPECT delete_bucket=true 10
QUIT
SQL
then
  cat "$log_file" >&2
  exit 1
fi

echo "Subscribe/KV smoke test passed"
