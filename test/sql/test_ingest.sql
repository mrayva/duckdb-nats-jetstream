-- Test suite for JetStream ingest control APIs
--
-- Prerequisites:
--   1. NATS server running
--   2. scripts/setup-streams.sh has seeded ingest_resume

LOAD 'build/release/extension/nats_js/nats_js.duckdb_extension';

CREATE TABLE ingest_out(
    stream_name VARCHAR,
    subject VARCHAR,
    sequence UBIGINT,
    ts TIMESTAMP,
    payload BLOB
);

.print ========================================
.print Test 1: Start ingest
.print ========================================

SELECT *
FROM nats_start_ingest(
    job_name := 'ingest_probe3',
    stream_name := 'ingest_resume',
    target_table := 'ingest_out',
    durable_name := 'duckdb_ingest_resume',
    url := 'nats://localhost:4222',
    batch_size := 4,
    poll_ms := 10000,
    fetch_timeout_ms := 100,
    start_seq := 1
);

.print
.print ========================================
.print Test 2: Give the ingest worker time to drain the seeded batch
.print ========================================

SELECT SUM(i)
FROM range(100000000) t(i);

.print
.print ========================================
.print Test 3: Ingest status after drain
.print ========================================

SELECT
    paused,
    pause_requested,
    rows_inserted,
    batches_committed,
    fetches_completed,
    last_batch_rows,
    last_committed_seq
FROM nats_ingest_status(job_name := 'ingest_probe3');

.print
.print ========================================
.print Test 4: Bounded ingest inserts the expected rows
.print ========================================

SELECT COUNT(*) AS inserted_rows
FROM ingest_out;

.print
.print ========================================
.print Test 5: Stop ingest
.print ========================================

SELECT *
FROM nats_stop_ingest(job_name := 'ingest_probe3');

.print
.print All ingest tests completed successfully!
.print ========================================
