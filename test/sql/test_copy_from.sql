-- Test suite for JetStream COPY FROM format
--
-- Prerequisites:
--   1. NATS server running
--   2. scripts/setup-streams.sh has seeded ingest_resume

LOAD 'build/release/extension/nats_js/nats_js.duckdb_extension';

.print ========================================
.print Test 1: Plain COPY FROM ingest_resume
.print ========================================

CREATE TABLE copy_plain(
    stream VARCHAR,
    subject VARCHAR,
    seq UBIGINT,
    ts_nats TIMESTAMP,
    payload BLOB
);

COPY copy_plain
FROM 'ingest_resume'
(FORMAT nats_js, url 'nats://127.0.0.1:4222');

WITH copy_stats AS (
    SELECT
        COUNT(*) AS row_count,
        MIN(seq) AS min_seq,
        MAX(seq) AS max_seq,
        MIN(subject) AS min_subject,
        MAX(subject) AS max_subject
    FROM copy_plain
)
SELECT
    CASE WHEN
        row_count = 4
        AND min_seq = 1
        AND max_seq = 4
        AND min_subject = 'ingest_resume.items'
        AND max_subject = 'ingest_resume.items'
    THEN 'ok'
    ELSE error('plain COPY FROM ingest_resume should load four sequential rows')
    END AS plain_copy_check
FROM copy_stats;

.print
.print ========================================
.print Test 2: Bounded COPY FROM with start/end sequence
.print ========================================

CREATE TABLE copy_range(
    stream VARCHAR,
    subject VARCHAR,
    seq UBIGINT,
    ts_nats TIMESTAMP,
    payload BLOB
);

COPY copy_range
FROM 'ingest_resume'
(FORMAT nats_js,
 url 'nats://127.0.0.1:4222',
 start_seq 2,
 end_seq 3);

WITH copy_stats AS (
    SELECT
        COUNT(*) AS row_count,
        MIN(seq) AS min_seq,
        MAX(seq) AS max_seq,
        MIN(subject) AS min_subject,
        MAX(subject) AS max_subject
    FROM copy_range
)
SELECT
    CASE WHEN
        row_count = 2
        AND min_seq = 2
        AND max_seq = 3
        AND min_subject = 'ingest_resume.items'
        AND max_subject = 'ingest_resume.items'
    THEN 'ok'
    ELSE error('bounded COPY FROM ingest_resume should load only sequences 2 and 3')
    END AS bounded_copy_check
FROM copy_stats;

.print
.print COPY FROM regression tests completed successfully!
.print ========================================
