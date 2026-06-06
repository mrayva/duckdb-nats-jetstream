-- Test suite for JetStream COPY FROM error handling
--
-- Prerequisites:
--   1. NATS server running
--   2. scripts/setup-streams.sh has seeded ingest_resume

LOAD 'build/release/extension/nats_js/nats_js.duckdb_extension';

.print ========================================
.print Test 1: Target schema mismatch should fail
.print Expected: Error message
.print ========================================

CREATE TABLE copy_bad_schema(
    stream VARCHAR,
    subject VARCHAR,
    seq UBIGINT,
    ts_nats TIMESTAMP,
    payload VARCHAR
);

COPY copy_bad_schema
FROM 'ingest_resume'
(FORMAT nats_js, url 'nats://127.0.0.1:4222');

.print
.print ========================================
.print Test 2: Conflicting subject filters should fail
.print Expected: Error message
.print ========================================

CREATE TABLE copy_bad_options(
    stream VARCHAR,
    subject VARCHAR,
    seq UBIGINT,
    ts_nats TIMESTAMP,
    payload BLOB
);

COPY copy_bad_options
FROM 'ingest_resume'
(FORMAT nats_js,
 url 'nats://127.0.0.1:4222',
 subject 'ingest_resume.items',
 subject_contains 'ingest_resume.items');

.print
.print COPY FROM error tests completed!
.print ========================================
