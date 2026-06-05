-- Test suite for JetStream COPY TO -> COPY FROM round-trip regression
--
-- Prerequisites:
--   1. NATS server running
--   2. scripts/setup-streams.sh has created copy_roundtrip

LOAD 'build/release/extension/nats_js/nats_js.duckdb_extension';

.print ========================================
.print Test 1: COPY TO then COPY FROM round-trip
.print ========================================

CREATE TABLE copy_roundtrip_source(
    subject VARCHAR,
    payload VARCHAR
);

INSERT INTO copy_roundtrip_source VALUES
    ('copy_roundtrip.alpha', 'hello'),
    ('copy_roundtrip.beta', 'world');

COPY copy_roundtrip_source
TO 'copy_roundtrip'
(FORMAT nats_js, url 'nats://127.0.0.1:4222');

CREATE TABLE copy_roundtrip_dest(
    stream_name VARCHAR,
    subject VARCHAR,
    sequence UBIGINT,
    ts_nats TIMESTAMP,
    payload BLOB
);

COPY copy_roundtrip_dest
FROM 'copy_roundtrip'
(FORMAT nats_js, url 'nats://127.0.0.1:4222');

WITH roundtrip_stats AS (
    SELECT
        COUNT(*) AS row_count,
        MIN(sequence) AS min_seq,
        MAX(sequence) AS max_seq,
        COUNT(*) FILTER (WHERE subject = 'copy_roundtrip.alpha' AND CAST(payload AS VARCHAR) = 'hello') AS alpha_count,
        COUNT(*) FILTER (WHERE subject = 'copy_roundtrip.beta' AND CAST(payload AS VARCHAR) = 'world') AS beta_count
    FROM copy_roundtrip_dest
)
SELECT
    CASE WHEN
        row_count = 2
        AND min_seq = 1
        AND max_seq = 2
        AND alpha_count = 1
        AND beta_count = 1
    THEN 'ok'
    ELSE error('COPY TO followed by COPY FROM should round-trip two published rows')
    END AS roundtrip_check
FROM roundtrip_stats;

.print COPY TO -> COPY FROM round-trip regression completed successfully!
.print ========================================

.print ========================================
.print Test 2: COPY TO with constant subject round-trip
.print ========================================

CREATE TABLE copy_roundtrip_const_source(
    payload VARCHAR
);

INSERT INTO copy_roundtrip_const_source VALUES
    ('gamma'),
    ('delta');

COPY copy_roundtrip_const_source
TO 'copy_roundtrip_const'
(FORMAT nats_js,
 url 'nats://127.0.0.1:4222',
 subject 'copy_roundtrip_const.constant');

CREATE TABLE copy_roundtrip_const_dest(
    stream_name VARCHAR,
    subject VARCHAR,
    sequence UBIGINT,
    ts_nats TIMESTAMP,
    payload BLOB
);

COPY copy_roundtrip_const_dest
FROM 'copy_roundtrip_const'
(FORMAT nats_js, url 'nats://127.0.0.1:4222');

WITH roundtrip_const_stats AS (
    SELECT
        COUNT(*) AS row_count,
        MIN(sequence) AS min_seq,
        MAX(sequence) AS max_seq,
        COUNT(*) FILTER (WHERE subject = 'copy_roundtrip_const.constant') AS constant_count,
        COUNT(*) FILTER (WHERE CAST(payload AS VARCHAR) = 'gamma') AS gamma_count,
        COUNT(*) FILTER (WHERE CAST(payload AS VARCHAR) = 'delta') AS delta_count
    FROM copy_roundtrip_const_dest
)
SELECT
    CASE WHEN
        row_count = 2
        AND min_seq = 1
        AND max_seq = 2
        AND constant_count = 2
        AND gamma_count = 1
        AND delta_count = 1
    THEN 'ok'
    ELSE error('COPY TO constant subject should round-trip two published rows')
    END AS roundtrip_const_check
FROM roundtrip_const_stats;

.print COPY TO constant-subject round-trip regression completed successfully!
.print ========================================
