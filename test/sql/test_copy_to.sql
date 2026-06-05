-- Test suite for JetStream COPY TO format
--
-- Prerequisites:
--   1. NATS server running
--   2. scripts/setup-streams.sh has created copy_out

LOAD 'build/release/extension/nats_js/nats_js.duckdb_extension';

.print ========================================
.print Test 1: COPY TO with subject/payload columns
.print ========================================

CREATE TABLE copy_source(
    subject VARCHAR,
    payload VARCHAR
);

INSERT INTO copy_source VALUES
    ('copy_out.alpha', 'hello'),
    ('copy_out.beta', 'world');

COPY copy_source
TO 'copy_out'
(FORMAT nats_js, url 'nats://127.0.0.1:4222');

WITH copy_stats AS (
    SELECT
        COUNT(*) AS row_count,
        MIN(subject) AS min_subject,
        MAX(subject) AS max_subject,
        COUNT(*) FILTER (WHERE CAST(payload AS VARCHAR) = 'hello') AS hello_count,
        COUNT(*) FILTER (WHERE CAST(payload AS VARCHAR) = 'world') AS world_count
    FROM nats_scan('copy_out', url := 'nats://127.0.0.1:4222')
)
SELECT
    CASE WHEN
        row_count = 2
        AND min_subject = 'copy_out.alpha'
        AND max_subject = 'copy_out.beta'
        AND hello_count = 1
        AND world_count = 1
    THEN 'ok'
    ELSE error('COPY TO should publish the two subject/payload rows')
    END AS copy_to_check
FROM copy_stats;

.print
.print Test 2: COPY TO with constant subject option
.print ========================================

CREATE TABLE copy_constant_payloads(
    payload VARCHAR
);

INSERT INTO copy_constant_payloads VALUES
    ('gamma'),
    ('delta');

COPY copy_constant_payloads
TO 'copy_out'
(FORMAT nats_js,
 url 'nats://127.0.0.1:4222',
 subject 'copy_out.constant');

WITH copy_stats AS (
    SELECT
        COUNT(*) AS row_count,
        COUNT(*) FILTER (WHERE subject = 'copy_out.constant') AS constant_count,
        COUNT(*) FILTER (WHERE CAST(payload AS VARCHAR) = 'gamma') AS gamma_count,
        COUNT(*) FILTER (WHERE CAST(payload AS VARCHAR) = 'delta') AS delta_count
    FROM nats_scan('copy_out', url := 'nats://127.0.0.1:4222')
)
SELECT
    CASE WHEN
        row_count = 4
        AND constant_count = 2
        AND gamma_count = 1
        AND delta_count = 1
    THEN 'ok'
    ELSE error('COPY TO constant subject should publish two additional rows')
    END AS copy_to_constant_check
FROM copy_stats;

.print
.print COPY TO regression tests completed successfully!
.print ========================================
