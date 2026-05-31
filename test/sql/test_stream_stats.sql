-- Test suite for JetStream metadata/statistics table functions
--
-- Prerequisites:
--   1. NATS server running
--   2. Telemetry fixtures published

LOAD 'build/release/extension/nats_js/nats_js.duckdb_extension';

.print ========================================
.print Test 1: Stream stats returns expected metadata columns
.print ========================================

SELECT
    stream,
    messages > 0 AS has_messages,
    bytes > 0 AS has_bytes,
    first_seq,
    last_seq,
    deleted_count,
    consumer_count,
    subject_count
FROM nats_stream_stats('telemetry', url := 'nats://127.0.0.1:4222');

.print
.print ========================================
.print Test 2: Stream info alias matches scan count
.print ========================================

SELECT
    s.messages AS stats_messages,
    c.scan_count,
    s.messages = c.scan_count AS counts_match
FROM nats_stream_info('telemetry', url := 'nats://127.0.0.1:4222') s
CROSS JOIN (
    SELECT COUNT(*) AS scan_count
    FROM nats_scan('telemetry', url := 'nats://127.0.0.1:4222')
) c;

.print
.print ========================================
.print Test 3: Resume bounds are available without scanning messages
.print ========================================

SELECT
    last_seq,
    last_seq + 1 AS next_start_seq
FROM nats_stream_stats('telemetry_proto', url := 'nats://127.0.0.1:4222');

.print
.print ========================================
.print Test 4: Range stats matches scan count for contiguous range
.print ========================================

SELECT
    r.available_messages,
    c.scan_count,
    r.available_messages = c.scan_count AS counts_match,
    r.has_gaps
FROM nats_stream_range_stats('telemetry_proto',
    url := 'nats://127.0.0.1:4222',
    start_seq := 1,
    end_seq := 500
) r
CROSS JOIN (
    SELECT COUNT(*) AS scan_count
    FROM nats_scan('telemetry_proto',
        url := 'nats://127.0.0.1:4222',
        start_seq := 1,
        end_seq := 500
    )
) c;

.print
.print ========================================
.print Test 5: Range stats flags ranges past the current stream tail
.print ========================================

SELECT
    requested_messages,
    overlap_messages,
    available_messages,
    has_gaps,
    ends_after_last
FROM nats_stream_range_stats('telemetry_proto',
    url := 'nats://127.0.0.1:4222',
    start_seq := 495,
    end_seq := 510
);

.print
.print ========================================
.print Test 6: Range stats reports deleted sequence gaps
.print ========================================

WITH range_stats AS (
    SELECT *
    FROM nats_stream_range_stats('stats_gaps',
        url := 'nats://127.0.0.1:4222',
        start_seq := 1,
        end_seq := 10
    )
)
SELECT
    CASE WHEN
        requested_messages = 10
        AND overlap_messages = 10
        AND deleted_in_range = 2
        AND available_messages = 8
        AND has_gaps
        AND NOT starts_before_first
        AND NOT ends_after_last
    THEN 'ok'
    ELSE error('stats_gaps full range should report two deleted messages and eight available messages')
    END AS deleted_gap_range_check
FROM range_stats;

.print
.print ========================================
.print Test 7: Deleted-gap range stats matches scan count
.print ========================================

SELECT
    r.deleted_in_range,
    r.available_messages,
    c.scan_count,
    CASE WHEN
        r.deleted_in_range = 2
        AND r.available_messages = c.scan_count
        AND r.has_gaps
    THEN 'ok'
    ELSE error('stats_gaps available_messages should match nats_scan count across deleted gaps')
    END AS deleted_gap_scan_count_check
FROM nats_stream_range_stats('stats_gaps',
    url := 'nats://127.0.0.1:4222',
    start_seq := 1,
    end_seq := 10
) r
CROSS JOIN (
    SELECT COUNT(*) AS scan_count
    FROM nats_scan('stats_gaps',
        url := 'nats://127.0.0.1:4222',
        start_seq := 1,
        end_seq := 10
    )
) c;

.print
.print ========================================
.print Test 8: Partial range only counts deleted gaps inside overlap
.print ========================================

WITH range_stats AS (
    SELECT *
    FROM nats_stream_range_stats('stats_gaps',
        url := 'nats://127.0.0.1:4222',
        start_seq := 4,
        end_seq := 8
    )
)
SELECT
    CASE WHEN
        requested_messages = 5
        AND overlap_messages = 5
        AND deleted_in_range = 1
        AND available_messages = 4
        AND has_gaps
    THEN 'ok'
    ELSE error('stats_gaps partial range should count only deleted sequence 7')
    END AS partial_deleted_gap_range_check
FROM range_stats;

.print
.print ========================================
.print Test 9: Bounded scan over deleted gaps returns only available messages
.print ========================================

SELECT
    CASE WHEN COUNT(*) = 8
    THEN 'ok'
    ELSE error('bounded nats_scan over stats_gaps should return eight available messages')
    END AS bounded_deleted_gap_scan_check
FROM nats_scan('stats_gaps',
    url := 'nats://127.0.0.1:4222',
    start_seq := 1,
    end_seq := 10,
    batch_size := 10,
    fetch_timeout_ms := 5000
);

.print
.print ========================================
.print All stream stats tests completed successfully!
.print ========================================
