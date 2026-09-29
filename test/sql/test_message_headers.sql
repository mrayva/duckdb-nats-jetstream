-- Optional NATS message headers are exposed as JSON only when requested.
-- The local integration harness publishes one message with repeated X-Tag values.

LOAD 'build/release/extension/nats_js/nats_js.duckdb_extension';

SELECT CASE WHEN count(*) = 1 AND bool_and(
    typeof(headers) = 'JSON'
    AND contains(headers::VARCHAR, '"X-Trace-Id":["trace-123"]')
    AND contains(headers::VARCHAR, '"X-Tag":["first","second"]')
) THEN 'ok' ELSE error('nats_scan should return header JSON and preserve repeated values') END AS headers_check
FROM nats_scan('telemetry', nats_subject := 'telemetry.dc1.power.header-test', headers := true);

SELECT CASE WHEN count(*) = 10 AND bool_and(headers::VARCHAR = '{}')
    THEN 'ok' ELSE error('headerless NATS messages should return an empty JSON object') END AS empty_headers_check
FROM nats_scan('telemetry', start_seq := 1, end_seq := 10, headers := true);

SELECT count(*) AS default_scan_rows
FROM nats_scan('telemetry', start_seq := 1, end_seq := 1);
