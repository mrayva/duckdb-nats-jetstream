-- Test suite for MessagePack extraction support in nats_scan.

LOAD 'build/release/extension/nats_js/nats_js.duckdb_extension';

.print ========================================
.print MessagePack extraction
.print ========================================

SELECT
    device_id,
    "metrics.kw",
    "metrics.voltage",
    online,
    reading_count,
    typeof(payload) AS payload_type
FROM nats_scan(
    'telemetry',
    nats_subject := 'telemetry.msgpack',
    msgpack_extract := ['device_id', 'metrics.kw', 'metrics.voltage', 'online', 'reading_count']
)
WHERE device_id = 'msgpack-device-1';
