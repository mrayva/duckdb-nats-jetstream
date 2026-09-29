-- Descriptor sets are an alternative schema source for protobuf extraction.
LOAD 'build/release/extension/nats_js/nats_js.duckdb_extension';

SELECT CASE WHEN count(*) > 0 AND count(device_id) = count(*) AND count(location_zone) = count(*)
    THEN 'ok' ELSE error('descriptor-set nats_scan should decode projected protobuf fields') END AS descriptor_set_scan
FROM nats_scan('telemetry_proto',
    proto_descriptor_set := '/tmp/nats-js-telemetry.protoset',
    proto_message := 'telemetry.Telemetry',
    proto_extract := ['device_id', 'location.zone']);

CREATE TABLE descriptor_set_copy_from (
    stream VARCHAR,
    subject VARCHAR,
    seq UBIGINT,
    ts_nats TIMESTAMP,
    payload BLOB,
    device_id VARCHAR,
    location_zone VARCHAR
);

COPY descriptor_set_copy_from
FROM 'telemetry_proto'
(FORMAT nats_js,
 url 'nats://127.0.0.1:4222',
 start_seq 1,
 end_seq 1,
 proto_descriptor_set '/tmp/nats-js-telemetry.protoset',
 proto_message 'telemetry.Telemetry',
 proto_extract ['device_id', 'location.zone']);

SELECT CASE WHEN count(*) = 1 AND count(device_id) = 1 AND count(location_zone) = 1 AND min(seq) = 1
    THEN 'ok' ELSE error('descriptor-set COPY FROM should decode the bounded protobuf row') END AS descriptor_set_copy_from
FROM descriptor_set_copy_from;
