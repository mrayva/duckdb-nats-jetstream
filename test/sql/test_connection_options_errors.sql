-- Connection security option validation. Each statement is expected to fail.

LOAD 'build/release/extension/nats_js/nats_js.duckdb_extension';

SELECT *
FROM nats_scan(
    'telemetry',
    tls_cert_file := '/tmp/client-cert.pem'
)
LIMIT 1;

SELECT *
FROM nats_stream_stats(
    'telemetry',
    credentials_file := '/definitely/missing/nats.creds'
);

CREATE TABLE copy_security_probe(
    stream VARCHAR,
    subject VARCHAR,
    seq UBIGINT,
    ts_nats TIMESTAMP,
    payload BLOB
);

COPY copy_security_probe FROM 'telemetry'
(FORMAT nats_js,
 url 'nats://127.0.0.1:4222',
 tls_key_file '/tmp/client-key.pem');
