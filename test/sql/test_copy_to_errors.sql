-- Test suite for JetStream COPY TO error handling
--
-- Prerequisites:
--   1. NATS server running
--   2. scripts/setup-streams.sh has created copy_out

LOAD 'build/release/extension/nats_js/nats_js.duckdb_extension';

.print ========================================
.print Test 1: Missing payload column should fail
.print Expected: Error message
.print ========================================

CREATE TABLE copy_missing_payload(
    subject VARCHAR
);

COPY copy_missing_payload
TO 'copy_out'
(FORMAT nats_js, url 'nats://127.0.0.1:4222');

.print
.print COPY TO error tests completed!
.print ========================================
