#!/usr/bin/env bash

set -e

NATS_URL="${NATS_URL:-nats://localhost:4222}"
if [ -n "${NATS_CLI:-}" ]; then
  NATS_BIN="${NATS_CLI}"
elif [ -x "$HOME/nats" ]; then
  NATS_BIN="$HOME/nats"
else
  NATS_BIN="nats"
fi

if [ "${RESET_STREAMS:-0}" = "1" ]; then
  echo "Resetting JetStream streams..."
  for stream in telemetry environmental telemetry_proto events stats_gaps ingest_resume; do
    "$NATS_BIN" stream rm "$stream" --force --server="${NATS_URL}" >/dev/null 2>&1 || true
  done
  for consumer in duckdb_ingest_test duckdb_ingest_test_v1 duckdb_ingest_test_fresh duckdb_ingest_probe3; do
    "$NATS_BIN" consumer rm stats_gaps "$consumer" --force --server="${NATS_URL}" >/dev/null 2>&1 || true
  done
fi

echo "Setting up JetStream streams..."

# Create telemetry stream for power monitoring data
"$NATS_BIN" stream add telemetry \
  --subjects "telemetry.>" \
  --storage file \
  --retention limits \
  --max-msgs=-1 \
  --max-bytes=-1 \
  --max-age=7d \
  --max-msg-size=1048576 \
  --discard old \
  --dupe-window=2m \
  --replicas=1 \
  --server="${NATS_URL}" \
  --defaults

echo "Created stream: telemetry"

# Create environmental stream for temperature/humidity data
"$NATS_BIN" stream add environmental \
  --subjects "environmental.>" \
  --storage file \
  --retention limits \
  --max-msgs=-1 \
  --max-bytes=-1 \
  --max-age=7d \
  --max-msg-size=1048576 \
  --discard old \
  --dupe-window=2m \
  --replicas=1 \
  --server="${NATS_URL}" \
  --defaults

echo "Created stream: environmental"

# Create telemetry_proto stream for protobuf test data
"$NATS_BIN" stream add telemetry_proto \
  --subjects "telemetry_proto.>" \
  --storage file \
  --retention limits \
  --max-msgs=-1 \
  --max-bytes=-1 \
  --max-age=7d \
  --max-msg-size=1048576 \
  --discard old \
  --dupe-window=2m \
  --replicas=1 \
  --server="${NATS_URL}" \
  --defaults

echo "Created stream: telemetry_proto"

# Create events stream for audit/system events
"$NATS_BIN" stream add events \
  --subjects "events.>" \
  --storage file \
  --retention limits \
  --max-msgs=-1 \
  --max-bytes=-1 \
  --max-age=30d \
  --max-msg-size=1048576 \
  --discard old \
  --dupe-window=2m \
  --replicas=1 \
  --server="${NATS_URL}" \
  --defaults

echo "Created stream: events"

# Create a small deterministic stream with deleted sequence gaps for range stats tests
"$NATS_BIN" stream add stats_gaps \
  --subjects "stats_gaps.>" \
  --storage file \
  --retention limits \
  --max-msgs=-1 \
  --max-bytes=-1 \
  --max-age=7d \
  --max-msg-size=1048576 \
  --discard old \
  --dupe-window=2m \
  --replicas=1 \
  --server="${NATS_URL}" \
  --defaults

echo "Created stream: stats_gaps"

"$NATS_BIN" pub --jetstream --quiet --count 10 --server="${NATS_URL}" stats_gaps.items "gap-test-{{Count}}" >/dev/null
"$NATS_BIN" stream rmm stats_gaps 3 --force --server="${NATS_URL}" >/dev/null
"$NATS_BIN" stream rmm stats_gaps 7 --force --server="${NATS_URL}" >/dev/null

echo "Seeded stream: stats_gaps with deleted sequences 3 and 7"

# Create a small deterministic stream for ingest checkpoint/resume tests
"$NATS_BIN" stream add ingest_resume \
  --subjects "ingest_resume.>" \
  --storage file \
  --retention limits \
  --max-msgs=-1 \
  --max-bytes=-1 \
  --max-age=7d \
  --max-msg-size=1048576 \
  --discard old \
  --dupe-window=2m \
  --replicas=1 \
  --server="${NATS_URL}" \
  --defaults

echo "Created stream: ingest_resume"

"$NATS_BIN" pub --jetstream --quiet --count 4 --server="${NATS_URL}" ingest_resume.items "resume-test-{{Count}}" >/dev/null

echo "Seeded stream: ingest_resume with 4 messages"

# Create test consumers
echo "Creating test consumers..."

"$NATS_BIN" consumer add telemetry etl-power \
  --filter "telemetry.dc1.power.>" \
  --ack explicit \
  --pull \
  --deliver all \
  --max-deliver=-1 \
  --max-pending=1000 \
  --replay instant \
  --server="${NATS_URL}" \
  --defaults

echo "Created consumer: etl-power"

"$NATS_BIN" consumer add telemetry analytics-consumer \
  --filter "telemetry.>" \
  --ack explicit \
  --pull \
  --deliver all \
  --max-deliver=-1 \
  --max-pending=1000 \
  --replay instant \
  --server="${NATS_URL}" \
  --defaults

echo "Created consumer: analytics-consumer"

echo ""
echo "Stream setup complete!"
echo ""
echo "Streams:"
"$NATS_BIN" stream list --server="${NATS_URL}"
echo ""
echo "Consumers:"
"$NATS_BIN" consumer list telemetry --server="${NATS_URL}"
