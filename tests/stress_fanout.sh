#!/usr/bin/env bash
# Stress test: fan-out to many subscribers with rapid publisher churn
# This test targets the zero-copy fan-out path with buffer stealing

set -e

PORT=1883
BROKER_PID=""
NUM_SUBS=100
NUM_PUBS=50
MSGS_PER_PUB=20

cleanup() {
    echo "Cleaning up..."
    pkill -9 mosquitto_sub 2>/dev/null || true
    pkill -9 mosquitto_pub 2>/dev/null || true
    if [[ -n "$BROKER_PID" ]] && kill -0 "$BROKER_PID" 2>/dev/null; then
        kill -TERM "$BROKER_PID" 2>/dev/null || true
        sleep 1
        kill -9 "$BROKER_PID" 2>/dev/null || true
    fi
}

trap cleanup EXIT

echo "=== Fan-out Stress Test ==="
echo "Config: $NUM_SUBS subs, $NUM_PUBS pubs × $MSGS_PER_PUB msgs = $((NUM_PUBS * MSGS_PER_PUB)) publishes"
echo ""

# Build and start broker
just build 2>&1 | tail -1
rm -f /tmp/broker_fanout.log
./bin/broker >/tmp/broker_fanout.log 2>&1 &
BROKER_PID=$!
sleep 0.5

if ! kill -0 "$BROKER_PID" 2>/dev/null; then
    echo "ERROR: Broker failed to start"
    cat /tmp/broker_fanout.log
    exit 1
fi

echo "Broker started (PID $BROKER_PID)"

# Start long-running subscribers
echo "Starting $NUM_SUBS subscribers..."
for i in $(seq 1 $NUM_SUBS); do
    mosquitto_sub -h localhost -p $PORT -t "fanout/#" -q 0 \
        --id "sub_$i" >/dev/null 2>&1 &
done

# Wait for subscribers to connect
sleep 1

# Rapid publisher churn - new connections for each message
echo "Starting $NUM_PUBS publishers with rapid connection churn..."
TOTAL_MSGS=$((NUM_PUBS * MSGS_PER_PUB))
SENT=0

for p in $(seq 1 $NUM_PUBS); do
    (
        for m in $(seq 1 $MSGS_PER_PUB); do
            # Each publish is a new connection (no -i persistent)
            mosquitto_pub -h localhost -p $PORT -t "fanout/topic$p" -q 0 \
                -m "P${p}M${m}-$(date +%s%N)" 2>/dev/null
        done
    ) &
done

echo "Waiting for publishers to complete..."
wait

# Give broker time to process
sleep 2

# Show stats
echo ""
echo "=== Broker Log ==="
cat /tmp/broker_fanout.log

echo ""
echo "=== Checking for errors ==="
if grep -E "(Unknown packet type|Packet error type|recv error err=-)" /tmp/broker_fanout.log; then
    echo ""
    echo "ERRORS DETECTED!"
    exit 1
else
    echo "No errors - test passed!"
fi
