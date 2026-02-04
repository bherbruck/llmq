#!/usr/bin/env bash
# High-stress test with 1000 clients to reproduce buffer corruption bugs
# Requires mosquitto-clients: apt install mosquitto-clients

set -e

PORT=1883
TOPIC="test/fanout"
BROKER_PID=""
NUM_SUBS=500
NUM_PUBS=500
MSGS_PER_PUB=10

cleanup() {
    echo "Cleaning up..."
    # Kill all mosquitto processes
    pkill -9 mosquitto_sub 2>/dev/null || true
    pkill -9 mosquitto_pub 2>/dev/null || true
    # Kill broker
    if [[ -n "$BROKER_PID" ]] && kill -0 "$BROKER_PID" 2>/dev/null; then
        kill -TERM "$BROKER_PID" 2>/dev/null || true
        sleep 1
        kill -9 "$BROKER_PID" 2>/dev/null || true
    fi
}

trap cleanup EXIT

echo "=== High-Stress MQTT Broker Test (1000 clients) ==="

# Build and start broker
echo "Building and starting broker..."
just build 2>&1 | tail -1
./bin/broker >/tmp/broker_stress.log 2>&1 &
BROKER_PID=$!
sleep 1

if ! kill -0 "$BROKER_PID" 2>/dev/null; then
    echo "ERROR: Broker failed to start"
    cat /tmp/broker_stress.log
    exit 1
fi

echo "Broker started (PID $BROKER_PID)"

# Start subscribers in background
echo "Starting $NUM_SUBS subscribers..."
for i in $(seq 1 $NUM_SUBS); do
    mosquitto_sub -h localhost -p $PORT -t "$TOPIC" -q 0 -C $((NUM_PUBS * MSGS_PER_PUB)) \
        --id "sub_$i" >/dev/null 2>&1 &
done

# Wait for subscribers to connect
sleep 2

# Start publishers
echo "Starting $NUM_PUBS publishers ($MSGS_PER_PUB msgs each)..."
for i in $(seq 1 $NUM_PUBS); do
    (
        for m in $(seq 1 $MSGS_PER_PUB); do
            mosquitto_pub -h localhost -p $PORT -t "$TOPIC" -q 0 \
                --id "pub_${i}_${m}" -m "Message $m from publisher $i"
        done
    ) &
done

# Wait for publishers to finish
echo "Waiting for publishers..."
wait

# Check broker log for errors
echo ""
echo "=== Broker Log ==="
cat /tmp/broker_stress.log

echo ""
echo "=== Checking for errors ==="
if grep -E "(Unknown packet type|Packet error|recv error|WARN)" /tmp/broker_stress.log; then
    echo ""
    echo "ERRORS DETECTED!"
    exit 1
else
    echo "No errors found - test passed!"
fi
