#!/usr/bin/env bash
# Rapid connect/disconnect test to trigger fd reuse race conditions
# This is the scenario where buffer corruption bugs appear

set -e

PORT=1883
BROKER_PID=""
ITERATIONS=3
CLIENTS_PER_WAVE=200
WAVES=5

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

echo "=== Rapid Connect/Disconnect Test ==="
echo "Testing fd reuse race conditions with rapid client churn"
echo ""

# Build and start broker
echo "Building broker..."
just build 2>&1 | tail -1

for iter in $(seq 1 $ITERATIONS); do
    echo ""
    echo "=== Iteration $iter of $ITERATIONS ==="

    # Start fresh broker
    rm -f /tmp/broker_rapid.log
    ./bin/broker >/tmp/broker_rapid.log 2>&1 &
    BROKER_PID=$!
    sleep 0.5

    if ! kill -0 "$BROKER_PID" 2>/dev/null; then
        echo "ERROR: Broker failed to start"
        cat /tmp/broker_rapid.log
        exit 1
    fi

    # Rapid waves of connect/disconnect
    for wave in $(seq 1 $WAVES); do
        echo "  Wave $wave: $CLIENTS_PER_WAVE clients connecting..."

        # Start subscribers that disconnect after 1 message
        for i in $(seq 1 $CLIENTS_PER_WAVE); do
            mosquitto_sub -h localhost -p $PORT -t "wave/$wave" -q 0 -C 1 \
                --id "sub_${wave}_${i}" >/dev/null 2>&1 &
        done

        # Brief pause to let connections establish
        sleep 0.2

        # Publish to make them all disconnect
        mosquitto_pub -h localhost -p $PORT -t "wave/$wave" -q 0 -m "trigger"

        # Wait for disconnects
        sleep 0.3
    done

    # Give broker time to process all disconnects
    sleep 1

    # Stop broker
    kill -TERM "$BROKER_PID" 2>/dev/null || true
    wait "$BROKER_PID" 2>/dev/null || true
    BROKER_PID=""

    # Check for errors
    echo "  Checking log for iteration $iter..."
    if grep -E "(Unknown packet type|Packet error type|recv error err=-)" /tmp/broker_rapid.log; then
        echo ""
        echo "ERRORS DETECTED in iteration $iter!"
        echo ""
        echo "Full log:"
        cat /tmp/broker_rapid.log
        exit 1
    fi

    echo "  Iteration $iter passed"
done

echo ""
echo "=== All $ITERATIONS iterations passed! ==="
echo ""
echo "Final broker log:"
cat /tmp/broker_rapid.log
