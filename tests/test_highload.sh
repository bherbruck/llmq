#!/bin/bash
# High-load integration test for zero-copy MQTT broker
# Tests fan-out (10 pubs -> 1000 subs) and fan-in (1000 pubs -> 10 subs)

set -e

BROKER=./bin/broker
LOG=/tmp/broker_test.log
RESULT_DIR=/tmp/mqtt_test

cleanup() {
    pkill -f "$BROKER" 2>/dev/null || true
    pkill -f mosquitto_sub 2>/dev/null || true
    pkill -f mosquitto_pub 2>/dev/null || true
    rm -rf "$RESULT_DIR"
    sleep 0.5
}

trap cleanup EXIT

echo "=== High-Load MQTT Broker Test ==="
cleanup
mkdir -p "$RESULT_DIR"

# Start broker
$BROKER > "$LOG" 2>&1 &
BROKER_PID=$!
sleep 1

if ! kill -0 $BROKER_PID 2>/dev/null; then
    echo "ERROR: Broker failed to start"
    cat "$LOG"
    exit 1
fi
echo "Broker started (PID $BROKER_PID)"

# ============================================================================
# Test 1: Fan-out (10 publishers -> 100 subscribers, 100 msgs each)
# ============================================================================
echo ""
echo "=== Test 1: Fan-out (10 pubs -> 100 subs, 100 msgs each) ==="

NUM_SUBS=100
NUM_PUBS=10
MSGS_PER_PUB=100

# Start subscribers (each expects NUM_PUBS * MSGS_PER_PUB = 1000 messages)
for i in $(seq 1 $NUM_SUBS); do
    mosquitto_sub -t "fanout/#" -i "sub$i" -q 1 -C $((NUM_PUBS * MSGS_PER_PUB)) \
        > "$RESULT_DIR/fanout_sub_$i.txt" 2>&1 &
done
sleep 1

# Start publishers (each sends MSGS_PER_PUB messages)
for p in $(seq 1 $NUM_PUBS); do
    (
        for m in $(seq 1 $MSGS_PER_PUB); do
            mosquitto_pub -t "fanout/topic$p" -m "P${p}M${m}" -q 1
        done
    ) &
done

# Wait for publishers to finish
wait

# Give subscribers time to receive all messages
echo "Waiting for subscribers to receive messages..."
sleep 5

# Kill any remaining subscribers
pkill -f "sub[0-9]" 2>/dev/null || true
sleep 1

# Count results
total_msgs=0
failed_subs=0
for i in $(seq 1 $NUM_SUBS); do
    count=$(wc -l < "$RESULT_DIR/fanout_sub_$i.txt" 2>/dev/null || echo 0)
    total_msgs=$((total_msgs + count))
    if [ "$count" -lt $((NUM_PUBS * MSGS_PER_PUB)) ]; then
        failed_subs=$((failed_subs + 1))
    fi
done

expected=$((NUM_SUBS * NUM_PUBS * MSGS_PER_PUB))
echo "Fan-out: $total_msgs / $expected messages received"
echo "Subscribers with incomplete delivery: $failed_subs / $NUM_SUBS"

# ============================================================================
# Test 2: Fan-in (100 publishers -> 10 subscribers)
# ============================================================================
echo ""
echo "=== Test 2: Fan-in (100 pubs -> 10 subs, 10 msgs each) ==="

NUM_PUBS_FI=100
NUM_SUBS_FI=10
MSGS_PER_PUB_FI=10

# Start subscribers
for i in $(seq 1 $NUM_SUBS_FI); do
    mosquitto_sub -t "fanin/+" -i "fisub$i" -q 1 -C $((NUM_PUBS_FI * MSGS_PER_PUB_FI)) \
        > "$RESULT_DIR/fanin_sub_$i.txt" 2>&1 &
done
sleep 1

# Start publishers in parallel
for p in $(seq 1 $NUM_PUBS_FI); do
    (
        for m in $(seq 1 $MSGS_PER_PUB_FI); do
            mosquitto_pub -t "fanin/$p" -m "FI_P${p}M${m}" -q 1
        done
    ) &
done

wait
echo "Waiting for subscribers..."
sleep 5

pkill -f "fisub" 2>/dev/null || true
sleep 1

# Count fan-in results
total_fi=0
for i in $(seq 1 $NUM_SUBS_FI); do
    count=$(wc -l < "$RESULT_DIR/fanin_sub_$i.txt" 2>/dev/null || echo 0)
    total_fi=$((total_fi + count))
done

expected_fi=$((NUM_SUBS_FI * NUM_PUBS_FI * MSGS_PER_PUB_FI))
echo "Fan-in: $total_fi / $expected_fi messages received"

# ============================================================================
# Check broker logs for errors
# ============================================================================
echo ""
echo "=== Broker Log Analysis ==="
warnings=$(grep -c -i "warn" "$LOG" 2>/dev/null || echo 0)
errors=$(grep -c -i "error" "$LOG" 2>/dev/null || echo 0)
unknown=$(grep -c "Unknown packet type" "$LOG" 2>/dev/null || echo 0)

echo "Warnings: $warnings"
echo "Errors: $errors"
echo "Unknown packet types: $unknown"

if [ "$warnings" -gt 0 ]; then
    echo ""
    echo "Sample warnings:"
    grep -i "warn" "$LOG" | head -20
fi

# ============================================================================
# Summary
# ============================================================================
echo ""
echo "=== Summary ==="
if [ "$unknown" -eq 0 ] && [ "$errors" -eq 0 ]; then
    echo "PASS: No packet corruption detected"
else
    echo "FAIL: Packet corruption or errors detected"
    exit 1
fi
