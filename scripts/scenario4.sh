#!/usr/bin/env bash
# scenario4.sh — Retry / DLQ
#
# A consumer always NACKs every message. With MAX_RETRIES=2, each message gets
# three chances (retry=0, retry=1, retry=2) before being moved to orders.dlq.
# A second consumer on the DLQ receives the dead-lettered message.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

[[ -f "$BIN/broker" ]] || build_binaries

WAL_DIR=$(mktemp -d)
PIDS=()

cleanup() {
    for pid in "${PIDS[@]:-}"; do kill "$pid" 2>/dev/null || true; done
    stop_server
    rm -rf "$WAL_DIR"
}
trap cleanup EXIT

header "Scenario 4 — Retry & Dead Letter Queue"
info "MAX_RETRIES=2  ->  message gets 3 delivery attempts, then moves to orders.dlq"
echo

start_server "$WAL_DIR" ":50051" MAX_RETRIES=2

info "Publishing 1 message..."
"$BIN/publisher" -queue orders -count 1 -payload poison
echo

# Consumer that always NACKs — will see the message 3 times, then it goes to DLQ.
info "Starting nack-consumer (always NACKs)..."
LOG_NACK=$(mktemp)
"$BIN/consumer" -id nack-consumer -queue orders -prefetch 1 -outcome nack > "$LOG_NACK" 2>&1 &
NACK_PID=$!
PIDS+=($NACK_PID)

# Wait until the consumer has NACKed 3 times (retry=0, 1, 2 → DLQ).
info "Waiting for nack-consumer to exhaust retries (3 NACKs)..."
END=$((SECONDS + 20))
while (( SECONDS < END )); do
    NACKS=$(grep -c "NACK" "$LOG_NACK" 2>/dev/null || true)
    NACKS="${NACKS:-0}"
    printf "\r  NACKs so far: %s / 3" "$NACKS"
    (( NACKS >= 3 )) && break
    sleep 0.5
done
echo; echo

echo "  nack-consumer output:"
cat "$LOG_NACK" | sed 's/^/    /'
echo

# Kill the nack-consumer — no more messages on orders queue expected.
kill "$NACK_PID" 2>/dev/null || true

# DLQ consumer.
info "Starting dlq-consumer on queue orders.dlq..."
LOG_DLQ=$(mktemp)
"$BIN/consumer" -id dlq-consumer -queue orders.dlq -prefetch 1 -count 1 > "$LOG_DLQ" 2>&1 &
DLQ_PID=$!
PIDS+=($DLQ_PID)

END=$((SECONDS + 10))
while kill -0 "$DLQ_PID" 2>/dev/null && (( SECONDS < END )); do
    sleep 0.5
done

echo "  dlq-consumer output:"
cat "$LOG_DLQ" | sed 's/^/    /'
echo

header "Results"
NACKS=$(grep -c "NACK" "$LOG_NACK" 2>/dev/null || true); NACKS="${NACKS:-0}"
DLQ_RECV=$(grep -c "recv" "$LOG_DLQ" 2>/dev/null || true); DLQ_RECV="${DLQ_RECV:-0}"

echo "  nack-consumer NACKed: $NACKS times"
echo "  dlq-consumer received: $DLQ_RECV message(s)"

if (( NACKS == 3 )); then
    ok "Message was delivered 3 times (retry=0,1,2) before being dead-lettered."
else
    warn "Expected 3 NACKs, got $NACKS"
fi

if (( DLQ_RECV == 1 )); then
    ok "Message appeared on orders.dlq after exhausting retries."
else
    warn "DLQ message not received (got $DLQ_RECV). Check MAX_RETRIES or timing."
fi

rm -f "$LOG_NACK" "$LOG_DLQ"
