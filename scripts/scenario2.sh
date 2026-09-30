#!/usr/bin/env bash
# scenario2.sh — Consumer crash and message reassignment
#
# A consumer receives messages but is killed (simulating a crash) before it
# can ACK them. The timeout scanner detects the stale in-flight entries and
# requeues the messages. A second consumer then receives them.
#
# DISPATCH_TIMEOUT=8s  SCAN_INTERVAL=2s
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

header "Scenario 2 — Consumer Crash & Message Reassignment"
info "DISPATCH_TIMEOUT=8s  SCAN_INTERVAL=2s"
info "consumer-1 will be killed after receiving messages."
info "After the timeout, consumer-2 should receive the orphaned messages."
echo

# Short timeout so we don't wait forever in the demo.
start_server "$WAL_DIR" ":50051" DISPATCH_TIMEOUT=8s SCAN_INTERVAL=2s

info "Publishing 5 messages…"
"$BIN/publisher" -queue orders -count 5 -payload job
echo

info "Starting consumer-1 (delay=60s — simulates a hung consumer)…"
LOG1=$(mktemp)
"$BIN/consumer" -id consumer-1 -queue orders -prefetch 5 -delay 60s > "$LOG1" 2>&1 &
C1_PID=$!
PIDS+=($C1_PID)

# Wait until consumer-1 has received at least one message.
info "Waiting for consumer-1 to pick up messages…"
END=$((SECONDS + 10))
while (( SECONDS < END )); do
    grep -q "recv" "$LOG1" 2>/dev/null && break
    sleep 0.5
done

RECV=$(grep -c "recv" "$LOG1" 2>/dev/null || true)
echo
echo -e "  consumer-1 received ${YELLOW}$RECV${NC} messages (holding them, not ACKing)"
echo

warn "Killing consumer-1 (PID $C1_PID) — simulating crash…"
kill -9 "$C1_PID" 2>/dev/null || true
echo

info "Starting consumer-2 (fast)…"
LOG2=$(mktemp)
"$BIN/consumer" -id consumer-2 -queue orders -prefetch 5 -delay 0 > "$LOG2" 2>&1 &
PIDS+=($!)

info "Waiting up to 15s for the timeout scanner to requeue and consumer-2 to receive…"
END=$((SECONDS + 15))
while (( SECONDS < END )); do
    DONE=$(grep -c "ACK" "$LOG2" 2>/dev/null || true)
    printf "\r  consumer-2 ACKed: %d / %d" "$DONE" "$RECV"
    (( DONE >= RECV )) && break
    sleep 0.5
done
echo; echo

header "Results"
echo "  consumer-1 log (crashed, no ACKs expected):"
grep "recv\|ACK\|NACK" "$LOG1" 2>/dev/null | sed 's/^/    /' || echo "    (no output)"
echo
echo "  consumer-2 log (should show requeued messages with retry=1):"
grep "recv\|ACK\|NACK" "$LOG2" 2>/dev/null | sed 's/^/    /' || echo "    (no output — timeout may not have fired yet)"
echo

REASSIGNED=$(grep -c "retry=1" "$LOG2" 2>/dev/null || true)
if (( REASSIGNED > 0 )); then
    ok "consumer-2 received $REASSIGNED reassigned message(s) with retry=1"
else
    warn "Messages not yet reassigned — increase DISPATCH_TIMEOUT or wait longer"
fi

rm -f "$LOG1" "$LOG2"
