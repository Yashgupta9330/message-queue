#!/usr/bin/env bash
# scenario3.sh — Durability (broker crash and restart)
#
# Publishes batch-A (5 messages), lets a consumer ACK all of them, then
# publishes batch-B (5 messages). The broker is killed before batch-B
# is dispatched. On restart, only batch-B must reappear; batch-A must not.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

[[ -f "$BIN/broker" ]] || build_binaries

WAL_DIR=$(mktemp -d)

cleanup() {
    stop_server 2>/dev/null || true
    rm -rf "$WAL_DIR"
}
trap cleanup EXIT

header "Scenario 3 --- Durability (Crash & Restart)"
info "Batch-A (records 1-5): published and fully ACKed before crash."
info "Batch-B (pending 1-5): published but broker is killed before dispatch."
info "After restart, only batch-B must reappear."
echo

# --- First session -----------------------------------------------------------
info "=== FIRST SESSION ==="
start_server "$WAL_DIR" ":50051"

info "Publishing batch-A (5 messages)..."
"$BIN/publisher" -queue orders -count 5 -payload record
echo

info "Starting consumer-1 -- will ACK all 5 batch-A messages then exit..."
LOG1=$(mktemp)
"$BIN/consumer" -id consumer-1 -queue orders -prefetch 5 -count 5 > "$LOG1" 2>&1
echo "  consumer-1 output:"
cat "$LOG1" | sed 's/^/    /'
echo
ok "consumer-1 ACKed all 5 batch-A messages."
echo

info "Publishing batch-B (5 messages) -- no consumer, will NOT be dispatched..."
"$BIN/publisher" -queue orders -count 5 -payload pending
echo

info "Batch-B is in WAL and in-memory queue, undelivered."
warn "Killing broker with SIGKILL -- simulating a hard crash..."
kill_server
echo
sleep 0.5

# --- Second session ----------------------------------------------------------
info "=== SECOND SESSION (restart) ==="
start_server "$WAL_DIR" ":50051"

info "Starting consumer-2 -- expects batch-B only (pending-1 to pending-5)..."
LOG2=$(mktemp)
"$BIN/consumer" -id consumer-2 -queue orders -prefetch 10 -count 5 > "$LOG2" 2>&1 &
C2_PID=$!

END=$((SECONDS + 15))
while kill -0 "$C2_PID" 2>/dev/null && (( SECONDS < END )); do
    sleep 0.5
done

echo
header "Results"
echo "  consumer-2 received (should be pending-1 to pending-5 only):"
cat "$LOG2" | sed 's/^/    /'
echo

BATCH_A=$(grep -c "record-" "$LOG2" 2>/dev/null || true); BATCH_A="${BATCH_A:-0}"
BATCH_B=$(grep -c "pending-" "$LOG2" 2>/dev/null || true); BATCH_B="${BATCH_B:-0}"

echo "  batch-A (record-*) redelivered: $BATCH_A  (must be 0)"
echo "  batch-B (pending-*) delivered:  $BATCH_B  (must be 5)"
echo ""

if (( BATCH_A == 0 )); then
    ok "Batch-A (ACKed before crash) was NOT redelivered."
else
    warn "ERROR: $BATCH_A batch-A message(s) incorrectly redelivered!"
fi

if (( BATCH_B == 5 )); then
    ok "All 5 batch-B messages (un-ACKed at crash) were redelivered."
else
    warn "Expected 5 batch-B redeliveries, got $BATCH_B"
fi

rm -f "$LOG1" "$LOG2"
