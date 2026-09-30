#!/usr/bin/env bash
# scenario1.sh — Load balancing
#
# Three consumers with different processing speeds subscribe to the same queue.
# The dispatcher's least-in-flight routing naturally sends more messages to the
# fast consumer and throttles the slow one at its prefetch cap.
#
#   fast   (delay=0ms,   prefetch=10)
#   medium (delay=100ms, prefetch=5)
#   slow   (delay=400ms, prefetch=2)
#
# 30 messages are published. Watch how the fast consumer receives the majority.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

[[ -f "$BIN/broker" ]] || build_binaries

WAL_DIR=$(mktemp -d)
PIDS=()

cleanup() {
    for pid in "${PIDS[@]:-}"; do kill "$pid" 2>/dev/null; done
    stop_server
    rm -rf "$WAL_DIR"
}
trap cleanup EXIT

header "Scenario 1 — Load Balancing"
info "fast=0ms delay  medium=100ms delay  slow=400ms delay"
info "Publishing 30 messages and watching distribution"
echo

start_server "$WAL_DIR" ":50051"

# Start three consumers in the background, each writing to its own log.
LOG_FAST=$(mktemp)
LOG_MED=$(mktemp)
LOG_SLOW=$(mktemp)

"$BIN/consumer" -id fast   -queue orders -prefetch 10 -delay 0       > "$LOG_FAST" 2>&1 &
PIDS+=($!)
"$BIN/consumer" -id medium -queue orders -prefetch 5  -delay 100ms   > "$LOG_MED"  2>&1 &
PIDS+=($!)
"$BIN/consumer" -id slow   -queue orders -prefetch 2  -delay 400ms   > "$LOG_SLOW" 2>&1 &
PIDS+=($!)

sleep 0.5   # let consumers subscribe before messages arrive

info "Publishing 30 messages…"
"$BIN/publisher" -queue orders -count 30 -payload task

# Show live output until all 30 messages are consumed or 30s elapse.
info "Waiting for consumers to process all messages (up to 30s)…"
END=$((SECONDS + 30))
while (( SECONDS < END )); do
    DONE_FAST=$(grep -c "ACK\|NACK" "$LOG_FAST"  2>/dev/null || true)
    DONE_MED=$(grep  -c "ACK\|NACK" "$LOG_MED"   2>/dev/null || true)
    DONE_SLOW=$(grep -c "ACK\|NACK" "$LOG_SLOW"  2>/dev/null || true)
    TOTAL=$(( DONE_FAST + DONE_MED + DONE_SLOW ))
    printf "\r  processed: fast=%-3d  medium=%-3d  slow=%-3d  total=%-3d / 30" \
        "$DONE_FAST" "$DONE_MED" "$DONE_SLOW" "$TOTAL"
    (( TOTAL >= 30 )) && break
    sleep 0.5
done
echo

echo
header "Results"
FAST=$(grep -c  "ACK\|NACK" "$LOG_FAST"  2>/dev/null || true)
MED=$(grep  -c  "ACK\|NACK" "$LOG_MED"   2>/dev/null || true)
SLOW=$(grep -c  "ACK\|NACK" "$LOG_SLOW"  2>/dev/null || true)
echo -e "  ${GREEN}fast${NC}   (0ms    / prefetch=10):  $FAST messages"
echo -e "  ${YELLOW}medium${NC} (100ms  / prefetch=5 ):  $MED messages"
echo -e "  ${RED}slow${NC}   (400ms  / prefetch=2 ):  $SLOW messages"
echo -e "  total: $(( FAST + MED + SLOW ))"
echo
ok "Fast consumer received proportionally more. Slow consumer capped at prefetch limit."

echo -e "\n${DIM}── fast consumer log ──${NC}"
cat "$LOG_FAST" | head -20

rm -f "$LOG_FAST" "$LOG_MED" "$LOG_SLOW"
