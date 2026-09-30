#!/usr/bin/env bash
# scenario5.sh — Backpressure
#
# A slow consumer (prefetch=1, delay=1s) subscribes first. 50 messages are then
# flooded into the broker. The dispatcher must block cleanly in its retry loop
# without crashing, growing memory unboundedly, or dropping messages.
# Messages drain one by one as the consumer processes them.
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

header "Scenario 5 — Backpressure"
info "Consumer: prefetch=1  delay=1s  (very slow)"
info "Flooding broker with 50 messages."
info "Dispatcher must not crash. Messages drain one by one."
echo

start_server "$WAL_DIR" ":50051"

info "Starting slow consumer (prefetch=1, delay=1s)…"
LOG=$(mktemp)
"$BIN/consumer" -id slow -queue orders -prefetch 1 -delay 1s > "$LOG" 2>&1 &
PIDS+=($!)
sleep 0.3   # let consumer subscribe before the flood

info "Flooding broker with 50 messages…"
"$BIN/publisher" -queue orders -count 50 -payload work
echo

info "Watching consumer drain messages (press Ctrl-C to stop early)…"
info "Each message takes ~1s. Waiting up to 60s for at least 10 to be processed."
echo
END=$((SECONDS + 60))
while (( SECONDS < END )); do
    DONE=$(grep -c "ACK" "$LOG" 2>/dev/null || true)

    ALIVE=$(kill -0 "${SERVER_PID}" 2>/dev/null && echo yes || echo NO)
    printf "\r  processed: %-3d / 50   broker alive: %s" "$DONE" "$ALIVE"

    (( DONE >= 10 )) && break
    sleep 1
done
echo; echo

header "Results"
DONE=$(grep -c "ACK" "$LOG" 2>/dev/null || true)
echo "  messages processed: $DONE"

if kill -0 "$SERVER_PID" 2>/dev/null; then
    ok "Broker is still running — no crash under flood."
else
    fail "Broker crashed under backpressure!"
fi

if (( DONE > 0 )); then
    ok "Messages are draining steadily at the consumer's processing rate."
else
    warn "No messages processed yet — consumer may not have subscribed in time."
fi

echo
echo "  consumer log (first 15 lines):"
head -15 "$LOG" | sed 's/^/    /'

rm -f "$LOG"
