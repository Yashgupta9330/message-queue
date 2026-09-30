#!/usr/bin/env bash
# Shared helpers sourced by all scenario scripts.

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="${ROOT}/bin"

# Colours (ANSI)
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; DIM='\033[2m'; NC='\033[0m'

log()    { echo -e "${DIM}$(date +%H:%M:%S)${NC}  $*"; }
ok()     { echo -e "${GREEN}checkmark${NC}  $*" | sed "s/checkmark/✓/"; }
info()   { echo -e "${CYAN}>${NC}  $*"; }
warn()   { echo -e "${YELLOW}!${NC}  $*"; }
fail()   { echo -e "${RED}x${NC}  $*"; exit 1; }
header() { echo -e "\n${BOLD}---  $*  ---${NC}\n"; }

build_binaries() {
    info "Building binaries..."
    mkdir -p "${BIN}"
    go build -o "${BIN}/broker"    "${ROOT}/cmd/broker"    || fail "Failed to build broker"
    go build -o "${BIN}/consumer"  "${ROOT}/cmd/consumer"  || fail "Failed to build consumer"
    go build -o "${BIN}/publisher" "${ROOT}/cmd/publisher" || fail "Failed to build publisher"
    echo "  Binaries ready in ${BIN}/"
}

# wait_for_server <host:port>
# Polls until the TCP port accepts connections (up to 15s).
wait_for_server() {
    local hostport="${1:-localhost:50051}"
    local host port
    host="${hostport%%:*}"
    port="${hostport##*:}"
    [[ -z "${host}" ]] && host="localhost"

    info "Waiting for broker at ${hostport}..."
    for i in $(seq 1 30); do
        nc -z "${host}" "${port}" 2>/dev/null && { echo "  Broker is ready."; return 0; }
        sleep 0.5
    done
    fail "Broker did not start within 15 seconds"
}

# start_server <wal_dir> <listen_addr> [KEY=VAL ...]
# Extra KEY=VAL args are forwarded to the broker as env vars.
# Sets globals: SERVER_PID, SERVER_LOG.
start_server() {
    local wal_dir="${1:-$(mktemp -d)}"
    local addr="${2:-:50051}"
    shift 2 2>/dev/null || shift $# 2>/dev/null || true

    # Kill any stale process already on this port before binding.
    local port="${addr##*:}"
    local stale
    stale=$(lsof -ti ":${port}" 2>/dev/null || true)
    if [[ -n "${stale}" ]]; then
        warn "Evicting stale process on port ${port} (PID ${stale})..."
        kill -9 ${stale} 2>/dev/null || true
        sleep 0.3
    fi

    SERVER_LOG=$(mktemp)
    info "Starting broker  WAL=${wal_dir}  addr=${addr}"

    # "$@" carries any extra KEY=VAL env vars passed after the first two args.
    env WAL_DIR="${wal_dir}" LISTEN_ADDR="${addr}" "$@" \
        "${BIN}/broker" >"${SERVER_LOG}" 2>&1 &
    SERVER_PID=$!

    wait_for_server "localhost${addr}"
}

stop_server() {
    local pid="${1:-${SERVER_PID:-0}}"
    if [[ "${pid}" -gt 0 ]] && kill -0 "${pid}" 2>/dev/null; then
        info "Stopping broker (PID ${pid})..."
        kill "${pid}" 2>/dev/null
        wait "${pid}" 2>/dev/null || true
        echo "  Broker stopped."
    fi
}

kill_server() {
    local pid="${1:-${SERVER_PID:-0}}"
    if [[ "${pid}" -gt 0 ]] && kill -0 "${pid}" 2>/dev/null; then
        warn "Killing broker (PID ${pid}) -- simulating hard crash"
        kill -9 "${pid}" 2>/dev/null || true
        echo "  Broker killed."
    fi
}

show_server_log() {
    local n="${1:-20}"
    echo ""
    echo "-- server log (last ${n} lines) --"
    tail -n "${n}" "${SERVER_LOG:-/dev/null}" | sed 's/^/  /'
    echo ""
}
