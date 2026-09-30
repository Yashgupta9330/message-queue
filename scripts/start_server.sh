#!/usr/bin/env bash
# start_server.sh — run the broker interactively for manual testing.
#
# Usage:
#   ./scripts/start_server.sh
#   ./scripts/start_server.sh /tmp/my-wal-dir :50051
#
# Environment variables (all optional):
#   WAL_DIR            default: data/wal
#   LISTEN_ADDR        default: :50051
#   MAX_RETRIES        default: 3
#   DISPATCH_TIMEOUT   default: 30s
#   SCAN_INTERVAL      default: 5s
set -euo pipefail
source "$(dirname "$0")/lib.sh"

WAL_DIR="${1:-${WAL_DIR:-$ROOT/data/wal}}"
LISTEN_ADDR="${2:-${LISTEN_ADDR:-:50051}}"

[[ -f "$BIN/broker" ]] || build_binaries

header "Starting broker"
info "WAL_DIR=$WAL_DIR"
info "LISTEN_ADDR=$LISTEN_ADDR"
echo

exec env WAL_DIR="$WAL_DIR" LISTEN_ADDR="$LISTEN_ADDR" "$BIN/broker"
