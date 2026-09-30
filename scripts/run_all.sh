#!/usr/bin/env bash
# run_all.sh — Run every Phase 9 scenario in sequence.
#
# Usage:
#   ./scripts/run_all.sh           # run all 5 scenarios
#   ./scripts/run_all.sh 1 3 5     # run specific scenarios
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

SCENARIOS=("${@:-1 2 3 4 5}")

build_binaries

PASS=0
FAIL=0

run_scenario() {
    local n=$1
    local script="$SCRIPT_DIR/scenario${n}.sh"
    if [[ ! -f "$script" ]]; then
        warn "scenario${n}.sh not found, skipping"
        return
    fi

    echo
    echo -e "${BOLD}════════════════════════════════════════${NC}"
    echo -e "${BOLD}  SCENARIO $n${NC}"
    echo -e "${BOLD}════════════════════════════════════════${NC}"

    if bash "$script"; then
        ok "Scenario $n PASSED"
        (( PASS++ )) || true
    else
        warn "Scenario $n FAILED (exit $?)"
        (( FAIL++ )) || true
    fi

    echo
    sleep 2   # brief pause between scenarios
}

for n in $SCENARIOS; do
    run_scenario "$n"
done

echo
echo -e "${BOLD}━━━  Summary  ━━━${NC}"
echo -e "  ${GREEN}passed: $PASS${NC}"
(( FAIL > 0 )) && echo -e "  ${RED}failed: $FAIL${NC}" || echo -e "  failed: $FAIL"
echo

(( FAIL == 0 )) && ok "All scenarios passed." || warn "$FAIL scenario(s) failed."
