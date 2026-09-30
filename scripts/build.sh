#!/usr/bin/env bash
# build.sh — compile all broker binaries into bin/
set -euo pipefail
source "$(dirname "$0")/lib.sh"
build_binaries
