#!/usr/bin/env bash
# Contract: ./run.sh <SIM_URL> runs the HTTP-only controller to scenario completion.
set -euo pipefail
SIM_URL="${1:?usage: ./run.sh <SIM_URL>}"
CONTROLLER_DB="${CONTROLLER_DB:-.controller/controller.sqlite3}"
export CONTROLLER_DB
exec python agentic_controller.py --url "$SIM_URL" --db "$CONTROLLER_DB"
