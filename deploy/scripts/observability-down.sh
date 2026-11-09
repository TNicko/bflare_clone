#!/usr/bin/env bash
# Stop the local Harvest observability stack.
# Volumes are kept by default (history survives restarts). Pass -v to wipe data.
#
# From repo root:
#   ./deploy/scripts/observability-down.sh
#   ./deploy/scripts/observability-down.sh -v
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

PROJECT="${COMPOSE_PROJECT_NAME:-harvest-obs}"
ENV_FILE="$ROOT/deploy/observability/.env"
COMPOSE=(docker compose -p "$PROJECT" -f deploy/observability/docker-compose.yml)
if [[ "$(uname -s)" == "Linux" ]]; then
  COMPOSE+=(-f deploy/observability/docker-compose.cadvisor-linux.yml)
fi
if [[ -f "$ENV_FILE" ]]; then
  COMPOSE+=(--env-file "$ENV_FILE")
fi

"${COMPOSE[@]}" down "$@"

echo "Observability stack (${PROJECT}) is down."
