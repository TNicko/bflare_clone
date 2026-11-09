#!/usr/bin/env bash
# Stop the Alloy agent on a workload host.
#
# From repo root:
#   ./deploy/scripts/observability-agent-down.sh
#   ./deploy/scripts/observability-agent-down.sh -v   # also remove volume
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

PROJECT="${COMPOSE_PROJECT_NAME:-harvest-obs-agent}"
ENV_FILE="$ROOT/deploy/observability/agent/.env"
COMPOSE=(docker compose -p "$PROJECT" -f deploy/observability/docker-compose.agent.yml)
if [[ -f "$ENV_FILE" ]]; then
  COMPOSE+=(--env-file "$ENV_FILE")
fi

"${COMPOSE[@]}" down "$@"
echo "Alloy agent (${PROJECT}) stopped."
