#!/usr/bin/env bash
# Build/start the fleet jobs agent on this host.
#
# From repo root:
#   ./deploy/scripts/jobs-agent-prod-up.sh
#   ./deploy/scripts/jobs-agent-prod-up.sh --build
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

ENV_FILE="$ROOT/backend/jobs/.env"
COMPOSE_FILE="$ROOT/deploy/docker-compose.jobs-agent.prod.yml"
PROJECT="${COMPOSE_PROJECT_NAME:-harvest-jobs-agent}"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Missing $ENV_FILE — copy from backend/jobs/.example.env and fill values." >&2
  echo "  Set a unique WORKER_ID (and TELEGRAM_SESSION_LOCATION_ID) per host." >&2
  exit 1
fi

ARGS=(up -d)
if [[ "${1:-}" == "--build" ]]; then
  ARGS+=(--build)
fi

docker compose -p "$PROJECT" -f "$COMPOSE_FILE" "${ARGS[@]}"
echo "Started harvest-jobs-agent (heartbeats job_workers, claims harvest_jobs)."
echo "Logs: docker logs -f harvest-jobs-agent"
