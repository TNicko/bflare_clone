#!/usr/bin/env bash
# Stop the production Harvest API Swarm stack.
# This removes the service and drops :8000 — disruptive (downtime until next up).
# From repo root: ./deploy/scripts/api-prod-down.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

STACK_NAME="${STACK_NAME:-harvest-prod}"

if ! docker info --format '{{.Swarm.LocalNodeState}}' 2>/dev/null | grep -qx active; then
  echo "Docker Swarm is not active; nothing to remove for stack ${STACK_NAME}."
  # Legacy Compose project leftover from pre-Swarm deploys.
  legacy_ids="$(docker ps -aq --filter "label=com.docker.compose.project=${STACK_NAME}")"
  if [[ -n "$legacy_ids" ]]; then
    echo "Removing legacy Compose project ${STACK_NAME}…"
    docker rm -f $legacy_ids
  fi
  if docker network ls --format '{{.Name}} {{.Driver}}' 2>/dev/null | grep -q "^${STACK_NAME}_default bridge$"; then
    docker network rm "${STACK_NAME}_default" 2>/dev/null || true
  fi
  exit 0
fi

if docker stack ls --format '{{.Name}}' 2>/dev/null | grep -qx "$STACK_NAME"; then
  echo "=== docker stack rm ${STACK_NAME} ==="
  docker stack rm "$STACK_NAME"
  # Wait until the stack *and* its overlay network are gone. stack rm returns before
  # the network is deleted; a following stack deploy then fails with:
  #   network harvest-prod_default not found
  for _ in $(seq 1 90); do
    stack_gone=0
    net_gone=0
    if ! docker stack ls --format '{{.Name}}' 2>/dev/null | grep -qx "$STACK_NAME"; then
      stack_gone=1
    fi
    if ! docker network ls --format '{{.Name}}' 2>/dev/null | grep -qx "${STACK_NAME}_default"; then
      net_gone=1
    fi
    if (( stack_gone && net_gone )); then
      break
    fi
    sleep 1
  done
  if docker network ls --format '{{.Name}}' 2>/dev/null | grep -qx "${STACK_NAME}_default"; then
    echo "Warning: ${STACK_NAME}_default still present — removing leftover network…" >&2
    docker network rm "${STACK_NAME}_default" 2>/dev/null || true
    sleep 2
  fi
else
  echo "Stack ${STACK_NAME} is not deployed."
fi

# Legacy Compose bridge with the same name blocks Swarm overlay recreate.
if docker network ls --format '{{.Name}} {{.Driver}}' 2>/dev/null | grep -q "^${STACK_NAME}_default bridge$"; then
  echo "Removing leftover Compose bridge ${STACK_NAME}_default…"
  docker network rm "${STACK_NAME}_default" 2>/dev/null || true
fi

echo "API stack (${STACK_NAME}) is down."
