#!/usr/bin/env bash
# Start / rolling-update the production Harvest API (Hetzner Swarm).
#
# Prereqs: Docker Swarm active, backend/services/api/.env with prod secrets.
# From repo root:
#   ./deploy/scripts/api-prod-up.sh
#   ./deploy/scripts/api-prod-down.sh
#
# Order: build image → alembic upgrade head → stack deploy (start-first roll) → wait healthy.
# If migrate fails, the existing Swarm service is left running.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

ENV_FILE="$ROOT/backend/services/api/.env"
STACK_FILE="$ROOT/deploy/docker-stack.api.prod.yml"
STACK_NAME="${STACK_NAME:-harvest-prod}"
SERVICE_NAME="${STACK_NAME}_api"
IMAGE_LATEST="${IMAGE_LATEST:-harvest-api:prod}"
HEALTH_URL="${HEALTH_URL:-http://127.0.0.1:8000/health}"
HEALTH_TIMEOUT_SEC="${HEALTH_TIMEOUT_SEC:-420}"
# Mutable :prod alone is ignored by stack deploy when the YAML is unchanged — tag each
# deploy so Swarm performs a start-first rolling update.
API_IMAGE="${API_IMAGE:-harvest-api:$(git rev-parse --short HEAD 2>/dev/null || date +%Y%m%d%H%M%S)}"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Missing $ENV_FILE — copy from .example.env and fill prod values." >&2
  exit 1
fi

if [[ ! -f "$STACK_FILE" ]]; then
  echo "Missing $STACK_FILE" >&2
  exit 1
fi

swarm_state="$(docker info --format '{{.Swarm.LocalNodeState}}' 2>/dev/null || true)"
if [[ "$swarm_state" != "active" ]]; then
  echo "Docker Swarm is not active (state=${swarm_state:-unknown})." >&2
  echo "One-time on this host: docker swarm init" >&2
  echo "See deploy/API_ZERO_DOWNTIME.md" >&2
  exit 1
fi

echo "=== docker compose build (${IMAGE_LATEST} → ${API_IMAGE}) ==="
# The stack file's image: key is ${API_IMAGE:-harvest-api:prod}. Unset API_IMAGE
# so Compose tags the build :prod; the sha tag is applied on the next line.
# stack deploy still receives API_IMAGE and ignores build:.
env -u API_IMAGE docker compose -f "$STACK_FILE" build "$@"
docker tag "$IMAGE_LATEST" "$API_IMAGE"

echo "=== alembic upgrade head ==="
docker run --rm \
  --env-file "$ENV_FILE" \
  -e ENVIRONMENT="${ENVIRONMENT:-production}" \
  -e LOG_LEVEL="${LOG_LEVEL:-INFO}" \
  -e LOG_FORMAT="${LOG_FORMAT:-json}" \
  -e PYTHONUNBUFFERED=1 \
  --add-host=host.docker.internal:host-gateway \
  "$API_IMAGE" \
  alembic upgrade head

echo "=== docker stack deploy (${STACK_NAME}, image=${API_IMAGE}) ==="
# If a previous stack rm left the overlay half-deleted, wait it out before deploy.
if docker network ls --format '{{.Name}}' 2>/dev/null | grep -qx "${STACK_NAME}_default"; then
  # Network exists — fine for update. If deploy failed earlier mid-create, ensure
  # it is an overlay (not a leftover Compose bridge).
  if docker network ls --format '{{.Name}} {{.Driver}}' 2>/dev/null | grep -q "^${STACK_NAME}_default bridge$"; then
    echo "Removing leftover Compose bridge ${STACK_NAME}_default before Swarm deploy…" >&2
    docker network rm "${STACK_NAME}_default"
    sleep 2
  fi
fi
API_IMAGE="$API_IMAGE" docker stack deploy -c "$STACK_FILE" "$STACK_NAME"

# stack deploy is a no-op when the image tag is unchanged (same git sha) even if
# the local image was rebuilt. It also won't replace a task that already exited.
# --force always schedules a fresh task with the current local image.
if docker service inspect "$SERVICE_NAME" >/dev/null 2>&1; then
  echo "=== docker service update --force (image=${API_IMAGE}) ==="
  docker service update --force --image "$API_IMAGE" "$SERVICE_NAME"
fi

echo "=== wait for healthy (${HEALTH_URL}, timeout ${HEALTH_TIMEOUT_SEC}s) ==="
deadline=$((SECONDS + HEALTH_TIMEOUT_SEC))
while (( SECONDS < deadline )); do
  update_state="$(
    docker service inspect "$SERVICE_NAME" \
      --format '{{if .UpdateStatus}}{{.UpdateStatus.State}}{{else}}completed{{end}}' \
      2>/dev/null || echo "missing"
  )"
  case "$update_state" in
    paused|rollback_paused|rollback_completed)
      echo "Service update failed (UpdateStatus=${update_state}). Recent tasks:" >&2
      docker service ps "$SERVICE_NAME" --no-trunc >&2 || true
      echo "--- recent service logs ---" >&2
      docker service logs --tail 80 "$SERVICE_NAME" >&2 || true
      exit 1
      ;;
    missing)
      # Service not created yet
      ;;
    updating|rollback_started)
      ;;
    completed|"")
      if curl -fsS "$HEALTH_URL" >/dev/null 2>&1; then
        echo "Service ${SERVICE_NAME} is up and ${HEALTH_URL} responds."
        break
      fi
      ;;
  esac
  sleep 2
done

if ! curl -fsS "$HEALTH_URL" >/dev/null 2>&1; then
  echo "Timed out waiting for ${HEALTH_URL}." >&2
  docker service ps "$SERVICE_NAME" --no-trunc >&2 || true
  docker service inspect "$SERVICE_NAME" \
    --format 'UpdateStatus={{if .UpdateStatus}}{{.UpdateStatus.State}} {{.UpdateStatus.Message}}{{else}}none{{end}}' \
    >&2 || true
  echo "--- recent service logs ---" >&2
  docker service logs --tail 120 "$SERVICE_NAME" >&2 || true
  exit 1
fi

echo ""
echo "API stack (${STACK_NAME}) is up (Swarm rolling update)."
echo "  Local health:  curl -fsS ${HEALTH_URL}"
echo "  Proxy public HTTPS → 127.0.0.1:8000 (firewall must deny public :8000)."
echo "  Service:       docker service ps ${SERVICE_NAME}"
echo "  Logs:          docker service logs -f ${SERVICE_NAME}"
echo ""
