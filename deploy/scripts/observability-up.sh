#!/usr/bin/env bash
# Start the Harvest observability stack (Alloy → Loki + Prometheus → Grafana).
#
# Collects Docker logs from harvest-dev (local) or harvest-prod (Hetzner) and
# cAdvisor metrics. Does not start the API — run api-prod-up / dev-up separately.
#
# Env (optional): deploy/observability/.env — see .example.env
#   GF_SECURITY_ADMIN_PASSWORD=…   # set a strong password on Hetzner
#
# From repo root:
#   ./deploy/scripts/observability-up.sh
#   ./deploy/scripts/observability-down.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

PROJECT="${COMPOSE_PROJECT_NAME:-harvest-obs}"
ENV_FILE="$ROOT/deploy/observability/.env"
COMPOSE=(docker compose -p "$PROJECT" -f deploy/observability/docker-compose.yml)
# Docker 29 on Linux needs containerd mounts; Docker Desktop cannot use them.
if [[ "$(uname -s)" == "Linux" ]]; then
  COMPOSE+=(-f deploy/observability/docker-compose.cadvisor-linux.yml)
fi
if [[ -f "$ENV_FILE" ]]; then
  COMPOSE+=(--env-file "$ENV_FILE")
fi

"${COMPOSE[@]}" up -d "$@"

echo ""
echo "Observability stack (${PROJECT}) is up."
if [[ -f "$ENV_FILE" ]]; then
  echo "  Env file:    deploy/observability/.env"
else
  echo "  Env file:    (none — defaults; on Hetzner: cp deploy/observability/.example.env deploy/observability/.env)"
fi
echo "  Grafana:     http://127.0.0.1:3000  (default admin/admin unless set in .env)"
echo "  Prometheus:  http://127.0.0.1:9090"
echo "  Loki:        http://127.0.0.1:3100"
echo "  Alloy:       http://127.0.0.1:12345"
echo "  cAdvisor:    http://127.0.0.1:8080"
echo ""
echo "All UI ports bind to localhost only. On Hetzner, Tailscale Serve Grafana:"
echo "  tailscale serve --bg 3000"
echo ""
echo "Retention: Loki logs 30d, Prometheus metrics 180d."
echo ""
echo "Verify:"
echo "  Local API:   curl -s http://127.0.0.1:8000/health"
echo "  Loki (dev):  {service=\"api\", container=\"harvest-api-dev\"}"
echo "  Loki (prod): {service=\"api\", container=\"harvest-prod_api\"}"
echo "  Dashboard:   Harvest / Harvest overview"
echo "  Harvesters:  Harvest / Harvester work"
echo "  Fleet hosts: Fleet / Fleet hosts (Pis + remote agents)"
echo ""
echo "See deploy/observability/README.md for details."
