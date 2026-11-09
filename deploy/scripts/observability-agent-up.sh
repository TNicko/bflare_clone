#!/usr/bin/env bash
# Start Alloy agent + node-exporter on a workload host (Pi / Hetzner work box).
# Ships Docker logs → Loki and host CPU/RAM → Prometheus on the obs host.
#
# From repo root on the workload host:
#   cp deploy/observability/agent/.example.env deploy/observability/agent/.env
#   # edit LOKI_PUSH_URL, PROMETHEUS_REMOTE_WRITE_URL, AGENT_HOSTNAME
#   ./deploy/scripts/observability-agent-up.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

PROJECT="${COMPOSE_PROJECT_NAME:-harvest-obs-agent}"
ENV_FILE="$ROOT/deploy/observability/agent/.env"
COMPOSE_FILE="$ROOT/deploy/observability/docker-compose.agent.yml"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Missing $ENV_FILE" >&2
  echo "  cp deploy/observability/agent/.example.env deploy/observability/agent/.env" >&2
  echo "  # set LOKI_PUSH_URL, PROMETHEUS_REMOTE_WRITE_URL, AGENT_HOSTNAME" >&2
  exit 1
fi

# shellcheck disable=SC1090
source "$ENV_FILE"
if [[ -z "${LOKI_PUSH_URL:-}" || "$LOKI_PUSH_URL" == *REPLACE_WITH_OBS_TAILSCALE* ]]; then
  echo "Set a real LOKI_PUSH_URL in $ENV_FILE" >&2
  exit 1
fi
if [[ -z "${PROMETHEUS_REMOTE_WRITE_URL:-}" || "$PROMETHEUS_REMOTE_WRITE_URL" == *REPLACE_WITH_OBS_TAILSCALE* ]]; then
  echo "Set a real PROMETHEUS_REMOTE_WRITE_URL in $ENV_FILE" >&2
  exit 1
fi
if [[ -z "${AGENT_HOSTNAME:-}" ]]; then
  echo "Set AGENT_HOSTNAME in $ENV_FILE (e.g. pi-01 — same as WORKER_ID)" >&2
  exit 1
fi

docker compose -p "$PROJECT" --env-file "$ENV_FILE" -f "$COMPOSE_FILE" up -d "$@"

echo ""
echo "Obs agent (${PROJECT}) is up on this host (${AGENT_HOSTNAME})."
echo "  Loki push:         $LOKI_PUSH_URL"
echo "  Prometheus write:  $PROMETHEUS_REMOTE_WRITE_URL"
echo "  Agent UI:          http://127.0.0.1:12346"
echo "  Logs:              docker logs -f harvest-alloy-agent"
echo ""
echo "In Grafana (obs host):"
echo "  Dashboards → Fleet → Fleet hosts"
echo "  Explore Loki:  {shipper=\"alloy-agent\", instance=\"${AGENT_HOSTNAME}\"}"
echo "  Explore Prom:  node_memory_MemTotal_bytes{instance=\"${AGENT_HOSTNAME}\"}"
echo ""
echo "See deploy/observability/README.md → Multi-host / Pis."
