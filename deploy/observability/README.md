# Observability (Alloy → Loki + Prometheus → Grafana)

Collect Docker logs and container metrics. Separate Compose project (`harvest-obs`)
from the API (`harvest-dev` Compose locally / `harvest-prod` **Swarm stack** on Hetzner).

The API should use `LOG_FORMAT=json` so Alloy can promote `level` / `logger` / `user` to
Loki labels and prefix each line with `[email|system]`. In Grafana Explore:

```logql
# Prefer service= (works for local Compose and Hetzner Swarm)
{service="api"}
{service="api", level="error"}
{service="api", user="you@email.com"}
{service="api", logger=~"uvicorn.access|services.api.*"}
# Hide Swarm/Caddy health polls and Prometheus scrapes (Alloy also drops these at ingest)
{service="api"} !~ `GET /health` !~ `GET /metrics`
{service="jobs-agent"}
# Local container name
{container="harvest-api-dev"}
{container="harvest-jobs-agent"}
# Hetzner Swarm service name (stable across task IDs)
{container="harvest-prod_api"}
```

API request timing is one line per HTTP request (`logger="services.api.src.request_timing"`).
Category times (`db_ms`, `s3_ms`, …) are time inside that client and can overlap.
`route` is the path template. `API_TRACE_SLOW_MS` (default 500) adds a `spans=` timeline.

```logql
# Slow requests
{service="api", logger="services.api.src.request_timing"} |= "request method="
| regexp `route=(?P<route>\S+) status=(?P<status>\d+) request_id=(?P<request_id>\S+) total_ms=(?P<total_ms>\d+) db_ms=(?P<db_ms>\d+)`
| total_ms > 500

# p95 wall time and p95 Postgres time by route
quantile_over_time(0.95,
  {service="api", logger="services.api.src.request_timing"} |= "request method="
  | regexp `route=(?P<route>\S+) total_ms=(?P<total_ms>[0-9]+)`
  | unwrap total_ms [1h]
) by (route)

quantile_over_time(0.95,
  {service="api", logger="services.api.src.request_timing"} |= "request method="
  | regexp `route=(?P<route>\S+) total_ms=\d+ db_ms=(?P<db_ms>[0-9]+)`
  | unwrap db_ms [1h]
) by (route)
```

```text
harvest-api-dev / harvest-prod_api.* / harvest-jobs-agent
        │  docker.sock
        ▼
   Grafana Alloy ──────────────► Loki        (30d retention)
   cAdvisor ───────────────────► Prometheus  (180d retention)
                                      │
                                      ▼
                                   Grafana :3000 (localhost)
```

## Prereqs

- Docker Engine + Compose v2 (Desktop locally, or Hetzner)
- API container running if you want API logs (optional for bringing the stack up)

## Start / stop

From repo root:

```bash
./deploy/scripts/observability-up.sh
./deploy/scripts/observability-down.sh      # keep volumes
./deploy/scripts/observability-down.sh -v  # wipe Loki/Prometheus/Grafana data
```

On Hetzner, copy and edit the observability env file (not the API `.env`):

```bash
cp deploy/observability/.example.env deploy/observability/.env
# set GF_SECURITY_ADMIN_PASSWORD=… (and optional GF_SERVER_ROOT_URL)
./deploy/scripts/observability-up.sh
```

| Service    | URL                         | Notes |
|------------|-----------------------------|--------|
| Grafana    | http://127.0.0.1:3000       | localhost only; Tailscale Serve for remote access |
| Prometheus | http://127.0.0.1:9090       | localhost only |
| Loki       | http://127.0.0.1:3100       | `/ready` |
| Alloy      | http://127.0.0.1:12345      | agent UI / metrics |
| cAdvisor   | http://127.0.0.1:8080       | container metrics |

## Verify end-to-end

1. Start the API if needed:

   ```bash
   # Local
   ./deploy/scripts/dev-up.sh
   # Hetzner
   ./deploy/scripts/api-prod-up.sh
   ```

2. Start observability:

   ```bash
   ./deploy/scripts/observability-up.sh
   ```

3. Generate traffic:

   ```bash
   curl -s http://127.0.0.1:8000/health
   ```

4. **Logs** — Grafana → Explore → datasource **Loki**:

   ```logql
   {service="api"}
   {service="api", level="info"}
   {service="api", level="error"}
   {container="harvest-prod_api"}
   ```

   Log lines are message-only; the level is a Loki label (`level="error"`,
   lowercase). On Grafana 13+, enable **Display log level** in the Logs panel /
   Explore display options.

5. **Metrics** — Explore → **Prometheus**:

   ```promql
   up
   machine_cpu_cores
   machine_memory_bytes
   sum by (container) (label_replace(rate(container_cpu_usage_seconds_total{image!=""}[5m]), "container", "$1", "image", ".*/([^/:]+):.*")) / scalar(machine_cpu_cores)
   ```

6. Open dashboard **Harvest → Harvest overview** (provisioned).

## Multi-host (obs on Hetzner, agents on work box + Pis)

Alloy only sees **local** Docker. Grafana/Loki/Prometheus live on host **Obs**.
Every other machine that runs workloads (`jobs-agent`, API)
runs a small **agent** stack that:

1. Pushes Docker logs → Loki over Tailscale
2. Runs **node-exporter** and remote-writes CPU/RAM → Prometheus

```text
[Pi / Work]                              [Obs: harvest-obs]
  harvest-jobs-agent ─┐
  harvest-alloy-agent ├──► Loki (:3100) ──► Grafana
  harvest-node-exporter┘──► Prometheus (:9090) ─► Fleet dashboard
```

### On Obs host (once)

1. In `deploy/observability/.env`, bind Loki **and** Prometheus to this machine’s
   Tailscale IP:

   ```bash
   # tailscale ip -4
   LOKI_HOST_BIND=100.x.y.z
   PROMETHEUS_HOST_BIND=100.x.y.z
   ```

2. Recreate the stack:

   ```bash
   ./deploy/scripts/observability-up.sh --force-recreate
   ```

3. Confirm from a Pi / Work host:

   ```bash
   curl -sS http://100.x.y.z:3100/ready
   curl -sS http://100.x.y.z:9090/-/ready
   ```

   Restrict Tailscale ACLs / Hetzner firewall so only your tailnet can hit
   `:3100` / `:9090` — never the public NIC.

### On each Pi (and the Hetzner work box)

1. Run `jobs-agent` as usual.

2. Ship logs + host metrics:

   ```bash
   cp deploy/observability/agent/.example.env deploy/observability/agent/.env
   # AGENT_HOSTNAME=pi-01          # same slug as WORKER_ID
   # LOKI_PUSH_URL=http://100.x.y.z:3100/loki/api/v1/push
   # PROMETHEUS_REMOTE_WRITE_URL=http://100.x.y.z:9090/api/v1/write
   ./deploy/scripts/observability-agent-up.sh
   ```

3. Check: `docker logs -f harvest-alloy-agent`

4. In Grafana (Obs):

   - **Dashboards → Fleet → Fleet hosts** — per-host CPU/RAM vs max + logs
   - Explore Loki: `{shipper="alloy-agent", instance="pi-01"}`
   - Explore Loki: `{service="jobs-agent"}`

Stop the agent: `./deploy/scripts/observability-agent-down.sh`

### Same-host (unchanged)

If Obs and workloads share one Docker host, you only need `observability-up.sh`
and the keep rules in `config/config.alloy` — no agent (cAdvisor covers that
host’s containers; use Fleet dashboard for remote Pis).

## Hetzner (Tailscale-only Grafana)

Do **not** put Grafana on the public API Caddy vhost. Obs UI ports are bound to
`127.0.0.1` only.

1. Pull and start (with a non-default password).
2. Serve Grafana on the tailnet:

   ```bash
   tailscale serve --bg 3000
   tailscale serve status
   ```

3. Open `https://<machine>.<tailnet>.ts.net/` from a device on the tailnet.
4. Restrict ACL so only your tailnet group can reach that host:443.
5. Optional later: grey-cloud `metrics.blackflare.io` → Tailscale `100.x` IP.

Keep the Hetzner firewall at 22/80/443 for the public API; never open
3000/3100/9090 to the world.

## What is collected

| Signal  | Source | Destination |
|---------|--------|-------------|
| Logs    | Docker stdout/stderr via Alloy (`harvest-dev` Compose or `harvest-prod` Swarm) | Loki |
| Metrics | cAdvisor (containers) + Harvest API `/metrics` (harvester work) scraped by Prometheus | Prometheus |

**Capacity on the overview dashboard:** **CPU** / **RAM** stats show host totals from
cAdvisor (`machine_cpu_cores`, `machine_memory_bytes`). Container CPU/memory panels
are **% of that host**. On Hetzner Linux you get named per-container series; on
Docker Desktop (macOS) per-container cAdvisor series are often missing.

App-level `GET /metrics` on the FastAPI API is scraped by Prometheus
(`job=harvest-api`, every 5s via `host.docker.internal:8000`). Series:

- `harvest_harvester_in_flight` — concurrent Telethon leases per harvester
- `harvest_harvester_work_started_total` / `_finished_total` — work start/end counters
- `harvest_harvester_session_connected` — 1 while the API holds that session

Grafana: **Harvest → Harvester work** (also a stacked panel on **Harvest overview**).
`/metrics` is unauthenticated like `/health` — scrape it on host `:8000` (firewall-denied
on Hetzner). Do **not** proxy `/metrics` through public Caddy.

**Docker Desktop (macOS):** logs work reliably. Per-container cAdvisor metrics can be
sparse or missing inside the Desktop VM; Linux (and Hetzner) hosts get full series.

## Retention

| Store      | Retention | Config |
|------------|-----------|--------|
| Loki       | 30 days   | `config/loki.yml` → `limits_config.retention_period: 720h` |
| Prometheus | 180 days  | compose `--storage.tsdb.retention.time=180d` |

## Files

| Path | Role |
|------|------|
| `docker-compose.yml` | Loki, Prometheus, Alloy, Grafana, cAdvisor |
| `config/loki.yml` | Loki single-process + retention |
| `config/prometheus.yml` | Scrape Alloy, cAdvisor, self, Harvest API `/metrics` |
| `config/config.alloy` | Docker logs (`harvest-dev` Compose \| `harvest-prod` Swarm) → Loki |
| `config/grafana/provisioning/` | Datasources + dashboard provider |
| `config/grafana/dashboards/harvest-overview.json` | Overview dashboard |
| `config/grafana/dashboards/harvest-harvesters.json` | Per-harvester work over time |

## Production notes

- Override `GF_SECURITY_ADMIN_PASSWORD` on shared hosts; put Grafana behind Tailscale.
- On an 8 GB API box, watch disk (`df -h`) and consider Compose memory limits so
  Loki/Prometheus cannot OOM the API.
- Prefer a dedicated disk budget for 180d metrics + 30d logs when traffic grows.

## Troubleshooting

**No API logs in Loki**

- Confirm the API / indexer containers: `docker ps --filter name=harvest-`
- Alloy keeps Compose projects `harvest-dev` / `harvest-indexer`, **or** Swarm stack
  namespace `harvest-prod` (not `harvest-obs`). See `config/config.alloy`.
- Wait ~15–30s after first start for discovery; re-check Explore time range.
- After switching Compose → Swarm on Hetzner (or editing Alloy), recreate Alloy:  
  `docker compose -p harvest-obs -f deploy/observability/docker-compose.yml up -d --force-recreate alloy`
- Recreate Alloy after editing `config.alloy` the same way.

**No container metrics**

- Prometheus → Status → Targets: `cadvisor`, `alloy`, `prometheus` should be UP.
- On the host:
  ```bash
  docker version --format '{{.Server.Version}}'
  docker logs harvest-cadvisor --tail 40
  curl -s 'http://127.0.0.1:8080/metrics' | grep container_memory_working_set_bytes | head
  ```
  Series should include an `image=` label (e.g. `…/harvest-api:prod`). Docker 29 often
  sets `name=` to the container id — the overview dashboard legends from `image` instead.
  If you only see `id="/"` (or nothing), use `ghcr.io/google/cadvisor:0.60.5` with
  `--disable_metrics=disk` and the containerd mounts in `docker-compose.yml`.

**Grafana datasource errors**

- Ensure Loki/Prometheus healthchecks passed:  
  `docker compose -p harvest-obs -f deploy/observability/docker-compose.yml ps`
