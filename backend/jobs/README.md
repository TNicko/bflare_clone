# Fleet jobs agent — bootstrap on a Pi or Hetzner host
#
# Prerequisites: Tailscale, Docker (or a venv with harvest-backend installed),
# git clone of this repo, network reachability to Postgres / S3 / deps.
# For Grafana logs + CPU/RAM on Hetzner obs, also run the Alloy agent — see
# deploy/observability/README.md → Multi-host / Pis.

## 1. Pull

```bash
cd /path/to/messenger
git pull
```

## 2. Central env (one file for all job tasks)

```bash
cp backend/jobs/.example.env backend/jobs/.env
```

Edit at least:

- `POSTGRES_DSN`
- `WORKER_ID` — unique per host (`pi-01`, `hetzner-1`, …)
- `WORKER_LABEL`
- `WORKER_MAX_CONCURRENT_JOBS` — based on RAM; later adjustable in SQL
- `WORKER_SEED_MAX_CONCURRENT=1` on first boot if you want the env value written to DB
- `TELEGRAM_*` session keys; set `TELEGRAM_SESSION_LOCATION_ID` to match this host
- AWS / Milvus / embedder URLs as needed for the capabilities you enable

Optional: `WORKER_CAPABILITIES=fetch_messages,embed_messages,...`
(empty = all task types)

## 3. Database

The database is assumed to already exist. The revisions under `backend/alembic/versions` are examples, not a bootstrap.

## 4. Start the agent

Docker (recommended, same as Hetzner):

```bash
./deploy/scripts/jobs-agent-prod-up.sh --build
docker logs -f harvest-jobs-agent
```

Or locally:

```bash
cd backend
python -m jobs.agent --loop
```

## 5. Verify

- `docker logs -f harvest-jobs-agent` shows the worker heartbeat
- `job_workers` has this host with status `online`
- Enqueue a `fetch_messages` row on `harvest_jobs` for this worker (or any capable worker)
- The job moves `queued` → `running` on that worker

## Notes

- `image_embedding` remains a long-running HTTP service under `services/`,
  not a fleet job.
- Change `max_concurrent_jobs` anytime in SQL; the agent
  picks it up on the next heartbeat.
