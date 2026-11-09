
# Search indexer (Chunk 4)

Keeps the Quickwit keyword index in sync with Postgres by draining a **transactional outbox**. This worker
carries every change forward so the live index never drifts.

## How it works

```
telegram_messages  --(AFTER INSERT/UPDATE/DELETE trigger)-->  search_outbox  --(this worker)-->  INDEX_SINKS
```

The indexer writes Quickwit only.

1. A Postgres trigger (Alembic `search outbox triggers`) writes one `search_outbox` row per change to
   `telegram_messages`, in the **same transaction** as the write — so a committed message is always queued.
2. The worker, in one transaction per batch:
   - claims rows with `FOR UPDATE SKIP LOCKED` (multi-worker safe),
   - **coalesces** by message id (latest event wins — collapses edit storms / idempotent re-upserts),
   - hydrates current rows from Postgres by `(id, date)` (partition-pruned), builds docs,
   - applies to Quickwit (delete-task then NDJSON ingest),
   - deletes the claimed outbox rows, then commits.

The outbox is **transient**: steady-state size is the unprocessed backlog, not one row per message. A
crash before commit leaves rows to be reprocessed (at-least-once; safe because apply is idempotent).

## Run it

```bash
# Long-running worker (what you'd run in dev/prod)
python -m services.indexer.cli run

# Process the current backlog once and exit (cron / tests)
python -m services.indexer.cli drain

# Inspect the queue
python -m services.indexer.cli status
```

### Local Docker

Included in `./deploy/scripts/dev-up.sh` via `deploy/docker-compose.indexer.dev.yml`.

### Production (Hetzner API host)

Separate Compose project from the API Swarm stack:

1. Copy `.example.env` → `backend/services/indexer/.env` on the host; set prod
   `POSTGRES_DSN` and `QUICKWIT_URL` (`http://harvest-quickwit:7280`).
2. From repo root:

```bash
./deploy/scripts/indexer-prod-up.sh
docker exec harvest-indexer python -m services.indexer.cli status
docker logs -f harvest-indexer
```

Stop with `./deploy/scripts/indexer-prod-down.sh`. See
`deploy/docker-compose.indexer.prod.yml`.

## Verify end-to-end (dev)

```bash
python -m services.indexer.cli status                         # depth: 0
# harvest or edit a message (or: psql -c "UPDATE telegram_messages SET content = content || ' zzz' WHERE id = '<uuid>'")
python -m services.indexer.cli status                         # depth: >0
python -m services.indexer.cli drain                          # consumed N events
# search Quickwit for "zzz" — the change is now searchable
```

## Operational notes

- **Backlog growth = stalled worker.** Alert on `status` depth / oldest-pending age.
- **Bloat:** high insert+delete churn creates dead tuples; rely on autovacuum (or make `search_outbox`
  `UNLOGGED` / day-partitioned later if churn is heavy).
- **Multiple workers** are safe (`SKIP LOCKED`); scale out by running more `run` processes.
- This worker handles `entity = 'message'` only.
