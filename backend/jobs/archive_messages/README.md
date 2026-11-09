# archive_messages (Phase 3a)

Copy **cold** `telegram_messages` months to immutable Parquet under
`s3://messenger-harvest/messages/telegram/dt=YYYY-MM/part-*.parquet`, then set
`archive_key` / `archive_row` / `archived_at` in Postgres.

- **Does not** strip fat columns (Phase 3c). Rows stay full in PG after archive.
- **Hot window:** current month + previous `MESSAGE_ARCHIVE_HOT_MONTHS` (default 3).
- **Phase 3b (wired):** API hydrate uses overlay → Parquet when the fat body is
  missing (`content` null and/or media is only a harvest stub) and `archive_key`
  is set; cold upserts gate fat columns on stripped rows and write overlays when
  `edit_date` / `deleted` changes.
- **Harvest on cold rows:** Parquet stays an immutable Telegram snapshot. After
  strip, Postgres may keep a skinny `{"harvest": {...}}` stub (S3 URI / phash /
  etc.). Hydrate merges that stub onto Parquet `media`. On-demand and fleet
  media downloads persist only the harvest stub on archived+stripped rows — they
  do not rewrite Parquet or write full-row overlays.

## Prerequisites

1. Alembic `a2c8f1e4b903_message_archive_columns` applied.
2. IAM PutObject on `messages/telegram/*` (Quickwit EC2 role already has this for the harvest bucket).

## Usage

```bash
cd backend
PYTHONPATH=libs:jobs:. python -m jobs.archive_messages.cli status
PYTHONPATH=libs:jobs:. python -m jobs.archive_messages.cli status --month 2024-07
PYTHONPATH=libs:jobs:. python -m jobs.archive_messages.cli run --month 2015-09
PYTHONPATH=libs:jobs:. python -m jobs.archive_messages.cli run --max-months 1
```

Verify after a month: spot-check Parquet row `id`/`content` against PG for the same `(id, date)`.

## Phase 3c — strip fat columns

After archive is complete for a month, NULL heavy fields in PG (bodies stay on S3).
`media` is reduced to a skinny `{"harvest": ...}` stub when harvest exists, otherwise
NULL — TL media is not kept in Postgres. Hydrate (Phase 3b) fills fat fields on read.
Triggers that would enqueue search/edit history are disabled for the duration of each
month’s strip. Strip walks each month with a `(date, id)` keyset cursor (not
“re-find first fat row from month start” every chunk) so progress stays roughly
linear.

```bash
# Pilot one small month first
PYTHONPATH=libs:jobs:. python -m jobs.archive_messages.cli strip --month 2015-09 --dry-run
PYTHONPATH=libs:jobs:. python -m jobs.archive_messages.cli strip --month 2015-09

# Then the rest (tmux recommended)
PYTHONPATH=libs:jobs:. python -m jobs.archive_messages.cli strip --max-months 500
```

After a large strip: `VACUUM (VERBOSE) telegram_messages;` (or per partition), then shrink
Supabase provisioned disk in the dashboard if you want the bill to drop.

If a strip process is **killed mid-month**, re-enable triggers (strip disables them while running):

```sql
ALTER TABLE telegram_messages ENABLE TRIGGER telegram_messages_search_outbox_upd;
ALTER TABLE telegram_messages ENABLE TRIGGER telegram_messages_save_edit_history;
```

## Cost note

~90M cold rows → ~1k–2k parts at 100k rows/part; PUT fees negligible; storage ~tens of GiB.
