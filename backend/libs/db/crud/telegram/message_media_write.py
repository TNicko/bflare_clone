"""Persist ``telegram_messages.media`` without re-fattening cold archive rows.

Hot / unstripped rows store the full slim media JSONB (including ``harvest``).
Archived+stripped rows (``archive_key`` set, ``content`` NULL) store only a
skinny ``{"harvest": {...}}`` stub — Telegram TL media lives in Parquet and is
merged with this stub on hydrate.

Always filter on ``(id, date)`` so monthly partitions can be pruned.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from db.utils import adapt_param

#: Full media on hot rows; harvest-only stub when archived+stripped.
PERSIST_MESSAGE_MEDIA_SQL = """
UPDATE telegram_messages AS tm
SET media = CASE
  WHEN tm.archive_key IS NOT NULL AND tm.content IS NULL THEN
    CASE
      WHEN v.m ? 'harvest'
           AND jsonb_typeof(v.m->'harvest') = 'object'
           AND v.m->'harvest' <> '{}'::jsonb
      THEN jsonb_build_object('harvest', v.m->'harvest')
      ELSE NULL
    END
  ELSE v.m
END,
    updated_at = now()
FROM (SELECT %s::jsonb AS m) AS v
WHERE tm.id = %s AND tm.date = %s::timestamptz
"""


async def persist_message_media(
    conn: Any,
    *,
    row_id: UUID,
    date: datetime,
    media: dict[str, Any],
) -> None:
    async with conn.cursor() as cur:
        await cur.execute(
            PERSIST_MESSAGE_MEDIA_SQL,
            (adapt_param(media), row_id, date),
        )
