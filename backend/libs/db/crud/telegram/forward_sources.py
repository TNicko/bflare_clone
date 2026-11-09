"""Persist forward-source edges (and optional coverage flags) from message batches."""

from __future__ import annotations

import logging
from typing import Any, Sequence
from uuid import UUID

from db.crud.telegram.coverage import mark_forward_sources_covered
from db.crud.telegram.forward_edges import (
    forward_edge_deltas_from_messages,
    increment_forward_edges,
)
from db.crud.telegram.unresolved_peer import ensure_web_peer_stub
from db.schemas.general.primitives import TelegramPeerTypeSchema

_lg = logging.getLogger(__name__)


async def persist_forward_sources_from_message_batch(
    conn: Any,
    *,
    messages: Sequence[Any],
    source_external_id: str,
    from_peer_type: TelegramPeerTypeSchema,
    harvester_id: UUID | None = None,
) -> int:
    """Increment ``telegram_forward_edges`` from one upserted message batch."""
    deltas = forward_edge_deltas_from_messages(
        messages,
        source_external_id=source_external_id,
    )
    if not deltas:
        return 0

    touched = await increment_forward_edges(
        conn,
        from_external_id=source_external_id,
        from_peer_type=from_peer_type,
        updates=deltas,
    )
    if harvester_id is None:
        return touched

    for delta in deltas:
        await ensure_web_peer_stub(
            conn,
            external_id=delta.to_external_id,
            peer_type=delta.to_peer_type,
            title=delta.last_from_name,
            harvester_id=harvester_id,
            mark_unavailable=False,
        )
    await mark_forward_sources_covered(
        conn,
        harvester_id=harvester_id,
        peer_external_id=source_external_id,
        peer_type=from_peer_type,
    )
    if touched:
        _lg.debug(
            "Forward sources persisted peer=%s targets=%s",
            source_external_id,
            touched,
        )
    return touched
