from __future__ import annotations

from collections.abc import Mapping
from typing import Any
from uuid import UUID

from pydantic import BaseModel

from db.schemas.general.primitives import PeerTypeSchema


class TelegramPeerSearchView(BaseModel):
    id: UUID
    external_id: str
    peer_type: PeerTypeSchema
    name: str

    @classmethod
    def from_peer_row(
        cls,
        row: Mapping[str, Any],
        *,
        peer_type: PeerTypeSchema,
        fallback_prefix: str,
    ) -> TelegramPeerSearchView:
        raw_id = row["id"]
        peer_id = raw_id if isinstance(raw_id, UUID) else UUID(str(raw_id))
        title = (row.get("title") or "").strip()
        username = (row.get("username") or "").strip()
        name = title or username or f"{fallback_prefix} {row['external_id']}"
        return cls(
            id=peer_id,
            external_id=str(row["external_id"]),
            peer_type=peer_type,
            name=name,
        )
