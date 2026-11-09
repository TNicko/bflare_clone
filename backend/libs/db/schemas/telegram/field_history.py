from __future__ import annotations

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, JsonValue

PeerFieldHistoryTypeSchema = Literal["user", "channel", "megagroup", "chat"]


class TelegramPeerFieldHistorySchema(BaseModel):
    id: UUID | None = None
    external_id: str
    peer_type: PeerFieldHistoryTypeSchema
    field_key: str
    old_value: JsonValue | None = None
    recorded_at: datetime
    created_at: datetime | None = None
