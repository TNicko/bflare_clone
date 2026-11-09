from __future__ import annotations

from datetime import datetime
from uuid import UUID
from pydantic import BaseModel

from db.schemas.general.primitives import TelegramPeerTypeSchema
from db.schemas.telegram.tl import ChatAdminRightsSchema, ChatBannedRightsSchema


class TelegramChatParticipantSchema(BaseModel):
    id: UUID | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None

    external_id: str
    user_id: str
    peer_type: TelegramPeerTypeSchema

    is_current_member: bool = True
    first_seen_at: datetime | None = None
    joined_at: datetime | None = None
    left_at: datetime | None = None

    # --- Telegram ---

    # Basic groups (peer_type="chat") — official TL: ChatParticipant*
    inviter_id: int | None = None
    rank: str | None = None

    # Channels / supergroups (peer_type is "channel","megagroup") — TL: ChannelParticipant*
    promoted_by: int | None = None
    admin_rights: ChatAdminRightsSchema | None = None
    kicked_by: int | None = None
    banned_rights: ChatBannedRightsSchema | None = None
    subscription_until_at: datetime | None = None
    via_request: bool | None = None
