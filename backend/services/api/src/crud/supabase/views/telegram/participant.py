from datetime import datetime
from uuid import UUID

from pydantic import BaseModel


class PeerParticipantView(BaseModel):
    id: UUID
    is_current_member: bool
    left_at: datetime | None = None
    first_seen_at: datetime | None = None
    updated_at: datetime | None = None
    rank: str | None = None
    joined_at: datetime | None = None
    created_at: datetime | None = None
