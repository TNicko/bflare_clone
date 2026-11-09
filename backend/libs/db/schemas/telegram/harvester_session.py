from __future__ import annotations

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel

SessionTypeSchema = Literal["api", "system"]
SessionAuthStateSchema = Literal["healthy", "error", "reset_required"]
SessionStatusSchema = Literal["inactive", "active"]


class TelegramHarvesterSessionSchema(BaseModel):
    """Row shape for ``telegram_harvester_sessions``."""

    id: UUID
    harvester_id: UUID
    label: str
    session_type: SessionTypeSchema
    auth_state: SessionAuthStateSchema
    status: SessionStatusSchema
    session_string: str | None = None
    auth_key: bytes | None = None
    dc_id: int | None = None
    server_address: str | None = None
    port: int | None = None
    leased_by_location_id: str | None = None
    leased_until: datetime | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


class TelegramHarvesterSessionLeaseHolderSchema(BaseModel):
    """Row shape for ``telegram_harvester_session_lease_holders``."""

    session_id: UUID
    server_id: str
    location_id: str
    last_heartbeat_at: datetime
    joined_at: datetime


class TelegramHarvesterSessionUpdateStateSchema(BaseModel):
    """Row shape for ``telegram_harvester_session_update_states``."""

    session_id: UUID
    entity_id: int
    pts: int
    qts: int
    seq: int
    state_date: datetime
    updated_at: datetime | None = None
