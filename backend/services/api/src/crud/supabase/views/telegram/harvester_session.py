from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field

from db.schemas.telegram.harvester_session import (
    SessionAuthStateSchema,
    SessionStatusSchema,
    SessionTypeSchema,
)


def _lease_holder_server_ids(raw: Any) -> list[str]:
    if not isinstance(raw, list):
        return []
    return [str(item) for item in raw]


class TelegramHarvesterSessionView(BaseModel):
    """API view of a harvester session (no auth secrets; includes lease holder ids)."""

    id: UUID
    harvester_id: UUID
    label: str
    session_type: SessionTypeSchema
    auth_state: SessionAuthStateSchema
    status: SessionStatusSchema
    dc_id: int | None = None
    server_address: str | None = None
    port: int | None = None
    leased_by_location_id: str | None = None
    leased_until: datetime | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    lease_holders: list[str] = Field(default_factory=list)

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> TelegramHarvesterSessionView:
        return cls(
            id=row["id"],
            harvester_id=row["harvester_id"],
            label=row["label"],
            session_type=row["session_type"],
            auth_state=row["auth_state"],
            status=row["status"],
            dc_id=row.get("dc_id"),
            server_address=row.get("server_address"),
            port=row.get("port"),
            leased_by_location_id=row.get("leased_by_location_id"),
            leased_until=row.get("leased_until"),
            created_at=row.get("created_at"),
            updated_at=row.get("updated_at"),
            lease_holders=_lease_holder_server_ids(row.get("lease_holders")),
        )


class TelegramHarvesterWithSessionsView(BaseModel):
    """Harvester account with nested session views for ``GET /telegram/accounts``."""

    id: UUID
    external_id: str
    label: str | None = None
    is_active: bool = True
    phone_number: str | None = None
    username: str | None = None
    first_name: str | None = None
    last_name: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    sessions: list[TelegramHarvesterSessionView] = Field(default_factory=list)

    @classmethod
    def from_row(
        cls,
        row: Mapping[str, Any],
        *,
        sessions: list[TelegramHarvesterSessionView],
    ) -> TelegramHarvesterWithSessionsView:
        return cls(
            id=row["id"],
            external_id=str(row["external_id"]),
            label=row.get("label"),
            is_active=bool(row.get("is_active", True)),
            phone_number=row.get("phone_number"),
            username=row.get("username"),
            first_name=row.get("first_name"),
            last_name=row.get("last_name"),
            created_at=row.get("created_at"),
            updated_at=row.get("updated_at"),
            sessions=sessions,
        )
