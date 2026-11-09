from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field


class UserMessengerSessionView(BaseModel):
    id: UUID
    account_id: UUID
    label: str
    session_type: str
    auth_state: str
    status: str
    dc_id: int | None = None
    server_address: str | None = None
    port: int | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> UserMessengerSessionView:
        return cls(
            id=row["id"],
            account_id=row["account_id"],
            label=row["label"],
            session_type=row["session_type"],
            auth_state=row["auth_state"],
            status=row["status"],
            dc_id=row.get("dc_id"),
            server_address=row.get("server_address"),
            port=row.get("port"),
            created_at=row.get("created_at"),
            updated_at=row.get("updated_at"),
        )


class UserMessengerAccountView(BaseModel):
    id: UUID
    external_id: str
    messenger: str
    label: str | None = None
    phone_number: str | None = None
    username: str | None = None
    first_name: str | None = None
    last_name: str | None = None
    is_active: bool = True
    created_at: datetime | None = None
    updated_at: datetime | None = None
    sessions: list[UserMessengerSessionView] = Field(default_factory=list)

    @classmethod
    def from_row(
        cls,
        row: dict[str, Any],
        *,
        sessions: list[UserMessengerSessionView] | None = None,
    ) -> UserMessengerAccountView:
        return cls(
            id=row["id"],
            external_id=row["external_id"],
            messenger=row["messenger"],
            label=row.get("label"),
            phone_number=row.get("phone_number"),
            username=row.get("username"),
            first_name=row.get("first_name"),
            last_name=row.get("last_name"),
            is_active=bool(row.get("is_active", True)),
            created_at=row.get("created_at"),
            updated_at=row.get("updated_at"),
            sessions=sessions or [],
        )
