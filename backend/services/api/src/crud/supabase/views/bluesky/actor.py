"""API views for Bluesky actors."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field


class BlueskyActorPartialView(BaseModel):
    """Catalogue row for Bluesky name search (DB-only)."""

    id: UUID
    did: str
    handle: str | None = None
    handle_valid: bool = True
    display_name: str | None = None
    description: str | None = None
    avatar_url: str | None = None
    avatar_s3_uri: str | None = None
    followers_count: int | None = None
    posts_count: int | None = None
    unavailable_at: datetime | None = None

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> BlueskyActorPartialView:
        raw_id = row["id"]
        actor_id = raw_id if isinstance(raw_id, UUID) else UUID(str(raw_id))
        return cls(
            id=actor_id,
            did=str(row["did"]),
            handle=row.get("handle"),
            handle_valid=bool(row.get("handle_valid", True)),
            display_name=row.get("display_name"),
            description=row.get("description"),
            avatar_url=row.get("avatar_url"),
            avatar_s3_uri=row.get("avatar_s3_uri"),
            followers_count=row.get("followers_count"),
            posts_count=row.get("posts_count"),
            unavailable_at=row.get("unavailable_at"),
        )


class BlueskyActorPagedResult(BaseModel):
    items: list[BlueskyActorPartialView] = Field(default_factory=list)
    total: int
    limit: int
    offset: int
