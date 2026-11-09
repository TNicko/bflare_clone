from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from pydantic import BaseModel


class CategorySimpleView(BaseModel):
    id: int
    slug: str
    label: str


class CategoryView(BaseModel):
    id: int
    slug: str
    label: str
    parent_slug: str | None = None
    parent_id: int | None = None
    parent_slug_resolved: str | None = None
    parent_label: str | None = None
    tier: int
    community_count: int

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> CategoryView:
        return cls(
            id=int(row["id"]),
            slug=str(row["slug"]),
            label=str(row["label"]),
            parent_slug=row.get("parent_slug"),
            parent_id=row.get("parent_id"),
            parent_slug_resolved=row.get("parent_slug_resolved"),
            parent_label=row.get("parent_label"),
            tier=int(row["tier"]),
            community_count=int(row["community_count"]),
        )
