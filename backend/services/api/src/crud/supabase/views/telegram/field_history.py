from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any

from pydantic import BaseModel, JsonValue


class FieldHistoryEntryView(BaseModel):
    """One superseded profile field value for any peer type (user, channel, megagroup, chat)."""

    field_key: str
    old_value: JsonValue | None = None
    recorded_at: datetime


def field_history_from_row(raw: Any) -> list[FieldHistoryEntryView]:
    if not raw or not isinstance(raw, list):
        return []
    out: list[FieldHistoryEntryView] = []
    for entry in raw:
        if isinstance(entry, FieldHistoryEntryView):
            out.append(entry)
            continue
        if not isinstance(entry, Mapping):
            continue
        field_key = entry.get("field_key")
        recorded_at = entry.get("recorded_at")
        if not isinstance(field_key, str) or not field_key.strip() or recorded_at is None:
            continue
        out.append(
            FieldHistoryEntryView(
                field_key=field_key.strip(),
                old_value=entry.get("old_value"),
                recorded_at=recorded_at,
            )
        )
    return out
