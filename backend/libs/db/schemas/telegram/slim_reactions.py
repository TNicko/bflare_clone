"""Slim message reactions JSONB (counts + can_see_list; no peer lists)."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

from .tl import TypeReactionSchema


class SlimReactionCountSchema(BaseModel):
    model_config = ConfigDict(extra="forbid")

    reaction: TypeReactionSchema
    count: int = Field(ge=0)


class SlimMessageReactionsSchema(BaseModel):
    """Persisted shape for ``telegram_messages.reactions`` / edits."""

    model_config = ConfigDict(extra="forbid")

    can_see_list: bool | None = None
    results: list[SlimReactionCountSchema] = Field(default_factory=list)
