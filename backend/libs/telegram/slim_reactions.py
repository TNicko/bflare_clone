"""Map full TL MessageReactions to slim persisted shapes."""

from __future__ import annotations

from typing import Any

from db.schemas.telegram.slim_reactions import (
    SlimMessageReactionsSchema,
    SlimReactionCountSchema,
)
from db.schemas.telegram.tl import TypeReactionSchema
from pydantic import TypeAdapter, ValidationError

from telegram.utils import normalize_tl_keys

_REACTION_ADAPTER = TypeAdapter(TypeReactionSchema)


def reaction_key(reaction: TypeReactionSchema | dict[str, Any]) -> str:
    """Stable PK fragment for a reaction type (emoji / custom / paid / empty)."""
    if isinstance(reaction, dict):
        data = normalize_tl_keys(reaction)
        tl = data.get("tl")
        if tl == "ReactionEmoji":
            return f"emoji:{data.get('emoticon') or ''}"
        if tl == "ReactionCustomEmoji":
            return f"custom:{data.get('document_id')}"
        if tl == "ReactionPaid":
            return "paid"
        return f"empty:{tl or 'unknown'}"
    tl = reaction.tl
    if tl == "ReactionEmoji":
        return f"emoji:{getattr(reaction, 'emoticon', '') or ''}"
    if tl == "ReactionCustomEmoji":
        return f"custom:{getattr(reaction, 'document_id', None)}"
    if tl == "ReactionPaid":
        return "paid"
    return f"empty:{tl}"


def reaction_total(reactions: SlimMessageReactionsSchema | dict[str, Any] | None) -> int:
    if reactions is None:
        return 0
    if isinstance(reactions, SlimMessageReactionsSchema):
        return sum(r.count for r in reactions.results)
    results = reactions.get("results") if isinstance(reactions, dict) else None
    if not isinstance(results, list):
        return 0
    total = 0
    for item in results:
        if isinstance(item, dict) and isinstance(item.get("count"), int):
            total += item["count"]
    return total


def slim_reactions_from_full(raw: Any) -> SlimMessageReactionsSchema | None:
    """Accept full TL MessageReactions, slim dict, or already-slim schema."""
    if raw is None:
        return None
    if isinstance(raw, SlimMessageReactionsSchema):
        return raw
    if not isinstance(raw, dict):
        return None

    data = normalize_tl_keys(raw)
    # Empty array / object defaults from DB
    if data == {} or data == []:
        return None

    can_see_list = data.get("can_see_list")
    if can_see_list is not None:
        can_see_list = bool(can_see_list)

    results_out: list[SlimReactionCountSchema] = []
    raw_results = data.get("results")
    if isinstance(raw_results, list):
        for item in raw_results:
            if not isinstance(item, dict):
                continue
            item = normalize_tl_keys(item)
            reaction_raw = item.get("reaction")
            count = item.get("count")
            if reaction_raw is None or not isinstance(count, int):
                continue
            try:
                reaction = _REACTION_ADAPTER.validate_python(reaction_raw)
            except ValidationError:
                continue
            results_out.append(SlimReactionCountSchema(reaction=reaction, count=count))

    if not results_out and can_see_list is None:
        return None

    return SlimMessageReactionsSchema(can_see_list=can_see_list, results=results_out)
