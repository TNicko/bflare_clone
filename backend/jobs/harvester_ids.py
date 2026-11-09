"""Parse harvester Telegram user ids out of harvest job params."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any


def normalize_harvester_external_ids(values: Sequence[str] | None) -> list[str]:
    """Split comma-separated tokens, drop blanks, and keep first-seen order."""
    out: list[str] = []
    for raw in values or ():
        for part in str(raw).split(","):
            external_id = part.strip()
            if external_id and external_id not in out:
                out.append(external_id)
    return out


def harvester_external_ids_from_params(params: Mapping[str, Any] | None) -> list[str]:
    """Ids from ``harvester_external_ids`` and a legacy single ``harvester_external_id``."""
    raw_params = dict(params or {})
    chunks: list[str] = []
    raw_ids = raw_params.get("harvester_external_ids")
    if isinstance(raw_ids, list):
        chunks.extend(str(item) for item in raw_ids)
    elif isinstance(raw_ids, str):
        chunks.append(raw_ids)
    single = raw_params.get("harvester_external_id")
    if isinstance(single, str):
        chunks.append(single)
    return normalize_harvester_external_ids(chunks)
