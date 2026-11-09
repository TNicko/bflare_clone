"""Which programmatic channel a search call is billed as.

The override is a context variable set by the MCP server around its in-process
``/v1`` call. It is not read from request headers, so a caller cannot relabel
a website or API request as MCP.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar

BILLED_CHANNELS = frozenset({"api", "mcp"})

_override: ContextVar[str | None] = ContextVar("usage_channel_override", default=None)


def is_billed_channel(channel: str) -> bool:
    return channel in BILLED_CHANNELS


def current_usage_channel(default: str) -> str:
    override = _override.get()
    if override in BILLED_CHANNELS:
        return override
    return default


@contextmanager
def use_usage_channel(channel: str) -> Iterator[None]:
    if channel not in BILLED_CHANNELS:
        raise ValueError(f"Unknown usage channel: {channel}")
    token = _override.set(channel)
    try:
        yield
    finally:
        _override.reset(token)
