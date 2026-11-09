"""Colored per-harvester log prefixes for API work and multi-harvester workers."""

from __future__ import annotations

import logging
from contextlib import contextmanager
from contextvars import ContextVar, Token
from typing import Iterator

from db.schemas.telegram.harvester import TelegramHarvesterSchema
from utils.harvester_colors import format_harvester_log_label, harvester_color_ansi
from utils.harvester_log import (  # noqa: F401
    HarvesterLogFilter,
    current_harvester_log_label,
    pop_harvester_log_label,
    push_harvester_log_label,
)

_ANSI_RESET = "\033[0m"

_active_harvester_logger: ContextVar[logging.Logger | logging.LoggerAdapter[logging.Logger] | None] = ContextVar(
    "active_harvester_logger", default=None
)


def harvester_display_name(harvester: TelegramHarvesterSchema) -> str:
    if harvester.label and harvester.label.strip():
        return harvester.label.strip()
    if harvester.username and harvester.username.strip():
        return f"@{harvester.username.strip()}"
    first = (harvester.first_name or "").strip()
    last = (harvester.last_name or "").strip()
    if first and last:
        return f"{first} {last}"
    if first:
        return first
    return harvester.external_id


def format_harvester_prefix(harvester: TelegramHarvesterSchema, *, color: bool | None = None) -> str:
    display = harvester_display_name(harvester)
    plain = f"[{display}]"
    if color is False:
        return plain
    prefix = format_harvester_log_label(display, color=color)
    if prefix == display:
        return plain
    return f"{harvester_color_ansi(display)}{plain}{_ANSI_RESET}"


class HarvesterLoggerAdapter(logging.LoggerAdapter[logging.Logger]):
    """Logger bound to a harvester. Message prefixing is done by ``HarvesterLogFilter``."""

    def process(self, msg: object, kwargs: dict[str, object]) -> tuple[object, dict[str, object]]:
        return msg, kwargs


def harvester_logger(
    base: logging.Logger,
    harvester: TelegramHarvesterSchema,
) -> HarvesterLoggerAdapter:
    return HarvesterLoggerAdapter(
        base,
        {
            "label": harvester_display_name(harvester),
            "prefix": format_harvester_prefix(harvester),
        },
    )


@contextmanager
def harvester_log_context(
    adapter: logging.Logger | logging.LoggerAdapter[logging.Logger],
) -> Iterator[None]:
    label = ""
    extra = getattr(adapter, "extra", None)
    if isinstance(extra, dict):
        label = str(extra.get("label") or "")
    token_logger: Token[logging.Logger | logging.LoggerAdapter[logging.Logger] | None] = _active_harvester_logger.set(
        adapter
    )
    push_harvester_log_label(label)
    try:
        yield
    finally:
        pop_harvester_log_label()
        _active_harvester_logger.reset(token_logger)


def get_active_harvester_logger(
    fallback: logging.Logger,
) -> logging.Logger | logging.LoggerAdapter[logging.Logger]:
    active = _active_harvester_logger.get()
    return active if active is not None else fallback
