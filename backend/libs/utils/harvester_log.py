"""Task-local harvester label for log lines (API work + worker pipelines)."""

from __future__ import annotations

import logging
import re
from contextvars import ContextVar
from uuid import UUID

from utils.harvester_colors import format_harvester_log_label

_ANSI_RE = re.compile(r"\033\[[0-9;]*m")
_harvester_label_stack: ContextVar[tuple[str, ...]] = ContextVar("harvester_label_stack", default=())
_SKIP_LOGGER_PREFIXES = ("uvicorn.access",)


def current_harvester_log_label() -> str | None:
    stack = _harvester_label_stack.get()
    if not stack:
        return None
    label = stack[-1].strip()
    return label or None


def push_harvester_log_label(label: str) -> None:
    text = label.strip()
    if not text or _looks_like_uuid(text):
        _harvester_label_stack.set(_harvester_label_stack.get() + ("",))
        return
    _harvester_label_stack.set(_harvester_label_stack.get() + (text,))


def pop_harvester_log_label() -> None:
    stack = _harvester_label_stack.get()
    _harvester_label_stack.set(stack[:-1] if stack else ())


def _looks_like_uuid(value: str) -> bool:
    try:
        UUID(value)
        return True
    except ValueError:
        return False


class HarvesterLogFilter(logging.Filter):
    """Prefix the record message with the in-flight harvester label."""

    def filter(self, record: logging.LogRecord) -> bool:
        if record.name.startswith(_SKIP_LOGGER_PREFIXES):
            return True
        label = current_harvester_log_label()
        if not label:
            return True
        record.harvester = label
        try:
            rendered = record.getMessage()
        except Exception:
            return True
        plain = _ANSI_RE.sub("", rendered)
        if plain.startswith(label) or plain.startswith(f"[{label}]"):
            return True
        record.msg = f"{format_harvester_log_label(label)} {rendered}"
        record.args = ()
        return True
