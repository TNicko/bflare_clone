"""Request-scoped user identity for log lines (API).

Defaults to ``system`` outside authenticated requests so every line is filterable.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from contextvars import ContextVar, Token
from collections.abc import Iterator

_DEFAULT_USER = "system"
_user_email: ContextVar[str] = ContextVar("log_user_email", default=_DEFAULT_USER)


def current_log_user() -> str:
    value = (_user_email.get() or "").strip()
    return value or _DEFAULT_USER


def set_log_user(email: str | None) -> Token[str]:
    text = (email or "").strip()
    return _user_email.set(text or _DEFAULT_USER)


def reset_log_user(token: Token[str]) -> None:
    _user_email.reset(token)


@contextmanager
def log_user_context(email: str | None) -> Iterator[None]:
    token = set_log_user(email)
    try:
        yield
    finally:
        reset_log_user(token)


class UserLogFilter(logging.Filter):
    """Set ``record.user`` from the request ContextVar (default ``system``)."""

    def filter(self, record: logging.LogRecord) -> bool:
        # Always set so format strings with ``%(user)s`` never KeyError.
        record.user = current_log_user()
        return True
