from __future__ import annotations

import asyncio
import logging
from typing import Any, cast

import psycopg
from psycopg.rows import AsyncRowFactory, DictRow, RowFactory, TupleRow, dict_row

#: ``dict_row`` typed as a (sync/async) row factory so ``connect`` returns ``Connection[DictRow]`` under
#: pyright/mypy. Same callable at runtime; the sync/async factory protocols differ only in cursor type.
_DICT_ROW = cast(RowFactory[DictRow], dict_row)
_ASYNC_DICT_ROW = cast(AsyncRowFactory[DictRow], dict_row)

_lg = logging.getLogger(__name__)

_NON_RETRYABLE_CONNECT_MARKERS = (
    "authentication failed",
    "password authentication failed",
    "ecircuitbreaker",
    "too many authentication failures",
    "invalid authorization specification",
    "no pg_hba.conf entry",
)


def is_non_retryable_connect_error(exc: BaseException) -> bool:
    """Return True when reconnecting with the same DSN will not help."""
    message = str(exc).casefold()
    return any(marker in message for marker in _NON_RETRYABLE_CONNECT_MARKERS)


async def rollback_quiet(conn: Any, *, context: str = "") -> None:
    """Best-effort rollback so a poisoned connection can run more commands.

    Postgres aborts the whole transaction after any statement error. Callers that
    catch and continue must rollback, or every later query fails with
    ``InFailedSqlTransaction``.

    Also clears a leaked ``conn.transaction()`` stack: if ``__aenter__`` increments
    ``_num_transactions`` then fails before ``__aexit__`` (common when entering a
    nested transaction on an already-aborted connection), explicit ``rollback()``
    raises ``ProgrammingError: Explicit rollback() forbidden within a Transaction
    context``. Resetting the counter lets cleanup succeed.
    """
    try:
        n = getattr(conn, "_num_transactions", 0) or 0
        if n:
            conn._num_transactions = 0
            _lg.warning(
                "Cleared leaked connection.transaction() nesting (%s)%s",
                n,
                f" ({context})" if context else "",
            )
        await conn.rollback()
    except Exception:
        _lg.exception(
            "Failed rolling back connection%s",
            f" ({context})" if context else "",
        )


def normalize_psycopg_dsn(dsn: str) -> str:
    """Accept SQLAlchemy-style URLs; return a libpq/psycopg conninfo URI."""
    cleaned = dsn.strip().strip("'").strip('"')
    if cleaned.startswith("postgresql+psycopg://"):
        return cleaned.replace("postgresql+psycopg://", "postgresql://", 1)
    if cleaned.startswith("postgres+psycopg://"):
        return cleaned.replace("postgres+psycopg://", "postgresql://", 1)
    return cleaned


def connect(dsn: str, *, autocommit: bool = False) -> psycopg.Connection[DictRow]:
    """Open a sync Postgres connection (dict rows, pooler-safe). Use as a context manager."""
    return psycopg.Connection[DictRow].connect(
        normalize_psycopg_dsn(dsn),
        row_factory=_DICT_ROW,
        prepare_threshold=None,
        autocommit=autocommit,
    )


async def connect_async(
    dsn: str,
    *,
    autocommit: bool = False,
    cursor_factory: type[psycopg.AsyncCursor[DictRow]] | None = None,
) -> psycopg.AsyncConnection[DictRow]:
    """Open an async Postgres connection (dict rows, pooler-safe). Use as an async context manager.

    ``cursor_factory`` is optional so the API can time statements without changing
    jobs that call this helper.
    """
    kwargs: dict[str, Any] = {}
    if cursor_factory is not None:
        kwargs["cursor_factory"] = cursor_factory
    return await psycopg.AsyncConnection[DictRow].connect(
        normalize_psycopg_dsn(dsn),
        row_factory=_ASYNC_DICT_ROW,
        prepare_threshold=None,
        autocommit=autocommit,
        **kwargs,
    )


async def connect_async_with_retry(
    dsn: str,
    *,
    autocommit: bool = False,
    cursor_factory: type[psycopg.AsyncCursor[DictRow]] | None = None,
    max_attempts: int = 8,
    base_delay_seconds: float = 1.0,
    max_delay_seconds: float = 30.0,
) -> psycopg.AsyncConnection[DictRow]:
    """Like ``connect_async``, retrying transient pooler/DNS/network failures."""
    delay = base_delay_seconds
    last_exc: BaseException | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            return await connect_async(dsn, autocommit=autocommit, cursor_factory=cursor_factory)
        except (psycopg.OperationalError, psycopg.InterfaceError, OSError) as exc:
            last_exc = exc
            if is_non_retryable_connect_error(exc):
                _lg.error("Postgres connect failed with non-retryable error: %s", exc)
                raise
            if attempt >= max_attempts:
                break
            _lg.warning(
                "Postgres connect attempt %s/%s failed (%s); retrying in %.1fs",
                attempt,
                max_attempts,
                exc,
                delay,
            )
            await asyncio.sleep(delay)
            delay = min(delay * 2, max_delay_seconds)
    assert last_exc is not None
    raise last_exc


def connect_tuple(dsn: str, *, autocommit: bool = False) -> psycopg.Connection[TupleRow]:
    """Open a sync Postgres connection (tuple rows, pooler-safe). Use as a context manager."""
    return psycopg.connect(normalize_psycopg_dsn(dsn), prepare_threshold=None, autocommit=autocommit)


async def connect_async_tuple(dsn: str, *, autocommit: bool = False) -> psycopg.AsyncConnection[TupleRow]:
    """Open an async Postgres connection (tuple rows, pooler-safe). Use as an async context manager."""
    return await psycopg.AsyncConnection.connect(
        normalize_psycopg_dsn(dsn), prepare_threshold=None, autocommit=autocommit
    )
