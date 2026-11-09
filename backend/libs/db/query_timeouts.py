"""Per-transaction query timeouts (pooler-safe via SET LOCAL)."""

from __future__ import annotations

from contextlib import asynccontextmanager
from collections.abc import AsyncIterator
from typing import Any


async def apply_local_timeouts(
    conn: Any,
    *,
    lock_timeout: str | None = "30s",
    statement_timeout: str = "120s",
) -> None:
    """Apply ``SET LOCAL`` timeouts. Caller must be inside ``conn.transaction()``."""
    if lock_timeout is not None:
        await conn.execute(f"SET LOCAL lock_timeout = '{lock_timeout}'")
    await conn.execute(f"SET LOCAL statement_timeout = '{statement_timeout}'")


@asynccontextmanager
async def read_query_transaction(
    conn: Any,
    *,
    lock_timeout: str | None = "30s",
    statement_timeout: str = "120s",
) -> AsyncIterator[None]:
    """One read transaction with extended lock/statement timeouts."""
    async with conn.transaction():
        await apply_local_timeouts(
            conn,
            lock_timeout=lock_timeout,
            statement_timeout=statement_timeout,
        )
        yield
