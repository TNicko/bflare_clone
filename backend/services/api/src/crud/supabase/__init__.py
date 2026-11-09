from __future__ import annotations

import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from psycopg import AsyncConnection
from psycopg.rows import DictRow

from db.connection import connect_async_with_retry
from services.api.src.config import ConfigFactory
from services.api.src.crud.supabase.tracing import TracingAsyncCursor
from utils.request_spans import record_span


@asynccontextmanager
async def get_conn(dsn: str | None = None) -> AsyncIterator[AsyncConnection[DictRow]]:
    """Open an async connection (autocommit). Uses POSTGRES_DSN from config if dsn not provided.

    Statements on this connection are timed when a request trace is active.

    Retries transient Docker Desktop / MagicDNS lookup flakes to the Supabase
    pooler (``failed to resolve host`` / ``No address associated``).
    """
    if dsn is None:
        dsn = ConfigFactory.get_config().postgres_dsn
    started = time.perf_counter()
    try:
        # Short retries: DNS flakes usually clear on the next lookup.
        conn = await connect_async_with_retry(
            dsn,
            autocommit=True,
            cursor_factory=TracingAsyncCursor,
            max_attempts=5,
            base_delay_seconds=0.2,
            max_delay_seconds=2.0,
        )
    finally:
        record_span("db", "connect", time.perf_counter() - started)
    async with conn:
        yield conn
