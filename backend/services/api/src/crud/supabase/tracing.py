"""Time Postgres statements on API connections. Jobs keep the default cursor."""

from __future__ import annotations

import time
from typing import Any

import psycopg
from psycopg.rows import DictRow

from utils.request_spans import record_span, sql_span_name


class TracingAsyncCursor(psycopg.AsyncCursor[DictRow]):
    async def execute(self, query: Any, params: Any = None, **kwargs: Any) -> Any:
        started = time.perf_counter()
        try:
            return await super().execute(query, params, **kwargs)
        finally:
            record_span("db", sql_span_name(query), time.perf_counter() - started)

    async def executemany(self, query: Any, params_seq: Any = None, **kwargs: Any) -> Any:
        started = time.perf_counter()
        try:
            return await super().executemany(query, params_seq, **kwargs)
        finally:
            record_span("db", sql_span_name(query), time.perf_counter() - started)
