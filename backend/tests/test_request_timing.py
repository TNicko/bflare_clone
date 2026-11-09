"""Request timing: span collection, SQL names, and the ASGI summary log."""

from __future__ import annotations

import asyncio
import logging

import psycopg
import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from services.api.src.crud.supabase.tracing import TracingAsyncCursor
from services.api.src.request_timing import RequestTimingMiddleware
from services.api.src.stripe_client import _StripeProxy
from utils.request_spans import (
    Span,
    current_trace,
    finish_request,
    format_request_summary,
    normalize_request_id,
    record_span,
    sql_span_name,
    start_request,
    timed,
)


def test_normalize_request_id_rejects_spaces_and_keeps_valid_ids() -> None:
    assert normalize_request_id("  trace-1  ") == "trace-1"
    minted = normalize_request_id("has space")
    assert minted != "has space"
    assert " " not in minted
    assert normalize_request_id(None) != ""


def test_sql_span_name_redacts_literals_and_keeps_placeholders() -> None:
    name = sql_span_name("SELECT  'secret phone'  FROM t WHERE id = %s")
    assert name == "SELECT ? FROM t WHERE id = %s"
    assert "secret" not in name


def test_record_span_outside_a_request_is_ignored() -> None:
    record_span("db", "connect", 0.05)
    assert current_trace() is None


def test_nested_async_span_is_recorded_only_inside_a_request() -> None:
    async def _run() -> None:
        async def inner() -> None:
            async with timed("embed", "text"):
                await asyncio.sleep(0)

        async with timed("s3", "presign"):
            await inner()

    asyncio.run(_run())
    assert current_trace() is None

    async def _inside() -> None:
        token = start_request("req-nested")
        try:

            async def inner() -> None:
                async with timed("embed", "text"):
                    await asyncio.sleep(0)

            await inner()
        finally:
            trace = finish_request(token)
        assert trace is not None
        assert [span.name for span in trace.spans] == ["text"]
        assert current_trace() is None

    asyncio.run(_inside())


def test_cursor_execute_records_statement_not_parameters(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict[str, object] = {}

    async def fake_execute(self: object, query: object, params: object = None, **kwargs: object) -> str:
        seen["params"] = params
        seen["kwargs"] = kwargs
        return "ok"

    monkeypatch.setattr(psycopg.AsyncCursor, "execute", fake_execute)

    async def _run() -> None:
        cur = TracingAsyncCursor.__new__(TracingAsyncCursor)
        await TracingAsyncCursor.execute(
            cur,
            "SELECT id FROM t WHERE phone = %s AND note = 'keep-out'",
            ("+15551212",),
            prepare=False,
        )
        assert seen["params"] == ("+15551212",)
        assert seen["kwargs"] == {"prepare": False}
        assert current_trace() is None

        token = start_request("req-db")
        try:
            await TracingAsyncCursor.execute(
                cur,
                "SELECT id FROM t WHERE phone = %s AND note = 'keep-out'",
                ("+15551212",),
            )
        finally:
            trace = finish_request(token)
        assert trace is not None
        assert len(trace.spans) == 1
        span = trace.spans[0]
        assert span.kind == "db"
        assert span.name == "SELECT id FROM t WHERE phone = %s AND note = ?"
        assert "+15551212" not in span.name
        assert "keep-out" not in span.name

        token = start_request("req-fail")
        try:
            with pytest.raises(RuntimeError, match="db down"):
                await _failing_execute(cur)
        finally:
            failed = finish_request(token)
        assert failed is not None
        assert len(failed.spans) == 1
        assert current_trace() is None

    async def _failing_execute(cur: TracingAsyncCursor) -> None:
        async def boom(self: object, query: object, params: object = None, **kwargs: object) -> None:
            raise RuntimeError("db down")

        monkeypatch.setattr(psycopg.AsyncCursor, "execute", boom)
        await TracingAsyncCursor.execute(cur, "SELECT 1", None)

    asyncio.run(_run())


def test_stripe_proxy_times_the_sdk_method() -> None:
    class _Sessions:
        def create(self, params: dict[str, str]) -> str:
            return params["id"]

    class _Checkout:
        sessions = _Sessions()

    class _Client:
        checkout = _Checkout()

    proxy = _StripeProxy(_Client(), "")
    token = start_request("stripe-1")
    try:
        assert proxy.checkout.sessions.create({"id": "cs"}) == "cs"
    finally:
        trace = finish_request(token)
    assert trace is not None
    assert [(span.kind, span.name) for span in trace.spans] == [("stripe", "checkout.sessions.create")]
    assert current_trace() is None


def test_summary_totals_slowest_and_span_cutoff() -> None:
    spans = [
        Span(kind="db", name="SELECT id FROM peers", start_ms=1, dur_ms=20),
        Span(kind="db", name="SELECT note FROM t", start_ms=30, dur_ms=50),
        Span(kind="telegram", name="acquire_any_client", start_ms=0, dur_ms=40),
    ]
    line = format_request_summary(
        method="GET",
        route="/telegram/peers/{external_id}",
        status=200,
        request_id="trace-1",
        total_ms=80,
        spans=spans,
        include_spans=False,
    )
    assert "method=GET" in line
    assert "route=/telegram/peers/{external_id}" in line
    assert "status=200" in line
    assert "request_id=trace-1" in line
    assert "total_ms=80" in line
    assert "db_ms=70" in line
    assert "db_n=2" in line
    assert "telegram_ms=40" in line
    assert "telegram_n=1" in line
    assert "s3_ms=0" in line
    assert "embed_n=0" in line
    assert "slowest=db:SELECT_note_FROM_t" in line
    assert "slowest_ms=50" in line
    assert "spans=" not in line

    detailed = format_request_summary(
        method="GET",
        route="/telegram/peers/{external_id}",
        status=200,
        request_id="trace-1",
        total_ms=80,
        spans=spans,
        include_spans=True,
    )
    assert (
        'spans="db:SELECT id FROM peers@1+20,db:SELECT note FROM t@30+50,telegram:acquire_any_client@0+40"' in detailed
    )

    long_spans = [Span(kind="db", name="SELECT " + ("col " * 30), start_ms=i, dur_ms=1) for i in range(45)]
    capped = format_request_summary(
        method="GET",
        route="/x",
        status=200,
        request_id="trace-2",
        total_ms=45,
        spans=long_spans,
        include_spans=True,
    )
    assert 'spans="' in capped
    assert capped.endswith('..."') or ",..." in capped.split('spans="', 1)[1]
    timeline = capped.split('spans="', 1)[1].rstrip('"')
    assert len(timeline) <= 1200 + 4


def _app() -> FastAPI:
    app = FastAPI()
    app.add_middleware(RequestTimingMiddleware)

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/items/{item_id}")
    async def get_item(item_id: str) -> dict[str, str]:
        async with timed("db", "SELECT 1"):
            return {"id": item_id}

    @app.get("/missing")
    def missing() -> None:
        raise HTTPException(status_code=404, detail="nope")

    return app


def _request_lines(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [record.getMessage() for record in caplog.records if record.getMessage().startswith("request ")]


def test_middleware_logs_route_template_and_clears_context(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO, logger="services.api.src.request_timing")
    client = TestClient(_app())

    health = client.get("/health")
    assert health.status_code == 200
    assert _request_lines(caplog) == []

    options = client.options("/items/1")
    assert options.status_code in (200, 405)
    assert _request_lines(caplog) == []

    first = client.get("/items/abc", headers={"x-request-id": "trace-a"})
    assert first.status_code == 200
    assert first.headers["x-request-id"] == "trace-a"
    second = client.get("/items/def", headers={"x-request-id": "trace-b"})
    assert second.headers["x-request-id"] == "trace-b"
    assert current_trace() is None

    lines = _request_lines(caplog)
    assert len(lines) == 2
    assert "route=/items/{item_id}" in lines[0]
    assert "status=200" in lines[0]
    assert "request_id=trace-a" in lines[0]
    assert "total_ms=" in lines[0]
    assert "db_n=1" in lines[0]
    assert "db_ms=" in lines[0]
    assert "request_id=trace-b" in lines[1]
    assert "db_n=1" in lines[1]
    assert "trace-a" not in lines[1]

    rejected = client.get("/items/1", headers={"x-request-id": "has space"})
    echoed = rejected.headers["x-request-id"]
    assert echoed != "has space"
    assert " " not in echoed
    assert any(f"request_id={echoed}" in line for line in _request_lines(caplog))

    missing = client.get("/missing")
    assert missing.status_code == 404
    assert any("status=404" in line and "route=/missing" in line for line in _request_lines(caplog))
    assert current_trace() is None


def test_middleware_includes_spans_when_threshold_is_zero(
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("API_TRACE_SLOW_MS", "0")
    caplog.set_level(logging.INFO, logger="services.api.src.request_timing")
    client = TestClient(_app())
    response = client.get("/items/1", headers={"x-request-id": "trace-slow"})
    assert response.status_code == 200
    lines = _request_lines(caplog)
    assert len(lines) == 1
    assert "request_id=trace-slow" in lines[0]
    assert 'spans="' in lines[0]
    assert "db:SELECT 1@" in lines[0]
    assert current_trace() is None
