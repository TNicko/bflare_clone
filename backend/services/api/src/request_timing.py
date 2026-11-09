"""Pure ASGI middleware that logs one timing summary per HTTP request.

Not ``BaseHTTPMiddleware``: that copies the context and would hide spans recorded
in the handler. Registered inside CORS and outside auth so API-key lookups are
part of the same trace, and CORS still stamps ACAO on the way out.
"""

from __future__ import annotations

import logging
import time

from starlette.types import ASGIApp, Message, Receive, Scope, Send

from utils.request_spans import (
    finish_request,
    format_request_summary,
    request_id_from_headers,
    slow_trace_ms,
    start_request,
)

lg = logging.getLogger(__name__)

_SKIP_PATHS = frozenset({"/health", "/health/telegram", "/metrics"})


class RequestTimingMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        method = str(scope.get("method") or "GET")
        path = str(scope.get("path") or "")
        if method == "OPTIONS" or path in _SKIP_PATHS:
            await self.app(scope, receive, send)
            return

        request_id = request_id_from_headers(scope.get("headers"))
        state = scope.setdefault("state", {})
        if isinstance(state, dict):
            state["request_id"] = request_id

        token = start_request(request_id)
        status = 500
        started = time.perf_counter()

        async def send_timed(message: Message) -> None:
            nonlocal status
            if message["type"] == "http.response.start":
                status = int(message["status"])
                headers = list(message.get("headers") or [])
                if not any(key.lower() == b"x-request-id" for key, _ in headers):
                    headers.append((b"x-request-id", request_id.encode("latin-1")))
                message = {**message, "headers": headers}
            await send(message)

        try:
            await self.app(scope, receive, send_timed)
        finally:
            total_ms = (time.perf_counter() - started) * 1000.0
            trace = finish_request(token)
            try:
                spans = trace.spans if trace is not None else []
                line = format_request_summary(
                    method=method,
                    route=_route_template(scope, path),
                    status=status,
                    request_id=request_id,
                    total_ms=total_ms,
                    spans=spans,
                    include_spans=total_ms >= slow_trace_ms(),
                )
                lg.info("%s", line)
            except Exception:
                lg.exception("request timing log failed")


def _route_template(scope: Scope, path: str) -> str:
    route = scope.get("route")
    template = getattr(route, "path", None)
    if isinstance(template, str) and template:
        return template
    return path or "/"
