"""Call this process's ``/v1`` routes with the caller's API key.

MCP tools use this so credit metering, live Telegram, and auth stay on the
existing search handlers. The usage channel is set here, not from a header.
"""

from __future__ import annotations

import logging
from typing import Any

import httpx

from services.api.src.auth.channel import use_usage_channel
from utils.request_spans import normalize_request_id

lg = logging.getLogger(__name__)

_app: Any = None

#: Query keys safe to echo in access-style MCP logs (no secrets / large bodies).
_LOG_QUERY_KEYS = ("q", "query", "scope", "kind", "limit", "offset", "live", "peer_type")


def bind_asgi_app(app: Any) -> None:
    global _app
    _app = app


def _params(values: dict[str, Any]) -> dict[str, str]:
    return {key: str(value) for key, value in values.items() if value is not None}


def _json(body: dict[str, Any] | None) -> dict[str, Any] | None:
    if body is None:
        return None
    return {key: value for key, value in body.items() if value is not None}


def _log_detail(
    *,
    params: dict[str, Any] | None,
    json_body: dict[str, Any] | None,
    files: dict[str, tuple[str, bytes, str]] | None,
) -> str:
    bits: list[str] = []
    merged: dict[str, Any] = {}
    if params:
        merged.update(params)
    if json_body:
        merged.update(json_body)
    for key in _LOG_QUERY_KEYS:
        if key not in merged or merged[key] is None:
            continue
        value = str(merged[key]).replace("\n", " ").strip()
        if len(value) > 80:
            value = value[:77] + "..."
        bits.append(f"{key}={value!r}")
    if files:
        bits.append(f"files={','.join(sorted(files))}")
    return " ".join(bits)


async def call_v1(
    *,
    authorization: str,
    method: str,
    path: str,
    params: dict[str, Any] | None = None,
    json_body: dict[str, Any] | None = None,
    files: dict[str, tuple[str, bytes, str]] | None = None,
) -> tuple[int, Any]:
    if _app is None:
        raise RuntimeError("API app is not bound for in-process /v1 calls")
    request_id = normalize_request_id(None)
    headers = {
        "authorization": authorization,
        "x-request-id": request_id,
    }
    detail = _log_detail(params=params, json_body=json_body, files=files)
    # httpx>=0.28 ASGITransport has no ``lifespan`` kwarg (that was never a
    # stable public option here). Default transport does not start app lifespan.
    # Wall-clock time is the request timing summary on the inner /v1 call.
    transport = httpx.ASGITransport(app=_app)
    try:
        async with httpx.AsyncClient(transport=transport, base_url="http://internal") as client:
            with use_usage_channel("mcp"):
                response = await client.request(
                    method,
                    path,
                    params=_params(params or {}),
                    json=_json(json_body),
                    files=files,
                    headers=headers,
                )
    except Exception:
        lg.exception(
            "MCP %s %s failed request_id=%s %s",
            method,
            path,
            request_id,
            detail,
        )
        raise
    lg.info(
        "MCP %s %s status=%s request_id=%s %s",
        method,
        path,
        response.status_code,
        request_id,
        detail,
    )
    try:
        body: Any = response.json()
    except Exception:
        body = {"detail": response.text}
    return response.status_code, body
