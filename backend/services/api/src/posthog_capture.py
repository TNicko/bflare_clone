"""PostHog capture for API calls. Failures are logged and never fail the request."""

from __future__ import annotations

import logging
from typing import Any

import httpx

from services.api.src.config import ConfigFactory

lg = logging.getLogger(__name__)


async def capture_api_search(
    *,
    distinct_id: str,
    route: str,
    meter: str,
    credits: int,
    plan_id: str | None,
    live: bool,
    status: str,
    error: str | None,
    query_hash: str | None,
    channel: str = "api",
) -> None:
    cfg = ConfigFactory.get_config()
    token = cfg.posthog_project_token.strip()
    if not token:
        return
    host = cfg.posthog_host.rstrip("/") or "https://us.i.posthog.com"
    payload: dict[str, Any] = {
        "api_key": token,
        "event": "api_search",
        "distinct_id": distinct_id,
        "properties": {
            "route": route,
            "meter": meter,
            "credits": credits,
            "plan_id": plan_id,
            "live": live,
            "status": status,
            "error": error,
            "query_hash": query_hash,
            "channel": channel,
        },
    }
    try:
        async with httpx.AsyncClient(timeout=2.0) as client:
            response = await client.post(f"{host}/capture/", json=payload)
            response.raise_for_status()
    except Exception:
        lg.exception("PostHog api_search capture failed")
