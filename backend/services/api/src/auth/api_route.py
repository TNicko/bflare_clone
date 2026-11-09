"""Which credential a request path accepts."""

from __future__ import annotations


def is_hidden_public_route(path: str) -> bool:
    """Live Telegram name search stays on the website. It is not a public API route."""
    return path.rstrip("/") == "/v1/name/telegram"


def requires_api_key(path: str) -> bool:
    """API keys are the only credential on ``/v1`` and ``/mcp``."""
    normalized = path.rstrip("/") or "/"
    return (
        normalized == "/v1" or normalized.startswith("/v1/") or normalized == "/mcp" or normalized.startswith("/mcp/")
    )
