"""MCP channel labeling and the image URL guard. No network."""

from __future__ import annotations

import asyncio

import pytest

from mcp.server.transport_security import TransportSecurityMiddleware

from services.api.src.auth.channel import current_usage_channel, is_billed_channel, use_usage_channel
from services.api.src.mcp_server import MCP_TRANSPORT_SECURITY, _authorization, mcp
from services.api.src.safe_image_fetch import SafeImageError, resolve_public_ips, validate_image_url


class _Headers:
    def __init__(self, value: str | None) -> None:
        self._value = value

    @property
    def headers(self):
        if self._value is None:
            return None
        return {"Authorization": self._value}


def test_billed_channels_are_api_and_mcp() -> None:
    assert is_billed_channel("api") is True
    assert is_billed_channel("mcp") is True
    assert is_billed_channel("app") is False
    assert current_usage_channel("api") == "api"
    with use_usage_channel("mcp"):
        assert current_usage_channel("api") == "mcp"
    assert current_usage_channel("api") == "api"


def test_mcp_tools_match_public_search() -> None:
    tools = asyncio.run(mcp.list_tools())
    names = {tool.name for tool in tools}
    assert names == {
        "search_name",
        "search_bluesky",
        "search_username",
        "search_keyword",
        "search_semantic",
        "search_phone",
        "search_image",
        "search_image_copies",
    }
    for tool in tools:
        assert tool.annotations is not None
        assert tool.annotations.read_only_hint is True
        assert tool.annotations.destructive_hint is False
        assert tool.annotations.model_dump(by_alias=True)["readOnlyHint"] is True


def test_mcp_transport_allows_local_and_public_hosts() -> None:
    guard = TransportSecurityMiddleware(MCP_TRANSPORT_SECURITY)
    assert guard._validate_host("127.0.0.1:8000") is True
    assert guard._validate_host("localhost:8000") is True
    assert guard._validate_host("api.blackflare.io") is True
    assert guard._validate_host("evil.example") is False
    assert guard._validate_origin(None) is True
    assert guard._validate_origin("https://app.blackflare.io") is True
    assert guard._validate_origin("https://evil.example") is False


def test_authorization_is_normalized() -> None:
    assert _authorization(_Headers(None)) is None  # type: ignore[arg-type]
    assert _authorization(_Headers("Bearer bf_live_abc")) == "Bearer bf_live_abc"
    assert _authorization(_Headers("bearer bf_live_abc")) == "Bearer bf_live_abc"
    assert _authorization(_Headers("Basic abc")) is None


@pytest.mark.parametrize(
    "url",
    [
        "http://example.com/a.jpg",
        "https://user:pw@example.com/a.jpg",
        "https://example.com:8443/a.jpg",
        "https://127.0.0.1/a.jpg",
        "https://10.1.2.3/a.jpg",
        "https://169.254.169.254/latest",
        "https://localhost/a.jpg",
        "https://metadata.google.internal/a.jpg",
        "https://example.local/a.jpg",
        "https://[::1]/a.jpg",
        "https://[::ffff:127.0.0.1]/a.jpg",
        "https://0.0.0.0/a.jpg",
    ],
)
def test_image_url_rejects_non_public_targets(url: str) -> None:
    with pytest.raises(SafeImageError):
        validate_image_url(url)


def test_image_url_accepts_public_https() -> None:
    url, host = validate_image_url("https://cdn.example.com/photo.jpg")
    assert url == "https://cdn.example.com/photo.jpg"
    assert host == "cdn.example.com"
    url, host = validate_image_url("https://8.8.8.8/photo.jpg")
    assert host == "8.8.8.8"


def test_blocked_names_are_not_resolved() -> None:
    with pytest.raises(SafeImageError):
        resolve_public_ips("localhost")
    with pytest.raises(SafeImageError):
        resolve_public_ips("metadata.google.internal")
