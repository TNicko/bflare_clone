"""MCP Streamable HTTP tools for the public search API.

Tools call the existing ``/v1`` routes in this process, so API-key auth, credit
weights, and live Telegram billing stay on that path. The bearer token is taken
from the MCP request and is not a tool argument.
"""

from __future__ import annotations

from typing import Any, Literal

from mcp.server import MCPServer
from mcp.server.mcpserver import Context
from mcp.server.transport_security import TransportSecuritySettings
from mcp.types import ToolAnnotations

from services.api.src.safe_image_fetch import SafeImageError, fetch_public_image
from services.api.src.v1_internal import bind_asgi_app, call_v1

mcp = MCPServer(
    name="Blackflare",
    instructions=(
        "Search indexed Telegram and Bluesky data with the caller's Blackflare API key. "
        "Credits are spent on the key sent as Authorization: Bearer. "
        "A response with status 402 means the key is out of credits and the search did not run."
    ),
)

# Cursor (and other clients) treat tools as "writes" unless readOnlyHint is set.
# These tools only search; credit metering is billing, not a data mutation.
_SEARCH_ANNOTATIONS = ToolAnnotations(
    readOnlyHint=True,
    destructiveHint=False,
    openWorldHint=True,
)

# Host is the header the client sends, not the bind address. Cursor usually omits Origin.
MCP_TRANSPORT_SECURITY = TransportSecuritySettings(
    enable_dns_rebinding_protection=True,
    allowed_hosts=[
        "localhost",
        "localhost:*",
        "127.0.0.1",
        "127.0.0.1:*",
        "[::1]",
        "[::1]:*",
        "api.blackflare.io",
        "api.blackflare.io:*",
    ],
    allowed_origins=[
        "http://localhost:*",
        "http://127.0.0.1:*",
        "http://[::1]:*",
        "https://api.blackflare.io",
        "https://app.blackflare.io",
    ],
)

NameScope = Literal["all", "community", "user"]
PeerType = Literal["channel", "group"]


def mount_mcp(app: Any) -> None:
    """Serve MCP at ``/mcp`` on the API app. Call once, before the app starts."""
    bind_asgi_app(app)
    app.mount(
        "/",
        mcp.streamable_http_app(
            stateless_http=True,
            json_response=True,
            transport_security=MCP_TRANSPORT_SECURITY,
        ),
    )


def _authorization(ctx: Context) -> str | None:
    headers = ctx.headers
    if not headers:
        return None
    for key, value in headers.items():
        if key.lower() != "authorization":
            continue
        parts = value.split(None, 1)
        if len(parts) == 2 and parts[0].lower() == "bearer" and parts[1].strip():
            return f"Bearer {parts[1].strip()}"
    return None


async def _search(
    ctx: Context,
    method: str,
    path: str,
    *,
    params: dict[str, Any] | None = None,
    json_body: dict[str, Any] | None = None,
    files: dict[str, tuple[str, bytes, str]] | None = None,
) -> dict[str, Any]:
    authorization = _authorization(ctx)
    if authorization is None:
        return {
            "ok": False,
            "status": 401,
            "error": {"code": "missing_api_key", "detail": "Send Authorization: Bearer with a Blackflare API key."},
        }
    status, body = await call_v1(
        authorization=authorization,
        method=method,
        path=path,
        params=params,
        json_body=json_body,
        files=files,
    )
    if status >= 400:
        return {"ok": False, "status": status, "error": body}
    return {"ok": True, "status": status, "result": body}


async def _image_file(image_url: str) -> tuple[dict[str, tuple[str, bytes, str]] | None, dict[str, Any] | None]:
    try:
        data, content_type, filename = await fetch_public_image(image_url)
    except SafeImageError as exc:
        return None, {"ok": False, "status": 400, "error": {"code": "invalid_image_url", "detail": str(exc)}}
    return {"image": (filename, data, content_type)}, None


@mcp.tool(structured_output=False, annotations=_SEARCH_ANNOTATIONS)
async def search_name(
    q: str,
    ctx: Context,
    scope: NameScope = "all",
    kind: NameScope = "all",
    limit: int = 10,
    offset: int = 0,
    live: bool = False,
) -> dict[str, Any]:
    """Search communities and users by name. Costs 1 credit.

    live=true also runs a Telegram community lookup when one is needed and adds 3 credits.
    That lookup is available on Pro and internal keys. A saved result of the same query does not add the 3.
    """
    return await _search(
        ctx,
        "GET",
        "/v1/name",
        params={"q": q, "scope": scope, "kind": kind, "limit": limit, "offset": offset, "live": live},
    )


@mcp.tool(structured_output=False, annotations=_SEARCH_ANNOTATIONS)
async def search_bluesky(
    q: str,
    ctx: Context,
    limit: int = 10,
    offset: int = 0,
    live: bool = False,
) -> dict[str, Any]:
    """Beta. Search Bluesky actors by name. Costs 1 credit.

    Coverage is still partial while this index is expanded. live=true does not change the cost.
    """
    return await _search(
        ctx,
        "GET",
        "/v1/name/bluesky",
        params={"q": q, "limit": limit, "offset": offset, "live": live},
    )


@mcp.tool(structured_output=False, annotations=_SEARCH_ANNOTATIONS)
async def search_username(q: str, ctx: Context) -> dict[str, Any]:
    """Look up a Telegram username. Costs 1 credit."""
    return await _search(ctx, "GET", "/v1/username", params={"q": q})


@mcp.tool(structured_output=False, annotations=_SEARCH_ANNOTATIONS)
async def search_keyword(
    q: str,
    ctx: Context,
    limit: int = 10,
    offset: int = 0,
    date_from: str | None = None,
    date_to: str | None = None,
    peer_type: PeerType | None = None,
) -> dict[str, Any]:
    """Search message text. Costs 1 credit. peer_type is channel or group.

    date_from and date_to accept 2024-08-15, 15 Aug 2024, 2024-08-15T14:30:00Z, or a unix timestamp. A date with no time starts at midnight UTC for date_from and includes that whole UTC day for date_to.
    """
    return await _search(
        ctx,
        "GET",
        "/v1/content/keyword",
        params={
            "q": q,
            "limit": limit,
            "offset": offset,
            "date_from": date_from,
            "date_to": date_to,
            "peer_type": peer_type,
        },
    )


@mcp.tool(structured_output=False, annotations=_SEARCH_ANNOTATIONS)
async def search_semantic(
    query: str,
    ctx: Context,
    limit: int = 10,
    offset: int = 0,
    date_from: str | None = None,
    date_to: str | None = None,
) -> dict[str, Any]:
    """Beta. Search messages by meaning. Costs 2 credits.

    Coverage is still partial while this index is expanded. date_from and date_to accept 2024-08-15, 15 Aug 2024, 2024-08-15T14:30:00Z, or a unix timestamp. A date with no time starts at midnight UTC for date_from and includes that whole UTC day for date_to.
    """
    return await _search(
        ctx,
        "POST",
        "/v1/content/semantic",
        json_body={
            "query": query,
            "limit": limit,
            "offset": offset,
            "date_from": date_from,
            "date_to": date_to,
        },
    )


@mcp.tool(structured_output=False, annotations=_SEARCH_ANNOTATIONS)
async def search_phone(q: str, ctx: Context) -> dict[str, Any]:
    """Look up a phone number. Costs 1 credit."""
    return await _search(ctx, "GET", "/v1/phone", params={"q": q})


@mcp.tool(structured_output=False, annotations=_SEARCH_ANNOTATIONS)
async def search_image(
    ctx: Context,
    image_url: str | None = None,
    search_id: str | None = None,
    limit: int = 50,
    offset: int = 0,
    date_from: str | None = None,
    date_to: str | None = None,
) -> dict[str, Any]:
    """Beta. Find messages with a visually similar image. A new search costs 2 credits.

    Coverage is still partial while this index is expanded. image_url is an HTTPS PNG or JPEG the caller can already fetch. date_from and date_to accept 2024-08-15, 15 Aug 2024, 2024-08-15T14:30:00Z, or a unix timestamp. A date with no time starts at midnight UTC for date_from and includes that whole UTC day for date_to. Pass the returned search_id with offset to read another page of the same search. That page costs 0 credits and does not upload the image again. The id lasts 20 minutes.
    """
    if search_id and search_id.strip():
        if "/" in search_id:
            return {
                "ok": False,
                "status": 400,
                "error": {"code": "invalid_search_id", "detail": "search_id is not valid"},
            }
        return await _search(
            ctx,
            "POST",
            "/v1/image/similarity",
            params={"search_id": search_id, "limit": limit, "offset": offset},
        )
    if not image_url:
        return {"ok": False, "status": 400, "error": {"detail": "image_url is required for a new search"}}
    files, error = await _image_file(image_url)
    if error is not None or files is None:
        return error or {"ok": False, "status": 400, "error": {"detail": "image URL was rejected"}}
    return await _search(
        ctx,
        "POST",
        "/v1/image/similarity",
        params={"limit": limit, "offset": offset, "date_from": date_from, "date_to": date_to},
        files=files,
    )


@mcp.tool(structured_output=False, annotations=_SEARCH_ANNOTATIONS)
async def search_image_copies(
    ctx: Context,
    image_url: str | None = None,
    message_id: str | None = None,
    date: str | None = None,
    phash: str | None = None,
    search_id: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> dict[str, Any]:
    """Beta. Find near-duplicate copies of an image. A new search costs 2 credits.

    Coverage is still partial while this index is expanded. Provide one of image_url, message_id with date, or phash. image_url is an HTTPS PNG or JPEG. message_id is a harvested message and date is its time, such as 2024-08-15T14:30:00Z or 15 Aug 2024 14:30. phash is 16 hex characters. Pass the returned search_id with offset to read another page. That page costs 0 credits.
    """
    if search_id and search_id.strip():
        if "/" in search_id:
            return {
                "ok": False,
                "status": 400,
                "error": {"code": "invalid_search_id", "detail": "search_id is not valid"},
            }
        return await _search(
            ctx,
            "POST",
            "/v1/image/copies",
            params={"search_id": search_id, "limit": limit, "offset": offset},
        )
    has_message = bool(message_id or date)
    has_phash = bool(phash and phash.strip())
    has_url = bool(image_url and image_url.strip())
    if sum((has_message, has_phash, has_url)) != 1:
        return {
            "ok": False,
            "status": 400,
            "error": {"detail": "Provide one of image_url, message_id with date, or phash."},
        }
    if has_message:
        if not message_id or not date:
            return {
                "ok": False,
                "status": 400,
                "error": {"detail": "message_id and date are required together."},
            }
        return await _search(
            ctx,
            "POST",
            "/v1/image/copies",
            params={"message_id": message_id, "date": date, "limit": limit, "offset": offset},
        )
    if has_phash:
        return await _search(
            ctx,
            "POST",
            "/v1/image/copies",
            params={"phash": phash, "limit": limit, "offset": offset},
        )
    files, error = await _image_file(image_url or "")
    if error is not None or files is None:
        return error or {"ok": False, "status": 400, "error": {"detail": "image URL was rejected"}}
    return await _search(
        ctx,
        "POST",
        "/v1/image/copies",
        params={"limit": limit, "offset": offset},
        files=files,
    )
