"""Download one HTTPS image for MCP tools, pinned to a public address.

The caller supplies a URL they already have. This fetch refuses private,
loopback, and link-local addresses, including after a redirect, so a tool
argument cannot be used to read internal services.
"""

from __future__ import annotations

import ipaddress
import socket
from urllib.parse import urljoin, urlsplit

import httpx

from services.api.src.models.search import ALLOWED_CONTENT_TYPES
from services.api.src.search_limits import IMAGE_UPLOAD_MAX_BYTES

_MAX_REDIRECTS = 3
_TIMEOUT = httpx.Timeout(15.0)
_BLOCKED_HOSTS = frozenset(
    {
        "localhost",
        "localhost.localdomain",
        "metadata.google.internal",
        "metadata.google",
    }
)
_MAGIC = (
    (b"\x89PNG\r\n\x1a\n", "image/png"),
    (b"\xff\xd8\xff", "image/jpeg"),
)


class SafeImageError(ValueError):
    """The URL or response is not an acceptable public image."""


def _blocked_name(host: str) -> bool:
    name = host.lower().rstrip(".")
    if name in _BLOCKED_HOSTS or name.endswith(".localhost") or name.endswith(".local"):
        return True
    return False


def _public_ip(address: str) -> bool:
    try:
        ip = ipaddress.ip_address(address)
    except ValueError:
        return False
    if getattr(ip, "ipv4_mapped", None) is not None:
        ip = ip.ipv4_mapped
    return not (
        ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_multicast or ip.is_reserved or ip.is_unspecified
    )


def resolve_public_ips(host: str) -> list[str]:
    """Return public A/AAAA addresses for ``host``. Raises ``SafeImageError``."""
    if _blocked_name(host):
        raise SafeImageError("image URL host is not allowed")
    try:
        infos = socket.getaddrinfo(host, 443, type=socket.SOCK_STREAM)
    except socket.gaierror as exc:
        raise SafeImageError("image URL host did not resolve") from exc
    ips: list[str] = []
    for info in infos:
        address = info[4][0]
        if not isinstance(address, str):
            continue
        if _public_ip(address) and address not in ips:
            ips.append(address)
    if not ips:
        raise SafeImageError("image URL does not resolve to a public address")
    return ips


def validate_image_url(url: str) -> tuple[str, str]:
    """Return ``(url, host)`` for an HTTPS URL on port 443 with no userinfo."""
    parsed = urlsplit(url.strip())
    if parsed.scheme != "https" or not parsed.hostname:
        raise SafeImageError("image URL must be https")
    if parsed.username or parsed.password:
        raise SafeImageError("image URL must not include credentials")
    if parsed.port not in (None, 443):
        raise SafeImageError("image URL must use port 443")
    host = parsed.hostname
    if _blocked_name(host):
        raise SafeImageError("image URL host is not allowed")
    try:
        ipaddress.ip_address(host)
    except ValueError:
        pass
    else:
        if not _public_ip(host):
            raise SafeImageError("image URL address is not public")
    return url.strip(), host


def _sniff(body: bytes, declared: str) -> str:
    for magic, content_type in _MAGIC:
        if body.startswith(magic):
            if declared != content_type:
                raise SafeImageError("image content type does not match the file")
            return content_type
    raise SafeImageError("image must be PNG or JPEG")


class _PinnedHTTPSTransport(httpx.AsyncBaseTransport):
    """Connect to a resolved public IP while keeping the hostname for TLS and Host."""

    def __init__(self, ip: str, hostname: str) -> None:
        self._ip = ip
        self._hostname = hostname
        self._inner = httpx.AsyncHTTPTransport()

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        request.headers["host"] = self._hostname
        request.extensions["sni_hostname"] = self._hostname
        request.url = request.url.copy_with(host=self._ip)
        return await self._inner.handle_async_request(request)

    async def aclose(self) -> None:
        await self._inner.aclose()


async def _read_limited(response: httpx.Response) -> bytes:
    chunks: list[bytes] = []
    size = 0
    async for chunk in response.aiter_bytes():
        size += len(chunk)
        if size > IMAGE_UPLOAD_MAX_BYTES:
            raise SafeImageError("image is larger than 10 MB")
        chunks.append(chunk)
    return b"".join(chunks)


def _pin_ip(ips: list[str]) -> str:
    for ip in ips:
        if ":" not in ip:
            return ip
    return ips[0]


async def fetch_public_image(url: str) -> tuple[bytes, str, str]:
    """Download one PNG or JPEG. Returns ``(bytes, content_type, filename)``.

    Redirects are followed manually, and each hop is resolved and checked again.
    The API key is never sent to the image host.
    """
    current, host = validate_image_url(url)
    for _hop in range(_MAX_REDIRECTS + 1):
        transport = _PinnedHTTPSTransport(_pin_ip(resolve_public_ips(host)), host)
        async with httpx.AsyncClient(
            transport=transport,
            timeout=_TIMEOUT,
            follow_redirects=False,
            headers={"user-agent": "BlackflareImageFetch/1.0"},
        ) as client:
            async with client.stream("GET", current) as response:
                if response.status_code in {301, 302, 303, 307, 308}:
                    location = response.headers.get("location")
                    if not location:
                        raise SafeImageError("image host redirected without a location")
                    current, host = validate_image_url(urljoin(current, location))
                    continue
                if response.status_code != 200:
                    raise SafeImageError("image host did not return the file")
                declared = response.headers.get("content-type", "").split(";", 1)[0].strip().lower()
                if declared not in ALLOWED_CONTENT_TYPES:
                    raise SafeImageError("image must be PNG or JPEG")
                body = await _read_limited(response)
        content_type = _sniff(body, declared)
        extension = "png" if content_type == "image/png" else "jpg"
        return body, content_type, f"image.{extension}"
    raise SafeImageError("image URL redirected too many times")
