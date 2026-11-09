"""Parse a public ``t.me/s/{username}`` preview page.

The profile page (``t.me/{username}``) only carries the username. The preview
page repeats a base64 JSON blob on each post (``data-view``) whose ``c`` field
is the raw channel id. That id is the stable peer key.
"""

from __future__ import annotations

import base64
import json
import re
from collections import Counter
from dataclasses import dataclass
from html import unescape
from typing import Literal

from telegram.utils import signed_peer_id_from_raw_channel_id

_DATA_VIEW_RE = re.compile(r'data-view="([A-Za-z0-9+/=_-]+)"')
_COUNTER_RE = re.compile(
    r'class="counter_value">([^<]+)</span>\s*<span class="counter_type">([^<]+)</span>',
    re.IGNORECASE,
)
_TITLE_RE = re.compile(
    r'class="tgme_channel_info_header_title"[^>]*>\s*<span[^>]*>(.*?)</span>',
    re.IGNORECASE | re.DOTALL,
)
_ABOUT_RE = re.compile(
    r'class="tgme_channel_info_description"[^>]*>(.*?)</div>',
    re.IGNORECASE | re.DOTALL,
)
_INFO_BLOCK_RE = re.compile(
    r'class="tgme_channel_info"(.*?)class="tgme_channel_info_counters"',
    re.IGNORECASE | re.DOTALL,
)
_PHOTO_RE = re.compile(r'<img[^>]+src="(https://cdn\d*\.telesco\.pe/[^"]+)"', re.IGNORECASE)
_MESSAGE_TEXT_RE = re.compile(
    r'<div[^>]*class="[^"]*tgme_widget_message_text[^"]*"[^>]*>(.*?)</div>',
    re.IGNORECASE | re.DOTALL,
)
_BR_RE = re.compile(r"<br\s*/?>", re.IGNORECASE)
_TAG_RE = re.compile(r"<[^>]+>")
_COMPACT_COUNT_RE = re.compile(r"^(\d+(?:\.\d+)?)([KMB])?$", re.IGNORECASE)

CommunityPeerType = Literal["channel", "megagroup"]


@dataclass(frozen=True, slots=True)
class PreviewPage:
    is_telegram_page: bool
    raw_channel_id: int | None = None
    external_id: str | None = None
    peer_type: CommunityPeerType | None = None
    title: str | None = None
    about: str | None = None
    photo_url: str | None = None
    participants_count: int | None = None
    online_count: int | None = None
    photos_count: int | None = None
    videos_count: int | None = None
    files_count: int | None = None
    links_count: int | None = None
    is_verified: bool | None = None
    is_scam: bool | None = None


def parse_compact_count(value: str) -> int | None:
    """Parse ``82``, ``59.5K``, ``1.2M``, and spaced ``59 524``."""
    text = value.strip().replace("\xa0", "").replace(" ", "").replace(",", "")
    match = _COMPACT_COUNT_RE.fullmatch(text)
    if match is None:
        return None
    number = float(match.group(1))
    suffix = (match.group(2) or "").upper()
    if suffix == "K":
        number *= 1_000
    elif suffix == "M":
        number *= 1_000_000
    elif suffix == "B":
        number *= 1_000_000_000
    return int(round(number))


def parse_preview_html(html: str) -> PreviewPage:
    """Extract the channel id and whatever public profile fields the preview has."""
    is_telegram_page = "tgme_" in html
    raw_channel_id = _raw_channel_id(html)
    external_id = str(signed_peer_id_from_raw_channel_id(raw_channel_id)) if raw_channel_id is not None else None
    counters = _counters(html)
    peer_type = _peer_type(counters)
    info_html = _info_block(html)
    verified = _class_flag(info_html, "verified")
    scam = _class_flag(info_html, "scam")
    return PreviewPage(
        is_telegram_page=is_telegram_page,
        raw_channel_id=raw_channel_id,
        external_id=external_id,
        peer_type=peer_type,
        title=_clean(_first(_TITLE_RE, html)),
        about=_clean(_first(_ABOUT_RE, html)),
        photo_url=_first(_PHOTO_RE, info_html) if info_html else None,
        participants_count=_participants(counters),
        online_count=counters.get("online"),
        photos_count=counters.get("photos") or counters.get("photo"),
        videos_count=counters.get("videos") or counters.get("video"),
        files_count=counters.get("files") or counters.get("file"),
        links_count=counters.get("links") or counters.get("link"),
        is_verified=True if verified else None,
        is_scam=True if scam else None,
    )


def parse_preview_message_texts(html: str, *, limit: int = 40) -> list[str]:
    """Extract recent post bodies from a ``t.me/s/{username}`` preview page.

    Used for ephemeral language labeling — not persisted as harvested messages.
    """
    if limit <= 0:
        return []
    out: list[str] = []
    seen: set[str] = set()
    for match in _MESSAGE_TEXT_RE.finditer(html or ""):
        text = _clean_message_html(match.group(1))
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
        if len(out) >= limit:
            break
    return out


def _clean_message_html(value: str) -> str | None:
    text = _BR_RE.sub("\n", value or "")
    text = unescape(_TAG_RE.sub(" ", text))
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    return text or None


def _raw_channel_id(html: str) -> int | None:
    counts: Counter[int] = Counter()
    for match in _DATA_VIEW_RE.finditer(html):
        channel_id = _channel_id_from_data_view(match.group(1))
        if channel_id is not None:
            counts[channel_id] += 1
    if not counts:
        return None
    return counts.most_common(1)[0][0]


def _channel_id_from_data_view(token: str) -> int | None:
    padded = token + ("=" * ((4 - len(token) % 4) % 4))
    try:
        payload = json.loads(base64.b64decode(padded))
    except (ValueError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    raw = payload.get("c")
    if isinstance(raw, bool) or not isinstance(raw, int) or raw == 0:
        return None
    return abs(raw)


def _counters(html: str) -> dict[str, int]:
    out: dict[str, int] = {}
    for match in _COUNTER_RE.finditer(html):
        count = parse_compact_count(match.group(1))
        label = unescape(match.group(2)).strip().lower()
        if count is None or not label:
            continue
        out[label] = count
    return out


def _peer_type(counters: dict[str, int]) -> CommunityPeerType | None:
    if "subscribers" in counters or "subscriber" in counters:
        return "channel"
    if "members" in counters or "member" in counters:
        return "megagroup"
    return None


def _participants(counters: dict[str, int]) -> int | None:
    for key in ("subscribers", "subscriber", "members", "member"):
        if key in counters:
            return counters[key]
    return None


def _info_block(html: str) -> str:
    match = _INFO_BLOCK_RE.search(html)
    return match.group(1) if match else ""


def _class_flag(info_html: str, needle: str) -> bool:
    """Match a marker only when it is a CSS class, not prose in the description."""
    return re.search(rf'class="[^"]*{re.escape(needle)}[^"]*"', info_html, re.IGNORECASE) is not None


def _first(pattern: re.Pattern[str], text: str) -> str | None:
    match = pattern.search(text)
    if match is None:
        return None
    return match.group(1)


def _clean(value: str | None) -> str | None:
    if value is None:
        return None
    text = unescape(_TAG_RE.sub(" ", value))
    text = re.sub(r"\s+", " ", text).strip()
    return text or None
