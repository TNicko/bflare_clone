"""Parse public ``t.me/s/{username}`` message widgets into harvestable records."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from html import unescape
from typing import Any

from telegram.tme.preview import parse_compact_count

_DATA_POST_RE = re.compile(
    r'<div class="tgme_widget_message[^"]*"\s+data-post="([^"]+)"\s+data-view="([^"]+)"',
    re.IGNORECASE,
)
_DATETIME_RE = re.compile(r'datetime="([^"]+)"', re.IGNORECASE)
_TEXT_RE = re.compile(
    r'<div[^>]*class="[^"]*tgme_widget_message_text[^"]*"[^>]*>(.*?)</div>',
    re.IGNORECASE | re.DOTALL,
)
_VIEWS_RE = re.compile(r'class="tgme_widget_message_views">([^<]+)', re.IGNORECASE)
_AUTHOR_RE = re.compile(r'class="tgme_widget_message_from_author"[^>]*>([^<]*)', re.IGNORECASE)
_FWD_USER_RE = re.compile(
    r'class="tgme_widget_message_forwarded_from"[^>]*>.*?'
    r'href="https?://t\.me/([A-Za-z0-9_]+)"',
    re.IGNORECASE | re.DOTALL,
)
_FWD_NAME_RE = re.compile(
    r'class="tgme_widget_message_forwarded_from"[^>]*>.*?<span[^>]*class="[^"]*tgme_widget_message_forwarded_from_name[^"]*"[^>]*>(.*?)</span>',
    re.IGNORECASE | re.DOTALL,
)
_PHOTO_URL_RE = re.compile(
    r'class="tgme_widget_message_photo_wrap"[^>]*style="[^"]*background-image:url\(\'([^\']+)\'\)',
    re.IGNORECASE,
)
_VIDEO_URL_RE = re.compile(
    r'<video[^>]+src="(https://[^"]+)"',
    re.IGNORECASE,
)
_LINK_PREVIEW_URL_RE = re.compile(
    r'class="tgme_widget_message_link_preview"[^>]*href="(https?://[^"]+)"',
    re.IGNORECASE,
)
_BR_RE = re.compile(r"<br\s*/?>", re.IGNORECASE)
_TAG_RE = re.compile(r"<[^>]+>")
_URL_IN_TEXT_RE = re.compile(r"https?://[^\s<>\"']+|t\.me/[A-Za-z0-9_/?=&%-]+", re.IGNORECASE)
_MENTION_RE = re.compile(r"(?<![A-Za-z0-9_])@([A-Za-z][A-Za-z0-9_]{3,})")


@dataclass(frozen=True, slots=True)
class ParsedTmeMessage:
    message_id: int
    date: datetime | None
    content: str | None
    views: int | None
    author_name: str | None
    fwd_username: str | None
    fwd_from_name: str | None
    has_photo: bool
    has_video: bool
    has_document: bool
    has_link_preview: bool
    photo_url: str | None
    video_url: str | None
    link_preview_url: str | None
    has_url: bool
    has_mention: bool


def parse_tme_s_messages(html: str) -> list[ParsedTmeMessage]:
    """Parse each ``tgme_widget_message`` block on a preview page."""
    out: list[ParsedTmeMessage] = []
    for match in _DATA_POST_RE.finditer(html or ""):
        data_post = match.group(1)
        start = match.start()
        next_m = _DATA_POST_RE.search(html, match.end())
        end = next_m.start() if next_m else min(len(html), start + 20000)
        block = html[start:end]
        try:
            message_id = int(data_post.rsplit("/", 1)[-1])
        except ValueError:
            continue

        text = None
        tm = _TEXT_RE.search(block)
        if tm:
            text = _clean_text(tm.group(1))

        views_raw = _VIEWS_RE.search(block)
        views = parse_compact_count(views_raw.group(1)) if views_raw else None
        author_m = _AUTHOR_RE.search(block)
        dt_m = _DATETIME_RE.search(block)
        date = _parse_datetime(dt_m.group(1)) if dt_m else None

        fwd_user_m = _FWD_USER_RE.search(block)
        fwd_name_m = _FWD_NAME_RE.search(block)
        fwd_username = (fwd_user_m.group(1).strip() if fwd_user_m else None) or None
        fwd_from_name = _clean_text(fwd_name_m.group(1)) if fwd_name_m else None

        has_photo = "tgme_widget_message_photo" in block
        has_video = (
            "tgme_widget_message_video" in block
            or "tgme_widget_message_video_player" in block
            or "tgme_widget_message_roundvideo" in block
        )
        has_document = "tgme_widget_message_document" in block
        has_link_preview = "tgme_widget_message_link_preview" in block

        photo_m = _PHOTO_URL_RE.search(block)
        video_m = _VIDEO_URL_RE.search(block)
        link_m = _LINK_PREVIEW_URL_RE.search(block)

        has_url = has_link_preview or bool(text and _URL_IN_TEXT_RE.search(text))
        has_mention = bool(text and _MENTION_RE.search(text))

        out.append(
            ParsedTmeMessage(
                message_id=message_id,
                date=date,
                content=text,
                views=views,
                author_name=(author_m.group(1).strip() if author_m else None) or None,
                fwd_username=fwd_username,
                fwd_from_name=fwd_from_name,
                has_photo=has_photo,
                has_video=has_video,
                has_document=has_document,
                has_link_preview=has_link_preview,
                photo_url=photo_m.group(1) if photo_m else None,
                video_url=video_m.group(1) if video_m else None,
                link_preview_url=link_m.group(1) if link_m else None,
                has_url=has_url,
                has_mention=has_mention,
            )
        )
    return out


def slim_media_from_parsed(msg: ParsedTmeMessage) -> dict[str, Any] | None:
    """Build a thin media JSON stub for inventory (no S3 harvest)."""
    if msg.has_photo:
        out: dict[str, Any] = {"tl": "MessageMediaPhoto"}
        if msg.photo_url:
            out["web_url"] = msg.photo_url
        return out
    if msg.has_video:
        out = {"tl": "MessageMediaDocument", "video": True, "mime_type": "video/mp4"}
        if msg.video_url:
            out["web_url"] = msg.video_url
        return out
    if msg.has_document:
        return {"tl": "MessageMediaDocument", "mime_type": "application/octet-stream"}
    if msg.has_link_preview:
        out = {"tl": "MessageMediaWebPage"}
        if msg.link_preview_url:
            out["url"] = msg.link_preview_url
        return out
    return None


def fwd_from_web(*, username: str | None, from_name: str | None) -> dict[str, Any] | None:
    if not username and not from_name:
        return None
    out: dict[str, Any] = {"source": "web"}
    if username:
        out["username"] = username.lstrip("@")
    if from_name:
        out["from_name"] = from_name
    return out


def _clean_text(html: str) -> str | None:
    text = _BR_RE.sub("\n", html or "")
    text = unescape(_TAG_RE.sub(" ", text))
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    return text or None


def _parse_datetime(raw: str) -> datetime | None:
    text = (raw or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None
