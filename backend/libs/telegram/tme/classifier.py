from __future__ import annotations

import re
from html import unescape

from telegram.tme.models import PeerClassification, TmeProfile

_TITLE_RE = re.compile(r"<title>([^<]+)</title>", re.IGNORECASE)
_DISPLAY_TITLE_RE = re.compile(
    r'class="tgme_page_title"[^>]*>.*?<span[^>]*>(.*?)</span>',
    re.IGNORECASE | re.DOTALL,
)
_EXTRA_RE = re.compile(r'class="tgme_page_extra"[^>]*>([^<]+)<', re.IGNORECASE)
_DESCRIPTION_RE = re.compile(
    r'class="tgme_page_description"[^>]*>(.*?)</div>',
    re.IGNORECASE | re.DOTALL,
)
_PHOTO_RE = re.compile(
    r'class="tgme_page_photo_image"[^>]*\ssrc="([^"]+)"',
    re.IGNORECASE,
)
_SUBSCRIBERS_RE = re.compile(
    r"([\d\s.,]+)\s+subscribers?",
    re.IGNORECASE,
)
_MEMBERS_RE = re.compile(
    r"([\d\s.,]+)\s+members?",
    re.IGNORECASE,
)
_ONLINE_RE = re.compile(
    r"([\d\s.,]+)\s+online",
    re.IGNORECASE,
)
_TAG_RE = re.compile(r"<[^>]+>")


def _clean_html_text(value: str | None) -> str | None:
    if value is None:
        return None
    text = unescape(_TAG_RE.sub(" ", value))
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def _parse_count(match: re.Match[str] | None) -> int | None:
    if match is None:
        return None
    digits = re.sub(r"[^\d]", "", match.group(1))
    if not digits:
        return None
    return int(digits)


def _first_group(pattern: re.Pattern[str], text: str) -> str | None:
    match = pattern.search(text)
    if match is None:
        return None
    return match.group(1)


def parse_tme_html(username: str, html: str, *, http_status: int | None = None) -> TmeProfile:
    html_title = _clean_html_text(_first_group(_TITLE_RE, html))
    display_title = _clean_html_text(_first_group(_DISPLAY_TITLE_RE, html))
    extra_text = _clean_html_text(_first_group(_EXTRA_RE, html))
    about = _clean_html_text(_first_group(_DESCRIPTION_RE, html))
    photo_url = _first_group(_PHOTO_RE, html)
    has_preview_channel = "Preview channel" in html

    participants_count = _parse_count(_SUBSCRIBERS_RE.search(extra_text or ""))
    if participants_count is None:
        participants_count = _parse_count(_MEMBERS_RE.search(extra_text or ""))
    online_count = _parse_count(_ONLINE_RE.search(extra_text or ""))

    profile = TmeProfile(
        username=username,
        html_title=html_title,
        display_title=display_title,
        extra_text=extra_text,
        about=about,
        has_preview_channel=has_preview_channel,
        photo_url=photo_url,
        participants_count=participants_count,
        online_count=online_count,
        http_status=http_status,
    )
    profile.classification = classify_tme_profile(profile, html)
    return profile


def classify_tme_profile(profile: TmeProfile, html: str) -> PeerClassification:
    title = profile.html_title or ""
    extra = (profile.extra_text or "").lower()
    display_title = profile.display_title
    about = (profile.about or "").lower()

    if "Launch @" in title:
        return "bot"

    if "View @" in title:
        if "subscribers" in extra or profile.has_preview_channel:
            return "channel"
        if "members" in extra:
            return "megagroup"
        return "error"

    if "Contact @" in title:
        generic_contact = "you can contact" in about and f"@{profile.username.lower()}" in about
        if display_title and not generic_contact:
            return "user"
        return "dead"

    return "error"
