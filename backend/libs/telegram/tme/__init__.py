"""t.me public page fetch + HTML classification (shared by API and ingest)."""

from __future__ import annotations

from telegram.tme.client import TmeClient
from telegram.tme.classifier import classify_tme_profile, parse_tme_html
from telegram.tme.models import PeerClassification, TmeProfile
from telegram.tme.messages import (
    ParsedTmeMessage,
    fwd_from_web,
    parse_tme_s_messages,
    slim_media_from_parsed,
)
from telegram.tme.preview import PreviewPage, parse_compact_count, parse_preview_html

__all__ = [
    "ParsedTmeMessage",
    "PeerClassification",
    "PreviewPage",
    "TmeClient",
    "TmeProfile",
    "classify_tme_profile",
    "fwd_from_web",
    "parse_compact_count",
    "parse_preview_html",
    "parse_tme_html",
    "parse_tme_s_messages",
    "slim_media_from_parsed",
]
