from __future__ import annotations

import logging
import math
import os
import re
import unicodedata
from dataclasses import dataclass

# Collapse runs of whitespace (including newlines/tabs) to a single space.
_WHITESPACE_RE = re.compile(r"\s+")
# Bare URL with no other text.
_URL_ONLY_RE = re.compile(r"^https?://\S+$", re.IGNORECASE)
# Single Telegram slash command token, e.g. /ban /mute /report.
_SLASH_COMMAND_RE = re.compile(r"^/[A-Za-z0-9_]+$")

# Ultra-short single-token fluff (Yes/No/Ok/Lol). Longer 1-token text is kept.
_ULTRA_SHORT_MAX_LEN = 5

# BGE-M3 context is 8192 tokens. Leave a little headroom; chars/4 is a coarse
# multilingual gate (dense scripts may use more tokens per char).
_DEFAULT_MAX_EMBED_TOKENS = 8000
_DEFAULT_CHARS_PER_TOKEN = 4.0
_DEFAULT_PREFIX_RATIO = 0.8

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class EmbedTextPrep:
    """Result of embed preprocess (shared by index + query paths)."""

    text: str
    truncated: bool = False


def _max_embed_tokens() -> int:
    raw = (os.getenv("EMBED_MAX_TOKENS") or "").strip()
    if raw:
        try:
            value = int(raw)
            if value > 0:
                return value
        except ValueError:
            pass
    return _DEFAULT_MAX_EMBED_TOKENS


def _chars_per_token() -> float:
    raw = (os.getenv("EMBED_CHARS_PER_TOKEN") or "").strip()
    if raw:
        try:
            value = float(raw)
            if value > 0:
                return value
        except ValueError:
            pass
    return _DEFAULT_CHARS_PER_TOKEN


def estimate_embed_tokens(text: str, *, chars_per_token: float | None = None) -> int:
    """Coarse token estimate for the embed length gate (``ceil(len / chars_per_token)``)."""
    cpt = _chars_per_token() if chars_per_token is None else chars_per_token
    if cpt <= 0:
        raise ValueError("chars_per_token must be > 0")
    return max(1, math.ceil(len(text) / cpt)) if text else 0


def truncate_text_for_embedding(
    text: str,
    *,
    max_tokens: int | None = None,
    chars_per_token: float | None = None,
    prefix_ratio: float = _DEFAULT_PREFIX_RATIO,
) -> EmbedTextPrep:
    """Fit ``text`` into the embed context window.

    Over-limit text keeps an 80/20 prefix+suffix by default (channel lede + tail).
    """
    if not text:
        return EmbedTextPrep(text="", truncated=False)

    token_budget = _max_embed_tokens() if max_tokens is None else max_tokens
    if token_budget <= 0:
        raise ValueError("max_tokens must be > 0")
    cpt = _chars_per_token() if chars_per_token is None else chars_per_token
    if cpt <= 0:
        raise ValueError("chars_per_token must be > 0")
    if not 0.0 < prefix_ratio <= 1.0:
        raise ValueError("prefix_ratio must be in (0, 1]")

    max_chars = max(1, int(token_budget * cpt))
    if len(text) <= max_chars:
        return EmbedTextPrep(text=text, truncated=False)

    # Reserve one char for the space between prefix and suffix.
    usable = max(1, max_chars - 1)
    prefix_len = max(1, int(usable * prefix_ratio))
    suffix_len = usable - prefix_len
    if suffix_len <= 0 or prefix_len + suffix_len >= len(text):
        clipped = text[:max_chars].rstrip()
        return EmbedTextPrep(text=clipped, truncated=True)

    head = text[:prefix_len].rstrip()
    tail = text[-suffix_len:].lstrip()
    if not tail:
        merged = head[:max_chars]
    elif not head:
        merged = tail[:max_chars]
    else:
        # Re-fit if rstrip/lstrip shortened either side.
        if len(head) + 1 + len(tail) > max_chars:
            avail = max_chars - len(head) - 1
            tail = tail[-avail:] if avail > 0 else ""
        merged = f"{head} {tail}".strip() if tail else head[:max_chars]
    return EmbedTextPrep(text=merged, truncated=True)


def normalize_text(text: str | None) -> str | None:
    """NFKC + strip + collapse whitespace. None only when empty/blank.

    Used for content hashing and as the base for embedding preprocess.
    Does **not** apply embed skip filters (short tokens, URLs, etc. stay).
    """
    if not text:
        return None
    normalized = unicodedata.normalize("NFKC", text)
    collapsed = _WHITESPACE_RE.sub(" ", normalized.strip())
    return collapsed or None


def prepare_embed_text(text: str) -> EmbedTextPrep | None:
    """Normalize + skip filters + context truncate. ``None`` means skip embedding.

    Shared by embed_messages and semantic search so query and corpus match.
    """
    collapsed = normalize_text(text)
    if collapsed is None:
        return None
    if not any(ch.isalpha() for ch in collapsed):
        return None
    if _URL_ONLY_RE.fullmatch(collapsed):
        return None
    if _SLASH_COMMAND_RE.fullmatch(collapsed):
        return None
    if " " not in collapsed and len(collapsed) <= _ULTRA_SHORT_MAX_LEN:
        return None
    return truncate_text_for_embedding(collapsed)


def prepare_text_for_embedding(text: str) -> str | None:
    """Normalize text for indexing and search; return None to skip embedding.

    After :func:`normalize_text`, skips:
    - no Unicode letters (emoji / punct / digit-only)
    - URL-only
    - slash-command-only (``/ban``)
    - ultra-short 1-token text (len <= 5)

    Then truncates to the BGE context window (prefix+suffix) when needed.
    """
    prep = prepare_embed_text(text)
    return None if prep is None else prep.text
