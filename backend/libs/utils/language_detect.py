"""Local content-language detection (Lingua) for peer labeling."""

from __future__ import annotations

import re
from dataclasses import dataclass
from functools import lru_cache
from typing import Sequence

# Undetermined — ISO 639-3; used when text is too short / low confidence.
UNDETERMINED = "und"

_URL_RE = re.compile(r"https?://\S+|www\.\S+", re.IGNORECASE)
_MENTION_RE = re.compile(r"[@#]\w+")
_TG_ENTITY_RE = re.compile(r"\bt\.me/\S+", re.IGNORECASE)
_WHITESPACE_RE = re.compile(r"\s+")
_LETTER_RE = re.compile(r"[^\W\d_]+", re.UNICODE)


@dataclass(frozen=True)
class LanguageGuess:
    code: str
    confidence: float
    chars_used: int


def clean_text_fragment(text: str) -> str:
    """Strip URLs/mentions so detection leans on real words."""
    s = str(text or "")
    s = _URL_RE.sub(" ", s)
    s = _TG_ENTITY_RE.sub(" ", s)
    s = _MENTION_RE.sub(" ", s)
    s = _WHITESPACE_RE.sub(" ", s).strip()
    return s


def normalize_message_samples(
    messages: Sequence[str],
    *,
    max_message_chars: int = 280,
) -> list[str]:
    out: list[str] = []
    for raw in messages:
        cleaned = clean_text_fragment(raw or "")
        if not cleaned:
            continue
        if len(cleaned) > max_message_chars:
            cleaned = cleaned[: max_message_chars - 1].rstrip() + "…"
        out.append(cleaned)
    return out


def message_letter_chars(messages: Sequence[str], *, max_message_chars: int = 280) -> int:
    """Letter-character budget from post bodies only (not title/about)."""
    return letter_char_count("\n".join(normalize_message_samples(messages, max_message_chars=max_message_chars)))


def build_detection_text(
    *,
    title: str | None,
    about: str | None,
    messages: Sequence[str],
    max_message_chars: int = 280,
    include_profile: bool = True,
) -> str:
    parts: list[str] = []
    if include_profile:
        for raw in (title, about):
            cleaned = clean_text_fragment(raw or "")
            if cleaned:
                parts.append(cleaned)
    parts.extend(normalize_message_samples(messages, max_message_chars=max_message_chars))
    return "\n".join(parts).strip()


def letter_char_count(text: str) -> int:
    return sum(len(m.group(0)) for m in _LETTER_RE.finditer(text or ""))


def _script_family(ch: str) -> str | None:
    """Coarse script bucket for a letter, or None for digits, marks, and symbols."""
    o = ord(ch)
    if 0x0041 <= o <= 0x005A or 0x0061 <= o <= 0x007A or 0x00C0 <= o <= 0x024F or 0x1E00 <= o <= 0x1EFF:
        return "latin"
    if 0x0400 <= o <= 0x052F:
        return "cyrillic"
    if 0x0600 <= o <= 0x06FF or 0x0750 <= o <= 0x077F or 0x08A0 <= o <= 0x08FF:
        return "arabic"
    if 0x0590 <= o <= 0x05FF:
        return "hebrew"
    if 0x0370 <= o <= 0x03FF:
        return "greek"
    if 0x0E00 <= o <= 0x0E7F:
        return "thai"
    if 0x0900 <= o <= 0x097F:
        return "devanagari"
    if (
        0x3400 <= o <= 0x4DBF
        or 0x4E00 <= o <= 0x9FFF
        or 0x3040 <= o <= 0x30FF
        or 0x1100 <= o <= 0x11FF
        or 0xAC00 <= o <= 0xD7AF
    ):
        return "cjk"
    if ch.isalpha():
        return "other"
    return None


def text_in_majority_script(text: str) -> str:
    """Keep the script that has more than half the letters.

    A Latin brand name inside a Chinese post is a different script. Scoring
    only the majority script keeps that name from deciding the label.
    """
    counts: dict[str, int] = {}
    for ch in text:
        family = _script_family(ch)
        if family is None:
            continue
        counts[family] = counts.get(family, 0) + 1
    total = sum(counts.values())
    if total == 0:
        return text
    family, count = max(counts.items(), key=lambda item: item[1])
    if count * 2 <= total:
        return text
    return "".join(ch for ch in text if _script_family(ch) in (None, family))


@lru_cache(maxsize=1)
def _detector():
    try:
        from lingua import LanguageDetectorBuilder
    except ImportError as exc:  # pragma: no cover - env / packaging
        raise ImportError(
            "lingua-language-detector is required for peer language labeling. "
            "Install with: pip install -e '.[label-peer-language]'"
        ) from exc

    # High-accuracy mode is the only detector. Low-accuracy mode returns on the
    # first exclusive fragment and mislabels mixed-script posts.
    return LanguageDetectorBuilder.from_all_languages().build()


def detect_language(
    text: str,
    *,
    min_chars: int = 40,
    min_confidence: float = 0.45,
) -> LanguageGuess:
    """Return ISO language code (prefer 639-1) or ``und``."""
    cleaned = clean_text_fragment(text)
    chars = letter_char_count(cleaned)
    if chars < min_chars:
        return LanguageGuess(code=UNDETERMINED, confidence=0.0, chars_used=chars)

    model_text = text_in_majority_script(cleaned)
    if letter_char_count(model_text) < min_chars:
        model_text = cleaned

    detector = _detector()
    values = detector.compute_language_confidence_values(model_text)
    if not values:
        return LanguageGuess(code=UNDETERMINED, confidence=0.0, chars_used=chars)

    top = values[0]
    confidence = float(top.value)
    if confidence < min_confidence:
        return LanguageGuess(code=UNDETERMINED, confidence=confidence, chars_used=chars)

    lang = top.language
    iso1 = getattr(lang, "iso_code_639_1", None)
    if iso1 is not None:
        code = str(iso1.name).lower()
    else:
        iso3 = getattr(lang, "iso_code_639_3", None)
        code = str(iso3.name).lower() if iso3 is not None else UNDETERMINED
    if not code or code == "none":
        code = UNDETERMINED
    return LanguageGuess(code=code, confidence=confidence, chars_used=chars)


def label_from_samples(
    *,
    title: str | None,
    about: str | None,
    messages: Sequence[str],
    min_chars: int = 40,
    min_confidence: float = 0.45,
) -> LanguageGuess:
    """Vote across profile text and each post.

    Low-accuracy Lingua can score one post as a different language at confidence
    1.0 and, once the posts are concatenated, assign that language to the whole
    peer. Each post gets one vote so a single miss cannot relabel the channel.
    """
    parts: list[str] = []
    profile = build_detection_text(title=title, about=about, messages=[], include_profile=True)
    if profile:
        parts.append(profile)
    parts.extend(normalize_message_samples(messages))

    votes: dict[str, int] = {}
    confidence_sum: dict[str, float] = {}
    chars = 0
    for part in parts:
        guess = detect_language(part, min_chars=min_chars, min_confidence=min_confidence)
        chars += guess.chars_used
        if guess.code == UNDETERMINED or guess.confidence < min_confidence:
            continue
        votes[guess.code] = votes.get(guess.code, 0) + 1
        confidence_sum[guess.code] = confidence_sum.get(guess.code, 0.0) + guess.confidence
    if votes:
        code = max(votes, key=lambda item: (votes[item], confidence_sum[item]))
        return LanguageGuess(
            code=code,
            confidence=confidence_sum[code] / votes[code],
            chars_used=chars,
        )

    joined = build_detection_text(title=title, about=about, messages=messages, include_profile=True)
    if not joined:
        return LanguageGuess(code=UNDETERMINED, confidence=0.0, chars_used=chars)
    fallback = detect_language(joined, min_chars=min_chars, min_confidence=min_confidence)
    return LanguageGuess(code=fallback.code, confidence=fallback.confidence, chars_used=chars + fallback.chars_used)
