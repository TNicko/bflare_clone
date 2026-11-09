"""Unit tests for local peer language detection (no DB)."""

from __future__ import annotations

from utils.language_detect import (
    UNDETERMINED,
    build_detection_text,
    clean_text_fragment,
    detect_language,
)


def test_clean_strips_urls_and_mentions() -> None:
    raw = "Hello @user see https://t.me/foo and www.example.com now"
    cleaned = clean_text_fragment(raw)
    assert "https://" not in cleaned
    assert "@user" not in cleaned
    assert "Hello" in cleaned


def test_build_detection_text_joins_profile_and_messages() -> None:
    text = build_detection_text(
        title="Iran News",
        about="Daily updates",
        messages=["بورس امروز مثبت بود", "قیمت دلار"],
    )
    assert "Iran News" in text
    assert "بورس" in text


def test_detect_english() -> None:
    import pytest

    pytest.importorskip("lingua")
    try:
        guess = detect_language(
            "The quick brown fox jumps over the lazy dog. "
            "This channel posts daily technology news and analysis for readers.",
            min_chars=20,
            min_confidence=0.3,
        )
    except ImportError as exc:
        pytest.skip(str(exc))
    assert guess.code == "en"
    assert guess.confidence >= 0.3


def test_detect_chinese_not_overridden_by_welsh_latin_token() -> None:
    """A casino handle that Lingua reads as Welsh must not relabel a Chinese post."""
    import pytest

    pytest.importorskip("lingua")
    zh = "柬埔寨警方通报一起跨境诈骗案件，涉案人员已被逮捕，受害者主要来自中国和美国。"
    try:
        guess = detect_language(zh * 4 + " 官方网址: diwei.app", min_chars=40, min_confidence=0.45)
    except ImportError as exc:
        pytest.skip(str(exc))
    assert guess.code == "zh"


def test_label_from_samples_ignores_one_outlier(monkeypatch) -> None:
    from utils.language_detect import LanguageGuess, label_from_samples

    def fake(text: str, **_kwargs: object) -> LanguageGuess:
        if "outlier" in text:
            return LanguageGuess(code="sl", confidence=1.0, chars_used=80)
        return LanguageGuess(code="en", confidence=1.0, chars_used=80)

    monkeypatch.setattr("utils.language_detect.detect_language", fake)
    guess = label_from_samples(
        title="Pavel Durov",
        about="Founder of Telegram.",
        messages=["english post about telegram " * 4] * 4 + ["outlier post " * 6],
    )
    assert guess.code == "en"


def test_detector_uses_high_accuracy(monkeypatch) -> None:
    import pytest

    lingua = pytest.importorskip("lingua")
    from utils.language_detect import _detector

    def fail(self: object) -> object:
        raise AssertionError("low accuracy mode must not be enabled")

    monkeypatch.setattr(lingua.LanguageDetectorBuilder, "with_low_accuracy_mode", fail)
    _detector.cache_clear()
    try:
        detector = _detector()
    finally:
        _detector.cache_clear()
    assert detector is not None


def test_detect_short_text_undetermined() -> None:
    import pytest

    pytest.importorskip("lingua")
    try:
        guess = detect_language("hi", min_chars=40, min_confidence=0.45)
    except ImportError as exc:
        pytest.skip(str(exc))
    assert guess.code == UNDETERMINED
