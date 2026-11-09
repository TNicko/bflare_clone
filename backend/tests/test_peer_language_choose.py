"""On-demand peer language selection from stored text (no DB)."""

from __future__ import annotations

from utils.language_detect import LanguageGuess
from services.api.src.peer_language import choose_language_code, language_is_unset


def test_language_is_unset_ignores_und() -> None:
    assert language_is_unset(None)
    assert language_is_unset("")
    assert language_is_unset("  ")
    assert not language_is_unset("und")
    assert not language_is_unset("zh")


def test_choose_language_code_skips_thin_text(monkeypatch) -> None:
    def _boom(*_args, **_kwargs):
        raise AssertionError("detector should not run")

    monkeypatch.setattr("services.api.src.peer_language.label_from_samples", _boom)
    assert choose_language_code(title="hi", about=None, messages=[]) is None


def test_choose_language_code_returns_detector_result(monkeypatch) -> None:
    monkeypatch.setattr(
        "services.api.src.peer_language.label_from_samples",
        lambda **_kwargs: LanguageGuess(code="zh", confidence=0.9, chars_used=80),
    )
    messages = ["这是一条足够长的中文消息，用来判断这个频道主要使用的语言，需要超过四十个汉字才开始检测。"]
    assert choose_language_code(title="频道", about=None, messages=messages) == "zh"
