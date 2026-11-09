"""Tests for embed text normalize + context truncation."""

from __future__ import annotations

from embedding.preprocess import (
    estimate_embed_tokens,
    prepare_embed_text,
    prepare_text_for_embedding,
    truncate_text_for_embedding,
)


def test_prepare_skips_url_only() -> None:
    assert prepare_text_for_embedding("https://example.com/a") is None


def test_prepare_keeps_normal_text() -> None:
    assert prepare_text_for_embedding("Hello world from the channel") == "Hello world from the channel"


def test_truncate_noop_when_under_limit() -> None:
    text = "short channel lede"
    prep = truncate_text_for_embedding(text, max_tokens=100, chars_per_token=4.0)
    assert prep.truncated is False
    assert prep.text == text


def test_truncate_prefix_suffix_80_20() -> None:
    # 100 tokens * 4 chars = 400 char budget; 80/20 of (budget-1) for join space.
    text = ("A" * 320) + ("B" * 200) + ("C" * 80)
    prep = truncate_text_for_embedding(text, max_tokens=100, chars_per_token=4.0, prefix_ratio=0.8)
    assert prep.truncated is True
    assert len(prep.text) <= 400
    assert prep.text.startswith("A")
    assert prep.text.endswith("C" * 80)
    assert "B" not in prep.text.replace(" ", "")
    # Roughly 80/20 split of the usable budget (399).
    head, _, tail = prep.text.partition(" ")
    assert len(head) >= 300
    assert len(tail) == 80


def test_prepare_embed_text_truncates_long_input() -> None:
    # Far over default 8000*4 char gate.
    body = ("Important lede about politics. " * 20) + ("x" * 50_000) + " Tail conclusion words here."
    prep = prepare_embed_text(body)
    assert prep is not None
    assert prep.truncated is True
    assert "Important lede" in prep.text
    assert "Tail conclusion" in prep.text
    assert estimate_embed_tokens(prep.text) <= 8000
