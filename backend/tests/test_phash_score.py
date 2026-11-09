"""Tests for shared pHash Hamming helpers."""

from __future__ import annotations

from phash.core import PHASH_BITS, match_score, passes_hamming_threshold


def test_passes_hamming_threshold() -> None:
    assert passes_hamming_threshold(None, max_hamming=5) is True
    assert passes_hamming_threshold(5, max_hamming=5) is True
    assert passes_hamming_threshold(5.0, max_hamming=5) is True
    assert passes_hamming_threshold(6, max_hamming=5) is False


def test_match_score() -> None:
    assert match_score(None) == 0.0
    assert match_score(0) == 1.0
    assert match_score(PHASH_BITS) == 0.0
    assert match_score(PHASH_BITS / 2) == 0.5
