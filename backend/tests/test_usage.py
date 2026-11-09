"""Usage ledger helpers: query hash, result inference, HTTP status mapping."""

from __future__ import annotations

from types import SimpleNamespace

from fastapi import HTTPException

from services.api.src.usage import (
    Meter,
    error_code_from_http,
    infer_result_count,
    infer_units,
    normalize_query,
    query_hash,
    status_for_http,
    store_query_text,
)


def test_query_hash_is_stable_and_case_insensitive() -> None:
    assert query_hash("  Foo   Bar ") == query_hash("foo bar")
    assert query_hash("  Foo   Bar ") == query_hash("ｆｏｏ  bar")
    assert query_hash("   ") is None
    assert query_hash(None) is None


def test_store_query_text_keeps_original_casing() -> None:
    assert store_query_text("  Foo Bar  ") == "Foo Bar"
    assert store_query_text("   ") is None


def test_normalize_query_collapses_whitespace() -> None:
    assert normalize_query("  Foo\nBar  ") == "foo bar"


def test_status_for_http() -> None:
    assert status_for_http(400) == "rejected"
    assert status_for_http(402) == "rejected"
    assert status_for_http(429) == "rejected"
    assert status_for_http(500) == "failed"
    assert status_for_http(503) == "failed"


def test_error_code_from_http_truncates() -> None:
    exc = HTTPException(status_code=400, detail="x" * 200)
    assert len(error_code_from_http(exc)) == 80
    coded = HTTPException(status_code=402, detail={"code": "quota_exceeded"})
    assert error_code_from_http(coded) == "quota_exceeded"


def test_infer_result_count_keyword_uses_full_total_not_user_lookup() -> None:
    keyword = SimpleNamespace(
        items=[object(), object()],
        total=438,
        users={"1": object()},
        peers={},
    )
    assert infer_result_count(keyword) == 438


def test_infer_result_count_from_search_shapes() -> None:
    name = SimpleNamespace(
        communities=SimpleNamespace(total=3, items=[]),
        users=SimpleNamespace(total=2, items=[]),
    )
    assert infer_result_count(name) == 5
    assert infer_result_count(SimpleNamespace(found=False, user=None)) == 0
    assert infer_result_count(SimpleNamespace(found=True, user=object())) == 1
    assert infer_result_count([object(), object()]) == 2
    assert infer_result_count(SimpleNamespace(synced_count=7, messages=[object()])) == 7


def test_infer_units_marks_skipped_telegram_name_search() -> None:
    units = infer_units(Meter.SEARCH_NAME_TELEGRAM, SimpleNamespace(skipped=True), {})
    assert units["searches"] == 0
    assert units["telegram_calls"] == 0

    live = infer_units(Meter.TELEGRAM_SYNC, SimpleNamespace(synced_count=12), {})
    assert live["messages_synced"] == 12
    assert live["searches"] == 1
