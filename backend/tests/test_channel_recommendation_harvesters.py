from uuid import uuid4

import pytest

from db.schemas.telegram.harvester import TelegramHarvesterSchema
from jobs.argv import params_to_argv
from jobs.harvester_ids import harvester_external_ids_from_params, normalize_harvester_external_ids
from jobs.params_env import params_to_env
from telegram.multi_harvester import omit_reserved_harvesters, select_worker_harvesters


def _harvester(external_id: str) -> TelegramHarvesterSchema:
    return TelegramHarvesterSchema(id=uuid4(), external_id=external_id, label=external_id)


def test_harvester_ids_from_params_merge_list_and_legacy_pin() -> None:
    assert harvester_external_ids_from_params(
        {
            "harvester_external_ids": ["10", " 20 ", "10"],
            "harvester_external_id": "30",
        }
    ) == ["10", "20", "30"]
    assert normalize_harvester_external_ids(["1, 2", "2", ""]) == ["1", "2"]
    assert harvester_external_ids_from_params({}) == []


def test_telegram_search_argv_repeats_harvester_flag() -> None:
    assert params_to_argv(
        "telegram_search",
        {"harvester_external_ids": ["10", "20"]},
    ) == [
        "--harvester-external-id",
        "10",
        "--harvester-external-id",
        "20",
    ]
    assert params_to_argv("telegram_search", {"query": "cats", "query_file": "q.txt"}) == []


def test_telegram_search_env_pins_only_a_single_harvester() -> None:
    assert params_to_env("telegram_search", {"harvester_external_ids": ["10", "20"]}) == {}
    assert params_to_env("telegram_search", {"harvester_external_ids": ["10"]}) == {
        "HARVESTER_EXTERNAL_ID": "10",
    }


def test_omit_reserved_harvesters_keeps_unassigned_accounts() -> None:
    pool = [_harvester("1"), _harvester("2"), _harvester("3")]
    assert [h.external_id for h in omit_reserved_harvesters(pool, {"2"})] == ["1", "3"]
    assert omit_reserved_harvesters(pool, set()) == pool


def test_select_worker_harvesters_keeps_request_order() -> None:
    pool = [_harvester("1"), _harvester("2"), _harvester("3")]
    selected = select_worker_harvesters(pool, ["3", "1"])
    assert [h.external_id for h in selected] == ["3", "1"]
    assert select_worker_harvesters(pool, []) == pool


def test_select_worker_harvesters_rejects_unknown_ids() -> None:
    pool = [_harvester("1")]
    with pytest.raises(ValueError, match="not found"):
        select_worker_harvesters(pool, ["9"])
