from datetime import UTC, datetime, timedelta

from db.crud.telegram.message import message_stat_deltas


def test_message_stat_deltas_counts_inserts_only() -> None:
    earlier = datetime(2026, 1, 1, tzinfo=UTC)
    later = earlier + timedelta(days=1)
    deltas = message_stat_deltas(
        [
            ("user-a", "peer-1", earlier, True),
            ("user-a", "peer-1", later, False),
            ("user-a", "peer-1", later, True),
            (None, "peer-1", later, True),
            ("user-b", "peer-2", None, True),
        ]
    )
    assert sorted(deltas) == [
        ("user-a", "peer-1", 2, later),
        ("user-b", "peer-2", 1, None),
    ]
