from services.api.src.search_message_peers import (
    author_buckets,
    patched_reply_to,
    reply_author_external_id,
    reply_body_snippet,
    reply_target_key,
    tl_peer_from_external_id,
)


def test_same_chat_reply_uses_source_and_message_id():
    key = reply_target_key(
        "-1001457643999",
        {"reply_to_msg_id": 614170},
    )
    assert key == ("-1001457643999", 614170)


def test_cross_chat_reply_uses_reply_peer():
    key = reply_target_key(
        "-1001457643999",
        {
            "reply_to_msg_id": "42",
            "reply_to_peer_id": {"tl": "PeerChannel", "channel_id": 1557643999},
        },
    )
    assert key == ("-1001557643999", 42)


def test_patch_fills_quote_and_user_author():
    patched = patched_reply_to(
        {"reply_to_msg_id": 614170},
        {"content": "  Must protect the golden gas goose  ", "from_external_id": "12345"},
        "-1001457643999",
    )
    assert patched["quote_text"] == "Must protect the golden gas goose"
    assert patched["reply_from"]["from_id"] == {"tl": "PeerUser", "user_id": 12345}


def test_patch_keeps_existing_quote_and_marks_deleted_message():
    kept = patched_reply_to(
        {"reply_to_msg_id": 1, "quote_text": " partial "},
        {"content": "full body", "from_external_id": "9"},
        "-1001",
    )
    assert kept["quote_text"] == " partial "

    deleted = reply_body_snippet({"content": None, "deleted": True, "media": None})
    assert deleted == "[deleted]"


def test_anonymous_channel_post_uses_the_chat():
    assert reply_author_external_id({"from_external_id": None}, "-1001457643999") == "-1001457643999"
    assert tl_peer_from_external_id("-1001457643999") == {
        "tl": "PeerChannel",
        "channel_id": 1457643999,
    }
    user_id, community_id = author_buckets("-1001457643999")
    assert user_id is None
    assert community_id == "-1001457643999"
