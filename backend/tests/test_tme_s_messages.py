"""Unit tests for ``t.me/s`` message HTML parsing."""

from __future__ import annotations

from telegram.tme.messages import fwd_from_web, parse_tme_s_messages, slim_media_from_parsed


_SAMPLE = """
<div class="tgme_widget_message js-widget_message" data-post="durov/123" data-view="abc">
  <div class="tgme_widget_message_user">
    <div class="tgme_widget_message_forwarded_from">
      Forwarded from
      <a href="https://t.me/otherchannel"><span class="tgme_widget_message_forwarded_from_name">Other</span></a>
    </div>
  </div>
  <div class="tgme_widget_message_text js-message_text" dir="auto">
    Hello @mention and https://example.com/path
  </div>
  <a class="tgme_widget_message_photo_wrap" style="background-image:url('https://cdn4.telesco.pe/file/photo.jpg')"></a>
  <span class="tgme_widget_message_views">1.2K</span>
  <time datetime="2024-06-01T12:00:00+00:00"></time>
</div>
<div class="tgme_widget_message js-widget_message" data-post="durov/122" data-view="def">
  <div class="tgme_widget_message_text js-message_text" dir="auto">plain text only</div>
  <a class="tgme_widget_message_link_preview" href="https://news.example/story"></a>
  <time datetime="2024-05-31T10:00:00+00:00"></time>
</div>
"""


def test_parse_tme_s_messages_basic_fields() -> None:
    msgs = parse_tme_s_messages(_SAMPLE)
    assert len(msgs) == 2
    newest = msgs[0]
    assert newest.message_id == 123
    assert newest.views == 1200
    assert newest.has_photo is True
    assert newest.has_url is True
    assert newest.has_mention is True
    assert newest.fwd_username == "otherchannel"
    assert newest.fwd_from_name == "Other"
    assert newest.photo_url and "telesco.pe" in newest.photo_url

    older = msgs[1]
    assert older.message_id == 122
    assert older.has_link_preview is True
    assert older.has_url is True
    assert older.has_mention is False


def test_slim_media_and_fwd_helpers() -> None:
    msgs = parse_tme_s_messages(_SAMPLE)
    media = slim_media_from_parsed(msgs[0])
    assert media is not None
    assert media["tl"] == "MessageMediaPhoto"
    assert media.get("web_url")

    webpage = slim_media_from_parsed(msgs[1])
    assert webpage is not None
    assert webpage["tl"] == "MessageMediaWebPage"
    assert webpage.get("url") == "https://news.example/story"

    fwd = fwd_from_web(username="Foo_Bar", from_name="Display")
    assert fwd == {"source": "web", "username": "Foo_Bar", "from_name": "Display"}
