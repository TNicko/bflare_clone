from __future__ import annotations

from embedding.image_models import (
    MOBILENET_EMBED_MODEL_SLUG,
    SIGLIP_EMBED_MODEL,
    resolve_image_embed_model,
    resolve_image_embed_service_url,
)
from jobs.embed_images.config import ConfigFactory
from utils import env as env_mod
from utils.env import getenv
from telegram.media import harvest_image_embedded, merge_harvest_into_media_dict, needs_embed
from telegram.media_kinds import parse_slim_media


def _photo(*, harvest: dict):
    return parse_slim_media({"tl": "MessageMediaPhoto", "harvest": harvest})


def test_resolve_image_embed_model_aliases() -> None:
    assert resolve_image_embed_model("siglip") == (SIGLIP_EMBED_MODEL, 1152)
    assert resolve_image_embed_model("mobilenet") == (MOBILENET_EMBED_MODEL_SLUG, 1280)


def test_mobilenet_does_not_inherit_siglip_service_url() -> None:
    assert (
        resolve_image_embed_service_url(
            MOBILENET_EMBED_MODEL_SLUG,
            service_url="http://harvest-image-embedding:11435",
        )
        == "http://localhost:11436"
    )
    assert (
        resolve_image_embed_service_url(
            MOBILENET_EMBED_MODEL_SLUG,
            service_url="http://harvest-image-embedding:11435",
            mobilenet_service_url="http://gpu:11436",
        )
        == "http://gpu:11436"
    )
    assert (
        resolve_image_embed_service_url(
            SIGLIP_EMBED_MODEL,
            service_url="http://harvest-image-embedding:11435",
        )
        == "http://harvest-image-embedding:11435"
    )


def test_legacy_image_embedding_id_counts_as_siglip_done() -> None:
    media = _photo(harvest={"s3_uri": "s3://b/k", "image_embedding_id": "11111111-1111-1111-1111-111111111111"})
    assert harvest_image_embedded(media, SIGLIP_EMBED_MODEL) is True
    assert harvest_image_embedded(media, MOBILENET_EMBED_MODEL_SLUG) is False
    assert needs_embed(media, SIGLIP_EMBED_MODEL) is False
    assert needs_embed(media, MOBILENET_EMBED_MODEL_SLUG) is True


def test_image_embedded_flags_are_per_model() -> None:
    media = _photo(
        harvest={
            "s3_uri": "s3://b/k",
            "image_embedded": {SIGLIP_EMBED_MODEL: True},
        }
    )
    assert harvest_image_embedded(media, SIGLIP_EMBED_MODEL) is True
    assert harvest_image_embedded(media, MOBILENET_EMBED_MODEL_SLUG) is False


def test_merge_sets_model_flag_without_embedding_id() -> None:
    media = _photo(harvest={"s3_uri": "s3://b/k"})
    updated = merge_harvest_into_media_dict(media, image_embedded_model=MOBILENET_EMBED_MODEL_SLUG)
    harvest = updated["harvest"]
    assert harvest["image_embedded"][MOBILENET_EMBED_MODEL_SLUG] is True
    assert "image_embedding_id" not in harvest
    parsed = parse_slim_media({"tl": "MessageMediaPhoto", "harvest": harvest})
    assert harvest_image_embedded(parsed, MOBILENET_EMBED_MODEL_SLUG) is True
    assert harvest_image_embedded(parsed, SIGLIP_EMBED_MODEL) is False


def test_merge_preserves_existing_model_flags() -> None:
    media = _photo(
        harvest={
            "s3_uri": "s3://b/k",
            "image_embedded": {SIGLIP_EMBED_MODEL: True},
        }
    )
    updated = merge_harvest_into_media_dict(media, image_embedded_model=MOBILENET_EMBED_MODEL_SLUG)
    flags = updated["harvest"]["image_embedded"]
    assert flags[SIGLIP_EMBED_MODEL] is True
    assert flags[MOBILENET_EMBED_MODEL_SLUG] is True


def test_cli_model_overrides_dotenv_snapshot() -> None:
    """``--model`` must update getenv(), not only os.environ (jobs/.env snapshot)."""
    prev_env = dict(env_mod._SERVICE_ENV)
    prev_loaded = env_mod._LOADED
    prev_cfg = ConfigFactory._config
    try:
        env_mod._LOADED = True
        env_mod._SERVICE_ENV = {**prev_env, "IMAGE_EMBEDDING_MODEL": "siglip"}
        ConfigFactory._config = None
        assert getenv("IMAGE_EMBEDDING_MODEL") == "siglip"
        ConfigFactory.apply_cli_overrides(model="mobilenet")
        assert getenv("IMAGE_EMBEDDING_MODEL") == "mobilenet"
        cfg = ConfigFactory.get_config()
        assert cfg.embedding_model == MOBILENET_EMBED_MODEL_SLUG
        assert cfg.vector_dim == 1280
    finally:
        env_mod._SERVICE_ENV = prev_env
        env_mod._LOADED = prev_loaded
        ConfigFactory._config = prev_cfg


def test_embed_images_argv_includes_model() -> None:
    from jobs.argv import params_to_argv

    assert params_to_argv("embed_images", {"model": "mobilenet"}) == ["--model", "mobilenet"]
