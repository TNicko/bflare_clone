"""HEIF/HEIC harvest helpers: web display derivatives and EXIF preservation."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from aws.s3 import make_s3_uri, parse_s3_uri
from phash.pillow_io import ensure_heif_opener

logger = logging.getLogger(__name__)

_HEIF_MIME_PREFIXES = ("image/heic", "image/heif", "image/heic-sequence", "image/heif-sequence")
_DISPLAY_SUFFIX = "-display.jpg"
_MAX_EXIF_JSON_BYTES = 256_000
_JPEG_QUALITY = 90


def is_heif_mime(mime: str | None) -> bool:
    if not mime:
        return False
    normalized = mime.strip().lower()
    return normalized.startswith(_HEIF_MIME_PREFIXES)


def display_key_for_object_key(key: str) -> str:
    """Derive a JPEG display key beside the canonical chat media object."""
    dirname, basename = key.rsplit("/", 1)
    return f"{dirname}/{basename}{_DISPLAY_SUFFIX}"


def display_uri_for_original_uri(original_uri: str) -> str:
    bucket, key = parse_s3_uri(original_uri)
    return make_s3_uri(bucket, display_key_for_object_key(key))


def _jsonable_exif_value(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, bytes):
        if len(value) > 4096:
            return {"_type": "bytes", "length": len(value), "preview_hex": value[:64].hex()}
        return {"_type": "bytes", "hex": value.hex()}
    if hasattr(value, "numerator") and hasattr(value, "denominator"):
        denom = value.denominator
        if denom:
            return float(value.numerator) / float(denom)
        return None
    return str(value)


def _gps_ifd(exif: Any) -> dict[Any, Any]:
    from PIL.ExifTags import IFD

    gps: Any = None
    get_ifd = getattr(exif, "get_ifd", None)
    if callable(get_ifd):
        try:
            gps = get_ifd(IFD.GPSInfo)
        except Exception:
            gps = None
    if not gps:
        raw = exif.get(0x8825)
        gps = raw if isinstance(raw, dict) else {}
    if hasattr(gps, "items"):
        return dict(gps.items())
    return dict(gps) if gps else {}


def extract_harvest_exif(source: Path) -> dict[str, Any]:
    """Extract EXIF/metadata from a raster file (including HEIC via pillow-heif)."""
    from PIL import Image
    from PIL.ExifTags import GPSTAGS, TAGS

    ensure_heif_opener()
    with Image.open(source) as img:
        mime = None
        get_mime = getattr(img, "get_format_mimetype", None)
        if callable(get_mime):
            mime = get_mime()
        out: dict[str, Any] = {
            "File:MIMEType": mime or Image.MIME.get(img.format or "", None),
            "File:ImageWidth": img.size[0],
            "File:ImageHeight": img.size[1],
            "File:FileType": img.format,
        }
        exif = img.getexif()
        if not exif:
            return {k: v for k, v in out.items() if v is not None}
        for tag_id, value in exif.items():
            name = TAGS.get(tag_id, str(tag_id))
            if name == "GPSInfo":
                continue
            out[f"EXIF:{name}"] = _jsonable_exif_value(value)
        for tag_id, value in _gps_ifd(exif).items():
            name = GPSTAGS.get(tag_id, str(tag_id))
            out[f"EXIF:GPS{name}"] = _jsonable_exif_value(value)
        return {k: v for k, v in out.items() if v is not None}


def _cap_exif_payload(exif: dict[str, Any]) -> dict[str, Any]:
    encoded = json.dumps(exif, default=str, ensure_ascii=True).encode("utf-8")
    if len(encoded) <= _MAX_EXIF_JSON_BYTES:
        return exif
    logger.warning("Truncating harvest EXIF payload from %s to %s bytes", len(encoded), _MAX_EXIF_JSON_BYTES)
    trimmed: dict[str, Any] = {}
    for key, value in exif.items():
        trimmed[key] = value
        if len(json.dumps(trimmed, default=str, ensure_ascii=True).encode("utf-8")) > _MAX_EXIF_JSON_BYTES:
            trimmed.pop(key, None)
            break
    trimmed["_truncated"] = True
    return trimmed


def transcode_heif_to_jpeg(source: Path, dest: Path, *, quality: int = _JPEG_QUALITY) -> None:
    """Write a browser-safe JPEG, copying EXIF from the HEIF master when present."""
    from PIL import Image

    ensure_heif_opener()
    with Image.open(source) as img:
        exif = img.getexif()
        rgb = img.convert("RGB")
        save_kwargs: dict[str, Any] = {"format": "JPEG", "quality": quality, "optimize": True}
        if exif:
            save_kwargs["exif"] = exif
        rgb.save(dest, **save_kwargs)


def prepare_heif_harvest_derivative(
    source: Path,
    *,
    tmp_dir: Path,
) -> tuple[Path, dict[str, Any], int]:
    """Build JPEG display file + capped EXIF dict from a local HEIF/HEIC master."""
    exif = _cap_exif_payload(extract_harvest_exif(source))
    display_path = tmp_dir / f"{source.stem}-display.jpg"
    transcode_heif_to_jpeg(source, display_path)
    return display_path, exif, display_path.stat().st_size
