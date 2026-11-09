"""pHash computation and Milvus binary-vector packing."""

from __future__ import annotations

from io import BytesIO

PHASH_BITS = 64


def compute_phash(image_bytes: bytes) -> int:
    """Return a 64-bit perceptual hash for ``image_bytes`` (pHash algorithm)."""
    from PIL import Image
    import imagehash

    from phash.pillow_io import ensure_heif_opener

    ensure_heif_opener()
    with Image.open(BytesIO(image_bytes)) as img:
        digest = imagehash.phash(img)
    return int(str(digest), 16)


def int_to_hex(value: int) -> str:
    """Encode a 64-bit pHash as a 16-character hex string."""
    return f"{value & ((1 << PHASH_BITS) - 1):016x}"


def hex_to_int(value: str) -> int:
    """Decode a hex pHash string to an integer."""
    return int(value.strip(), 16)


def phash_to_milvus_bytes(phash: int) -> bytes:
    """Pack a 64-bit pHash into 8 bytes for Milvus ``BINARY_VECTOR`` dim=64."""
    return int(phash & ((1 << PHASH_BITS) - 1)).to_bytes(PHASH_BITS // 8, byteorder="big", signed=False)


def hamming_distance(a: int, b: int) -> int:
    """Bit Hamming distance between two 64-bit pHash values."""
    return (a ^ b).bit_count()


def phash_hex_from_bytes(image_bytes: bytes) -> str:
    """Compute pHash and return a 16-character hex string."""
    return int_to_hex(compute_phash(image_bytes))


def passes_hamming_threshold(distance: float | None, *, max_hamming: int) -> bool:
    """True when Milvus Hamming distance is within ``max_hamming`` (None = keep)."""
    if distance is None:
        return True
    return float(distance) <= float(max_hamming)


def match_score(distance: float | None) -> float:
    """Higher is better: 1.0 = identical, 0.0 = max 64-bit distance."""
    if distance is None:
        return 0.0
    return max(0.0, 1.0 - (float(distance) / PHASH_BITS))
