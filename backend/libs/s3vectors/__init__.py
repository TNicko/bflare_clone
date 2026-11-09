"""S3 Vectors derived ANN indexes for message (bge-m3) and image (SigLIP2) embeddings.

Parquet embstore remains the durable source of truth; this package upserts/queries
Amazon S3 Vectors for semantic / reverse-image search.
"""

from __future__ import annotations

from s3vectors.client import (
    build_metadata_filter,
    cosine_similarity_from_distance,
    dedupe_put_items,
    delete_image_vectors,
    delete_message_vectors,
    get_client,
    image_vector_put_item,
    message_vector_put_item,
    put_image_vectors,
    put_message_vectors,
    search_image_vectors,
    search_message_vectors,
)
from s3vectors.constants import (
    DEFAULT_IMAGES_INDEX_NAME,
    DEFAULT_INDEX_NAME,
    DEFAULT_PUT_BATCH_SIZE,
    DEFAULT_PUT_CONCURRENCY,
    DEFAULT_VECTOR_BUCKET,
    IMAGES_DIM,
    IMAGES_EMBED_MODEL_SLUG,
    MESSAGES_DIM,
    MESSAGES_EMBED_MODEL_SLUG,
    META_DATE,
    META_MESSAGE_ID,
    META_MSG_ID,
    META_PEER_TYPE,
    META_S3_KEY,
    META_SOURCE_EXTERNAL_ID,
)

__all__ = [
    "DEFAULT_IMAGES_INDEX_NAME",
    "DEFAULT_INDEX_NAME",
    "DEFAULT_PUT_BATCH_SIZE",
    "DEFAULT_PUT_CONCURRENCY",
    "DEFAULT_VECTOR_BUCKET",
    "IMAGES_DIM",
    "IMAGES_EMBED_MODEL_SLUG",
    "MESSAGES_DIM",
    "MESSAGES_EMBED_MODEL_SLUG",
    "META_DATE",
    "META_MESSAGE_ID",
    "META_MSG_ID",
    "META_PEER_TYPE",
    "META_S3_KEY",
    "META_SOURCE_EXTERNAL_ID",
    "build_metadata_filter",
    "cosine_similarity_from_distance",
    "dedupe_put_items",
    "delete_image_vectors",
    "delete_message_vectors",
    "get_client",
    "image_vector_put_item",
    "message_vector_put_item",
    "put_image_vectors",
    "put_message_vectors",
    "search_image_vectors",
    "search_message_vectors",
]
