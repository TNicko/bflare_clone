"""S3 Vectors constants for messages (bge-m3) and images (SigLIP2) ANN indexes."""

from __future__ import annotations

from embedding.constants import IMAGES_EMBED_MODEL, IMAGES_VECTOR_DIM, MESSAGES_EMBED_MODEL, MESSAGES_VECTOR_DIM

#: Must match ``infra/s3-vectors`` defaults / terraform outputs.
DEFAULT_VECTOR_BUCKET = "blackflare-message-vectors"
DEFAULT_INDEX_NAME = "messages-bge-m3-v1"
DEFAULT_IMAGES_INDEX_NAME = "images-siglip2-v1"

MESSAGES_EMBED_MODEL_SLUG = MESSAGES_EMBED_MODEL
MESSAGES_DIM = MESSAGES_VECTOR_DIM

IMAGES_EMBED_MODEL_SLUG = IMAGES_EMBED_MODEL
IMAGES_DIM = IMAGES_VECTOR_DIM

#: Filterable metadata keys (keep tiny — billed on query data-processed).
META_DATE = "date"
META_SOURCE_EXTERNAL_ID = "source_external_id"
META_PEER_TYPE = "peer_type"

#: Non-filterable on the messages index (configured in Terraform).
META_MESSAGE_ID = "message_id"

#: Non-filterable on the images index (configured in Terraform).
META_S3_KEY = "s3_key"
META_MSG_ID = "msg_id"

#: PutVectors batch size ceiling (AWS max = 500).
DEFAULT_PUT_BATCH_SIZE = 500

#: Concurrent PutVectors calls (AWS: up to ~5 × 500 = 2500 vectors/sec per index).
DEFAULT_PUT_CONCURRENCY = 5
