"""Embedding model names and dimensions for search, the embstore, and MobileNet."""

from __future__ import annotations

#: Text embedding model (Ollama ``bge-m3``).
MESSAGES_EMBED_MODEL = "bge-m3"
MESSAGES_VECTOR_DIM = 1024

#: SigLIP2 image embedding model (embstore path ``siglip2-so400m-p16-384``).
IMAGES_EMBED_MODEL = "siglip2-so400m-p16-384"
IMAGES_VECTOR_DIM = 1152

#: TensorFlow MobileNetV3-Large. Stored in the embstore; not used by image search.
MOBILENET_EMBED_MODEL = "mobilenet-v3-large"
MOBILENET_VECTOR_DIM = 1280
MOBILENET_INPUT_SIZE = 224
