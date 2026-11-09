"""Embedding-layer exceptions (HTTP client, preprocess, etc.)."""


class EmbeddingServiceError(Exception):
    """Embedding HTTP service (Ollama) unreachable, timed out, or returned an error."""
