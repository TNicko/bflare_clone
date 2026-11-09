"""Max lengths for search query strings (API + keep in sync with client constants)."""

# Name / title ILIKE (Telegram titles are longer; keep queries bounded).
NAME_QUERY_MAX_LENGTH = 128

# Telegram usernames are 5–32 chars; allow a leading "@".
USERNAME_QUERY_MAX_LENGTH = 33

# E.164 digits plus common formatting (+, spaces, dashes).
PHONE_QUERY_MAX_LENGTH = 20

# Keyword full-text / ILIKE — longer strings are costly and rarely useful.
KEYWORD_QUERY_MAX_LENGTH = 256

# Semantic / embedding queries (model input is truncated beyond this range).
SEMANTIC_QUERY_MAX_LENGTH = 512

# Graph peer picker (title / username), same class as name search.
PEER_QUERY_MAX_LENGTH = 128

# Reverse / hash image search uploads (keep in sync with client searchLimits).
IMAGE_UPLOAD_MAX_COUNT = 5
# Public API and MCP accept one image per call. The website can still send several.
API_IMAGE_UPLOAD_MAX_COUNT = 1
IMAGE_UPLOAD_MAX_BYTES = 10 * 1024 * 1024  # 10 MiB per file
IMAGE_UPLOAD_MAX_LABEL = "10 MB"
