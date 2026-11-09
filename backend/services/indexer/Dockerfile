# Build from backend/ (workspace root):
#   docker build -f deploy/Dockerfile.indexer -t harvest-indexer .
FROM python:3.14-bookworm
WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:0.12.19 /uv /usr/local/bin/uv

COPY pyproject.toml uv.lock ./
COPY libs ./libs
COPY services/indexer ./services/indexer

RUN uv sync --frozen --no-dev
ENV PATH="/app/.venv/bin:$PATH"

CMD ["python", "-m", "services.indexer.cli", "run"]
