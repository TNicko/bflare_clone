# Build from the backend/ directory (repo context that contains pyproject.toml + libs):
#
#   docker build -f jobs/embed_images/Dockerfile -t embed-images .
#
# .env is excluded from the image (.dockerignore). Pass config at runtime:
#
#   docker run --rm --env-file jobs/embed_images/.env embed-images
#
# Or mount the file where load_service_dotenv looks:
#
#   docker run --rm \
#     -v "$PWD/jobs/embed_images/.env:/app/jobs/embed_images/.env:ro" \
#     embed-images
#
# Leave it running until complete (resume-safe via embed cursors / Ctrl+C graceful stop):
#
#   docker run -d --name embed-images --restart on-failure:3 \
#     --env-file jobs/embed_images/.env \
#     embed-images
#
#   docker logs -f embed-images

FROM python:3.14-slim-bookworm
WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    FORCE_COLOR=1 \
    PYTHONDONTWRITEBYTECODE=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends libpq5 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:0.12.19 /uv /usr/local/bin/uv

COPY pyproject.toml uv.lock ./
COPY libs ./libs
COPY jobs/embed_images ./jobs/embed_images

RUN uv sync --frozen --no-dev --extra embed-images
ENV PATH="/app/.venv/bin:$PATH"

ENTRYPOINT ["python", "-m", "jobs.embed_images.cli"]
CMD []
