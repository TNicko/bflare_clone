# 3.12: reliable manylinux/aarch64 wheels for pydantic-core etc. on Pis.
# (python:3.14 often yields "No matching distribution found for pydantic" on ARM.)
FROM python:3.12-slim-bookworm
WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    FORCE_COLOR=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends libpq5 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:0.12.19 /uv /usr/local/bin/uv

COPY pyproject.toml uv.lock ./
COPY libs ./libs
COPY jobs ./jobs

RUN uv sync --frozen --no-dev \
    --extra embstore \
    --extra phash-backfill \
    --extra download-message-media \
    --extra embed-images \
    --extra channel_recommendations \
    --extra label-peer-language
ENV PATH="/app/.venv/bin:$PATH"

ENTRYPOINT ["python", "-m", "jobs.agent"]
CMD ["--loop"]
