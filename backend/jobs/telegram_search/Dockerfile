FROM python:3.14-slim-bookworm
WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    FORCE_COLOR=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends libpq5 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:0.12.19 /uv /usr/local/bin/uv

COPY pyproject.toml uv.lock ./
COPY libs ./libs
COPY jobs/telegram_search ./jobs/telegram_search

RUN uv sync --frozen --no-dev
ENV PATH="/app/.venv/bin:$PATH"

ENTRYPOINT ["python", "-m", "jobs.telegram_search.cli"]
CMD ["--help"]
