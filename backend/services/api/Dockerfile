FROM python:3.14-bookworm
EXPOSE 8000
WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:0.12.19 /uv /usr/local/bin/uv

COPY pyproject.toml uv.lock ./
COPY libs ./libs
COPY services/api ./services/api
# Imported by API on-demand sync (sync_peer_messages → forward_graph.db)
# and on-demand language labeling (jobs.label_peer_language.detect).
COPY jobs/__init__.py ./jobs/__init__.py
COPY jobs/forward_graph ./jobs/forward_graph
COPY jobs/label_peer_language ./jobs/label_peer_language
COPY alembic.ini ./
COPY alembic ./alembic

RUN uv sync --frozen --no-dev --extra api
ENV PATH="/app/.venv/bin:$PATH"

CMD ["uvicorn", "services.api.src.main:app", "--host", "0.0.0.0", "--port", "8000"]
