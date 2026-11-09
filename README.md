# Harvest backend

API for a messenger data search app. It serves Telegram channels, groups, users, and messages, plus keyword, semantic, and image search. Bluesky name search is included. Auth, usage limits, and billing sit on the same service.

## Layout

- `backend/services/api` — the HTTP API
- `backend/services/text_embedding` and `backend/services/image_embedding` — embedding servers the search routes call
- `backend/services/indexer` — keeps the Quickwit keyword index in sync with Postgres
- `backend/jobs` — background workers (history fetch, embedding, archive). The API does not start them.
- `backend/libs` — shared database, Telegram, storage, and billing code
- `deploy/` — compose files for the API, embedding servers, indexer, and jobs

## Run the API

From `backend/`:

```bash
uv sync --extra api
cp services/api/.example.env services/api/.env
uv run uvicorn services.api.src.main:app --reload --port 8000
```

Fill in `services/api/.env`. Postgres is required. Keyword search can use Postgres (`SEARCH_KEYWORD_BACKEND=pg`) or Quickwit. Semantic and image search also need the embedding servers, S3 Vectors, and AWS credentials. Telegram routes need API id, API hash, and a session. Stripe and email stay off when those keys are empty.

`backend/alembic/versions` has three recent revisions as showcased examples.
