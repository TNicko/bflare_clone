# Harvest API

FastAPI server in `services/api`. From `backend/`:

```bash
uv sync --frozen --extra dev --extra api
uv run uvicorn services.api.src.main:app --reload --host 0.0.0.0 --port 8000
```

Config is `services/api/.env`. Docker build is `services/api/Dockerfile` (context `backend/`). Local compose is `deploy/docker-compose.api.dev.yml`.
