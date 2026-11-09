# Backend

Python package in this directory. CI installs it on Python 3.14.

## Commands

From `backend/`:

- Install: `uv sync --frozen --extra dev --extra api`
- Lint: `uv run ruff check`
- Format check: `uv run ruff format --check`
- Types: `uv run mypy`
- Tests: `uv run pytest`

## Do not touch

Ruff `target-version` is `py312` because the jobs agent image is Python 3.12. Do not use syntax only Python 3.14 can parse, including PEP 758 `except` groups.
