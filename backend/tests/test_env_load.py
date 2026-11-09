"""Which .env file load_service_dotenv selects."""

from __future__ import annotations

from pathlib import Path

import pytest

from utils import env as env_mod


@pytest.fixture
def env_snapshot():
    saved_env = dict(env_mod._SERVICE_ENV)
    saved_loaded = env_mod._LOADED
    yield
    env_mod._SERVICE_ENV = saved_env
    env_mod._LOADED = saved_loaded


def test_service_loads_only_its_own_env(tmp_path: Path, env_snapshot: None, monkeypatch: pytest.MonkeyPatch) -> None:
    service_src = tmp_path / "services" / "api" / "src"
    service_src.mkdir(parents=True)
    (tmp_path / "services" / "api" / ".env").write_text("POSTGRES_DSN=from-service\n", encoding="utf-8")
    (service_src / ".env").write_text("POSTGRES_DSN=from-src\nONLY_SRC=1\n", encoding="utf-8")
    (tmp_path / "jobs").mkdir()
    (tmp_path / "jobs" / ".env").write_text("POSTGRES_DSN=from-jobs\n", encoding="utf-8")
    monkeypatch.setenv("POSTGRES_DSN", "from-shell")
    monkeypatch.setenv("ONLY_SHELL", "1")

    env_mod.load_service_dotenv(service_src / "config.py", force=True)

    assert env_mod.getenv("POSTGRES_DSN") == "from-service"
    assert env_mod.getenv("ONLY_SRC") is None
    assert env_mod.getenv("ONLY_SHELL") is None


def test_job_loads_only_jobs_env(tmp_path: Path, env_snapshot: None, monkeypatch: pytest.MonkeyPatch) -> None:
    task = tmp_path / "jobs" / "embed_images"
    task.mkdir(parents=True)
    (tmp_path / "jobs" / ".env").write_text("POSTGRES_DSN=from-jobs\n", encoding="utf-8")
    (task / ".env").write_text("POSTGRES_DSN=from-task\nONLY_TASK=1\n", encoding="utf-8")
    monkeypatch.setenv("ONLY_SHELL", "1")

    env_mod.load_service_dotenv(task / "config.py", force=True)

    assert env_mod.getenv("POSTGRES_DSN") == "from-jobs"
    assert env_mod.getenv("ONLY_TASK") is None
    assert env_mod.getenv("ONLY_SHELL") is None


def test_script_without_explicit_file_uses_shell(
    tmp_path: Path, env_snapshot: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    (scripts / ".env").write_text("POSTGRES_DSN=from-scripts-file\n", encoding="utf-8")
    monkeypatch.setenv("POSTGRES_DSN", "from-shell")

    env_mod.load_service_dotenv(scripts / "tool.py", force=True)

    assert env_mod.getenv("POSTGRES_DSN") == "from-shell"


def test_script_explicit_env_file_loads_that_file_only(
    tmp_path: Path, env_snapshot: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    env_file = tmp_path / "services" / "api" / ".env"
    env_file.parent.mkdir(parents=True)
    env_file.write_text("POSTGRES_DSN=from-api\n", encoding="utf-8")
    monkeypatch.setenv("POSTGRES_DSN", "from-shell")
    monkeypatch.setenv("ONLY_SHELL", "1")

    env_mod.load_service_dotenv(env_file, force=True)

    assert env_mod.getenv("POSTGRES_DSN") == "from-api"
    assert env_mod.getenv("ONLY_SHELL") is None


def test_missing_service_file_uses_shell(tmp_path: Path, env_snapshot: None, monkeypatch: pytest.MonkeyPatch) -> None:
    service = tmp_path / "services" / "api"
    service.mkdir(parents=True)
    monkeypatch.setenv("POSTGRES_DSN", "from-shell")

    env_mod.load_service_dotenv(service / "config.py", force=True)

    assert env_mod.getenv("POSTGRES_DSN") == "from-shell"
