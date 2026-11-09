"""Jobs agent config (central ``jobs/.env``)."""

from __future__ import annotations

from pydantic import BaseModel

from utils.env import env_csv, env_int, env_truthy, getenv, load_service_dotenv

from db.crud.system.jobs import TASK_TYPES

load_service_dotenv(__file__)


class AgentConfig(BaseModel):
    environment: str = "local"
    log_level: str = "INFO"
    postgres_dsn: str = ""

    worker_id: str = ""
    worker_label: str = ""
    worker_capabilities: list[str] = []
    #: Seeded into DB on first register only (unless WORKER_SEED_MAX_CONCURRENT=1).
    worker_max_concurrent_jobs: int = 1
    worker_seed_max_concurrent: bool = False

    hostname: str | None = None
    tailscale_ip: str | None = None
    agent_version: str = "0.1.0"
    git_sha: str | None = None

    poll_idle_seconds: float = 3.0
    heartbeat_seconds: float = 15.0
    lease_seconds: int = 120
    reclaim_leases: bool = True

    def validate_required(self) -> None:
        missing: list[str] = []
        if not self.postgres_dsn.strip():
            missing.append("POSTGRES_DSN")
        if not self.worker_id.strip():
            missing.append("WORKER_ID")
        if missing:
            raise ValueError(f"Missing or invalid environment variables: {', '.join(missing)}")


class ConfigFactory:
    _config: AgentConfig | None = None

    @classmethod
    def get_config(cls) -> AgentConfig:
        if cls._config is None:
            caps = env_csv("WORKER_CAPABILITIES")
            if not caps:
                caps = list(TASK_TYPES)
            label = getenv("WORKER_LABEL") or getenv("WORKER_ID") or ""
            cls._config = AgentConfig(
                environment=getenv("ENVIRONMENT", "local"),
                log_level=getenv("LOG_LEVEL", "INFO"),
                postgres_dsn=getenv("POSTGRES_DSN", ""),
                worker_id=(getenv("WORKER_ID") or "").strip(),
                worker_label=label.strip(),
                worker_capabilities=caps,
                worker_max_concurrent_jobs=max(1, min(env_int("WORKER_MAX_CONCURRENT_JOBS") or 1, 32)),
                worker_seed_max_concurrent=env_truthy("WORKER_SEED_MAX_CONCURRENT", default=False),
                hostname=getenv("WORKER_HOSTNAME") or None,
                tailscale_ip=getenv("WORKER_TAILSCALE_IP") or None,
                agent_version=getenv("WORKER_AGENT_VERSION") or "0.1.0",
                git_sha=getenv("WORKER_GIT_SHA") or None,
                poll_idle_seconds=float(getenv("WORKER_POLL_IDLE_SECONDS") or "3"),
                heartbeat_seconds=float(getenv("WORKER_HEARTBEAT_SECONDS") or "15"),
                lease_seconds=int(getenv("WORKER_LEASE_SECONDS") or "120"),
                reclaim_leases=env_truthy("WORKER_RECLAIM_LEASES", default=True),
            )
        return cls._config
