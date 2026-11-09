import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response

from db.crud.user.usage import sweep_stale_usage_events
from quickwit.async_client import get_async_client as get_quickwit_async_client
from services.api.src.config import ConfigFactory
from telegram.api_session_pool import create_api_session_pool
from telegram.harvester_metrics import render_prometheus
from utils.logger import configure_logging
from services.api.src.routers import (
    general_router,
    telegram_router,
    search_router,
    project_router,
    auth_router,
    user_accounts_router,
    billing_router,
)
from services.api.src.crud.supabase import get_conn
from services.api.src.auth.middleware import PlatformAuthMiddleware
from services.api.src.mcp_server import mcp, mount_mcp
from services.api.src.request_timing import RequestTimingMiddleware

# Configure as early as possible so uvicorn access/error lines use the same format.
_cfg_bootstrap = ConfigFactory.get_config()
configure_logging(level=_cfg_bootstrap.log_level)


async def _usage_sweeper_loop(stop: asyncio.Event) -> None:
    lg = logging.getLogger(__name__)
    while not stop.is_set():
        try:
            async with get_conn() as conn:
                abandoned = await sweep_stale_usage_events(conn)
            if abandoned:
                lg.info("Marked %s abandoned usage events failed", abandoned)
        except Exception:
            lg.exception("Usage event sweeper failed")
        try:
            await asyncio.wait_for(stop.wait(), timeout=60)
        except TimeoutError:
            continue


@asynccontextmanager
async def lifespan(app: FastAPI):
    cfg = ConfigFactory.get_config()
    configure_logging(level=cfg.log_level)

    lg = logging.getLogger(__name__)
    app.state.quickwit = None
    keyword_backend = cfg.search_keyword_backend
    if keyword_backend == "quickwit":
        app.state.quickwit = get_quickwit_async_client(
            cfg.quickwit_url,
            timeout=cfg.quickwit_timeout_secs,
        )
        lg.info(
            "Keyword search backend: quickwit (%s, timeout=%.1fs)",
            cfg.quickwit_url,
            cfg.quickwit_timeout_secs,
        )
    else:
        lg.info("Keyword search backend: postgres (set SEARCH_KEYWORD_BACKEND=quickwit to enable)")

    if cfg.search_semantic_backend != "s3vectors" or cfg.search_image_backend != "s3vectors":
        lg.warning(
            "SEARCH_SEMANTIC_BACKEND=%s SEARCH_IMAGE_BACKEND=%s are ignored; dense search uses S3 Vectors",
            cfg.search_semantic_backend,
            cfg.search_image_backend,
        )
    lg.info("Dense search backends: S3 Vectors (semantic and image)")

    pool = None
    if cfg.telegram_api_id > 0 and cfg.telegram_api_hash.strip():
        try:
            pool = create_api_session_pool(
                telegram_api_id=cfg.telegram_api_id,
                telegram_api_hash=cfg.telegram_api_hash,
                get_conn=get_conn,
            )
            await pool.warm_up()
            app.state.telegram_pool = pool
        except Exception:
            lg.exception("Telegram session pool warm-up failed")

    stop_usage_sweeper = asyncio.Event()
    usage_sweeper_task = asyncio.create_task(
        _usage_sweeper_loop(stop_usage_sweeper),
        name="usage-event-sweeper",
    )

    async with mcp.session_manager.run():
        try:
            yield
        finally:
            stop_usage_sweeper.set()
            usage_sweeper_task.cancel()
            try:
                await usage_sweeper_task
            except asyncio.CancelledError:
                pass

            if pool is not None:
                await pool.shutdown()
            if app.state.quickwit is not None:
                await app.state.quickwit.aclose()


app = FastAPI(
    title="Harvest API",
    summary="Harvest API server",
    description="API for harvest client: DB, S3, and sync. Runs separately from workers.",
    version="0.1.0",
    lifespan=lifespan,
)

_cfg = _cfg_bootstrap
# Auth innermost, then request timing, CORS outermost so ACAO headers are applied
# even on short-circuit 401s. (Last add_middleware is outermost.) Timing sits
# outside auth so API-key lookups are included in the request summary.
app.add_middleware(PlatformAuthMiddleware)
app.add_middleware(RequestTimingMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=_cfg.platform_cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth_router, prefix="/auth")
app.include_router(general_router, prefix="/general", tags=["general"])
app.include_router(telegram_router, prefix="/telegram", tags=["telegram"])
app.include_router(search_router, prefix="/search", tags=["search"])
app.include_router(search_router, prefix="/v1", tags=["public"])
app.include_router(project_router, prefix="/project", tags=["project"])
app.include_router(user_accounts_router, prefix="/user")
app.include_router(billing_router, prefix="/billing")


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/metrics")
def metrics(request: Request) -> Response:
    """Prometheus scrape endpoint (unauthenticated; keep off public Caddy)."""
    pool = getattr(request.app.state, "telegram_pool", None)
    body, media_type = render_prometheus(pool)
    return Response(content=body, media_type=media_type)


@app.get("/health/telegram")
def health_telegram() -> dict[str, object]:
    pool = getattr(app.state, "telegram_pool", None)
    if pool is None:
        return {"status": "unconfigured", "connected_harvesters": [], "missing_harvesters": []}
    return {
        "status": "ok",
        "connected_harvesters": [str(hid) for hid in pool.sessions.keys()],
        "missing_harvesters": [str(hid) for hid in pool.failures.keys()],
    }


# After the API routes so /health, /v1, and /search match first. The MCP app owns /mcp.
mount_mcp(app)
