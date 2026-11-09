from services.api.src.routers.general import router as general_router
from services.api.src.routers.telegram import router as telegram_router
from services.api.src.routers.search import router as search_router
from services.api.src.routers.project import router as project_router
from services.api.src.routers.auth import router as auth_router
from services.api.src.routers.user_accounts import router as user_accounts_router
from services.api.src.routers.billing import router as billing_router

__all__ = [
    "general_router",
    "telegram_router",
    "search_router",
    "project_router",
    "auth_router",
    "user_accounts_router",
    "billing_router",
]
