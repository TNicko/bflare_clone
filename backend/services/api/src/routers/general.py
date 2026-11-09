import logging
from typing import List

from fastapi import APIRouter, HTTPException, Query, Request

from services.api.src.config import ConfigFactory
from services.api.src.crud.s3.media import presign_community_views
from services.api.src.crud.supabase import get_conn
from services.api.src.crud.supabase.peers import get_category, list_categories, list_category_communities
from services.api.src.crud.supabase.views.general.category import CategoryView
from services.api.src.entitlements import require_internal_plan
from services.api.src.models.general import CategoryPeersPagedResult, CategoryPeersResponse

lg = logging.getLogger(__name__)

router = APIRouter()


@router.get("/categories", response_model=List[CategoryView])
async def get_categories(request: Request) -> List[CategoryView]:
    """List all community categories (internal plan)."""
    await require_internal_plan(request)
    async with get_conn() as conn:
        return await list_categories(conn)


@router.get("/categories/{category_slug}", response_model=CategoryPeersResponse)
async def get_peers_by_category(
    request: Request,
    category_slug: str,
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
    q: str | None = Query(
        None,
        max_length=100,
        description="Optional title / username / id filter",
    ),
) -> CategoryPeersResponse:
    await require_internal_plan(request)
    async with get_conn() as conn:
        category = await get_category(conn, category_slug=category_slug)
        if category is None:
            raise HTTPException(status_code=404, detail="Category not found")
        peers, total = await list_category_communities(
            conn,
            category_slug=category_slug,
            limit=limit,
            offset=offset,
            q=q,
        )

    region = ConfigFactory.get_config().aws_region.strip()
    peers = await presign_community_views(peers, region=region)

    return CategoryPeersResponse(
        category=category,
        peers=CategoryPeersPagedResult(
            items=peers,
            total=total,
            limit=limit,
            offset=offset,
        ),
    )
