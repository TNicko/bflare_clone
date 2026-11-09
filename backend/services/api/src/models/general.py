from typing import List

from pydantic import BaseModel

from services.api.src.crud.supabase.views.general.category import CategoryView
from services.api.src.crud.supabase.views.general.community import CommunityView


class CategoryPeersPagedResult(BaseModel):
    items: List[CommunityView]
    total: int
    limit: int
    offset: int


class CategoryPeersResponse(BaseModel):
    """API envelope for a category with its communities."""

    category: CategoryView
    peers: CategoryPeersPagedResult
