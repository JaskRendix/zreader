from __future__ import annotations

from fastapi import APIRouter, Request

router = APIRouter()


@router.get("/stats", summary="Service statistics")
async def get_stats(request: Request):
    """
    Return runtime statistics from the shared StatsService instance stored in app.state.
    """
    stats = request.app.state.stats
    return stats.snapshot().to_dict()
