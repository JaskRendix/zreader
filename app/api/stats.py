from __future__ import annotations

import time

from fastapi import APIRouter

from app.services.stats_service import StatsService

router = APIRouter()

stats = StatsService()  # global instance


@router.get("/stats", summary="Service statistics")
async def get_stats():
    """
    Returns counters and throughput metrics for the NDJSON processing service.
    Useful for dashboards, monitoring, and debugging.
    """
    return {
        "uptime_seconds": stats.uptime(),
        "lines_total": stats.lines_total,
        "lines_valid": stats.lines_valid,
        "lines_invalid": stats.lines_invalid,
        "lines_filtered_out": stats.lines_filtered_out,
        "lines_emitted": stats.lines_emitted,
        "throughput_lps": stats.throughput_lps(),
    }
