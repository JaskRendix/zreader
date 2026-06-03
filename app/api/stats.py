from __future__ import annotations

from fastapi import APIRouter, Request, status
from pydantic import BaseModel, Field

router = APIRouter()


class StatsSnapshotResponse(BaseModel):
    """
    OpenAPI-compliant schema representing a frozen snapshot of system-wide
    runtime performance and data throughput metrics.
    """

    uptime_seconds: float = Field(
        ..., description="Total application uptime in seconds."
    )
    lines_total: int = Field(..., description="Total NDJSON lines processed.")
    lines_valid: int = Field(..., description="Lines successfully validated.")
    lines_invalid: int = Field(
        ..., description="Lines rejected due to validation errors."
    )
    lines_filtered_out: int = Field(..., description="Lines removed by active filters.")
    lines_emitted: int = Field(
        ..., description="Lines emitted after full pipeline processing."
    )
    throughput_lps: float = Field(
        ..., description="Current processing throughput in lines per second."
    )


@router.get(
    "/stats",
    summary="Service statistics",
    response_model=StatsSnapshotResponse,
    status_code=status.HTTP_200_OK,
)
async def get_stats(request: Request):
    """
    Return a strongly-typed snapshot of global runtime statistics from the
    shared StatsService instance stored in `app.state.stats`.
    """
    stats_service = request.app.state.stats
    return stats_service.snapshot()
