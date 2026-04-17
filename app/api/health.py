from __future__ import annotations

from fastapi import APIRouter

router = APIRouter()


@router.get("/health", summary="Health check")
async def health_check():
    """
    Lightweight health endpoint for readiness/liveness probes.
    """
    return {
        "status": "ok",
        "service": "zreader-service",
        "message": "healthy",
    }
