from __future__ import annotations

from fastapi import APIRouter

from app.config import settings

router = APIRouter()


@router.get("/health", summary="Health check")
async def health_check() -> dict[str, str]:
    """
    Basic readiness/liveness endpoint.

    Returns:
        - static service status
        - service name from settings
        - version from settings
    """
    return {
        "status": "ok",
        "service": settings.service_name,
        "version": settings.version,
    }
