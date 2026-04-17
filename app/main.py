from __future__ import annotations

from fastapi import FastAPI

from app.api.filter import router as filter_router
from app.api.health import router as health_router
from app.api.process import router as process_router
from app.api.stats import router as stats_router
from app.api.stream import router as stream_router
from app.api.transform import router as transform_router
from app.api.upload import router as upload_router


def create_app() -> FastAPI:
    app = FastAPI(
        title="zreader-service",
        version="1.0.0",
        description="Async NDJSON processing service for .zst compressed data",
    )

    # Register routers
    app.include_router(upload_router)
    app.include_router(process_router)
    app.include_router(stream_router)
    app.include_router(filter_router)
    app.include_router(transform_router)
    app.include_router(health_router)
    app.include_router(stats_router)

    return app


app = create_app()
