from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI

from app.api.filter import router as filter_router
from app.api.health import router as health_router
from app.api.process import router as process_router
from app.api.stats import router as stats_router
from app.api.stream import router as stream_router
from app.api.transform import router as transform_router
from app.api.upload import router as upload_router
from app.config import settings
from app.services.stats_service import StatsService


def _configure_logging() -> None:
    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    _configure_logging()
    app.state.stats = StatsService()
    logging.getLogger(__name__).info(
        "Starting %s v%s (log_level=%s)",
        settings.service_name,
        settings.version,
        settings.log_level,
    )

    yield

    logging.getLogger(__name__).info("Shutting down %s", settings.service_name)


def create_app() -> FastAPI:
    app = FastAPI(
        title=settings.service_name,
        version=settings.version,
        description="Async NDJSON processing service for .zst compressed data",
        lifespan=lifespan,
    )

    app.include_router(health_router)
    app.include_router(stats_router)
    app.include_router(upload_router)
    app.include_router(process_router)
    app.include_router(stream_router)
    app.include_router(filter_router)
    app.include_router(transform_router)

    return app


app = create_app()
