from __future__ import annotations

import os
from dataclasses import dataclass


def _env_int(name: str, default: int) -> int:
    val = os.getenv(name)
    if val is None:
        return default
    try:
        return int(val)
    except ValueError:
        return default


@dataclass(frozen=True)
class Settings:
    """
    Centralized configuration for the zreader-service.
    All values can be overridden via environment variables.
    """

    # Streaming + decompression
    chunk_size: int = _env_int("ZREADER_CHUNK_SIZE", 16384)
    max_queue_size: int = _env_int("ZREADER_MAX_QUEUE_SIZE", 8)

    # API limits
    max_upload_size_mb: int = _env_int("ZREADER_MAX_UPLOAD_MB", 512)

    # Logging
    log_level: str = os.getenv("ZREADER_LOG_LEVEL", "INFO")

    # Service metadata
    service_name: str = os.getenv("ZREADER_SERVICE_NAME", "zreader-service")
    version: str = os.getenv("ZREADER_VERSION", "1.0.0")


settings = Settings()
