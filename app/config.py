from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field


def _env_int(name: str, default: int) -> int:
    """Read an integer from an environment variable, falling back to default.

    Logs a warning instead of silently swallowing bad values so operators
    know when a misconfigured env var is being ignored.
    """
    val = os.getenv(name)
    if val is None:
        return default
    try:
        return int(val)
    except ValueError:
        logging.warning(
            "Config: %s=%r is not a valid integer, using default %d",
            name,
            val,
            default,
        )
        return default


def _env_str(name: str, default: str, choices: list[str] | None = None) -> str:
    """Read a string from an environment variable.

    If choices is provided, validates the value against the list and falls
    back to the default (with a warning) if it does not match.
    """
    val = os.getenv(name, default)
    if choices and val not in choices:
        logging.warning(
            "Config: %s=%r is not one of %s, using default %r",
            name,
            val,
            choices,
            default,
        )
        return default
    return val


@dataclass(frozen=True)
class Settings:
    """
    Centralised, immutable configuration for the zreader-service.

    All values can be overridden via environment variables. The module-level
    `settings` singleton is created once at import time — import it directly:

        from app.config import settings
    """

    chunk_size: int = field(
        default_factory=lambda: _env_int("ZREADER_CHUNK_SIZE", 16384)
    )
    max_queue_size: int = field(
        default_factory=lambda: _env_int("ZREADER_MAX_QUEUE_SIZE", 8)
    )

    max_upload_size_mb: int = field(
        default_factory=lambda: _env_int("ZREADER_MAX_UPLOAD_MB", 512)
    )

    log_level: str = field(
        default_factory=lambda: _env_str(
            "ZREADER_LOG_LEVEL",
            "INFO",
            choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        )
    )

    service_name: str = field(
        default_factory=lambda: os.getenv("ZREADER_SERVICE_NAME", "zreader-service")
    )
    version: str = field(default_factory=lambda: os.getenv("ZREADER_VERSION", "1.0.0"))

    @property
    def max_upload_size_bytes(self) -> int:
        """Upload limit expressed in bytes, ready to pass to request size checks."""
        return self.max_upload_size_mb * 1024 * 1024


settings = Settings()
