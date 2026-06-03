from __future__ import annotations

from pathlib import Path
from typing import AsyncIterator

import aiofiles

from app.core.validators import ValidationStats
from app.services.stream_service import StreamService


class FileService:
    """
    Utilities for processing local .zst files through the same async NDJSON
    pipeline used by the HTTP API layer.

    This class is a thin wrapper around StreamService. It handles file-system
    access and exposes high-level helpers for reading .zst files as validated,
    filtered, transformed objects or NDJSON strings.
    """

    def __init__(self, *, chunk_size: int = 65536) -> None:
        self.chunk_size = chunk_size
        self.stream_service = StreamService(chunk_size=chunk_size)

    async def process_file(
        self,
        path: str | Path,
        stats: ValidationStats | None = None,
    ) -> AsyncIterator[dict]:
        """
        Process a local .zst file and yield validated, filtered, transformed objects.
        """
        path = Path(path)

        if not path.exists():
            raise FileNotFoundError(f"Target zst file not found at path: {path}")

        byte_stream = self._iter_file_bytes(path)

        async for obj in self.stream_service.process_stream(byte_stream, stats=stats):
            yield obj

    async def process_file_as_ndjson(
        self,
        path: str | Path,
        stats: ValidationStats | None = None,
    ) -> AsyncIterator[str]:
        """
        Process a local .zst file and yield compact NDJSON strings *without*
        trailing newlines.
        """
        path = Path(path)

        if not path.exists():
            raise FileNotFoundError(f"Target zst file not found at path: {path}")

        byte_stream = self._iter_file_bytes(path)

        async for line in self.stream_service.process_as_ndjson(
            byte_stream, stats=stats
        ):
            yield line

    async def _iter_file_bytes(self, path: Path) -> AsyncIterator[bytes]:
        """
        Internal helper: yield raw file bytes in fixed-size blocks.
        """
        async with aiofiles.open(path, "rb") as f:
            while True:
                chunk = await f.read(self.chunk_size)
                if not chunk:
                    break
                yield chunk
