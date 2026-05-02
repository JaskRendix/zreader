from __future__ import annotations

from typing import AsyncIterator

import aiofiles

from app.services.stream_service import StreamService


class FileService:
    """
    Utilities for processing local .zst files through the same async pipeline
    used by the HTTP API layer.

    This class is a thin wrapper around StreamService that handles file I/O.
    It does not duplicate byte-reading logic — iter_bytes_from_file in
    app.core.ndjson_stream is the canonical implementation; this delegates
    to StreamService.process_file() which uses it internally.
    """

    def __init__(self, *, chunk_size: int = 16384) -> None:
        self.chunk_size = chunk_size
        self.stream_service = StreamService(chunk_size=chunk_size)

    async def process_file(self, path: str) -> AsyncIterator[dict]:
        """
        Process a local .zst file and yield validated, filtered, transformed
        objects.

        Delegates entirely to StreamService.process_file() so that all
        pipeline logic lives in one place.
        """
        async for obj in self.stream_service.process_file(path):
            yield obj

    async def process_file_as_ndjson(self, path: str) -> AsyncIterator[str]:
        """
        Process a local .zst file and yield serialised NDJSON lines.
        """
        byte_stream = self._iter_file_bytes(path)
        async for line in self.stream_service.process_as_ndjson(byte_stream):
            yield line

    async def _iter_file_bytes(self, path: str) -> AsyncIterator[bytes]:
        """
        Internal helper: yield raw file bytes in fixed-size chunks.

        Not part of the public API — external callers should use
        app.core.ndjson_stream.iter_bytes_from_file directly.
        """
        async with aiofiles.open(path, "rb") as f:
            while True:
                chunk = await f.read(self.chunk_size)
                if not chunk:
                    break
                yield chunk
