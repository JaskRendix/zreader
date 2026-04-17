from __future__ import annotations

from typing import AsyncIterator

import aiofiles

from app.services.stream_service import StreamService


class FileService:
    """
    Utilities for processing local .zst files using the same async pipeline
    used by the API layer.
    """

    def __init__(self, *, chunk_size: int = 16384) -> None:
        self.chunk_size = chunk_size
        self.stream_service = StreamService(chunk_size=chunk_size)

    async def iter_file_bytes(self, path: str) -> AsyncIterator[bytes]:
        """
        Async generator yielding file bytes in fixed-size chunks.
        """
        async with aiofiles.open(path, "rb") as f:
            while True:
                chunk = await f.read(self.chunk_size)
                if not chunk:
                    break
                yield chunk

    async def process_file(self, path: str) -> AsyncIterator[dict]:
        """
        Process a local .zst file and yield validated, filtered, transformed objects.
        """
        byte_stream = self.iter_file_bytes(path)
        async for obj in self.stream_service.process(byte_stream):
            yield obj

    async def process_file_as_ndjson(self, path: str) -> AsyncIterator[str]:
        """
        Process a local .zst file and yield NDJSON lines.
        """
        byte_stream = self.iter_file_bytes(path)
        async for line in self.stream_service.process_as_ndjson(byte_stream):
            yield line
