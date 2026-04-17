from __future__ import annotations

from typing import AsyncIterator

from fastapi import Request


class AsyncByteStream:
    """
    Wraps an async byte source (e.g., FastAPI Request.stream())
    into a clean, typed async iterator of bytes.
    """

    def __init__(
        self,
        request: Request,
        chunk_size: int = 16384,
    ) -> None:
        self.request = request
        self.chunk_size = chunk_size

    async def __aiter__(self) -> AsyncIterator[bytes]:
        async for chunk in self.request.stream():
            if chunk:
                yield chunk


async def iter_bytes_from_file(
    file_path: str,
    chunk_size: int = 16384,
) -> AsyncIterator[bytes]:
    """
    Async generator that yields bytes from a local file.
    Useful for tests, benchmarks, and offline processing.
    """
    import aiofiles

    async with aiofiles.open(file_path, "rb") as f:
        while True:
            chunk = await f.read(chunk_size)
            if not chunk:
                break
            yield chunk


class NDJSONStream:
    """
    Splits incoming decompressed bytes into NDJSON lines.
    """

    async def iter_lines(self, chunks: AsyncIterator[bytes]) -> AsyncIterator[str]:
        buffer = ""

        async for chunk in chunks:
            buffer += chunk.decode()

            while "\n" in buffer:
                line, buffer = buffer.split("\n", 1)
                if line.strip():
                    yield line

        if buffer.strip():
            yield buffer
