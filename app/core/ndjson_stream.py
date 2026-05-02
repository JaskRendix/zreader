from __future__ import annotations

from typing import AsyncIterator

from fastapi import Request


class AsyncByteStream:
    """
    Wraps a FastAPI Request into a clean async iterator of raw bytes.

    Note: chunk sizes are controlled by the ASGI server / client; the
    chunk_size parameter here has no effect and is intentionally removed.
    """

    def __init__(self, request: Request) -> None:
        self.request = request

    async def __aiter__(self) -> AsyncIterator[bytes]:
        async for chunk in self.request.stream():
            if chunk:
                yield chunk


async def iter_bytes_from_file(
    file_path: str,
    chunk_size: int = 16384,
) -> AsyncIterator[bytes]:
    """
    Async generator that yields bytes from a local file in fixed-size chunks.
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
    Splits a stream of decompressed *text* chunks into individual NDJSON lines.

    Expects AsyncIterator[str] — i.e. the output of
    AsyncZstdDecompressor.decompress_stream — not raw bytes.
    """

    async def iter_lines(self, chunks: AsyncIterator[str]) -> AsyncIterator[str]:
        """
        Yield one complete NDJSON line per iteration.
        Trailing content with no terminating newline is also yielded.
        Blank lines are skipped.
        """
        buffer = ""
        async for chunk in chunks:
            buffer += chunk
            while "\n" in buffer:
                line, buffer = buffer.split("\n", 1)
                if line.strip():
                    yield line

        # Yield any remainder that had no terminating newline.
        if buffer.strip():
            yield buffer
