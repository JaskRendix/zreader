from __future__ import annotations

import asyncio
from typing import AsyncIterator

import zstandard as zstd


class AsyncZstdDecompressor:
    """
    Asynchronous, chunked Zstandard decompressor for streaming NDJSON.
    """

    def __init__(self, chunk_size: int = 16384) -> None:
        self.chunk_size = chunk_size

    async def decompress_stream(
        self,
        stream: AsyncIterator[bytes],
    ) -> AsyncIterator[str]:
        """
        Accepts an async iterator of compressed .zst chunks
        and yields decompressed text chunks.
        """

        dctx = zstd.ZstdDecompressor()
        decompressor = dctx.decompressobj()

        buffer = bytearray()

        async for chunk in stream:
            if not chunk:
                continue

            out = decompressor.decompress(chunk)
            if out:
                buffer.extend(out)

                # yield full chunks
                while len(buffer) >= self.chunk_size:
                    yield buffer[: self.chunk_size].decode("utf-8", errors="replace")
                    del buffer[: self.chunk_size]

            await asyncio.sleep(0)

        # flush remaining decompressed data
        while True:
            out = decompressor.flush()
            if not out:
                break
            buffer.extend(out)

        if buffer:
            yield buffer.decode("utf-8", errors="replace")

    async def decompress_lines(
        self,
        stream: AsyncIterator[bytes],
    ) -> AsyncIterator[str]:
        """
        High-level interface:
        yields NDJSON lines one by one.
        """

        buffer = ""

        async for chunk in self.decompress_stream(stream):
            text = buffer + chunk
            lines = text.split("\n")

            for line in lines[:-1]:
                yield line

            buffer = lines[-1]

        if buffer:
            yield buffer
