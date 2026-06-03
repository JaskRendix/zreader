from __future__ import annotations

import asyncio
import codecs
from typing import AsyncIterator

import zstandard as zstd


class AsyncZstdDecompressor:
    """
    Asynchronous, chunked Zstandard decompressor for streaming NDJSON.
    Single-frame oriented, with incremental UTF-8 decoding.
    """

    def __init__(self, chunk_size: int = 16384) -> None:
        self.chunk_size = chunk_size

    async def decompress_stream(
        self,
        stream: AsyncIterator[bytes],
    ) -> AsyncIterator[str]:
        """
        Accepts an async iterator of compressed .zst chunks and yields decoded
        UTF-8 text chunks.

        This implementation:

        - uses an incremental UTF-8 decoder so multi-byte characters can span chunks
        - treats any zstd.ZstdError as corruption and raises ValueError
        - supports only single-frame .zst streams (python-zstandard limitation)
        - emits fixed-size character chunks based on chunk_size
        """
        dctx = zstd.ZstdDecompressor()
        decompressor = dctx.decompressobj()

        decoder = codecs.getincrementaldecoder("utf-8")()
        buffer = ""

        async for chunk in stream:
            if not chunk:
                continue

            try:
                out = decompressor.decompress(chunk)
            except zstd.ZstdError as exc:
                raise ValueError(f"Corrupt zstd stream: {exc}") from exc

            if out:
                text_chunk = decoder.decode(out)
                if text_chunk:
                    buffer += text_chunk

            while len(buffer) >= self.chunk_size:
                yield buffer[: self.chunk_size]
                buffer = buffer[self.chunk_size :]

            await asyncio.sleep(0)

        # Flush remaining compressed data from zstd
        try:
            final_out = decompressor.flush()
        except zstd.ZstdError as exc:
            raise ValueError(f"Corrupt zstd stream during flush: {exc}") from exc

        if final_out:
            buffer += decoder.decode(final_out)

        # Flush any remaining partial UTF-8 sequence
        buffer += decoder.decode(b"", final=True)

        if buffer:
            yield buffer

    async def decompress_lines(
        self,
        stream: AsyncIterator[bytes],
    ) -> AsyncIterator[str]:
        """
        High-level interface: yields complete NDJSON lines one by one.
        Uses a rolling 'pending' fragment to avoid O(N^2) re-slicing.
        """
        pending = ""

        async for chunk in self.decompress_stream(stream):
            if not chunk:
                continue

            combined = pending + chunk
            lines = combined.split("\n")
            pending = lines.pop()

            for line in lines:
                yield line

        if pending:
            yield pending
