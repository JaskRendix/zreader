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
        Accepts an async iterator of compressed .zst chunks and yields
        decompressed text chunks.

        Design notes
        ------------
        - The raw decompressed bytes are accumulated in a bytearray and only
          decoded to str once we have a complete UTF-8 sequence.  Slicing the
          bytearray at a fixed byte offset can split a multi-byte character,
          so we decode the whole buffer each iteration and split on character
          count instead.
        - zstd's decompressobj raises ZstdError if you call .decompress()
          after the compressed frame has been fully consumed.  We guard with
          a try/except so the caller never sees that internal error.
        - Raises UnicodeDecodeError if the stream is not valid UTF-8.
        """
        dctx = zstd.ZstdDecompressor()
        decompressor = dctx.decompressobj()
        raw_buffer = bytearray()
        text_buffer = ""

        async for chunk in stream:
            if not chunk:
                continue

            try:
                out = decompressor.decompress(chunk)
            except zstd.ZstdError as exc:
                # ZstdError fires both for corrupt data AND for feeding bytes
                # after a frame has already ended cleanly.  Re-raise only when
                # we haven't produced any output yet (corrupt input); otherwise
                # treat it as a normal end-of-stream signal.
                if not text_buffer and not raw_buffer:
                    raise
                break

            if out:
                raw_buffer.extend(out)

            # Decode in one pass to avoid splitting multi-byte characters.
            if raw_buffer:
                text_buffer += raw_buffer.decode("utf-8")
                raw_buffer.clear()

            # Yield complete chunk_size *character* slices.
            while len(text_buffer) >= self.chunk_size:
                yield text_buffer[: self.chunk_size]
                text_buffer = text_buffer[self.chunk_size :]

            await asyncio.sleep(0)

        # Decode any remaining raw bytes from the last iteration.
        if raw_buffer:
            text_buffer += raw_buffer.decode("utf-8")

        if text_buffer:
            yield text_buffer

    async def decompress_lines(
        self,
        stream: AsyncIterator[bytes],
    ) -> AsyncIterator[str]:
        """
        High-level interface: yields complete NDJSON lines one by one.
        Delegates to decompress_stream so chunking logic lives in one place.
        """
        buffer = ""
        async for chunk in self.decompress_stream(stream):
            buffer += chunk
            while "\n" in buffer:
                line, buffer = buffer.split("\n", 1)
                yield line

        # Yield any trailing content with no terminating newline.
        if buffer:
            yield buffer
