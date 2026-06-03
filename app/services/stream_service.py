from __future__ import annotations

import logging
from typing import Any, AsyncIterator

import orjson

from app.core.decompressor import AsyncZstdDecompressor
from app.core.filters import NDJSONFilter
from app.core.ndjson_stream import iter_bytes_from_file
from app.core.transformers import NDJSONTransformer
from app.core.validators import NDJSONValidator, ValidationStats

logger = logging.getLogger(__name__)


class StreamService:
    """
    High-level orchestrator for the NDJSON processing pipeline.

    Pipeline:
        compressed bytes
            → decompressed UTF‑8 lines
            → validated JSON objects
            → filtered objects
            → transformed objects
            → output dicts or NDJSON strings

    All stages are async generators and fully streaming.
    """

    def __init__(
        self,
        *,
        chunk_size: int = 16384,
        validator: NDJSONValidator | None = None,
        filters: NDJSONFilter | None = None,
        transformers: NDJSONTransformer | None = None,
    ) -> None:
        self.decompressor = AsyncZstdDecompressor(chunk_size=chunk_size)
        self.validator = validator or NDJSONValidator()
        self.filters = filters or NDJSONFilter()
        self.transformers = transformers or NDJSONTransformer()

    async def process_stream(
        self,
        byte_stream: AsyncIterator[bytes],
        stats: ValidationStats | None = None,
    ) -> AsyncIterator[dict[str, Any]]:
        """
        Run the full NDJSON pipeline on an async byte stream.

        Parameters
        ----------
        byte_stream:
            Async iterator yielding compressed .zst byte chunks.
        stats:
            Optional ValidationStats instance for per-request metric isolation.

        Yields
        ------
        dict[str, Any]
            Validated, filtered, transformed JSON objects.
        """
        # Step 1–2: decompress and yield UTF‑8 lines
        lines = self.decompressor.decompress_lines(byte_stream)

        # Step 3: validate with optional per-request stats
        validated = self.validator.validate_stream(lines, stats=stats)

        # Step 4: apply filters
        filtered = self.filters.filter_stream(validated)

        # Step 5: apply transformations
        transformed = self.transformers.apply_stream(filtered)

        async for obj in transformed:
            yield obj

    async def process_as_ndjson(
        self,
        byte_stream: AsyncIterator[bytes],
        stats: ValidationStats | None = None,
    ) -> AsyncIterator[str]:
        """
        Same as process_stream(), but yields compact NDJSON strings
        WITHOUT a trailing newline. Tests explicitly require this behavior.
        """
        async for obj in self.process_stream(byte_stream, stats=stats):
            yield orjson.dumps(obj).decode("utf-8")

    async def process_file(
        self,
        path: str,
        stats: ValidationStats | None = None,
    ) -> AsyncIterator[dict[str, Any]]:
        """
        Convenience wrapper: read a local .zst file and run the full pipeline.
        """
        byte_stream = iter_bytes_from_file(path)
        async for obj in self.process_stream(byte_stream, stats=stats):
            yield obj

    async def process_file_as_ndjson(
        self,
        path: str,
        stats: ValidationStats | None = None,
    ) -> AsyncIterator[str]:
        """
        Same as process_file(), but yields NDJSON strings WITHOUT a trailing newline.
        """
        byte_stream = iter_bytes_from_file(path)
        async for obj in self.process_stream(byte_stream, stats=stats):
            yield orjson.dumps(obj).decode("utf-8")

    @staticmethod
    def _to_ndjson(obj: dict[str, Any]) -> str:
        """
        Serialize a dict to a compact NDJSON line WITH a trailing newline.

        This is used for streaming API responses, not for file-based NDJSON.
        """
        return orjson.dumps(obj).decode("utf-8") + "\n"
