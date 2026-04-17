from __future__ import annotations

from typing import Any, AsyncIterator

from app.core.decompressor import AsyncZstdDecompressor
from app.core.filters import NDJSONFilter
from app.core.ndjson_stream import iter_bytes_from_file
from app.core.transformers import NDJSONTransformer
from app.core.validators import NDJSONValidator


class StreamService:
    """
    High-level orchestrator for NDJSON processing:
    - async .zst decompression
    - NDJSON line splitting
    - Pydantic validation
    - filtering
    - transformation
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

    async def process(
        self,
        byte_stream: AsyncIterator[bytes],
    ) -> AsyncIterator[dict[str, Any]]:
        """
        Full pipeline:
        compressed bytes -> decompressed lines -> validated objects -> filtered -> transformed
        """

        # 1. decompress .zst into NDJSON lines
        lines = self.decompressor.decompress_lines(byte_stream)

        # 2. validate NDJSON objects
        validated = self.validator.validate_stream(lines)

        # 3. apply filters
        filtered = self.filters.filter_stream(validated)

        # 4. apply transformations
        transformed = self.transformers.apply_stream(filtered)

        # 5. yield final objects
        async for obj in transformed:
            yield obj

    async def process_as_ndjson(
        self,
        byte_stream: AsyncIterator[bytes],
    ) -> AsyncIterator[str]:
        """
        Same as process(), but yields serialized NDJSON lines.
        """

        async for obj in self.process(byte_stream):
            # obj is a dict at this point
            yield self._to_ndjson(obj)

    @staticmethod
    def _to_ndjson(obj: dict[str, Any]) -> str:
        """
        Serialize a dict to a compact NDJSON line.
        """
        import orjson

        return orjson.dumps(obj).decode("utf-8")

    async def process_file(self, path: str):
        """
        Convenience wrapper:
        read a .zst file from disk and run it through the full pipeline.
        """
        byte_stream = iter_bytes_from_file(path)
        async for obj in self.process(byte_stream):
            yield obj
