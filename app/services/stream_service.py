from __future__ import annotations

import json
import logging
from typing import Any, AsyncIterator

import orjson

from app.core.decompressor import AsyncZstdDecompressor
from app.core.filters import NDJSONFilter
from app.core.ndjson_stream import NDJSONStream, iter_bytes_from_file
from app.core.transformers import NDJSONTransformer
from app.core.validators import NDJSONValidator

logger = logging.getLogger(__name__)


class StreamService:
    """
    High-level orchestrator for the NDJSON processing pipeline:
      compressed bytes → decompressed text → line splitting → validation
      → filtering → transformation → output

    Parameters
    ----------
    chunk_size:
        Decompression buffer size in bytes.
    validator:
        NDJSONValidator instance. Defaults to a permissive validator that
        accepts any JSON object.
    filters:
        NDJSONFilter instance. Defaults to no filters (pass-through).
    transformers:
        NDJSONTransformer instance. Defaults to no transforms (pass-through).
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
        self.line_splitter = NDJSONStream()
        self.validator = validator or NDJSONValidator()
        self.filters = filters or NDJSONFilter()
        self.transformers = transformers or NDJSONTransformer()

    async def process(
        self,
        byte_stream: AsyncIterator[bytes],
    ) -> AsyncIterator[dict[str, Any]]:
        """
        Full pipeline: compressed bytes → validated, filtered, transformed dicts.

        Steps
        -----
        1. Decompress .zst chunks into text chunks (AsyncZstdDecompressor).
        2. Split text chunks into individual NDJSON lines (NDJSONStream).
        3. Validate each line with Pydantic (NDJSONValidator).
        4. Apply filter predicates (NDJSONFilter).
        5. Apply transform functions (NDJSONTransformer).
        """
        # Step 1+2: decompress then split into lines.
        # decompress_stream yields str chunks; NDJSONStream.iter_lines consumes them.
        text_chunks = self.decompressor.decompress_stream(byte_stream)
        lines = self.line_splitter.iter_lines(text_chunks)

        # Step 3–5: validate → filter → transform, all lazy async generators.
        validated = self.validator.validate_stream(lines)
        filtered = self.filters.filter_stream(validated)
        transformed = self.transformers.apply_stream(filtered)

        async for obj in transformed:
            yield obj

    async def process_as_ndjson(
        self,
        byte_stream: AsyncIterator[bytes],
    ) -> AsyncIterator[str]:
        """
        Same as process(), but yields compact serialised NDJSON lines.
        """
        async for obj in self.process(byte_stream):
            yield self._to_ndjson(obj)

    async def process_file(
        self,
        path: str,
    ) -> AsyncIterator[dict[str, Any]]:
        """
        Convenience wrapper: read a local .zst file and run the full pipeline.

        Prefer FileService.process_file() from the API layer; this method
        exists for quick scripting and tests.
        """
        byte_stream = iter_bytes_from_file(path)
        async for obj in self.process(byte_stream):
            yield obj

    @staticmethod
    def _to_ndjson(obj: dict[str, Any]) -> str:
        """Serialise a dict to a compact NDJSON line using orjson."""
        return orjson.dumps(obj).decode("utf-8")
