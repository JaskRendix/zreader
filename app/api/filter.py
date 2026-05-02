from __future__ import annotations

from typing import Any, AsyncGenerator

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from app.config import settings
from app.core.filters import (
    NDJSONFilter,
    field_equals,
    field_exists,
    field_in,
    field_not_exists,
    numeric_range,
)
from app.core.ndjson_stream import AsyncByteStream
from app.services.stream_service import StreamService

router = APIRouter()


class FilterSpec(BaseModel):
    equals: dict[str, Any] | None = None
    in_set: dict[str, list[Any]] | None = None
    exists: list[str] | None = None
    not_exists: list[str] | None = None
    numeric: dict[str, dict[str, float]] | None = None


def build_filter(spec: FilterSpec) -> NDJSONFilter:
    f = NDJSONFilter()

    if spec.equals:
        for field, value in spec.equals.items():
            f.add(field_equals(field, value))

    if spec.in_set:
        for field, values in spec.in_set.items():
            f.add(field_in(field, values))

    if spec.exists:
        for field in spec.exists:
            f.add(field_exists(field))

    if spec.not_exists:
        for field in spec.not_exists:
            f.add(field_not_exists(field))

    if spec.numeric:
        for field, bounds in spec.numeric.items():
            f.add(
                numeric_range(
                    field,
                    min_val=bounds.get("min"),
                    max_val=bounds.get("max"),
                )
            )

    return f


@router.post("/filter", summary="Apply filters to a .zst NDJSON stream")
async def filter_zst(request: Request, spec: FilterSpec) -> StreamingResponse:
    """
    Apply a set of field-based filters to a .zst-compressed NDJSON stream.

    - Validates the JSON filter specification.
    - Rejects requests exceeding the configured max payload size.
    - Streams decompressed NDJSON lines through the filter pipeline.
    - Emits filtered lines as NDJSON.
    - On processing errors, returns a single NDJSON error object instead of failing the stream.
    """
    content_length = request.headers.get("content-length")
    if content_length and int(content_length) > settings.max_upload_size_bytes:
        raise HTTPException(
            status_code=413,
            detail=f"Payload too large. Max {settings.max_upload_size_mb} MB.",
        )

    byte_stream = AsyncByteStream(request)
    filters = build_filter(spec)
    service = StreamService(filters=filters, chunk_size=settings.chunk_size)

    async def ndjson_stream() -> AsyncGenerator[str, None]:
        try:
            async for line in service.process_as_ndjson(byte_stream):
                yield line + "\n"
        except Exception as exc:
            yield f'{{"error": "Filter processing failed: {exc}"}}\n'

    return StreamingResponse(ndjson_stream(), media_type="application/x-ndjson")
