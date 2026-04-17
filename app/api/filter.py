from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

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
    numeric: dict[str, dict[str, float]] | None = None  # { field: {min: x, max: y} }


def build_filter(spec: FilterSpec) -> NDJSONFilter:
    """
    Convert a FilterSpec into an NDJSONFilter with concrete predicates.
    """
    f = NDJSONFilter()

    # field == value
    if spec.equals:
        for field, value in spec.equals.items():
            f.add(field_equals(field, value))

    # field in [...]
    if spec.in_set:
        for field, values in spec.in_set.items():
            f.add(field_in(field, values))

    # field exists
    if spec.exists:
        for field in spec.exists:
            f.add(field_exists(field))

    # field not exists
    if spec.not_exists:
        for field in spec.not_exists:
            f.add(field_not_exists(field))

    # numeric ranges
    if spec.numeric:
        for field, bounds in spec.numeric.items():
            f.add(
                numeric_range(
                    field,
                    min=bounds.get("min"),
                    max=bounds.get("max"),
                )
            )

    return f


@router.post("/filter", summary="Apply filters to a .zst NDJSON stream")
async def filter_zst(request: Request, spec: FilterSpec):
    """
    Accepts:
      - JSON filter specification in the request body
      - .zst-compressed NDJSON stream in the request body (binary)

    Returns:
      - NDJSON stream of filtered objects
    """

    byte_stream = AsyncByteStream(request)
    filters = build_filter(spec)
    service = StreamService(filters=filters)

    async def ndjson_stream():
        async for line in service.process_as_ndjson(byte_stream):
            yield line + "\n"

    return StreamingResponse(
        ndjson_stream(),
        media_type="application/x-ndjson",
    )
