from __future__ import annotations

from collections.abc import Callable
from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from app.core.ndjson_stream import AsyncByteStream
from app.core.transformers import (
    NDJSONTransformer,
    add_field,
    drop_fields,
    map_field,
    rename_field,
)
from app.services.stream_service import StreamService

router = APIRouter()


class TransformSpec(BaseModel):
    rename: dict[str, str] | None = None  # { old: new }
    drop: list[str] | None = None  # [field1, field2]
    add: dict[str, Any] | None = None  # { field: value }
    map: dict[str, str] | None = None  # { field: "lambda x: ..." } (simple expressions)


def build_transformer(spec: TransformSpec) -> NDJSONTransformer:
    """
    Convert a TransformSpec into an NDJSONTransformer with concrete functions.
    """
    t = NDJSONTransformer()

    # rename fields
    if spec.rename:
        for old, new in spec.rename.items():
            t.add(rename_field(old, new))

    # drop fields
    if spec.drop:
        t.add(drop_fields(spec.drop))

    # add constant fields
    if spec.add:
        for field, value in spec.add.items():
            t.add(add_field(field, value))

    # map fields using simple Python expressions
    # WARNING: intentionally restricted to safe eval of simple lambdas
    if spec.map:
        for field, expr in spec.map.items():
            # Example: "lambda x: x * 2"
            try:
                fn: Callable[[Any], Any] = eval(expr, {"__builtins__": {}})
            except Exception:
                raise ValueError(f"Invalid map expression for field '{field}': {expr}")

            if not callable(fn):
                raise ValueError(f"Map expression for field '{field}' must be callable")

            t.add(map_field(field, fn))

    return t


@router.post("/transform", summary="Apply transformations to a .zst NDJSON stream")
async def transform_zst(request: Request, spec: TransformSpec):
    """
    Accepts:
      - JSON transformation specification in the request body
      - .zst-compressed NDJSON stream in the request body (binary)

    Returns:
      - NDJSON stream of transformed objects
    """

    byte_stream = AsyncByteStream(request)
    transformer = build_transformer(spec)
    service = StreamService(transformers=transformer)

    async def ndjson_stream():
        async for line in service.process_as_ndjson(byte_stream):
            yield line + "\n"

    return StreamingResponse(
        ndjson_stream(),
        media_type="application/x-ndjson",
    )
