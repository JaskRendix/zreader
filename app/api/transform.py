from __future__ import annotations

from typing import Any, AsyncGenerator

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from app.config import settings
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

# Safe built-in operations exposed via the map spec.
# Extending this dict is the only way to add new operations —
# no arbitrary code execution is possible.
_SAFE_MAP_OPS: dict[str, Any] = {
    "upper": str.upper,
    "lower": str.lower,
    "strip": str.strip,
    "int": int,
    "float": float,
    "str": str,
    "bool": bool,
    "abs": abs,
    "negate": lambda x: -x,
    "not": lambda x: not x,
}


class TransformSpec(BaseModel):
    rename: dict[str, str] | None = None
    drop: list[str] | None = None
    add: dict[str, Any] | None = None
    map: dict[str, str] | None = (
        None  # { field: op_name } — op must be in _SAFE_MAP_OPS
    )


def build_transformer(spec: TransformSpec) -> NDJSONTransformer:
    t = NDJSONTransformer(on_error="log")

    if spec.rename:
        for old, new in spec.rename.items():
            t.add(rename_field(old, new))

    if spec.drop:
        t.add(drop_fields(spec.drop))

    if spec.add:
        for field, value in spec.add.items():
            t.add(add_field(field, value))

    if spec.map:
        for field, op_name in spec.map.items():
            fn = _SAFE_MAP_OPS.get(op_name)
            if fn is None:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        f"Unknown map operation '{op_name}' for field '{field}'. "
                        f"Allowed: {sorted(_SAFE_MAP_OPS)}"
                    ),
                )
            t.add(map_field(field, fn))

    return t


@router.post("/transform", summary="Apply transformations to a .zst NDJSON stream")
async def transform_zst(request: Request, spec: TransformSpec) -> StreamingResponse:
    """
    Apply field transformations to a .zst-compressed NDJSON stream.

    - Validates payload size.
    - Builds a transformer using safe, predefined map operations.
    - Streams transformed NDJSON lines.
    - Emits a single NDJSON error object on processing failure.
    """
    content_length = request.headers.get("content-length")
    if content_length and int(content_length) > settings.max_upload_size_bytes:
        raise HTTPException(
            status_code=413,
            detail=f"Payload too large. Max {settings.max_upload_size_mb} MB.",
        )

    byte_stream = AsyncByteStream(request)
    try:
        transformer = build_transformer(spec)
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    service = StreamService(transformers=transformer, chunk_size=settings.chunk_size)

    async def ndjson_stream() -> AsyncGenerator[str, None]:
        try:
            async for line in service.process_as_ndjson(byte_stream):
                yield line + "\n"
        except Exception as exc:
            yield f'{{"error": "Transform processing failed: {exc}"}}\n'

    return StreamingResponse(ndjson_stream(), media_type="application/x-ndjson")
