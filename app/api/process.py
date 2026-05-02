from __future__ import annotations

from typing import AsyncGenerator

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse

from app.config import settings
from app.core.ndjson_stream import AsyncByteStream
from app.services.stream_service import StreamService

router = APIRouter()


@router.post("/process", summary="Stream NDJSON from a .zst request body")
async def process_zst(request: Request) -> StreamingResponse:
    """
    Stream NDJSON from a .zst-compressed request body.

    - Rejects payloads exceeding the configured max size.
    - Decompresses and streams NDJSON lines.
    - Emits a single NDJSON error object on processing failure.
    """
    content_length = request.headers.get("content-length")
    if content_length and int(content_length) > settings.max_upload_size_bytes:
        raise HTTPException(
            status_code=413,
            detail=f"Payload too large. Max {settings.max_upload_size_mb} MB.",
        )

    byte_stream = AsyncByteStream(request)
    service = StreamService(chunk_size=settings.chunk_size)

    async def ndjson_stream() -> AsyncGenerator[str, None]:
        try:
            async for line in service.process_as_ndjson(byte_stream):
                yield line + "\n"
        except Exception as exc:
            yield f'{{"error": "Stream processing failed: {exc}"}}\n'

    return StreamingResponse(ndjson_stream(), media_type="application/x-ndjson")
