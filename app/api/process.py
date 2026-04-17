from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from app.core.ndjson_stream import AsyncByteStream
from app.services.stream_service import StreamService

router = APIRouter()


@router.post("/process", summary="Stream NDJSON from request body (.zst compressed)")
async def process_zst(request: Request):
    """
    Accepts a .zst-compressed NDJSON stream in the request body.
    Streams decompressed NDJSON lines back to the client.
    """

    byte_stream = AsyncByteStream(request)
    service = StreamService()

    async def ndjson_stream():
        async for line in service.process_as_ndjson(byte_stream):
            yield line + "\n"

    return StreamingResponse(
        ndjson_stream(),
        media_type="application/x-ndjson",
    )
