from __future__ import annotations

from typing import AsyncIterator

from fastapi import APIRouter, HTTPException, UploadFile
from fastapi.responses import StreamingResponse

from app.services.stream_service import StreamService

router = APIRouter()


async def _iter_file_chunks(
    file: UploadFile, chunk_size: int = 16384
) -> AsyncIterator[bytes]:
    """
    Async generator that yields file chunks without loading the whole file into memory.
    """
    while True:
        chunk = await file.read(chunk_size)
        if not chunk:
            break
        yield chunk


@router.post("/upload", summary="Upload a .zst file and stream decompressed NDJSON")
async def upload_zst(file: UploadFile):
    """
    Accepts a .zst file upload and streams decompressed NDJSON lines.
    """

    if not file.filename.endswith(".zst"):
        raise HTTPException(status_code=400, detail="File must have .zst extension")

    service = StreamService()

    async def ndjson_stream():
        byte_stream = _iter_file_chunks(file)
        async for line in service.process_as_ndjson(byte_stream):
            yield line + "\n"

    return StreamingResponse(
        ndjson_stream(),
        media_type="application/x-ndjson",
    )
