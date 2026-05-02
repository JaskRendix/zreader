from __future__ import annotations

from typing import AsyncGenerator, AsyncIterator

from fastapi import APIRouter, HTTPException, UploadFile
from fastapi.responses import StreamingResponse

from app.config import settings
from app.services.stream_service import StreamService

router = APIRouter()


async def _iter_file_chunks(
    file: UploadFile,
    chunk_size: int = 16384,
) -> AsyncIterator[bytes]:
    while True:
        chunk = await file.read(chunk_size)
        if not chunk:
            break
        yield chunk


@router.post("/upload", summary="Upload a .zst file and stream decompressed NDJSON")
async def upload_zst(file: UploadFile) -> StreamingResponse:
    """
    Handle .zst file uploads.

    - Validates filename, media type, and file size.
    - Streams decompressed NDJSON lines.
    - Emits a single NDJSON error object on processing failure.
    """
    if not file.filename or not file.filename.endswith(".zst"):
        raise HTTPException(status_code=400, detail="File must have a .zst extension.")

    if file.content_type not in (
        "application/zstd",
        "application/octet-stream",
        "application/x-zstd",
    ):
        raise HTTPException(
            status_code=415,
            detail="Unsupported media type. Send a .zst compressed file.",
        )

    if file.size and file.size > settings.max_upload_size_bytes:
        raise HTTPException(
            status_code=413,
            detail=f"File too large. Max {settings.max_upload_size_mb} MB.",
        )

    service = StreamService(chunk_size=settings.chunk_size)

    async def ndjson_stream() -> AsyncGenerator[str, None]:
        try:
            byte_stream = _iter_file_chunks(file, chunk_size=settings.chunk_size)
            async for line in service.process_as_ndjson(byte_stream):
                yield line + "\n"
        except Exception as exc:
            yield f'{{"error": "Upload processing failed: {exc}"}}\n'

    return StreamingResponse(ndjson_stream(), media_type="application/x-ndjson")
