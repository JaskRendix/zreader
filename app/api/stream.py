from __future__ import annotations

from typing import AsyncIterator

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from app.services.stream_service import StreamService

router = APIRouter()


async def _iter_websocket_bytes(
    ws: WebSocket, chunk_size: int = 16384
) -> AsyncIterator[bytes]:
    """
    Convert incoming WebSocket binary messages into an async byte stream.
    Each message is treated as a chunk of the compressed .zst file.
    """
    try:
        while True:
            data = await ws.receive_bytes()
            if data:
                yield data
    except WebSocketDisconnect:
        return


@router.websocket("/stream")
async def stream_zst(ws: WebSocket):
    """
    WebSocket endpoint for streaming .zst-compressed NDJSON.
    Client sends binary chunks; server streams decompressed NDJSON lines.
    """

    await ws.accept()
    service = StreamService()

    try:
        byte_stream = _iter_websocket_bytes(ws)

        async for line in service.process_as_ndjson(byte_stream):
            await ws.send_text(line)

    except WebSocketDisconnect:
        # Client disconnected; nothing to clean up
        return
