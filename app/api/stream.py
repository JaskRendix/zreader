from __future__ import annotations

from typing import AsyncIterator

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from starlette.websockets import WebSocketState

from app.config import settings
from app.services.stream_service import StreamService

router = APIRouter()


async def _iter_websocket_bytes(
    ws: WebSocket,
) -> AsyncIterator[bytes]:
    try:
        while True:
            data = await ws.receive_bytes()
            if data:
                yield data
    except WebSocketDisconnect:
        return


@router.websocket("/stream")
async def stream_zst(ws: WebSocket) -> None:
    await ws.accept()
    service = StreamService(chunk_size=settings.chunk_size)

    try:
        byte_stream = _iter_websocket_bytes(ws)
        async for line in service.process_as_ndjson(byte_stream):
            if ws.client_state != WebSocketState.CONNECTED:
                break
            await ws.send_text(line)
    except WebSocketDisconnect:
        return
    except Exception as exc:
        if ws.client_state == WebSocketState.CONNECTED:
            await ws.send_text(f'{{"error": "{exc}"}}')
    finally:
        if ws.client_state == WebSocketState.CONNECTED:
            await ws.close()
