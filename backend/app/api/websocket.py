import asyncio
import logging

import orjson
from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from app.core.redis import redis_client

logger = logging.getLogger("websocket")
router = APIRouter(tags=["websocket"])


@router.websocket("/ws/live")
async def ws_live(websocket: WebSocket):
    await websocket.accept()
    last_payload = None
    try:
        while True:
            raw = await redis_client.get("dashboard:snapshot")
            if raw is not None and raw != last_payload:
                await websocket.send_text(raw if isinstance(raw, str) else raw.decode())
                last_payload = raw
            await asyncio.sleep(2)
    except WebSocketDisconnect:
        logger.info("WebSocket client disconnected")
    except Exception as e:  # noqa: BLE001
        logger.error("WebSocket error: %s", e)
