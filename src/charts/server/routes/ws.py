"""WebSocket endpoint for real-time streaming data."""

from __future__ import annotations

from fastapi import APIRouter
from starlette.websockets import WebSocket, WebSocketDisconnect

router = APIRouter()


@router.websocket("/ws/stream")
async def ws_stream(websocket: WebSocket) -> None:
    await websocket.accept()

    from charts.server.app import server_state

    sm = server_state.stream_manager

    # Send initial status
    await websocket.send_json({
        "type": "status",
        "streaming": sm is not None and sm.is_connected,
    })

    try:
        while True:
            data = await websocket.receive_json()

            if "subscribe" in data:
                symbols = data["subscribe"]
                channels = data.get("channels", ["quotes"])
                if sm:
                    await sm.subscribe(symbols, channels)
                await websocket.send_json({
                    "type": "subscribed",
                    "symbols": symbols,
                })

            elif "unsubscribe" in data:
                symbols = data["unsubscribe"]
                if sm:
                    await sm.unsubscribe(symbols)
                await websocket.send_json({
                    "type": "unsubscribed",
                    "symbols": symbols,
                })

    except WebSocketDisconnect:
        pass
