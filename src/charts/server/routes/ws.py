"""WebSocket endpoint for real-time streaming data."""

from __future__ import annotations

import asyncio
from datetime import datetime

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

    # Queue for pushing quotes to this client
    quote_queue: asyncio.Queue = asyncio.Queue()

    def _on_quote(symbol: str, price: float, size: float, ts: datetime) -> None:
        quote_queue.put_nowait({
            "type": "quote",
            "symbol": symbol,
            "price": price,
            "size": size,
            "timestamp": ts.isoformat(),
        })

    # Register callback if streaming is active
    if sm:
        sm.on_quote(_on_quote)

    async def _push_quotes() -> None:
        """Forward queued quotes to the WebSocket client."""
        try:
            while True:
                msg = await quote_queue.get()
                await websocket.send_json(msg)
        except (WebSocketDisconnect, Exception):
            pass

    push_task = asyncio.create_task(_push_quotes())

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
    finally:
        push_task.cancel()
