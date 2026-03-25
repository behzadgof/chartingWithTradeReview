"""UI state persistence routes: /api/state, /api/state/all."""

from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, Query, Request
from fastapi.responses import JSONResponse

router = APIRouter()


def _state_dir() -> Path | None:
    from charts.server.app import server_state
    raw = server_state.state_dir
    return Path(raw) if raw else None


@router.get("/api/state")
def state_single(key: str | None = Query(None)):
    from charts.server.state import load_state

    if not key:
        return JSONResponse({"error": "Missing key"}, 400)
    value = load_state(_state_dir(), key)
    return JSONResponse({"key": key, "value": value})


@router.get("/api/state/all")
def state_all():
    from charts.server.state import load_all_state

    return JSONResponse(load_all_state(_state_dir()))


@router.post("/api/state")
async def state_save(request: Request):
    from charts.server.state import save_state

    try:
        body = await request.json()
        key = body.get("key")
        value = body.get("value")
    except Exception:
        return JSONResponse({"error": "Invalid JSON"}, 400)
    if not key:
        return JSONResponse({"error": "Missing key"}, 400)
    ok = save_state(_state_dir(), key, value)
    return JSONResponse({"ok": ok}, 200 if ok else 500)


@router.post("/api/state/delete")
async def state_delete(request: Request):
    from charts.server.state import delete_state

    try:
        body = await request.json()
        key = body.get("key")
    except Exception:
        return JSONResponse({"error": "Invalid JSON"}, 400)
    if not key:
        return JSONResponse({"error": "Missing key"}, 400)
    ok = delete_state(_state_dir(), key)
    return JSONResponse({"ok": ok})
