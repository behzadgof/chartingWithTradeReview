"""Firebase cloud sync routes: /api/firebase/push, pull, test."""

from __future__ import annotations

import datetime
from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

router = APIRouter()


def _state_dir() -> Path | None:
    from charts.server.app import server_state
    raw = server_state.state_dir
    return Path(raw) if raw else None


def _get_firebase_config() -> dict | None:
    from charts.server.state import load_state

    cfg = load_state(_state_dir(), "orb_firebase")
    if not cfg or not cfg.get("projectId") or not cfg.get("databaseSecret"):
        return None
    return cfg


@router.post("/api/firebase/push")
def firebase_push():
    from charts.server.state import load_all_state
    from charts.server.firebase import firebase_write

    cfg = _get_firebase_config()
    if not cfg:
        return JSONResponse({"ok": False, "error": "Firebase not configured"}, 400)
    all_state = load_all_state(_state_dir())
    all_state.pop("orb_firebase", None)
    all_state["_lastModified"] = datetime.datetime.now(
        datetime.timezone.utc
    ).isoformat()
    ok = firebase_write(cfg["projectId"], "state", cfg["databaseSecret"], all_state)
    return JSONResponse({"ok": ok})


@router.post("/api/firebase/pull")
def firebase_pull():
    from charts.server.state import save_state
    from charts.server.firebase import firebase_read

    cfg = _get_firebase_config()
    if not cfg:
        return JSONResponse({"ok": False, "error": "Firebase not configured"}, 400)
    data = firebase_read(cfg["projectId"], "state", cfg["databaseSecret"])
    if data is None:
        return JSONResponse({"ok": False, "error": "Failed to read from Firebase"})
    for key, value in data.items():
        if key.startswith("_"):
            continue
        save_state(_state_dir(), key, value)
    return JSONResponse({"ok": True, "state": data})


@router.post("/api/firebase/test")
async def firebase_test(request: Request):
    from charts.server.firebase import firebase_read

    try:
        body = await request.json()
        project_id = body.get("projectId", "")
        secret = body.get("databaseSecret", "")
    except Exception:
        return JSONResponse({"error": "Invalid JSON"}, 400)
    if not project_id or not secret:
        return JSONResponse({"ok": False, "error": "Missing projectId or databaseSecret"})
    result = firebase_read(project_id, "", secret)
    if result is None:
        return JSONResponse({"ok": False, "error": "Connection failed — check project ID and secret"})
    return JSONResponse({"ok": True})
