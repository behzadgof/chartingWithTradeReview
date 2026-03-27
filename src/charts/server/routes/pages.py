"""Page-serving routes: /, /market, /trades."""

from __future__ import annotations

import json

from fastapi import APIRouter
from fastapi.responses import HTMLResponse, RedirectResponse

from charts.server.routes._template import _TEMPLATES_DIR, _load_template

router = APIRouter()


@router.get("/", response_class=HTMLResponse)
def root():
    from charts.server.app import server_state

    if not server_state.trades:
        return RedirectResponse("/market", status_code=302)

    raw = (_TEMPLATES_DIR / "app.html").read_text(encoding="utf-8")
    html = raw.replace("{{HAS_TRADES}}", "true")
    return HTMLResponse(html)


@router.get("/market", response_class=HTMLResponse)
def market():
    return HTMLResponse(_load_template("market.html"))


@router.get("/trades", response_class=HTMLResponse)
def trades():
    from charts.server.app import server_state

    html = _load_template("trades.html")

    if server_state.trades is not None:
        trades_json = json.dumps([t.to_dict() for t in server_state.trades])
        html = html.replace(
            "var __TRADES_INLINE__ = null;",
            f"var __TRADES_INLINE__ = {trades_json};",
        )
    if server_state.summary is not None:
        summary_json = json.dumps(server_state.summary.to_dict())
        html = html.replace(
            "var __SUMMARY_INLINE__ = null;",
            f"var __SUMMARY_INLINE__ = {summary_json};",
        )
    if server_state.bars_by_date is not None:
        bars_json = json.dumps(server_state.bars_by_date)
        html = html.replace(
            "var __BARS_INLINE__ = null;",
            f"var __BARS_INLINE__ = {bars_json};",
        )

    return HTMLResponse(html)
