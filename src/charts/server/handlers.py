"""HTTP request handlers for the chart server.

Routes:
    GET  /                   → redirect to /market or /trades
    GET  /market             → market.html (asset chart viewer)
    GET  /trades             → trades.html (trade review)
    GET  /api/symbols        → JSON symbol list
    GET  /api/bars           → JSON bars (params: symbol, start, end, timeframe)
    GET  /api/quotes/live    → JSON live quotes (params: symbols)
    GET  /api/trades         → JSON trade records
    GET  /api/trades/summary → JSON summary stats
    GET  /api/state?key=...  → load a single UI state key
    GET  /api/state/all      → bulk-load all UI state keys
    POST /api/state          → save a UI state key  (body: {"key":…, "value":…})
    POST /api/state/delete   → delete a UI state key (body: {"key":…})
"""

from __future__ import annotations

import http.server
import json
import urllib.parse
from pathlib import Path
from typing import Any

from charts.server.data import (
    fetch_bars,
    fetch_live_quotes,
    fetch_quotes,
    get_available_symbols,
)


# Template directory (relative to this file's package)
_TEMPLATES_DIR = Path(__file__).resolve().parent.parent / "templates"


def _load_template(name: str) -> str:
    """Read an HTML template and perform shared-code substitution."""
    template_path = _TEMPLATES_DIR / name
    html = template_path.read_text(encoding="utf-8")

    # Substitute vendored JS library
    lib_path = _TEMPLATES_DIR / "lightweight-charts.js"
    if lib_path.exists():
        html = html.replace("{{LIB_JS}}", lib_path.read_text(encoding="utf-8"))
    else:
        html = html.replace("{{LIB_JS}}", "/* lightweight-charts.js not found */")

    # Substitute shared CSS
    css_path = _TEMPLATES_DIR / "_shared.css"
    if css_path.exists():
        html = html.replace("{{SHARED_CSS}}", css_path.read_text(encoding="utf-8"))
    else:
        html = html.replace("{{SHARED_CSS}}", "")

    # Substitute shared indicator JS
    js_path = _TEMPLATES_DIR / "_indicators.js"
    if js_path.exists():
        html = html.replace(
            "{{SHARED_INDICATORS_JS}}", js_path.read_text(encoding="utf-8"),
        )
    else:
        html = html.replace("{{SHARED_INDICATORS_JS}}", "")

    # Substitute drawing tools CSS
    drawing_css_path = _TEMPLATES_DIR / "_drawing.css"
    if drawing_css_path.exists():
        html = html.replace(
            "{{DRAWING_CSS}}", drawing_css_path.read_text(encoding="utf-8"),
        )
    else:
        html = html.replace("{{DRAWING_CSS}}", "")

    # Substitute drawing tools JS
    drawing_js_path = _TEMPLATES_DIR / "_drawing.js"
    if drawing_js_path.exists():
        html = html.replace(
            "{{DRAWING_JS}}", drawing_js_path.read_text(encoding="utf-8"),
        )
    else:
        html = html.replace("{{DRAWING_JS}}", "")

    return html


class ChartRequestHandler(http.server.BaseHTTPRequestHandler):
    """HTTP handler for the unified chart server.

    Server-level state is accessed via ``self.server`` which is expected
    to be a ``ChartHTTPServer`` instance carrying configuration.
    """

    def do_GET(self) -> None:  # noqa: N802
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        params = urllib.parse.parse_qs(parsed.query)

        if path in ("/", "/index.html"):
            self._redirect()
        elif path == "/market":
            self._serve_market()
        elif path == "/trades":
            self._serve_trades()
        elif path == "/api/symbols":
            self._send_json(get_available_symbols(self._cfg("cache_dir")))
        elif path == "/api/bars":
            self._serve_bars(params)
        elif path == "/api/quotes/live":
            self._serve_live_quotes(params)
        elif path == "/api/quotes":
            self._serve_quotes(params)
        elif path == "/api/trades":
            self._serve_trades_api()
        elif path == "/api/trades/summary":
            self._serve_trades_summary()
        elif path.startswith("/api/trades/bars/"):
            trade_date = path.rsplit("/", 1)[-1]
            self._serve_trade_bars(trade_date)
        elif path == "/api/state/all":
            self._serve_state_all()
        elif path == "/api/state":
            self._serve_state_single(params)
        elif path.startswith("/api/"):
            # Unknown API route — return empty JSON instead of 404
            self._send_json({})
        else:
            self.send_error(404)

    # -- Helpers ---------------------------------------------------------------

    def _cfg(self, key: str) -> Any:
        """Access server-level configuration."""
        return getattr(self.server, key, None)

    def _redirect(self) -> None:
        trades = self._cfg("trades")
        target = "/trades" if trades else "/market"
        self.send_response(302)
        self.send_header("Location", target)
        self.end_headers()

    def _send_json(self, data: object, status: int = 200) -> None:
        body = json.dumps(data).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_html(self, html: str) -> None:
        body = html.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    # -- Market data -----------------------------------------------------------

    def _serve_market(self) -> None:
        html = _load_template("market.html")
        self._send_html(html)

    def _serve_bars(self, params: dict) -> None:
        symbol = (params.get("symbol") or [None])[0]
        start = (params.get("start") or [None])[0]
        end = (params.get("end") or [None])[0]
        timeframe = (params.get("timeframe") or ["1min"])[0]

        if not symbol or not start or not end:
            self._send_json({"error": "Missing symbol, start, or end"}, 400)
            return

        try:
            bars = fetch_bars(
                symbol, start, end, timeframe,
                cache_dir=self._cfg("cache_dir"),
                manager=self._cfg("market_data"),
            )
        except Exception:
            bars = []
        self._send_json(bars)

    def _serve_quotes(self, params: dict | None = None) -> None:
        try:
            params = params or {}
            syms_raw = (params.get("symbols") or [None])[0]
            if syms_raw:
                symbols = [s.strip().upper() for s in syms_raw.split(",") if s.strip()]
            else:
                symbols = get_available_symbols(self._cfg("cache_dir"))
            quotes = fetch_quotes(
                symbols,
                cache_dir=self._cfg("cache_dir"),
                manager=self._cfg("market_data"),
            )
            self._send_json(quotes)
        except Exception:
            self._send_json({})

    def _serve_live_quotes(self, params: dict) -> None:
        try:
            syms_raw = (params.get("symbols") or [None])[0]
            if syms_raw:
                symbols = [s.strip().upper() for s in syms_raw.split(",") if s.strip()]
            else:
                symbols = get_available_symbols(self._cfg("cache_dir"))
            quotes = fetch_live_quotes(
                symbols,
                manager=self._cfg("market_data"),
            )
            self._send_json(quotes)
        except Exception:
            self._send_json({})

    # -- Trade review ----------------------------------------------------------

    def _serve_trades(self) -> None:
        html = _load_template("trades.html")

        # Inject trade data for server-backed mode
        trades = self._cfg("trades")
        summary = self._cfg("summary")
        bars_by_date = self._cfg("bars_by_date")

        if trades is not None:
            trades_json = json.dumps([t.to_dict() for t in trades])
            html = html.replace(
                "var __TRADES_INLINE__ = null;",
                f"var __TRADES_INLINE__ = {trades_json};",
            )
        if summary is not None:
            summary_json = json.dumps(summary.to_dict())
            html = html.replace(
                "var __SUMMARY_INLINE__ = null;",
                f"var __SUMMARY_INLINE__ = {summary_json};",
            )
        if bars_by_date is not None:
            bars_json = json.dumps(bars_by_date)
            html = html.replace(
                "var __BARS_INLINE__ = null;",
                f"var __BARS_INLINE__ = {bars_json};",
            )

        self._send_html(html)

    def _serve_trades_api(self) -> None:
        trades = self._cfg("trades")
        if trades:
            self._send_json([t.to_dict() for t in trades])
        else:
            self._send_json([])

    def _serve_trades_summary(self) -> None:
        summary = self._cfg("summary")
        if summary:
            self._send_json(summary.to_dict())
        else:
            self._send_json({})

    def _serve_trade_bars(self, trade_date: str) -> None:
        bars_by_date = self._cfg("bars_by_date")
        if bars_by_date and trade_date in bars_by_date:
            self._send_json(bars_by_date[trade_date])
            return

        # Find the symbol that traded on this date
        trades = self._cfg("trades")
        symbol = ""
        if trades:
            for t in trades:
                if t.date == trade_date:
                    symbol = t.symbol
                    break
            if not symbol:
                symbol = trades[0].symbol

        if not symbol:
            self._send_json([])
            return

        bars = fetch_bars(
            symbol=symbol,
            start=trade_date,
            end=trade_date,
            timeframe="5min",
            cache_dir=self._cfg("cache_dir"),
            manager=self._cfg("market_data"),
        )
        # Cache the result for subsequent requests
        if bars_by_date is not None:
            bars_by_date[trade_date] = bars
        self._send_json(bars)

    # -- UI state persistence ---------------------------------------------------

    def do_POST(self) -> None:  # noqa: N802
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path

        if path == "/api/state":
            self._save_state()
        elif path == "/api/state/delete":
            self._delete_state()
        else:
            self.send_error(404)

    def _read_body(self) -> bytes:
        length = int(self.headers.get("Content-Length", 0))
        if length > 1_048_576:  # 1 MB guard
            return b""
        return self.rfile.read(length)

    def _state_dir(self) -> Path | None:
        raw = self._cfg("state_dir")
        return Path(raw) if raw else None

    def _serve_state_single(self, params: dict) -> None:
        from charts.server.state import load_state

        key = (params.get("key") or [None])[0]
        if not key:
            self._send_json({"error": "Missing key"}, 400)
            return
        value = load_state(self._state_dir(), key)
        self._send_json({"key": key, "value": value})

    def _serve_state_all(self) -> None:
        from charts.server.state import load_all_state

        self._send_json(load_all_state(self._state_dir()))

    def _save_state(self) -> None:
        from charts.server.state import save_state

        try:
            raw = self._read_body()
            if not raw:
                self._send_json({"error": "Empty or oversized body"}, 400)
                return
            body = json.loads(raw)
            key = body.get("key")
            value = body.get("value")
        except (json.JSONDecodeError, UnicodeDecodeError):
            self._send_json({"error": "Invalid JSON"}, 400)
            return
        if not key:
            self._send_json({"error": "Missing key"}, 400)
            return
        ok = save_state(self._state_dir(), key, value)
        self._send_json({"ok": ok}, 200 if ok else 500)

    def _delete_state(self) -> None:
        from charts.server.state import delete_state

        try:
            body = json.loads(self._read_body())
            key = body.get("key")
        except (json.JSONDecodeError, UnicodeDecodeError):
            self._send_json({"error": "Invalid JSON"}, 400)
            return
        if not key:
            self._send_json({"error": "Missing key"}, 400)
            return
        ok = delete_state(self._state_dir(), key)
        self._send_json({"ok": ok})

    def log_message(self, format: str, *args: object) -> None:
        """Suppress per-request logging."""
        pass
