"""HTTP request handlers for the chart server.

Routes:
    GET /                   → redirect to /market or /trades
    GET /market             → market.html (asset chart viewer)
    GET /trades             → trades.html (trade review)
    GET /api/symbols        → JSON symbol list
    GET /api/bars           → JSON bars (params: symbol, start, end, timeframe)
    GET /api/trades         → JSON trade records
    GET /api/trades/summary → JSON summary stats
"""

from __future__ import annotations

import http.server
import json
import urllib.parse
from pathlib import Path
from typing import Any

from charts.server.data import fetch_bars, get_available_symbols


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
        elif path == "/api/trades":
            self._serve_trades_api()
        elif path == "/api/trades/summary":
            self._serve_trades_summary()
        elif path.startswith("/api/trades/bars/"):
            trade_date = path.rsplit("/", 1)[-1]
            self._serve_trade_bars(trade_date)
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

        bars = fetch_bars(
            symbol, start, end, timeframe,
            cache_dir=self._cfg("cache_dir"),
            manager=self._cfg("market_data"),
        )
        self._send_json(bars)

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

    def log_message(self, format: str, *args: object) -> None:
        """Suppress per-request logging."""
        pass
