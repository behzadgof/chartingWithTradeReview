"""Unified chart server for market data browsing and trade review.

Serves interactive TradingView Lightweight Charts backed by either
cached parquet data or a MarketDataManager instance.  Optionally
serves trade review pages when trade data is provided.
"""

from __future__ import annotations

import http.server
import socketserver
import webbrowser
from typing import Any

from charts.models.trade import TradeRecord
from charts.models.results import BacktestSummary
from charts.server.handlers import ChartRequestHandler


class ChartHTTPServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    """Threaded HTTP server with chart-server configuration attached."""

    daemon_threads = True

    # These attributes are set by ChartServer and accessed by the handler
    market_data: Any = None
    trades: list[TradeRecord] | None = None
    summary: BacktestSummary | None = None
    bars_by_date: dict[str, list[dict]] | None = None
    cache_dir: str | None = None
    state_dir: str | None = None


class ChartServer:
    """Unified chart server for market data and trade review.

    Usage::

        # Market data only
        server = ChartServer(cache_dir="data/cache")
        server.serve()

        # Trade review
        server = ChartServer(trades=trades, summary=summary)
        server.serve()

        # Both modes (market data + trades)
        server = ChartServer(
            market_data=manager,
            trades=trades,
            summary=summary,
        )
        server.serve()
    """

    def __init__(
        self,
        market_data: Any = None,
        trades: list[TradeRecord] | None = None,
        summary: BacktestSummary | None = None,
        bars_by_date: dict[str, list[dict]] | None = None,
        cache_dir: str | None = None,
        state_dir: str | None = None,
        port: int = 5555,
        auto_open: bool = True,
    ) -> None:
        self.market_data = market_data
        self.trades = trades
        self.summary = summary
        self.bars_by_date = bars_by_date
        self.cache_dir = cache_dir
        self.state_dir = state_dir
        self.port = port
        self.auto_open = auto_open

    def serve(self) -> None:
        """Start the HTTP server (blocking)."""
        server = ChartHTTPServer(("", self.port), ChartRequestHandler)

        # Attach configuration to the server instance so the handler can access it
        server.market_data = self.market_data
        server.trades = self.trades
        server.summary = self.summary
        server.bars_by_date = self.bars_by_date
        server.cache_dir = self.cache_dir
        server.state_dir = self.state_dir

        url = f"http://localhost:{self.port}"
        mode = "trades" if self.trades else "market"

        print(f"Chart Server ({mode} mode)")
        if self.cache_dir:
            print(f"  Cache:  {self.cache_dir}")
        if self.state_dir:
            print(f"  State:  {self.state_dir}")
        if self.trades:
            print(f"  Trades: {len(self.trades)}")
        print(f"  URL:    {url}")
        print("\nPress Ctrl+C to stop.\n")

        if self.auto_open:
            webbrowser.open(url)

        try:
            server.serve_forever()
        except KeyboardInterrupt:
            print("\nStopped.")
        finally:
            server.server_close()
