"""Unified chart server for market data browsing and trade review.

Serves interactive TradingView Lightweight Charts backed by either
cached parquet data or a MarketDataManager instance.  Optionally
serves trade review pages when trade data is provided.

Now powered by FastAPI/Starlette with WebSocket support.
"""

from __future__ import annotations

import webbrowser
from typing import Any

from fastapi import FastAPI


class ServerState:
    """Mutable server-wide state accessible from all route handlers."""

    market_data: Any = None
    trades: list | None = None
    summary: Any = None
    bars_by_date: dict[str, list[dict]] | None = None
    cache_dir: str | None = None
    state_dir: str | None = None
    stream_manager: Any = None


server_state = ServerState()


def _build_app() -> FastAPI:
    """Create and return the FastAPI application with all routes."""
    app = FastAPI(title="ORB Charts", docs_url=None, redoc_url=None)

    from charts.server.routes.pages import router as pages_router
    from charts.server.routes.api import router as api_router
    from charts.server.routes.state import router as state_router
    from charts.server.routes.firebase import router as firebase_router

    app.include_router(pages_router)
    app.include_router(api_router)
    app.include_router(state_router)
    app.include_router(firebase_router)

    return app


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
        trades: list | None = None,
        summary: Any = None,
        bars_by_date: dict[str, list[dict]] | None = None,
        cache_dir: str | None = None,
        state_dir: str | None = None,
        port: int = 5555,
        auto_open: bool = True,
    ) -> None:
        server_state.market_data = market_data
        server_state.trades = trades
        server_state.summary = summary
        server_state.bars_by_date = bars_by_date
        server_state.cache_dir = cache_dir
        server_state.state_dir = state_dir
        self.port = port
        self.auto_open = auto_open

    def serve(self) -> None:
        """Start the server (blocking)."""
        import uvicorn

        url = f"http://localhost:{self.port}"
        views = ["market"]
        if server_state.trades:
            views.append("trades")

        print(f"Chart Server (views: {', '.join(views)})")
        if server_state.cache_dir:
            print(f"  Cache:  {server_state.cache_dir}")
        if server_state.state_dir:
            print(f"  State:  {server_state.state_dir}")
        if server_state.trades:
            print(f"  Trades: {len(server_state.trades)}")
        print(f"  URL:    {url}")
        print("\nPress Ctrl+C to stop.\n")

        if self.auto_open:
            webbrowser.open(url)

        app = _build_app()
        uvicorn.run(app, host="0.0.0.0", port=self.port, log_level="warning")
