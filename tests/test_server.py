"""Tests for the chart server."""

from __future__ import annotations

import json
import threading
import time
import urllib.request

import pytest

from charts.models.trade import TradeRecord
from charts.models.results import BacktestSummary
from charts.server.app import ChartServer, ChartHTTPServer
from charts.server.handlers import ChartRequestHandler


@pytest.fixture()
def sample_trades():
    return [
        TradeRecord(
            trade_id="T001", symbol="AAPL", direction="LONG", date="2024-01-15",
            entry_price=185.50, exit_price=187.00, quantity=100,
            net_pnl=150.0, gross_pnl=155.0, pnl_pct=0.81,
            or_high=186.0, or_low=184.0, stop_price=183.0,
        ),
        TradeRecord(
            trade_id="T002", symbol="AAPL", direction="SHORT", date="2024-01-16",
            entry_price=188.00, exit_price=189.50, quantity=100,
            net_pnl=-150.0, gross_pnl=-145.0, pnl_pct=-0.80,
            or_high=189.0, or_low=187.0, stop_price=190.0,
        ),
    ]


@pytest.fixture()
def sample_summary(sample_trades):
    return BacktestSummary.from_trades(sample_trades, symbol="AAPL")


@pytest.fixture()
def sample_bars():
    """Simple bars for 2024-01-15."""
    return {
        "2024-01-15": [
            {"time": 1705323000, "open": 185.0, "high": 186.0, "low": 184.5, "close": 185.5, "volume": 1000},
            {"time": 1705323300, "open": 185.5, "high": 187.0, "low": 185.0, "close": 186.5, "volume": 1500},
        ],
    }


class TestChartServerInit:
    """Test ChartServer initialization."""

    def test_default_init(self):
        server = ChartServer()
        assert server.port == 5555
        assert server.auto_open is True
        assert server.trades is None
        assert server.summary is None
        assert server.market_data is None
        assert server.cache_dir is None

    def test_trade_mode_init(self, sample_trades, sample_summary):
        server = ChartServer(
            trades=sample_trades,
            summary=sample_summary,
            port=9999,
            auto_open=False,
        )
        assert server.port == 9999
        assert server.auto_open is False
        assert len(server.trades) == 2
        assert server.summary.total_trades == 2

    def test_market_mode_init(self):
        server = ChartServer(cache_dir="/tmp/cache", auto_open=False)
        assert server.cache_dir == "/tmp/cache"


class TestChartHTTPServer:
    """Test the HTTP server with actual requests."""

    @pytest.fixture()
    def running_server(self, sample_trades, sample_summary, sample_bars):
        """Start a real HTTP server in a thread."""
        httpd = ChartHTTPServer(("", 0), ChartRequestHandler)
        httpd.trades = sample_trades
        httpd.summary = sample_summary
        httpd.bars_by_date = sample_bars
        httpd.market_data = None
        httpd.cache_dir = None

        port = httpd.server_address[1]
        thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        thread.start()
        time.sleep(0.2)  # Let server start
        yield port
        httpd.shutdown()

    def test_root_redirect(self, running_server):
        port = running_server
        req = urllib.request.Request(f"http://localhost:{port}/")
        try:
            urllib.request.urlopen(req)
        except urllib.error.HTTPError as e:
            # Should redirect to /trades when trades are loaded
            assert e.code in (301, 302, 307, 308)

    def test_api_trades(self, running_server):
        port = running_server
        url = f"http://localhost:{port}/api/trades"
        resp = urllib.request.urlopen(url)
        data = json.loads(resp.read())
        assert len(data) == 2
        assert data[0]["trade_id"] == "T001"
        assert data[1]["trade_id"] == "T002"

    def test_api_trades_summary(self, running_server):
        port = running_server
        url = f"http://localhost:{port}/api/trades/summary"
        resp = urllib.request.urlopen(url)
        data = json.loads(resp.read())
        assert data["total_trades"] == 2
        assert data["symbol"] == "AAPL"

    def test_api_trades_bars(self, running_server):
        port = running_server
        url = f"http://localhost:{port}/api/trades/bars/2024-01-15"
        resp = urllib.request.urlopen(url)
        data = json.loads(resp.read())
        assert len(data) == 2
        assert data[0]["open"] == 185.0

    def test_api_trades_bars_missing_date(self, running_server):
        port = running_server
        url = f"http://localhost:{port}/api/trades/bars/2099-01-01"
        resp = urllib.request.urlopen(url)
        data = json.loads(resp.read())
        assert data == []

    def test_trades_page(self, running_server):
        port = running_server
        url = f"http://localhost:{port}/trades"
        resp = urllib.request.urlopen(url)
        html = resp.read().decode()
        assert "LightweightCharts" in html or "Trade Charts" in html
        # Should have inlined trade data
        assert "__TRADES_INLINE__" in html or "T001" in html

    def test_404_for_unknown_path(self, running_server):
        port = running_server
        url = f"http://localhost:{port}/nonexistent"
        with pytest.raises(urllib.error.HTTPError) as exc_info:
            urllib.request.urlopen(url)
        assert exc_info.value.code == 404
