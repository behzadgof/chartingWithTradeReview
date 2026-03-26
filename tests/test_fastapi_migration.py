"""Comprehensive tests for the FastAPI chart server migration.

Uses starlette.testclient.TestClient with _build_app() so that all routes
are exercised without starting a real server process.  server_state is reset
before each test to keep tests independent.

Note: tests execute against the installed ``charts`` package.  The installed
``_build_app()`` mounts ``charts.server.handlers.router`` (page + API routes)
and ``charts.server.websocket.router`` (WebSocket routes).
"""

from __future__ import annotations

import pytest
from starlette.testclient import TestClient

from charts.models.results import BacktestSummary
from charts.server.app import _build_app, server_state


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _reset_server_state(state_dir: str | None = None) -> None:
    """Reset server_state to a clean baseline before each test."""
    server_state.market_data = None
    server_state.trades = None
    server_state.summary = None
    server_state.bars_by_date = None
    server_state.cache_dir = None
    server_state.state_dir = state_dir
    server_state.stream_manager = None


@pytest.fixture()
def client(tmp_path):
    """TestClient with a clean server_state and a temporary state directory."""
    _reset_server_state(state_dir=str(tmp_path / "state"))
    app = _build_app()
    with TestClient(app, raise_server_exceptions=True) as c:
        yield c
    _reset_server_state()


@pytest.fixture()
def sample_summary(sample_trades):
    """BacktestSummary derived from the shared sample_trades fixture."""
    return BacktestSummary.from_trades(sample_trades, symbol="AAPL")


@pytest.fixture()
def client_with_trades(tmp_path, sample_trades, sample_summary):
    """TestClient pre-loaded with trade data."""
    _reset_server_state(state_dir=str(tmp_path / "state"))
    server_state.trades = sample_trades
    server_state.summary = sample_summary
    app = _build_app()
    with TestClient(app, raise_server_exceptions=True) as c:
        yield c
    _reset_server_state()


# ---------------------------------------------------------------------------
# 1. Page routes
# ---------------------------------------------------------------------------

class TestPageRoutes:
    """Tests for page-serving routes: /, /market, /trades."""

    def test_root_no_trades_redirects_to_market(self, client):
        """GET / with no trades should 302-redirect to /market."""
        resp = client.get("/", follow_redirects=False)
        assert resp.status_code == 302
        assert "/market" in resp.headers["location"]

    def test_root_with_trades_redirects_to_trades(self, client_with_trades):
        """GET / with trades loaded should 302-redirect to /trades."""
        resp = client_with_trades.get("/", follow_redirects=False)
        assert resp.status_code == 302
        assert "/trades" in resp.headers["location"]

    def test_root_with_trades_follows_to_html(self, client_with_trades):
        """GET / with trades, following redirect, should return 200 HTML."""
        resp = client_with_trades.get("/", follow_redirects=True)
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]

    def test_market_page_returns_200(self, client):
        """GET /market should return 200 with an HTML body."""
        resp = client.get("/market")
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]

    def test_market_page_contains_asset_charts_title(self, client):
        """GET /market HTML should contain the page title 'Asset Charts'."""
        resp = client.get("/market")
        assert "Asset Charts" in resp.text

    def test_trades_page_returns_200(self, client):
        """GET /trades should always return 200 (even when trades is None)."""
        resp = client.get("/trades")
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]

    def test_trades_page_returns_200_with_trades(self, client_with_trades):
        """GET /trades with loaded trades should return 200."""
        resp = client_with_trades.get("/trades")
        assert resp.status_code == 200

    def test_trades_page_inlines_trade_data(self, client_with_trades):
        """GET /trades with loaded trades should inline the trade JSON."""
        resp = client_with_trades.get("/trades")
        # The handler replaces __TRADES_INLINE__ = null with actual JSON
        assert "var __TRADES_INLINE__ = null;" not in resp.text
        assert "__TRADES_INLINE__" in resp.text


# ---------------------------------------------------------------------------
# 2. State API
# ---------------------------------------------------------------------------

class TestStateAPI:
    """Tests for /api/state persistence endpoints."""

    def test_save_state(self, client):
        """POST /api/state with key+value should return {ok: true}."""
        resp = client.post("/api/state", json={"key": "test_key", "value": "test_val"})
        assert resp.status_code == 200
        assert resp.json()["ok"] is True

    def test_load_state(self, client):
        """GET /api/state?key=test_key should return the saved value."""
        client.post("/api/state", json={"key": "test_key", "value": "test_val"})
        resp = client.get("/api/state", params={"key": "test_key"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["key"] == "test_key"
        assert data["value"] == "test_val"

    def test_load_state_all_contains_key(self, client):
        """GET /api/state/all should return a dict containing the saved key."""
        client.post("/api/state", json={"key": "test_key", "value": "test_val"})
        resp = client.get("/api/state/all")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, dict)
        assert "test_key" in data
        assert data["test_key"] == "test_val"

    def test_delete_state(self, client):
        """POST /api/state/delete with {key} should return {ok: true}."""
        client.post("/api/state", json={"key": "test_key", "value": "test_val"})
        resp = client.post("/api/state/delete", json={"key": "test_key"})
        assert resp.status_code == 200
        assert resp.json()["ok"] is True

    def test_load_deleted_key_returns_none(self, client):
        """GET /api/state?key=test_key after delete should return value: null."""
        client.post("/api/state", json={"key": "test_key", "value": "test_val"})
        client.post("/api/state/delete", json={"key": "test_key"})
        resp = client.get("/api/state", params={"key": "test_key"})
        assert resp.status_code == 200
        assert resp.json()["value"] is None

    def test_save_state_missing_key_returns_400(self, client):
        """POST /api/state without a key field should return 400."""
        resp = client.post("/api/state", json={"value": "no_key_here"})
        assert resp.status_code == 400

    def test_load_state_missing_key_param_returns_400(self, client):
        """GET /api/state without ?key param should return 400."""
        resp = client.get("/api/state")
        assert resp.status_code == 400

    def test_save_and_load_complex_value(self, client):
        """State values can be arbitrary JSON-serialisable objects."""
        payload = {"panels": [{"symbol": "AAPL"}, {"symbol": "MSFT"}]}
        client.post("/api/state", json={"key": "layout", "value": payload})
        resp = client.get("/api/state", params={"key": "layout"})
        assert resp.json()["value"] == payload

    def test_state_all_empty_when_no_keys(self, client):
        """GET /api/state/all with no saved keys returns an empty dict."""
        resp = client.get("/api/state/all")
        assert resp.status_code == 200
        assert resp.json() == {}

    def test_delete_nonexistent_key_returns_ok(self, client):
        """Deleting a key that doesn't exist should still return ok."""
        resp = client.post("/api/state/delete", json={"key": "never_existed"})
        assert resp.status_code == 200
        assert resp.json()["ok"] is True

    def test_state_all_contains_multiple_saved_keys(self, client):
        """GET /api/state/all should return all saved keys."""
        client.post("/api/state", json={"key": "key_a", "value": 1})
        client.post("/api/state", json={"key": "key_b", "value": 2})
        resp = client.get("/api/state/all")
        data = resp.json()
        assert data["key_a"] == 1
        assert data["key_b"] == 2


# ---------------------------------------------------------------------------
# 3. Data API (no market_data / cache)
# ---------------------------------------------------------------------------

class TestDataAPI:
    """Tests for /api/* data endpoints without a live market-data source."""

    def test_symbols_returns_200_and_list(self, client):
        """GET /api/symbols should return 200 with a list (may be empty)."""
        resp = client.get("/api/symbols")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    def test_symbols_empty_when_no_cache(self, client):
        """With no cache_dir set, symbols list should be empty."""
        resp = client.get("/api/symbols")
        assert resp.json() == []

    def test_bars_without_params_returns_400(self, client):
        """GET /api/bars without required params should return 400."""
        resp = client.get("/api/bars")
        assert resp.status_code == 400
        assert "error" in resp.json()

    def test_bars_missing_symbol_returns_400(self, client):
        """GET /api/bars missing symbol should return 400."""
        resp = client.get("/api/bars", params={"start": "2024-01-15", "end": "2024-01-15"})
        assert resp.status_code == 400

    def test_bars_missing_start_returns_400(self, client):
        """GET /api/bars missing start should return 400."""
        resp = client.get("/api/bars", params={"symbol": "AAPL", "end": "2024-01-15"})
        assert resp.status_code == 400

    def test_bars_missing_end_returns_400(self, client):
        """GET /api/bars missing end should return 400."""
        resp = client.get("/api/bars", params={"symbol": "AAPL", "start": "2024-01-15"})
        assert resp.status_code == 400

    def test_bars_with_all_params_returns_list(self, client):
        """GET /api/bars with all params returns a list (empty when no data)."""
        resp = client.get(
            "/api/bars",
            params={"symbol": "AAPL", "start": "2024-01-15", "end": "2024-01-15"},
        )
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    def test_bars_batch_without_params_returns_400(self, client):
        """GET /api/bars/batch without required params should return 400."""
        resp = client.get("/api/bars/batch")
        assert resp.status_code == 400

    def test_bars_batch_with_params_returns_dict(self, client):
        """GET /api/bars/batch with valid params returns a dict keyed by symbol."""
        resp = client.get(
            "/api/bars/batch",
            params={"symbols": "AAPL,MSFT", "start": "2024-01-15", "end": "2024-01-15"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, dict)
        assert "AAPL" in data
        assert "MSFT" in data

    def test_quotes_returns_200_and_dict(self, client):
        """GET /api/quotes should return 200 with a dict."""
        resp = client.get("/api/quotes")
        assert resp.status_code == 200
        assert isinstance(resp.json(), dict)

    def test_quotes_live_returns_200_and_dict(self, client):
        """GET /api/quotes/live should return 200 with a dict."""
        resp = client.get("/api/quotes/live")
        assert resp.status_code == 200
        assert isinstance(resp.json(), dict)

    def test_trades_no_trades_returns_empty_list(self, client):
        """GET /api/trades with no trades loaded should return empty list."""
        resp = client.get("/api/trades")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_trades_summary_no_summary_returns_empty_dict(self, client):
        """GET /api/trades/summary with no summary should return empty dict."""
        resp = client.get("/api/trades/summary")
        assert resp.status_code == 200
        assert resp.json() == {}

    def test_trades_returns_list_when_trades_loaded(self, client_with_trades):
        """GET /api/trades with loaded trades returns a non-empty list."""
        resp = client_with_trades.get("/api/trades")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        assert len(data) > 0

    def test_trades_summary_returns_dict_when_loaded(self, client_with_trades):
        """GET /api/trades/summary with loaded summary returns a non-empty dict."""
        resp = client_with_trades.get("/api/trades/summary")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, dict)
        assert len(data) > 0

    def test_trade_bars_unknown_date_returns_empty_list(self, client_with_trades):
        """GET /api/trades/bars/<date> for unknown date with no cache returns empty."""
        resp = client_with_trades.get("/api/trades/bars/2099-01-01")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    def test_trade_bars_no_trades_returns_empty_list(self, client):
        """GET /api/trades/bars/<date> with no trades at all returns empty list."""
        resp = client.get("/api/trades/bars/2024-01-15")
        assert resp.status_code == 200
        assert resp.json() == []


# ---------------------------------------------------------------------------
# 4. WebSocket
# ---------------------------------------------------------------------------

class TestWebSocket:
    """Tests for the /ws/stream WebSocket endpoint."""

    def test_ws_stream_connects_and_sends_status(self, client):
        """Connecting to /ws/stream should receive an initial status message."""
        with client.websocket_connect("/ws/stream") as ws:
            msg = ws.receive_json()
            assert msg["type"] == "status"
            assert "streaming" in msg

    def test_ws_stream_status_streaming_false_without_manager(self, client):
        """Without a stream_manager, status.streaming should be False."""
        with client.websocket_connect("/ws/stream") as ws:
            msg = ws.receive_json()
            assert msg["streaming"] is False

    def test_ws_stream_subscribe_returns_subscribed(self, client):
        """Sending a subscribe message should return a subscribed response."""
        with client.websocket_connect("/ws/stream") as ws:
            ws.receive_json()  # consume the initial status message
            ws.send_json({"subscribe": ["BTC-USD"], "channels": ["quotes"]})
            resp = ws.receive_json()
            assert resp["type"] == "subscribed"
            assert "BTC-USD" in resp["symbols"]

    def test_ws_stream_unsubscribe_returns_unsubscribed(self, client):
        """Sending an unsubscribe message should return an unsubscribed response."""
        with client.websocket_connect("/ws/stream") as ws:
            ws.receive_json()  # initial status
            ws.send_json({"subscribe": ["BTC-USD"]})
            ws.receive_json()  # subscribed ack
            ws.send_json({"unsubscribe": ["BTC-USD"]})
            resp = ws.receive_json()
            assert resp["type"] == "unsubscribed"
            assert "BTC-USD" in resp["symbols"]
