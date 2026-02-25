"""Tests for the data layer (bar loading, aggregation, timezone, quotes)."""

from __future__ import annotations

import calendar as cal_mod
from datetime import date, datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from charts.server.data import (
    _filter_trading_days,
    aggregate_bars,
    bars_to_json,
    fetch_bars,
    fetch_bars_batch,
    fetch_live_quotes,
    fetch_quotes,
    load_bars_from_cache,
    load_bars_from_manager,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_1min_df(start: str, n: int) -> pd.DataFrame:
    """Create n 1-min bars starting from `start` timestamp string."""
    ts = pd.date_range(start, periods=n, freq="1min")
    return pd.DataFrame({
        "timestamp": ts,
        "open": range(100, 100 + n),
        "high": range(101, 101 + n),
        "low": range(99, 99 + n),
        "close": range(100, 100 + n),
        "volume": [1000] * n,
    })


# ---------------------------------------------------------------------------
# Trading day filter
# ---------------------------------------------------------------------------

class TestFilterTradingDays:
    def test_removes_weekend(self):
        # 2024-01-20 = Saturday, 2024-01-21 = Sunday
        sat = _make_1min_df("2024-01-20 10:00", 3)
        sun = _make_1min_df("2024-01-21 10:00", 3)
        weekday = _make_1min_df("2024-01-22 10:00", 3)  # Monday (not a holiday)
        df = pd.concat([sat, sun, weekday], ignore_index=True)
        result = _filter_trading_days(df)
        # Only Monday bars remain
        assert all(result["timestamp"].dt.weekday < 5)
        assert len(result) == 3

    def test_keeps_weekdays(self):
        df = _make_1min_df("2024-01-17 10:00", 5)  # Wednesday (not a holiday)
        result = _filter_trading_days(df)
        assert len(result) == 5

    def test_empty_df(self):
        result = _filter_trading_days(pd.DataFrame())
        assert result.empty


# ---------------------------------------------------------------------------
# Bar aggregation
# ---------------------------------------------------------------------------

class TestAggregation:
    def test_1min_passthrough(self):
        df = _make_1min_df("2024-01-17 10:00", 5)
        result = aggregate_bars(df, "1min")
        assert len(result) == 5
        assert list(result.columns) == list(df.columns)

    def test_5min_aggregation(self):
        df = _make_1min_df("2024-01-17 10:00", 10)
        result = aggregate_bars(df, "5min")
        assert len(result) == 2
        # First 5-min bar OHLCV (bars 0-4: open 100-104, high 101-105, low 99-103)
        row = result.iloc[0]
        assert row["open"] == 100   # first open
        assert row["high"] == 105   # max high of first 5
        assert row["low"] == 99     # min low of first 5
        assert row["close"] == 104  # last close
        assert row["volume"] == 5000

    def test_1hour_aggregation(self):
        df = _make_1min_df("2024-01-17 10:00", 60)
        result = aggregate_bars(df, "1hour")
        assert len(result) == 1

    def test_custom_timeframe_regex(self):
        df = _make_1min_df("2024-01-17 10:00", 130)
        result = aggregate_bars(df, "65min")
        # Pandas aligns resample bins to calendar boundaries, so count varies
        assert len(result) >= 2

    def test_2hour_aggregation(self):
        df = _make_1min_df("2024-01-17 10:00", 120)
        result = aggregate_bars(df, "2hour")
        assert len(result) == 1

    def test_1day_aggregation(self):
        day1 = _make_1min_df("2024-01-17 10:00", 5)
        day2 = _make_1min_df("2024-01-18 10:00", 5)
        df = pd.concat([day1, day2], ignore_index=True)
        result = aggregate_bars(df, "1day")
        assert len(result) == 2


# ---------------------------------------------------------------------------
# bars_to_json
# ---------------------------------------------------------------------------

class TestBarsToJson:
    def test_output_format(self):
        df = _make_1min_df("2024-01-17 10:00", 2)
        result = bars_to_json(df)
        assert len(result) == 2
        bar = result[0]
        assert set(bar.keys()) == {"time", "open", "high", "low", "close", "volume"}

    def test_timegm_conversion(self):
        """Timestamps should use timegm (ET-as-UTC trick)."""
        df = _make_1min_df("2024-01-17 10:00", 1)
        result = bars_to_json(df)
        ts = df.iloc[0]["timestamp"]
        expected = cal_mod.timegm(ts.timetuple())
        assert result[0]["time"] == expected

    def test_rounding(self):
        df = pd.DataFrame({
            "timestamp": [pd.Timestamp("2024-01-17 10:00")],
            "open": [100.12345],
            "high": [101.56789],
            "low": [99.99999],
            "close": [100.11111],
            "volume": [1234],
        })
        result = bars_to_json(df)
        bar = result[0]
        assert bar["open"] == 100.1235
        assert bar["high"] == 101.5679
        assert bar["low"] == 100.0
        assert bar["close"] == 100.1111
        assert bar["volume"] == 1234


# ---------------------------------------------------------------------------
# load_bars_from_manager — timezone conversion
# ---------------------------------------------------------------------------

class TestLoadBarsFromManager:
    def test_converts_utc_to_et(self):
        """Manager bars with UTC timestamps should be converted to tz-naive ET."""
        mock_bar = MagicMock()
        # 15:00 UTC = 10:00 ET (in January, EST = UTC-5)
        mock_bar.timestamp = datetime(2024, 1, 15, 15, 0, tzinfo=__import__("zoneinfo").ZoneInfo("UTC"))
        mock_bar.open = 100
        mock_bar.high = 101
        mock_bar.low = 99
        mock_bar.close = 100.5
        mock_bar.volume = 1000

        manager = MagicMock()
        manager.get_bars.return_value = [mock_bar]

        df = load_bars_from_manager("AAPL", "2024-01-17", "2024-01-17", manager)
        assert not df.empty
        ts = df.iloc[0]["timestamp"]
        # Should be tz-naive
        assert ts.tzinfo is None
        # Should be 10:00 ET
        assert ts.hour == 10

    def test_returns_empty_on_no_bars(self):
        manager = MagicMock()
        manager.get_bars.return_value = []
        df = load_bars_from_manager("AAPL", "2024-01-17", "2024-01-17", manager)
        assert df.empty

    def test_falls_back_to_provider(self):
        """When manager.get_bars raises, should try raw provider."""
        mock_bar = MagicMock()
        mock_bar.timestamp = datetime(2024, 1, 15, 10, 0)
        mock_bar.open = 100
        mock_bar.high = 101
        mock_bar.low = 99
        mock_bar.close = 100
        mock_bar.volume = 500

        provider = MagicMock()
        provider.get_bars.return_value = [mock_bar]

        manager = MagicMock()
        manager.get_bars.side_effect = ValueError("validation failed")
        manager.providers = [provider]

        df = load_bars_from_manager("AAPL", "2024-01-17", "2024-01-17", manager)
        assert not df.empty
        assert len(df) == 1


# ---------------------------------------------------------------------------
# load_bars_from_cache
# ---------------------------------------------------------------------------

class TestLoadBarsFromCache:
    def test_loads_parquet_files(self, tmp_path):
        sym_dir = tmp_path / "AAPL"
        sym_dir.mkdir()
        df = _make_1min_df("2024-01-17 10:00", 60)
        df.to_parquet(sym_dir / "1min_2024-01-17_2024-01-17.parquet")

        result = load_bars_from_cache("AAPL", "2024-01-17", "2024-01-17", tmp_path)
        assert not result.empty
        assert len(result) == 60

    def test_empty_for_missing_symbol(self, tmp_path):
        result = load_bars_from_cache("ZZZZ", "2024-01-17", "2024-01-17", tmp_path)
        assert result.empty

    def test_filters_extended_hours(self, tmp_path):
        """Bars outside 4:00-20:00 should be filtered out."""
        sym_dir = tmp_path / "TEST"
        sym_dir.mkdir()
        # Include bars from midnight to 3:59 (should be removed)
        df = _make_1min_df("2024-01-17 02:00", 180)  # 02:00-05:00
        df.to_parquet(sym_dir / "1min_2024-01-17_2024-01-17.parquet")

        result = load_bars_from_cache("TEST", "2024-01-17", "2024-01-17", tmp_path)
        # Only bars from 04:00 onward should remain
        assert all(result["timestamp"].dt.hour >= 4)

    def test_date_range_filter(self, tmp_path):
        """Only files overlapping the requested range should be loaded."""
        sym_dir = tmp_path / "SPY"
        sym_dir.mkdir()
        df1 = _make_1min_df("2024-01-10 10:00", 60)
        df1.to_parquet(sym_dir / "1min_2024-01-10_2024-01-10.parquet")
        df2 = _make_1min_df("2024-01-17 10:00", 60)
        df2.to_parquet(sym_dir / "1min_2024-01-17_2024-01-17.parquet")

        result = load_bars_from_cache("SPY", "2024-01-17", "2024-01-17", tmp_path)
        # Should only load the Jan 17 file
        assert all(result["timestamp"].dt.date == date(2024, 1, 17))


# ---------------------------------------------------------------------------
# fetch_bars (high-level)
# ---------------------------------------------------------------------------

class TestFetchBars:
    def test_cache_only(self, tmp_path):
        sym_dir = tmp_path / "AAPL"
        sym_dir.mkdir()
        df = _make_1min_df("2024-01-17 10:00", 5)
        df.to_parquet(sym_dir / "1min_2024-01-17_2024-01-17.parquet")

        result = fetch_bars("AAPL", "2024-01-17", "2024-01-17", cache_dir=tmp_path)
        assert len(result) == 5
        assert all(isinstance(b, dict) for b in result)

    def test_manager_fallback(self):
        mock_bar = MagicMock()
        mock_bar.timestamp = datetime(2024, 1, 17, 10, 0)
        mock_bar.open = 100
        mock_bar.high = 101
        mock_bar.low = 99
        mock_bar.close = 100
        mock_bar.volume = 500

        manager = MagicMock()
        manager.get_bars.return_value = [mock_bar]

        result = fetch_bars("AAPL", "2024-01-17", "2024-01-17", manager=manager)
        assert len(result) == 1

    def test_empty_result(self):
        result = fetch_bars("AAPL", "2024-01-17", "2024-01-17")
        assert result == []

    def test_aggregation_applied(self, tmp_path):
        sym_dir = tmp_path / "AAPL"
        sym_dir.mkdir()
        df = _make_1min_df("2024-01-17 10:00", 10)
        df.to_parquet(sym_dir / "1min_2024-01-17_2024-01-17.parquet")

        result = fetch_bars(
            "AAPL", "2024-01-17", "2024-01-17",
            timeframe="5min", cache_dir=tmp_path,
        )
        assert len(result) == 2

    def test_gap_backfill_requests_missing_trading_days(self):
        cache_df = pd.concat(
            [
                _make_1min_df("2024-01-17 10:00", 1),
                _make_1min_df("2024-01-19 10:00", 1),
            ],
            ignore_index=True,
        )

        def _manager_side_effect(symbol, start, end, manager):
            if (symbol, start, end) == ("AAPL", "2024-01-18", "2024-01-18"):
                return _make_1min_df("2024-01-18 10:00", 1)
            if (symbol, start, end) == ("AAPL", "2024-01-22", "2024-01-22"):
                return _make_1min_df("2024-01-22 10:00", 1)
            return pd.DataFrame()

        with patch("charts.server.data.load_bars_from_cache", return_value=cache_df):
            with patch("charts.server.data.load_bars_from_manager", side_effect=_manager_side_effect) as mock_mgr:
                bars = fetch_bars(
                    "AAPL",
                    "2024-01-17",
                    "2024-01-22",
                    cache_dir="dummy",
                    manager=object(),
                )

        assert len(bars) == 4
        calls = [(c.args[0], c.args[1], c.args[2]) for c in mock_mgr.call_args_list]
        assert ("AAPL", "2024-01-18", "2024-01-18") in calls
        assert ("AAPL", "2024-01-22", "2024-01-22") in calls

    def test_near_today_tail_refresh_uses_provider(self):
        cache_df = _make_1min_df("2024-01-18 10:00", 1)
        cache_df["close"] = [100.0]

        provider = MagicMock()
        provider.get_bars.return_value = [
            {
                "timestamp": datetime(2024, 1, 18, 10, 0),
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.0,
                "volume": 1000,
            },
            {
                "timestamp": datetime(2024, 1, 18, 15, 59),
                "open": 109.0,
                "high": 111.0,
                "low": 108.5,
                "close": 110.0,
                "volume": 2000,
            },
        ]

        manager = MagicMock()
        manager.providers = [provider]

        with patch("charts.server.data.load_bars_from_cache", return_value=cache_df):
            with patch("charts.server.data.date") as mock_date:
                mock_date.today.return_value = date(2024, 1, 18)
                mock_date.fromisoformat.side_effect = lambda s: date.fromisoformat(s)
                bars = fetch_bars(
                    "AAPL",
                    "2024-01-18",
                    "2024-01-18",
                    cache_dir="dummy",
                    manager=manager,
                )

        assert provider.get_bars.called
        assert bars
        assert bars[-1]["close"] == 110.0

    def test_large_range_uses_fast_edge_backfill(self, monkeypatch):
        cache_df = _make_1min_df("2024-01-10 10:00", 1)
        manager = MagicMock()

        calls: list[tuple[str, str, str]] = []

        def _mgr_side_effect(symbol, start, end, _manager):
            calls.append((symbol, start, end))
            return pd.DataFrame()

        monkeypatch.setenv("CHARTS_DEEP_GAP_SCAN_DAYS", "7")
        with patch("charts.server.data.load_bars_from_cache", return_value=cache_df):
            with patch("charts.server.data.load_bars_from_manager", side_effect=_mgr_side_effect):
                with patch("charts.server.data.load_bars_from_primary_provider", return_value=pd.DataFrame()):
                    _ = fetch_bars(
                        "AAPL",
                        "2024-01-01",
                        "2024-03-01",
                        cache_dir="dummy",
                        manager=manager,
                    )

        assert ("AAPL", "2024-01-01", "2024-01-09") in calls
        assert ("AAPL", "2024-01-11", "2024-03-01") in calls
        assert len(calls) <= 3

    def test_non_1min_uses_native_provider_timeframe(self):
        native_df = pd.DataFrame(
            {
                "timestamp": [pd.Timestamp("2024-01-17 10:00"), pd.Timestamp("2024-01-17 10:05")],
                "open": [100.0, 101.0],
                "high": [101.0, 102.0],
                "low": [99.5, 100.5],
                "close": [100.5, 101.5],
                "volume": [1000, 1200],
            }
        )
        with patch("charts.server.data.load_bars_from_primary_provider", return_value=native_df) as mock_provider:
            bars = fetch_bars(
                "NVDA",
                "2024-01-17",
                "2024-01-17",
                timeframe="5min",
                manager=object(),
            )
        assert len(bars) == 2
        assert mock_provider.call_args.kwargs.get("timeframe") == "5min"


# ---------------------------------------------------------------------------
# fetch_quotes
# ---------------------------------------------------------------------------

class TestFetchQuotes:
    def test_from_cache(self, tmp_path):
        sym_dir = tmp_path / "AAPL"
        sym_dir.mkdir()
        # Two trading days of data
        bars = []
        for d in ["2024-01-17", "2024-01-16"]:
            bars.append(_make_1min_df(f"{d} 10:00", 60))
        df = pd.concat(bars, ignore_index=True)
        df.to_parquet(sym_dir / "1min_2024-01-17_2024-01-16.parquet")

        result = fetch_quotes(["AAPL"], cache_dir=tmp_path)
        assert "AAPL" in result
        q = result["AAPL"]
        assert "price" in q
        assert "change" in q
        assert "changePct" in q
        assert q["source"] == "cache"

    def test_empty_for_missing_symbol(self, tmp_path):
        result = fetch_quotes(["ZZZZ"], cache_dir=tmp_path)
        assert result == {}

    def test_no_sources(self):
        result = fetch_quotes(["AAPL"])
        assert result == {}

    def test_falls_back_to_manager_when_cache_missing_symbol(self, tmp_path):
        manager = MagicMock()
        bar1 = MagicMock()
        bar1.timestamp = datetime(2024, 1, 17, 10, 0)
        bar1.open = 100
        bar1.high = 101
        bar1.low = 99
        bar1.close = 100
        bar1.volume = 500
        bar2 = MagicMock()
        bar2.timestamp = datetime(2024, 1, 17, 10, 1)
        bar2.open = 100
        bar2.high = 102
        bar2.low = 100
        bar2.close = 101
        bar2.volume = 700
        manager.get_bars.return_value = [bar1, bar2]

        result = fetch_quotes(["AAPL"], cache_dir=tmp_path, manager=manager)
        assert "AAPL" in result
        assert result["AAPL"]["source"] == "provider"

    def test_falls_back_to_primary_provider_for_quote(self):
        provider = MagicMock()
        provider.get_bars.return_value = [
            {
                "timestamp": datetime(2024, 1, 17, 0, 0),
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.0,
                "volume": 500,
            },
            {
                "timestamp": datetime(2024, 1, 18, 0, 0),
                "open": 101.0,
                "high": 103.0,
                "low": 100.5,
                "close": 102.0,
                "volume": 700,
            },
        ]

        manager = MagicMock()
        manager.providers = [provider]
        manager.get_bars.side_effect = RuntimeError("should not be required")

        result = fetch_quotes(["AAPL"], manager=manager)
        assert "AAPL" in result
        assert result["AAPL"]["price"] == 102.0
        assert result["AAPL"]["source"] == "provider"

    def test_stale_cache_prefers_provider(self, tmp_path):
        sym_dir = tmp_path / "AAPL"
        sym_dir.mkdir()
        stale = pd.concat(
            [
                _make_1min_df("2024-01-17 10:00", 1),
                _make_1min_df("2024-01-17 10:01", 1),
            ],
            ignore_index=True,
        )
        stale.loc[0, "close"] = 100.0
        stale.loc[1, "close"] = 101.0
        stale.to_parquet(sym_dir / "1min_2024-01-17_2024-01-17.parquet")

        provider = MagicMock()
        provider.get_bars.return_value = [
            {
                "timestamp": datetime(2026, 2, 20, 0, 0),
                "open": 263.0,
                "high": 264.0,
                "low": 262.5,
                "close": 263.5,
                "volume": 1000,
            },
            {
                "timestamp": datetime(2026, 2, 21, 0, 0),
                "open": 264.0,
                "high": 265.0,
                "low": 263.8,
                "close": 264.4,
                "volume": 1200,
            },
        ]

        manager = MagicMock()
        manager.providers = [provider]

        result = fetch_quotes(["AAPL"], cache_dir=tmp_path, manager=manager)
        assert "AAPL" in result
        assert result["AAPL"]["source"] == "provider"
        assert result["AAPL"]["price"] == 264.4

    def test_stale_cache_refresh_disabled_keeps_cache(self, tmp_path):
        sym_dir = tmp_path / "AAPL"
        sym_dir.mkdir()
        stale = pd.concat(
            [
                _make_1min_df("2024-01-17 10:00", 1),
                _make_1min_df("2024-01-17 10:01", 1),
            ],
            ignore_index=True,
        )
        stale.loc[0, "close"] = 100.0
        stale.loc[1, "close"] = 101.0
        stale.to_parquet(sym_dir / "1min_2024-01-17_2024-01-17.parquet")

        provider = MagicMock()
        provider.get_bars.return_value = [
            {
                "timestamp": datetime(2026, 2, 21, 0, 0),
                "open": 264.0,
                "high": 265.0,
                "low": 263.8,
                "close": 264.4,
                "volume": 1200,
            },
            {
                "timestamp": datetime(2026, 2, 22, 0, 0),
                "open": 265.0,
                "high": 266.0,
                "low": 264.5,
                "close": 265.2,
                "volume": 900,
            },
        ]
        manager = MagicMock()
        manager.providers = [provider]

        result = fetch_quotes(["AAPL"], cache_dir=tmp_path, manager=manager, refresh_stale=False)
        assert "AAPL" in result
        assert result["AAPL"]["source"] == "cache"
        assert result["AAPL"]["price"] == 101.0

    def test_refresh_disabled_missing_cache_fetches_provider(self, tmp_path):
        provider = MagicMock()
        provider.get_bars.return_value = [
            {
                "timestamp": datetime(2026, 2, 21, 0, 0),
                "open": 10.0,
                "high": 11.0,
                "low": 9.0,
                "close": 10.5,
                "volume": 100,
            },
            {
                "timestamp": datetime(2026, 2, 22, 0, 0),
                "open": 10.5,
                "high": 11.5,
                "low": 10.1,
                "close": 11.0,
                "volume": 80,
            },
        ]
        manager = MagicMock()
        manager.providers = [provider]

        result = fetch_quotes(["ZZZZ"], cache_dir=tmp_path, manager=manager, refresh_stale=False)
        assert "ZZZZ" in result
        assert result["ZZZZ"]["source"] == "provider"
        assert provider.get_bars.called


# ---------------------------------------------------------------------------
# fetch_live_quotes
# ---------------------------------------------------------------------------

class TestFetchLiveQuotes:
    def test_none_manager(self):
        assert fetch_live_quotes(["AAPL"], manager=None) == {}

    def test_manager_get_live_quotes(self):
        manager = MagicMock()
        manager.get_live_quotes.return_value = {"AAPL": {"price": 101.25}}
        result = fetch_live_quotes(["AAPL"], manager=manager)
        assert result["AAPL"]["price"] == 101.25
        assert result["AAPL"]["source"] == "live"

    def test_manager_get_live_quotes_ignores_zero_prices(self):
        manager = MagicMock()
        manager.get_live_quotes.return_value = {
            "AAPL": {"price": 0},
            "MSFT": {"price": 410.5},
        }
        result = fetch_live_quotes(["AAPL", "MSFT"], manager=manager)
        assert "AAPL" not in result
        assert result["MSFT"]["price"] == 410.5
        assert result["MSFT"]["source"] == "live"

    def test_polygon_batch_snapshot_fallback(self):
        provider = MagicMock()
        provider.api_key = "test-key"
        provider.base_url = "https://api.polygon.io"
        response = MagicMock()
        response.json.return_value = {
            "tickers": [
                {
                    "ticker": "AAPL",
                    "lastTrade": {"p": 189.42},
                    "todaysChange": 1.23,
                    "todaysChangePerc": 0.65,
                }
            ]
        }
        provider.session.get.return_value = response

        manager = MagicMock()
        manager.providers = [provider]

        result = fetch_live_quotes(["AAPL"], manager=manager)
        assert "AAPL" in result
        assert result["AAPL"]["price"] == 189.42
        assert result["AAPL"]["source"] == "live"

    def test_polygon_batch_snapshot_ignores_zero_prices(self):
        provider = MagicMock()
        provider.api_key = "test-key"
        provider.base_url = "https://api.polygon.io"
        response = MagicMock()
        response.json.return_value = {
            "tickers": [
                {"ticker": "AAPL", "lastTrade": {"p": 0}},
                {"ticker": "MSFT", "lastTrade": {"p": 402.1}},
            ]
        }
        provider.session.get.return_value = response

        manager = MagicMock()
        manager.providers = [provider]

        result = fetch_live_quotes(["AAPL", "MSFT"], manager=manager)
        assert "AAPL" not in result
        assert "MSFT" in result
        assert result["MSFT"]["price"] == 402.1

    def test_get_quotes_fallback(self):
        quote = MagicMock()
        quote.symbol = "MSFT"
        quote.last_price = 402.1
        quote.bid_price = 402.0
        quote.ask_price = 402.2

        manager = MagicMock()
        manager.get_live_quotes.side_effect = AttributeError
        manager.providers = []
        manager.get_quotes.return_value = [quote]

        result = fetch_live_quotes(["MSFT"], manager=manager)
        assert "MSFT" in result
        assert result["MSFT"]["price"] == 402.1
        assert result["MSFT"]["source"] == "live"


# ---------------------------------------------------------------------------
# fetch_bars_batch
# ---------------------------------------------------------------------------

class TestFetchBarsBatch:
    def test_empty_symbols(self):
        result = fetch_bars_batch([], "2024-01-17", "2024-01-17")
        assert result == {}

    def test_multiple_symbols_from_cache(self, tmp_path):
        for sym in ["AAPL", "MSFT"]:
            sym_dir = tmp_path / sym
            sym_dir.mkdir()
            df = _make_1min_df("2024-01-17 10:00", 5)
            df.to_parquet(sym_dir / "1min_2024-01-17_2024-01-17.parquet")

        result = fetch_bars_batch(
            ["AAPL", "MSFT"], "2024-01-17", "2024-01-17", cache_dir=tmp_path,
        )
        assert "AAPL" in result
        assert "MSFT" in result
        assert len(result["AAPL"]) == 5
        assert len(result["MSFT"]) == 5

    def test_missing_symbol_returns_empty_list(self, tmp_path):
        result = fetch_bars_batch(
            ["NONEXIST"], "2024-01-17", "2024-01-17", cache_dir=tmp_path,
        )
        assert result["NONEXIST"] == []
