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
