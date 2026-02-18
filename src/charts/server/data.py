"""Data layer for the chart server.

Handles bar fetching (from parquet cache or MarketDataManager),
timeframe aggregation, and symbol discovery.
"""

from __future__ import annotations

import calendar as cal_mod
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from marketdata.calendar import is_trading_day as _is_trading_day
from marketdata.utils import (
    aggregate_bars_df,
    bars_to_dataframe,
    convert_timestamps_to_et,
    derive_quote_from_bars,
    filter_trading_hours,
)


def _filter_trading_days(df: pd.DataFrame) -> pd.DataFrame:
    """Remove bars that fall on non-trading days (weekends/holidays)."""
    if df.empty:
        return df
    mask = df["timestamp"].dt.date.map(_is_trading_day)
    return df[mask]


def get_available_symbols(cache_dir: str | Path | None = None) -> list[str]:
    """Return sorted list of symbols with cached 1-min parquet data."""
    if cache_dir is None:
        return []
    cache = Path(cache_dir)
    symbols: list[str] = []
    if cache.exists():
        for d in sorted(cache.iterdir()):
            if d.is_dir() and any(d.glob("1min_*.parquet")):
                symbols.append(d.name)
    return symbols


def load_bars_from_cache(
    symbol: str, start: str, end: str, cache_dir: str | Path,
) -> pd.DataFrame:
    """Load 1-min bars from parquet cache, filtered to [start, end]."""
    sym_dir = Path(cache_dir) / symbol.upper()
    if not sym_dir.exists():
        return pd.DataFrame()

    frames: list[pd.DataFrame] = []
    for pf in sorted(sym_dir.glob("1min_*.parquet")):
        parts = pf.stem.split("_")
        if len(parts) < 3:
            continue
        file_start, file_end = parts[1], parts[2]
        if file_end >= start and file_start <= end:
            frames.append(pd.read_parquet(pf))

    if not frames:
        return pd.DataFrame()

    # Normalize tz-aware timestamps to tz-naive ET before concat
    for i, f in enumerate(frames):
        frames[i] = convert_timestamps_to_et(f)
    df = pd.concat(frames, ignore_index=True)
    df = df.sort_values("timestamp").drop_duplicates(subset=["timestamp"])

    start_dt = pd.Timestamp(start)
    end_dt = pd.Timestamp(end) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
    df = df[(df["timestamp"] >= start_dt) & (df["timestamp"] <= end_dt)]

    df = filter_trading_hours(df)
    return _filter_trading_days(df)


def load_bars_from_manager(
    symbol: str,
    start: str,
    end: str,
    manager: Any,
) -> pd.DataFrame:
    """Load bars using a MarketDataManager instance.

    Tries the manager first; on validation failure, falls back to the
    raw provider to bypass strict ORB-specific validation rules.
    """
    bars = None
    try:
        bars = manager.get_bars(
            symbol,
            date.fromisoformat(start),
            date.fromisoformat(end),
            timeframe="1min",
        )
    except Exception:
        # Validation failed — try raw provider directly
        if hasattr(manager, "providers") and manager.providers:
            try:
                bars = manager.providers[0].get_bars(
                    symbol,
                    date.fromisoformat(start),
                    date.fromisoformat(end),
                    timeframe="1min",
                )
            except Exception:
                return pd.DataFrame()

    if not bars:
        return pd.DataFrame()

    return bars_to_dataframe(bars, tz="America/New_York")


def aggregate_bars(df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    """Aggregate 1-min bars to a larger timeframe via pandas resample."""
    return aggregate_bars_df(df, timeframe)


def bars_to_json(df: pd.DataFrame) -> list[dict[str, Any]]:
    """Convert DataFrame rows to JSON-serializable bar dicts.

    Timestamps are converted to Unix seconds using timegm so the
    TradingView chart displays Eastern Time correctly (since the
    library renders UTC, and our timestamps are already in ET).
    """
    bars: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        ts = row["timestamp"]
        unix_ts = cal_mod.timegm(ts.timetuple())
        bars.append({
            "time": unix_ts,
            "open": round(float(row["open"]), 4),
            "high": round(float(row["high"]), 4),
            "low": round(float(row["low"]), 4),
            "close": round(float(row["close"]), 4),
            "volume": int(row["volume"]),
        })
    return bars


def fetch_bars(
    symbol: str,
    start: str,
    end: str,
    timeframe: str = "1min",
    cache_dir: str | Path | None = None,
    manager: Any = None,
) -> list[dict[str, Any]]:
    """High-level bar fetcher: cache → manager → aggregate → JSON.

    Returns a list of JSON-serializable bar dicts ready for the API response.
    """
    # Try cache first; if cache doesn't cover the full range, supplement
    # with manager data for the missing dates only (no overlap merge).
    df = pd.DataFrame()
    if cache_dir:
        df = load_bars_from_cache(symbol, start, end, cache_dir)
    if manager:
        if df.empty:
            df = load_bars_from_manager(symbol, start, end, manager)
        else:
            # Only fetch dates beyond what the cache covers
            cache_max = df["timestamp"].max().date()
            end_date = date.fromisoformat(end)
            if cache_max < end_date:
                next_day = cache_max + timedelta(days=1)
                mgr_df = load_bars_from_manager(
                    symbol, next_day.isoformat(), end, manager,
                )
                if not mgr_df.empty:
                    df = pd.concat([df, mgr_df], ignore_index=True)
                    df = df.sort_values("timestamp")
    if df.empty:
        return []

    df = _filter_trading_days(df)
    if df.empty:
        return []

    if timeframe != "1min":
        df = aggregate_bars(df, timeframe)
        if df.empty:
            return []

    return bars_to_json(df)


def fetch_bars_batch(
    symbols: list[str],
    start: str,
    end: str,
    timeframe: str = "1min",
    cache_dir: str | Path | None = None,
    manager: Any = None,
) -> dict[str, list[dict[str, Any]]]:
    """Fetch bars for multiple symbols.

    Returns ``{symbol: bars_list}`` where each value is the same format
    as :func:`fetch_bars`.
    """
    return {
        sym: fetch_bars(sym, start, end, timeframe, cache_dir, manager)
        for sym in symbols
    }


def _load_latest_bars_from_cache(
    symbol: str, cache_dir: str | Path,
) -> pd.DataFrame:
    """Load the most recent parquet file for a symbol (for quotes).

    Scans filenames to find the latest data rather than using a fixed
    lookback window, so quotes work regardless of data age.
    """
    sym_dir = Path(cache_dir) / symbol.upper()
    if not sym_dir.exists():
        return pd.DataFrame()

    # Find the parquet file with the most recent end date
    latest_file = None
    latest_end = ""
    for pf in sym_dir.glob("1min_*.parquet"):
        parts = pf.stem.split("_")
        if len(parts) < 3:
            continue
        file_end = parts[2]
        if file_end > latest_end:
            latest_end = file_end
            latest_file = pf

    if latest_file is None:
        return pd.DataFrame()

    df = pd.read_parquet(latest_file)
    if df.empty:
        return df

    df = df.sort_values("timestamp").drop_duplicates(subset=["timestamp"])
    df = filter_trading_hours(df)
    return _filter_trading_days(df)


def fetch_quotes(
    symbols: list[str],
    cache_dir: str | Path | None = None,
    manager: Any = None,
) -> dict[str, dict[str, Any]]:
    """Fetch latest quote (last price, change, %change) for each symbol.

    Returns ``{symbol: {price, change, changePct, prevClose, volume, source}}``
    using the most recent available data (not limited to a fixed lookback).
    """
    quotes: dict[str, dict[str, Any]] = {}
    for sym in symbols:
        try:
            bars = None
            if cache_dir:
                df = _load_latest_bars_from_cache(sym, cache_dir)
                if not df.empty and len(df) >= 2:
                    # Use dataframe_to_bars for conversion
                    from marketdata.utils import dataframe_to_bars
                    bars = dataframe_to_bars(df)
            elif manager:
                today = date.today().isoformat()
                start = (date.today() - pd.Timedelta(days=10)).isoformat()
                try:
                    bars = manager.get_bars(
                        sym, date.fromisoformat(start),
                        date.fromisoformat(today), timeframe="1min",
                    )
                except Exception:
                    pass

            if not bars or len(bars) < 2:
                continue

            result = derive_quote_from_bars(bars)
            if result is None:
                continue

            result["source"] = "cache"
            quotes[sym] = result
        except Exception:
            continue

    return quotes


def fetch_live_quotes(
    symbols: list[str],
    manager: Any = None,
) -> dict[str, dict[str, Any]]:
    """Fetch live quotes from a MarketDataManager via provider API.

    Falls back gracefully per symbol — if live quote fails, that symbol
    is simply omitted from the result.

    Returns ``{symbol: {price, bid, ask, change, changePct, source}}``.
    """
    if manager is None:
        return {}

    # Delegate to manager's get_live_quotes if available
    if hasattr(manager, "get_live_quotes"):
        quotes = manager.get_live_quotes(symbols)
        # Add source field
        for sym in quotes:
            quotes[sym]["source"] = "live"
        return quotes

    return {}
