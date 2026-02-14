"""Data layer for the chart server.

Handles bar fetching (from parquet cache or MarketDataManager),
timeframe aggregation, and symbol discovery.
"""

from __future__ import annotations

import calendar as cal_mod
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd


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

    df = pd.concat(frames, ignore_index=True)
    df = df.sort_values("timestamp").drop_duplicates(subset=["timestamp"])

    start_dt = pd.Timestamp(start)
    end_dt = pd.Timestamp(end) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
    df = df[(df["timestamp"] >= start_dt) & (df["timestamp"] <= end_dt)]

    # Extended hours: 4 AM - 20:00
    df = df[
        (df["timestamp"].dt.hour >= 4)
        & (df["timestamp"].dt.hour * 60 + df["timestamp"].dt.minute <= 20 * 60)
    ]
    return df


def load_bars_from_manager(
    symbol: str,
    start: str,
    end: str,
    manager: Any,
) -> pd.DataFrame:
    """Load bars using a MarketDataManager instance."""
    try:
        bars = manager.get_bars(
            symbol,
            date.fromisoformat(start),
            date.fromisoformat(end),
            timeframe="1min",
        )
    except Exception:
        return pd.DataFrame()

    if not bars:
        return pd.DataFrame()

    rows = []
    for b in bars:
        rows.append({
            "timestamp": b.timestamp,
            "open": b.open,
            "high": b.high,
            "low": b.low,
            "close": b.close,
            "volume": b.volume,
        })
    return pd.DataFrame(rows)


def aggregate_bars(df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    """Aggregate 1-min bars to a larger timeframe via pandas resample."""
    freq_map: dict[str, str] = {
        "1min": "1min",
        "2min": "2min",
        "3min": "3min",
        "5min": "5min",
        "10min": "10min",
        "15min": "15min",
        "30min": "30min",
        "1hour": "1h",
        "2hour": "2h",
        "4hour": "4h",
        "1day": "1D",
    }
    freq = freq_map.get(timeframe, timeframe)
    if freq == "1min":
        return df

    df = df.set_index("timestamp")
    agg = (
        df.resample(freq)
        .agg({
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum",
        })
        .dropna()
    )
    return agg.reset_index()


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
    # Try cache first
    if cache_dir:
        df = load_bars_from_cache(symbol, start, end, cache_dir)
    elif manager:
        df = load_bars_from_manager(symbol, start, end, manager)
    else:
        return []

    if df.empty:
        return []

    if timeframe != "1min":
        df = aggregate_bars(df, timeframe)
        if df.empty:
            return []

    return bars_to_json(df)
