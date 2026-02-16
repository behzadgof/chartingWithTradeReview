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

# Eastern timezone for converting manager (UTC) timestamps to ET
try:
    from zoneinfo import ZoneInfo
    _ET = ZoneInfo("America/New_York")
except ImportError:
    _ET = None  # type: ignore[assignment]

# Market calendar — use full NYSE calendar if available, else weekday-only fallback
try:
    from marketdata.calendar import is_trading_day as _is_trading_day
except ImportError:
    def _is_trading_day(d: date) -> bool:  # type: ignore[misc]
        return d.weekday() < 5


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
        if hasattr(f["timestamp"].dt, "tz") and f["timestamp"].dt.tz is not None:
            if _ET is not None:
                frames[i] = f.assign(
                    timestamp=f["timestamp"].dt.tz_convert(_ET).dt.tz_localize(None)
                )
            else:
                frames[i] = f.assign(timestamp=f["timestamp"].dt.tz_localize(None))
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

    rows = []
    for b in bars:
        ts = b.timestamp
        # Convert to ET then strip timezone to match cache convention (tz-naive ET)
        if hasattr(ts, "tzinfo") and ts.tzinfo is not None:
            if _ET is not None:
                ts = ts.astimezone(_ET).replace(tzinfo=None)
            else:
                ts = ts.replace(tzinfo=None)
        rows.append({
            "timestamp": ts,
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
    freq = freq_map.get(timeframe)
    if freq is None:
        # Normalize custom timeframes: "65min" → "65min", "3hour" → "3h"
        import re
        m = re.match(r"^(\d+)(min|hour|day)$", timeframe)
        if m:
            unit_map = {"min": "min", "hour": "h", "day": "D"}
            freq = m.group(1) + unit_map[m.group(2)]
        else:
            freq = timeframe  # pass through as-is to pandas
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

    # Filter to extended hours only
    df = df[
        (df["timestamp"].dt.hour >= 4)
        & (df["timestamp"].dt.hour * 60 + df["timestamp"].dt.minute <= 20 * 60)
    ]
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
            if cache_dir:
                df = _load_latest_bars_from_cache(sym, cache_dir)
            elif manager:
                today = date.today().isoformat()
                start = (date.today() - pd.Timedelta(days=10)).isoformat()
                df = load_bars_from_manager(sym, start, today, manager)
            else:
                continue

            if df.empty or len(df) < 2:
                continue

            # Aggregate to daily to get open/close per day
            df_daily = df.set_index("timestamp").resample("1D").agg({
                "open": "first", "high": "max", "low": "min",
                "close": "last", "volume": "sum",
            }).dropna()

            if df_daily.empty:
                continue

            last_close = float(df_daily.iloc[-1]["close"])
            last_vol = int(df_daily.iloc[-1]["volume"])
            if len(df_daily) >= 2:
                prev_close = float(df_daily.iloc[-2]["close"])
            else:
                prev_close = float(df_daily.iloc[-1]["open"])

            chg = round(last_close - prev_close, 4)
            chg_pct = round((chg / prev_close) * 100, 2) if prev_close else 0.0

            quotes[sym] = {
                "price": round(last_close, 2),
                "change": chg,
                "changePct": chg_pct,
                "prevClose": round(prev_close, 2),
                "volume": last_vol,
                "source": "cache",
            }
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

    quotes: dict[str, dict[str, Any]] = {}

    # Get raw provider for prevDay fallback
    provider = None
    if hasattr(manager, "providers") and manager.providers:
        provider = manager.providers[0]

    for sym in symbols:
        try:
            # Try get_snapshot first (includes change/changePct)
            if hasattr(manager, "get_snapshot"):
                snap = manager.get_snapshot(sym)
                q = snap.quote
                price = float(q.last_price or 0) if q.last_price else 0.0
                bid = float(q.bid_price or 0)
                ask = float(q.ask_price or 0)
                chg = round(float(snap.change), 4) if snap.change else 0
                chg_pct = round(float(snap.change_pct), 2) if snap.change_pct else 0

                # If last_price is 0 (market closed), derive from change
                if not price and chg and chg_pct:
                    prev_close = abs(chg / (chg_pct / 100)) if chg_pct else 0
                    price = round(prev_close + chg, 4)

                # Still no price — try raw prevDay from provider REST API
                if not price and provider and hasattr(provider, "session"):
                    try:
                        api_key = getattr(provider, "api_key", "")
                        base = getattr(provider, "base_url", "https://api.polygon.io")
                        url = f"{base}/v2/snapshot/locale/us/markets/stocks/tickers/{sym.upper()}"
                        resp = provider.session.get(url, params={"apiKey": api_key})
                        raw = resp.json().get("ticker", {})
                        # Use day close if available, else prevDay close
                        day_c = raw.get("day", {}).get("c", 0)
                        prev_c = raw.get("prevDay", {}).get("c", 0)
                        if day_c:
                            price = round(float(day_c), 4)
                            prev_close = float(prev_c) if prev_c else 0
                            chg = round(price - prev_close, 4) if prev_close else 0
                            chg_pct = round((chg / prev_close) * 100, 2) if prev_close else 0
                        elif prev_c:
                            price = round(float(prev_c), 4)
                            chg = 0
                            chg_pct = 0
                    except Exception:
                        pass

                if not price:
                    continue

                quotes[sym] = {
                    "price": round(price, 4),
                    "bid": round(bid, 4),
                    "ask": round(ask, 4),
                    "change": chg,
                    "changePct": chg_pct,
                    "source": "live",
                }
            elif hasattr(manager, "get_quote"):
                q = manager.get_quote(sym)
                price = float(q.last_price or q.mid_price or 0)
                if not price:
                    continue
                quotes[sym] = {
                    "price": round(price, 4),
                    "bid": round(float(q.bid_price or 0), 4),
                    "ask": round(float(q.ask_price or 0), 4),
                    "source": "live",
                }
        except Exception:
            continue

    return quotes
