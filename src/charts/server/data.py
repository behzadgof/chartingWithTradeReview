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

try:
    from marketdata.calendar import is_trading_day as _is_trading_day
    from marketdata.utils import (
        aggregate_bars_df,
        bars_to_dataframe,
        convert_timestamps_to_et,
        dataframe_to_bars,
        derive_quote_from_bars,
        filter_trading_hours,
    )
except ModuleNotFoundError:
    def _is_trading_day(d: date) -> bool:
        return d.weekday() < 5

    def convert_timestamps_to_et(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty or "timestamp" not in df.columns:
            return df
        out = df.copy()
        ts = pd.to_datetime(out["timestamp"])
        if getattr(ts.dt, "tz", None) is not None:
            ts = ts.dt.tz_convert("America/New_York").dt.tz_localize(None)
        out["timestamp"] = ts
        return out

    def filter_trading_hours(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty or "timestamp" not in df.columns:
            return df
        out = convert_timestamps_to_et(df)
        mins = out["timestamp"].dt.hour * 60 + out["timestamp"].dt.minute
        return out[(mins >= 4 * 60) & (mins < 20 * 60)]

    def aggregate_bars_df(df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
        if df.empty or timeframe == "1min":
            return df

        tf = timeframe.strip().lower()
        if tf.endswith("min"):
            minutes = int(tf[:-3])
            rule = f"{minutes}min"
        elif tf.endswith("hour"):
            hours = int(tf[:-4])
            rule = f"{hours}h"
        elif tf.endswith("day"):
            days = int(tf[:-3])
            rule = f"{days}D"
        else:
            raise ValueError(f"Unsupported timeframe: {timeframe}")

        out = convert_timestamps_to_et(df).copy()
        out = out.sort_values("timestamp").set_index("timestamp")
        agg = out.resample(rule, label="left", closed="left").agg(
            {
                "open": "first",
                "high": "max",
                "low": "min",
                "close": "last",
                "volume": "sum",
            }
        )
        agg = agg.dropna(subset=["open", "high", "low", "close"]).reset_index()
        return agg

    def bars_to_dataframe(bars: list[Any], tz: str = "America/New_York") -> pd.DataFrame:
        if not bars:
            return pd.DataFrame()

        rows: list[dict[str, Any]] = []
        for bar in bars:
            if isinstance(bar, dict):
                rows.append(
                    {
                        "timestamp": bar.get("timestamp") or bar.get("time"),
                        "open": bar.get("open"),
                        "high": bar.get("high"),
                        "low": bar.get("low"),
                        "close": bar.get("close"),
                        "volume": bar.get("volume", 0),
                    }
                )
            else:
                rows.append(
                    {
                        "timestamp": getattr(bar, "timestamp", None),
                        "open": getattr(bar, "open", None),
                        "high": getattr(bar, "high", None),
                        "low": getattr(bar, "low", None),
                        "close": getattr(bar, "close", None),
                        "volume": getattr(bar, "volume", 0),
                    }
                )

        out = pd.DataFrame(rows)
        if out.empty:
            return out

        ts = pd.to_datetime(out["timestamp"])
        if getattr(ts.dt, "tz", None) is not None:
            ts = ts.dt.tz_convert(tz).dt.tz_localize(None)
        out["timestamp"] = ts
        out = out.dropna(subset=["timestamp", "open", "high", "low", "close"]) 
        return out

    def dataframe_to_bars(df: pd.DataFrame) -> list[dict[str, Any]]:
        if df.empty:
            return []
        out = []
        for _, row in df.iterrows():
            out.append(
                {
                    "timestamp": row["timestamp"],
                    "open": float(row["open"]),
                    "high": float(row["high"]),
                    "low": float(row["low"]),
                    "close": float(row["close"]),
                    "volume": int(row.get("volume", 0)),
                }
            )
        return out

    def _bar_close(bar: Any) -> float:
        if isinstance(bar, dict):
            return float(bar["close"])
        return float(getattr(bar, "close"))

    def _bar_volume(bar: Any) -> int:
        if isinstance(bar, dict):
            return int(bar.get("volume", 0))
        return int(getattr(bar, "volume", 0))

    def derive_quote_from_bars(bars: list[Any]) -> dict[str, Any] | None:
        if not bars or len(bars) < 2:
            return None
        prev_close = _bar_close(bars[-2])
        price = _bar_close(bars[-1])
        if prev_close == 0:
            return None
        change = price - prev_close
        return {
            "price": price,
            "change": change,
            "changePct": (change / prev_close) * 100.0,
            "prevClose": prev_close,
            "volume": _bar_volume(bars[-1]),
        }


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
    symbol: str,
    start: str,
    end: str,
    cache_dir: str | Path,
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
    """Load bars using a MarketDataManager instance."""
    bars = None
    try:
        bars = manager.get_bars(
            symbol,
            date.fromisoformat(start),
            date.fromisoformat(end),
            timeframe="1min",
        )
    except Exception:
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
    """Convert DataFrame rows to JSON-serializable bar dicts."""
    bars: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        ts = row["timestamp"]
        unix_ts = cal_mod.timegm(ts.timetuple())
        bars.append(
            {
                "time": unix_ts,
                "open": round(float(row["open"]), 4),
                "high": round(float(row["high"]), 4),
                "low": round(float(row["low"]), 4),
                "close": round(float(row["close"]), 4),
                "volume": int(row["volume"]),
            }
        )
    return bars


def fetch_bars(
    symbol: str,
    start: str,
    end: str,
    timeframe: str = "1min",
    cache_dir: str | Path | None = None,
    manager: Any = None,
) -> list[dict[str, Any]]:
    """High-level bar fetcher: cache -> manager -> aggregate -> JSON."""
    df = pd.DataFrame()
    if cache_dir:
        df = load_bars_from_cache(symbol, start, end, cache_dir)
    if manager:
        if df.empty:
            df = load_bars_from_manager(symbol, start, end, manager)
        else:
            cache_max = df["timestamp"].max().date()
            end_date = date.fromisoformat(end)
            if cache_max < end_date:
                next_day = cache_max + timedelta(days=1)
                mgr_df = load_bars_from_manager(symbol, next_day.isoformat(), end, manager)
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
    """Fetch bars for multiple symbols."""
    return {sym: fetch_bars(sym, start, end, timeframe, cache_dir, manager) for sym in symbols}


def _load_latest_bars_from_cache(symbol: str, cache_dir: str | Path) -> pd.DataFrame:
    """Load the most recent parquet file for a symbol (for quotes)."""
    sym_dir = Path(cache_dir) / symbol.upper()
    if not sym_dir.exists():
        return pd.DataFrame()

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
    """Fetch latest quote (last price, change, %%change) for each symbol."""
    quotes: dict[str, dict[str, Any]] = {}
    for sym in symbols:
        try:
            bars = None
            source = "cache"
            if cache_dir:
                df = _load_latest_bars_from_cache(sym, cache_dir)
                if not df.empty and len(df) >= 2:
                    bars = dataframe_to_bars(df)
            if (not bars or len(bars) < 2) and manager:
                today = date.today().isoformat()
                start = (date.today() - timedelta(days=10)).isoformat()
                for tf in ("1day", "1min"):
                    try:
                        bars = manager.get_bars(
                            sym,
                            date.fromisoformat(start),
                            date.fromisoformat(today),
                            timeframe=tf,
                        )
                        if bars and len(bars) >= 2:
                            source = "provider"
                            break
                    except Exception:
                        continue

            if not bars or len(bars) < 2:
                continue

            result = derive_quote_from_bars(bars)
            if result is None:
                continue

            result["source"] = source
            quotes[sym] = result
        except Exception:
            continue

    return quotes


def fetch_live_quotes(symbols: list[str], manager: Any = None) -> dict[str, dict[str, Any]]:
    """Fetch live quotes from a MarketDataManager via provider API."""
    if manager is None:
        return {}

    normalized: list[str] = []
    seen: set[str] = set()
    for sym in symbols:
        key = str(sym or "").upper().strip()
        if key and key not in seen:
            normalized.append(key)
            seen.add(key)
    if not normalized:
        return {}

    if hasattr(manager, "get_live_quotes"):
        try:
            quotes = manager.get_live_quotes(normalized)
            if isinstance(quotes, dict):
                cleaned: dict[str, dict[str, Any]] = {}
                for sym, payload in quotes.items():
                    if not isinstance(payload, dict):
                        continue
                    try:
                        price = float(payload.get("price"))
                    except Exception:
                        continue
                    if price <= 0:
                        continue
                    item = dict(payload)
                    item["price"] = price
                    item["source"] = "live"
                    cleaned[str(sym).upper().strip()] = item
                if cleaned:
                    return cleaned
        except Exception:
            pass

    # Polygon batch snapshot fallback (single request for many symbols).
    providers = getattr(manager, "providers", None)
    if providers:
        provider = providers[0]
        session = getattr(provider, "session", None)
        base_url = getattr(provider, "base_url", None)
        api_key = getattr(provider, "api_key", None)
        if session is not None and base_url and api_key:
            try:
                out: dict[str, dict[str, Any]] = {}
                chunk_size = 200
                for i in range(0, len(normalized), chunk_size):
                    chunk = normalized[i : i + chunk_size]
                    resp = session.get(
                        f"{base_url}/v2/snapshot/locale/us/markets/stocks/tickers",
                        params={"apiKey": api_key, "tickers": ",".join(chunk)},
                    )
                    if hasattr(resp, "raise_for_status"):
                        resp.raise_for_status()
                    payload = resp.json() if hasattr(resp, "json") else {}
                    tickers = payload.get("tickers", []) if isinstance(payload, dict) else []
                    for row in tickers:
                        sym = str(row.get("ticker", "")).upper().strip()
                        if not sym:
                            continue
                        last_trade = row.get("lastTrade") or {}
                        day = row.get("day") or {}
                        price = last_trade.get("p")
                        if price is None:
                            price = day.get("c")
                        if price is None:
                            continue
                        try:
                            price = float(price)
                        except Exception:
                            continue
                        if price <= 0:
                            continue
                        change = row.get("todaysChange")
                        change_pct = row.get("todaysChangePerc")
                        out[sym] = {
                            "price": price,
                            "change": float(change) if change is not None else None,
                            "changePct": float(change_pct) if change_pct is not None else None,
                            "source": "live",
                        }
                if out:
                    return out
            except Exception:
                pass

    # Generic provider fallback: synthesize a quote from Quote model objects.
    out: dict[str, dict[str, Any]] = {}

    def _price_from_quote_obj(q: Any) -> float | None:
        last = getattr(q, "last_price", None)
        if last is not None:
            try:
                if float(last) > 0:
                    return float(last)
            except Exception:
                pass
        bid = getattr(q, "bid_price", None)
        ask = getattr(q, "ask_price", None)
        try:
            if bid is not None and ask is not None and float(bid) > 0 and float(ask) > 0:
                return (float(bid) + float(ask)) / 2.0
        except Exception:
            pass
        return None

    if hasattr(manager, "get_quotes"):
        try:
            quotes = manager.get_quotes(normalized)
            for q in quotes:
                sym = str(getattr(q, "symbol", "")).upper().strip()
                price = _price_from_quote_obj(q)
                if sym and price is not None:
                    out[sym] = {
                        "price": float(price),
                        "change": None,
                        "changePct": None,
                        "source": "live",
                    }
        except Exception:
            pass

    if not out and hasattr(manager, "get_quote"):
        for sym in normalized:
            try:
                q = manager.get_quote(sym)
                price = _price_from_quote_obj(q)
                if price is not None:
                    out[sym] = {
                        "price": float(price),
                        "change": None,
                        "changePct": None,
                        "source": "live",
                    }
            except Exception:
                continue

    return out
