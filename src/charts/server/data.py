"""Data layer for the chart server.

Handles bar fetching (from parquet cache or MarketDataManager),
timeframe aggregation, and symbol discovery.
"""

from __future__ import annotations

import calendar as cal_mod
import concurrent.futures
import os
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


def _filter_regular_hours(df: pd.DataFrame) -> pd.DataFrame:
    """Filter bars to regular US equity session (09:30-16:00 ET)."""
    if df.empty or "timestamp" not in df.columns:
        return df
    out = convert_timestamps_to_et(df)
    mins = out["timestamp"].dt.hour * 60 + out["timestamp"].dt.minute
    return out[(mins >= (9 * 60 + 30)) & (mins < (16 * 60))]


def _trading_days_between(start: date, end: date) -> list[date]:
    """Return all trading days in [start, end]."""
    days: list[date] = []
    current = start
    while current <= end:
        if _is_trading_day(current):
            days.append(current)
        current += timedelta(days=1)
    return days


def _contiguous_ranges(days: list[date]) -> list[tuple[date, date]]:
    """Group sorted days into contiguous date ranges."""
    if not days:
        return []
    ranges: list[tuple[date, date]] = []
    start = days[0]
    prev = days[0]
    for day in days[1:]:
        if day == prev + timedelta(days=1):
            prev = day
            continue
        ranges.append((start, prev))
        start = day
        prev = day
    ranges.append((start, prev))
    return ranges


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except Exception:
        return default


def _previous_trading_day(d: date) -> date:
    cur = d - timedelta(days=1)
    while not _is_trading_day(cur):
        cur -= timedelta(days=1)
    return cur


def _load_polygon_grouped_daily_quotes(
    symbols: list[str],
    manager: Any,
) -> dict[str, dict[str, Any]]:
    """Fetch many daily quotes at once from Polygon grouped daily endpoint."""
    providers = getattr(manager, "providers", None)
    if not isinstance(providers, (list, tuple)) or not providers:
        return {}
    provider = providers[0]
    session = getattr(provider, "session", None)
    base_url = getattr(provider, "base_url", None)
    api_key = getattr(provider, "api_key", None)
    if session is None or not base_url or not api_key:
        return {}

    wanted = {str(s).upper().strip() for s in symbols if str(s).strip()}
    if not wanted:
        return {}

    def _fetch_grouped(day: date) -> dict[str, dict[str, Any]]:
        try:
            resp = session.get(
                f"{base_url}/v2/aggs/grouped/locale/us/market/stocks/{day.isoformat()}",
                params={"adjusted": "true", "apiKey": api_key},
            )
            if hasattr(resp, "raise_for_status"):
                resp.raise_for_status()
            payload = resp.json() if hasattr(resp, "json") else {}
            rows = payload.get("results", []) if isinstance(payload, dict) else []
            out: dict[str, dict[str, Any]] = {}
            for row in rows:
                sym = str(row.get("T", "")).upper().strip()
                if sym and sym in wanted:
                    out[sym] = row
            return out
        except Exception:
            return {}

    cur_day = date.today()
    while not _is_trading_day(cur_day):
        cur_day -= timedelta(days=1)

    current = _fetch_grouped(cur_day)
    attempts = 0
    while not current and attempts < 5:
        cur_day = _previous_trading_day(cur_day)
        current = _fetch_grouped(cur_day)
        attempts += 1
    if not current:
        return {}

    prev_day = _previous_trading_day(cur_day)
    previous = _fetch_grouped(prev_day)

    quotes: dict[str, dict[str, Any]] = {}
    for sym, row in current.items():
        close = row.get("c")
        if close is None:
            continue
        try:
            price = float(close)
        except Exception:
            continue
        if price <= 0:
            continue

        prev_close = None
        prev_row = previous.get(sym)
        if isinstance(prev_row, dict):
            pc = prev_row.get("c")
            try:
                if pc is not None:
                    prev_close = float(pc)
            except Exception:
                prev_close = None

        change = None
        change_pct = None
        if prev_close is not None and prev_close != 0:
            change = price - prev_close
            change_pct = (change / prev_close) * 100.0

        quotes[sym] = {
            "price": price,
            "change": change,
            "changePct": change_pct,
            "prevClose": prev_close,
            "volume": int(row.get("v", 0) or 0),
            "source": "provider",
        }

    return quotes


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
    timeframe: str = "1min",
) -> pd.DataFrame:
    """Load bars using a MarketDataManager instance."""
    bars = None
    try:
        bars = manager.get_bars(
            symbol,
            date.fromisoformat(start),
            date.fromisoformat(end),
            timeframe=timeframe,
        )
    except Exception:
        if hasattr(manager, "providers") and manager.providers:
            try:
                bars = manager.providers[0].get_bars(
                    symbol,
                    date.fromisoformat(start),
                    date.fromisoformat(end),
                    timeframe=timeframe,
                )
            except Exception:
                return pd.DataFrame()

    if not bars:
        return pd.DataFrame()

    return bars_to_dataframe(bars, tz="America/New_York")


def load_bars_from_primary_provider(
    symbol: str,
    start: str,
    end: str,
    manager: Any,
    timeframe: str = "1min",
) -> pd.DataFrame:
    """Load bars directly from the first provider, bypassing manager cache."""
    providers = getattr(manager, "providers", None)
    if not isinstance(providers, (list, tuple)) or not providers:
        return pd.DataFrame()
    provider = providers[0]
    if not hasattr(provider, "get_bars"):
        return pd.DataFrame()
    try:
        bars = provider.get_bars(
            symbol,
            date.fromisoformat(start),
            date.fromisoformat(end),
            timeframe=timeframe,
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
    if df.empty:
        return []

    ts = df["timestamp"]
    unix_times = [cal_mod.timegm(t.timetuple()) for t in ts]
    opens = [round(float(v), 4) for v in df["open"].tolist()]
    highs = [round(float(v), 4) for v in df["high"].tolist()]
    lows = [round(float(v), 4) for v in df["low"].tolist()]
    closes = [round(float(v), 4) for v in df["close"].tolist()]
    volumes = df["volume"].fillna(0).astype(int).tolist()

    bars: list[dict[str, Any]] = []
    for i in range(len(unix_times)):
        bars.append(
            {
                "time": int(unix_times[i]),
                "open": float(opens[i]),
                "high": float(highs[i]),
                "low": float(lows[i]),
                "close": float(closes[i]),
                "volume": int(volumes[i]),
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
    session: str = "extended",
) -> list[dict[str, Any]]:
    """High-level bar fetcher: cache -> manager -> aggregate -> JSON."""
    tf = str(timeframe or "1min").strip().lower()
    start_date = date.fromisoformat(start)
    end_date = date.fromisoformat(end)

    # Fast path: prefer native provider timeframe for non-1min requests.
    if manager and tf != "1min":
        df_native = load_bars_from_primary_provider(symbol, start, end, manager, timeframe=tf)
        if df_native.empty:
            df_native = load_bars_from_manager(symbol, start, end, manager, timeframe=tf)
        if not df_native.empty:
            df_native = _filter_trading_days(df_native)
            if session == "regular" and not tf.endswith("day"):
                df_native = _filter_regular_hours(df_native)
            if df_native.empty:
                return []
            return bars_to_json(df_native)

    # Fast path: if cache has 1-min bars, aggregate locally for higher
    # timeframes when provider-native fetch is unavailable.
    if tf != "1min" and cache_dir:
        df_cached = load_bars_from_cache(symbol, start, end, cache_dir)
        if not df_cached.empty:
            if manager and end_date >= (date.today() - timedelta(days=3)):
                tail_start = max(start_date, df_cached["timestamp"].max().date())
                fresh_tail = load_bars_from_primary_provider(
                    symbol,
                    tail_start.isoformat(),
                    end,
                    manager,
                    timeframe="1min",
                )
                if fresh_tail.empty:
                    fresh_tail = load_bars_from_manager(
                        symbol,
                        tail_start.isoformat(),
                        end,
                        manager,
                        timeframe="1min",
                    )
                if not fresh_tail.empty:
                    df_cached = pd.concat([df_cached, fresh_tail], ignore_index=True)
                    df_cached = df_cached.sort_values("timestamp").drop_duplicates(subset=["timestamp"])

            df_cached = _filter_trading_days(df_cached)
            if session == "regular" and not tf.endswith("day"):
                df_cached = _filter_regular_hours(df_cached)
            if df_cached.empty:
                return []
            df_cached = aggregate_bars(df_cached, tf)
            if df_cached.empty:
                return []
            return bars_to_json(df_cached)

    df = pd.DataFrame()
    if cache_dir:
        df = load_bars_from_cache(symbol, start, end, cache_dir)
    if manager:
        if df.empty:
            df = load_bars_from_manager(symbol, start, end, manager)
        else:
            cache_min = df["timestamp"].min().date()
            cache_max = df["timestamp"].max().date()
            ranges_to_backfill: list[tuple[date, date]] = []

            # Fast path: fill leading/trailing edges with at most two calls.
            if start_date < cache_min:
                ranges_to_backfill.append((start_date, cache_min - timedelta(days=1)))
            if end_date > cache_max:
                ranges_to_backfill.append((cache_max + timedelta(days=1), end_date))

            # Deep gap scan is optional and only enabled for short date ranges.
            deep_scan_days = _env_int("CHARTS_DEEP_GAP_SCAN_DAYS", 14)
            max_gap_ranges = _env_int("CHARTS_DEEP_GAP_SCAN_MAX_RANGES", 6)
            span_days = (end_date - start_date).days + 1
            if deep_scan_days > 0 and span_days <= deep_scan_days:
                available_days = set(df["timestamp"].dt.date.tolist())
                required_days = _trading_days_between(start_date, end_date)
                missing_days = sorted(d for d in required_days if d not in available_days)
                gap_ranges = _contiguous_ranges(missing_days)
                if len(gap_ranges) <= max_gap_ranges:
                    ranges_to_backfill.extend(gap_ranges)

            if ranges_to_backfill:
                # De-duplicate overlapping ranges while preserving order.
                unique_ranges: list[tuple[date, date]] = []
                seen: set[tuple[date, date]] = set()
                for r in ranges_to_backfill:
                    if r[0] > r[1]:
                        continue
                    if r not in seen:
                        unique_ranges.append(r)
                        seen.add(r)

                backfills: list[pd.DataFrame] = []
                for range_start, range_end in unique_ranges:
                    # Prefer direct provider for ranges that may include latest
                    # data to avoid serving stale manager cache.
                    prefer_provider = range_end >= (date.today() - timedelta(days=3))
                    fetched = pd.DataFrame()
                    if prefer_provider:
                        fetched = load_bars_from_primary_provider(
                            symbol,
                            range_start.isoformat(),
                            range_end.isoformat(),
                            manager,
                        )
                    if fetched.empty:
                        fetched = load_bars_from_manager(
                            symbol,
                            range_start.isoformat(),
                            range_end.isoformat(),
                            manager,
                        )
                    if not fetched.empty:
                        backfills.append(fetched)
                if backfills:
                    df = pd.concat([df, *backfills], ignore_index=True)
                    df = df.sort_values("timestamp").drop_duplicates(subset=["timestamp"])

        # Refresh the tail window for near-current ranges so latest bars do not
        # stay stale when the last cached day already exists.
        if not df.empty and end_date >= (date.today() - timedelta(days=3)):
            tail_start = max(start_date, df["timestamp"].max().date())
            fresh_tail = load_bars_from_primary_provider(
                symbol,
                tail_start.isoformat(),
                end,
                manager,
            )
            if fresh_tail.empty:
                fresh_tail = load_bars_from_manager(
                    symbol,
                    tail_start.isoformat(),
                    end,
                    manager,
                )
            if not fresh_tail.empty:
                df = pd.concat([df, fresh_tail], ignore_index=True)
                df = df.sort_values("timestamp").drop_duplicates(subset=["timestamp"])
    if df.empty:
        return []

    df = _filter_trading_days(df)
    if session == "regular" and not tf.endswith("day"):
        df = _filter_regular_hours(df)
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
    session: str = "extended",
) -> dict[str, list[dict[str, Any]]]:
    """Fetch bars for multiple symbols."""
    return {
        sym: fetch_bars(sym, start, end, timeframe, cache_dir, manager, session=session)
        for sym in symbols
    }


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
    refresh_stale: bool = True,
) -> dict[str, dict[str, Any]]:
    """Fetch latest quote (last price, change, %%change) for each symbol."""
    max_cache_age_days = _env_int("CHARTS_QUOTES_CACHE_MAX_AGE_DAYS", 1)
    stale_cutoff = date.today() - timedelta(days=max_cache_age_days)
    prefetched: dict[str, dict[str, Any]] = {}
    if manager and len(symbols) >= 8:
        prefetched = _load_polygon_grouped_daily_quotes(symbols, manager)

    def _fetch_one(sym: str) -> tuple[str, dict[str, Any] | None]:
        try:
            key = str(sym).upper().strip()
            if key in prefetched:
                return key, prefetched[key]
            bars = None
            source = "cache"
            cache_last_date: date | None = None
            if cache_dir:
                df = _load_latest_bars_from_cache(sym, cache_dir)
                if not df.empty and len(df) >= 2:
                    cache_last_date = df["timestamp"].max().date()
                    bars = dataframe_to_bars(df)
            cache_is_stale = cache_last_date is None or cache_last_date < stale_cutoff
            if manager and ((not bars or len(bars) < 2) or (refresh_stale and cache_is_stale)):
                today = date.today().isoformat()
                start = (date.today() - timedelta(days=10)).isoformat()
                for tf in ("1day", "1min"):
                    df = load_bars_from_primary_provider(
                        sym,
                        start,
                        today,
                        manager,
                        timeframe=tf,
                    )
                    if not df.empty and len(df) >= 2:
                        bars = dataframe_to_bars(df)
                        source = "provider"
                        break
            if manager and (not bars or len(bars) < 2):
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
                return sym, None

            result = derive_quote_from_bars(bars)
            if result is None:
                return sym, None

            result["source"] = source
            return sym, result
        except Exception:
            return sym, None

    quotes: dict[str, dict[str, Any]] = {}
    if not symbols:
        return quotes

    # Parallelize symbol quote resolution to avoid long serial latency when
    # watchlists contain many symbols.
    max_workers = min(8, max(1, len(symbols)))
    if max_workers == 1:
        sym, payload = _fetch_one(symbols[0])
        if payload is not None:
            quotes[sym] = payload
        return quotes

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(_fetch_one, sym) for sym in symbols]
        for fut in concurrent.futures.as_completed(futures):
            sym, payload = fut.result()
            if payload is not None:
                quotes[sym] = payload

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
