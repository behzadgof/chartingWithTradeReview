"""Data API routes: /api/symbols, /api/bars, /api/quotes, /api/trades."""

from __future__ import annotations

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from charts.server.data import (
    fetch_bars,
    fetch_bars_batch,
    fetch_live_quotes,
    fetch_quotes,
    get_available_symbols,
)

router = APIRouter()


def _state():
    from charts.server.app import server_state
    return server_state


# -- Symbols -----------------------------------------------------------------

@router.get("/api/symbols")
def api_symbols():
    return JSONResponse(get_available_symbols(_state().cache_dir))


# -- Bars --------------------------------------------------------------------

@router.get("/api/bars")
def api_bars(
    symbol: str | None = Query(None),
    start: str | None = Query(None),
    end: str | None = Query(None),
    timeframe: str = Query("1min"),
    session: str = Query("extended"),
):
    if not symbol or not start or not end:
        return JSONResponse({"error": "Missing symbol, start, or end"}, 400)
    try:
        bars = fetch_bars(
            symbol, start, end, timeframe,
            cache_dir=_state().cache_dir,
            manager=_state().market_data,
            session=session,
        )
    except Exception:
        bars = []
    return JSONResponse(bars)


@router.get("/api/bars/batch")
def api_bars_batch(
    symbols: str | None = Query(None),
    start: str | None = Query(None),
    end: str | None = Query(None),
    timeframe: str = Query("1min"),
    session: str = Query("extended"),
):
    if not symbols or not start or not end:
        return JSONResponse({"error": "Missing symbols, start, or end"}, 400)
    syms = [s.strip().upper() for s in symbols.split(",") if s.strip()]
    try:
        result = fetch_bars_batch(
            syms, start, end, timeframe,
            cache_dir=_state().cache_dir,
            manager=_state().market_data,
            session=session,
        )
    except Exception:
        result = {s: [] for s in syms}
    return JSONResponse(result)


# -- Quotes ------------------------------------------------------------------

@router.get("/api/quotes")
def api_quotes(
    symbols: str | None = Query(None),
    refresh: str = Query("0"),
):
    try:
        do_refresh = refresh.strip().lower() in {"1", "true", "yes", "y", "on"}
        if symbols:
            syms = [s.strip().upper() for s in symbols.split(",") if s.strip()]
        else:
            syms = get_available_symbols(_state().cache_dir)
        quotes = fetch_quotes(
            syms,
            cache_dir=_state().cache_dir,
            manager=_state().market_data,
            refresh_stale=do_refresh,
        )
        return JSONResponse(quotes)
    except Exception:
        return JSONResponse({})


@router.get("/api/quotes/live")
def api_quotes_live(symbols: str | None = Query(None)):
    try:
        if symbols:
            syms = [s.strip().upper() for s in symbols.split(",") if s.strip()]
        else:
            syms = get_available_symbols(_state().cache_dir)
        quotes = fetch_live_quotes(syms, manager=_state().market_data)
        return JSONResponse(quotes)
    except Exception:
        return JSONResponse({})


# -- Trades ------------------------------------------------------------------

@router.get("/api/trades")
def api_trades():
    trades = _state().trades
    if trades:
        return JSONResponse([t.to_dict() for t in trades])
    return JSONResponse([])


@router.get("/api/trades/summary")
def api_trades_summary():
    summary = _state().summary
    if summary:
        return JSONResponse(summary.to_dict())
    return JSONResponse({})


@router.get("/api/trades/bars/{trade_date}")
def api_trade_bars(trade_date: str):
    st = _state()
    bars_by_date = st.bars_by_date
    if bars_by_date and trade_date in bars_by_date:
        return JSONResponse(bars_by_date[trade_date])

    trades = st.trades
    symbol = ""
    if trades:
        for t in trades:
            if t.date == trade_date:
                symbol = t.symbol
                break
        if not symbol:
            symbol = trades[0].symbol

    if not symbol:
        return JSONResponse([])

    bars = fetch_bars(
        symbol=symbol,
        start=trade_date,
        end=trade_date,
        timeframe="5min",
        cache_dir=st.cache_dir,
        manager=st.market_data,
    )
    if bars_by_date is not None:
        bars_by_date[trade_date] = bars
    return JSONResponse(bars)
