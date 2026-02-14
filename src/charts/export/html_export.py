"""Generate self-contained HTML trade review charts.

Produces a single HTML file with all JavaScript and data inlined,
viewable offline without a server.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from charts.models.trade import TradeRecord
from charts.models.results import BacktestSummary

_TEMPLATES_DIR = Path(__file__).resolve().parent.parent / "templates"


def generate_trade_html(
    trades: list[TradeRecord],
    summary: BacktestSummary,
    output_path: str | Path,
    bars_by_date: dict[str, list[dict[str, Any]]] | None = None,
    cache_dir: str | Path | None = None,
) -> None:
    """Generate a self-contained HTML trade review chart.

    Args:
        trades: List of trade records to visualize.
        summary: Summary statistics.
        output_path: Path to write the HTML file.
        bars_by_date: Optional pre-loaded bars keyed by date string.
        cache_dir: Optional cache directory for loading bars from parquet.
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Load trade review template
    template_path = _TEMPLATES_DIR / "trades.html"
    html = template_path.read_text(encoding="utf-8")

    # Substitute vendored JS library
    lib_path = _TEMPLATES_DIR / "lightweight-charts.js"
    if lib_path.exists():
        html = html.replace("{{LIB_JS}}", lib_path.read_text(encoding="utf-8"))

    # Substitute shared CSS
    css_path = _TEMPLATES_DIR / "_shared.css"
    if css_path.exists():
        html = html.replace("{{SHARED_CSS}}", css_path.read_text(encoding="utf-8"))
    else:
        html = html.replace("{{SHARED_CSS}}", "")

    # Substitute shared indicator JS
    js_path = _TEMPLATES_DIR / "_indicators.js"
    if js_path.exists():
        html = html.replace(
            "{{SHARED_INDICATORS_JS}}", js_path.read_text(encoding="utf-8"),
        )
    else:
        html = html.replace("{{SHARED_INDICATORS_JS}}", "")

    # If bars not provided, try loading from cache
    if bars_by_date is None and cache_dir:
        bars_by_date = _load_bars_for_trades(trades, cache_dir)

    # Inline trade data
    trades_json = json.dumps([t.to_dict() for t in trades])
    summary_json = json.dumps(summary.to_dict())
    bars_json = json.dumps(bars_by_date or {})

    html = html.replace(
        "var __TRADES_INLINE__ = null;",
        f"var __TRADES_INLINE__ = {trades_json};",
    )
    html = html.replace(
        "var __SUMMARY_INLINE__ = null;",
        f"var __SUMMARY_INLINE__ = {summary_json};",
    )
    html = html.replace(
        "var __BARS_INLINE__ = null;",
        f"var __BARS_INLINE__ = {bars_json};",
    )

    output_path.write_text(html, encoding="utf-8")


def _load_bars_for_trades(
    trades: list[TradeRecord], cache_dir: str | Path,
) -> dict[str, list[dict[str, Any]]]:
    """Load bar data from parquet cache for each trade date."""
    from charts.server.data import fetch_bars

    bars_by_date: dict[str, list[dict[str, Any]]] = {}
    seen_dates: set[str] = set()

    for trade in trades:
        if trade.date in seen_dates:
            continue
        seen_dates.add(trade.date)

        bars = fetch_bars(
            symbol=trade.symbol,
            start=trade.date,
            end=trade.date,
            timeframe="5min",
            cache_dir=cache_dir,
        )
        if bars:
            bars_by_date[trade.date] = bars

    return bars_by_date
