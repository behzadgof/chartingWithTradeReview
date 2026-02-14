"""JSON and CSV serialization for trade records and summaries.

Supports two JSON formats:
  - ai_orb format: {"config": {...}, "trades": [...], "total_trades": N, ...}
  - charts-native: {"trades": [...], "summary": {...}}
Auto-detection is based on the presence of "config" key.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from charts.models.trade import TradeRecord
from charts.models.results import BacktestSummary


def _is_orb_format(data: dict[str, Any]) -> bool:
    """Detect ai_orb BacktestResults JSON format."""
    return "config" in data and "trades" in data


def load_trades_json(
    path: str | Path,
) -> tuple[list[TradeRecord], BacktestSummary]:
    """Load trades from JSON file. Auto-detects ai_orb vs charts-native format.

    Returns:
        Tuple of (list of TradeRecord, BacktestSummary).
    """
    path = Path(path)
    with open(path) as f:
        data = json.load(f)

    if _is_orb_format(data):
        trades = [TradeRecord.from_orb_dict(t) for t in data["trades"]]
        summary = BacktestSummary.from_orb_results(data)
    else:
        trades = [TradeRecord.from_dict(t) for t in data.get("trades", [])]
        summary_data = data.get("summary", {})
        if summary_data:
            summary = BacktestSummary.from_dict(summary_data)
        else:
            summary = BacktestSummary.from_trades(trades)

    return trades, summary


def save_trades_json(
    path: str | Path,
    trades: list[TradeRecord],
    summary: BacktestSummary,
) -> None:
    """Save trades and summary to charts-native JSON format."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    data = {
        "trades": [t.to_dict() for t in trades],
        "summary": summary.to_dict(),
    }
    with open(path, "w") as f:
        json.dump(data, f, indent=2)


# CSV column order for export
_CSV_COLUMNS = [
    "trade_id", "symbol", "direction", "date",
    "signal_time", "fill_time", "exit_time",
    "entry_price", "exit_price", "quantity", "stop_price",
    "gross_pnl", "net_pnl", "commissions", "pnl_pct", "r_multiple",
    "mae", "mfe",
    "or_high", "or_low", "pm_high", "pm_low", "pit_pdh", "pit_pdl",
    "composite_score", "confidence_level", "exit_reason",
]


def load_trades_csv(path: str | Path) -> list[TradeRecord]:
    """Load trades from CSV file."""
    path = Path(path)
    trades: list[TradeRecord] = []
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            trades.append(TradeRecord.from_dict(row))
    return trades


def save_trades_csv(
    path: str | Path,
    trades: list[TradeRecord],
) -> None:
    """Save trades to CSV file."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    # Collect all score keys across all trades
    all_score_keys: list[str] = []
    seen: set[str] = set()
    for t in trades:
        for k in t.scores:
            if k not in seen:
                all_score_keys.append(k)
                seen.add(k)

    columns = list(_CSV_COLUMNS) + all_score_keys

    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        for t in trades:
            row = t.to_dict()
            # Flatten scores into top-level columns
            for k, v in t.scores.items():
                row[k] = v
            # Remove nested dicts that don't go into CSV
            row.pop("scores", None)
            row.pop("metadata", None)
            row.pop("targets", None)
            writer.writerow(row)
