"""CSV export for trade records.

Convenience wrapper around charts.models.serialization.save_trades_csv.
"""

from __future__ import annotations

from pathlib import Path

from charts.models.trade import TradeRecord
from charts.models.serialization import save_trades_csv


def export_trades_csv(
    trades: list[TradeRecord],
    output_path: str | Path,
) -> None:
    """Export trade records to CSV file.

    Args:
        trades: List of trade records.
        output_path: Path to write the CSV file.
    """
    save_trades_csv(output_path, trades)
