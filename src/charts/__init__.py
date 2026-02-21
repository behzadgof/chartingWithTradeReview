"""Interactive market data and trade review charting package.

Quick start::

    from charts import ChartServer
    server = ChartServer(port=5555)
    server.serve()
"""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version

from charts.models.results import BacktestSummary
from charts.models.trade import TradeRecord
from charts.server.app import ChartServer

try:
    from charts.reports import ChartGenerator
except Exception:  # noqa: BLE001
    ChartGenerator = None  # type: ignore[assignment]

try:
    __version__ = version("charts")
except PackageNotFoundError:
    __version__ = "0.0.0"

__all__ = [
    "BacktestSummary",
    "ChartGenerator",
    "ChartServer",
    "TradeRecord",
    "__version__",
]
