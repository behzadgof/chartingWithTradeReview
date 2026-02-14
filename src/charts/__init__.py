"""Interactive market data and trade review charts.

Quick start::

    from charts import ChartServer
    server = ChartServer(port=5555)
    server.serve()
"""

__version__ = "0.1.0"

from charts.models.trade import TradeRecord
from charts.models.results import BacktestSummary
from charts.server.app import ChartServer

__all__ = [
    "ChartServer",
    "TradeRecord",
    "BacktestSummary",
    "__version__",
]
