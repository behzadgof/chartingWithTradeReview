# charts

Interactive market data, trade review, and performance report charts.

## Quick Start

```python
from charts import ChartServer

# Market data browser (requires marketdata package)
from marketdata import create_manager_from_env
server = ChartServer(market_data=create_manager_from_env())
server.serve()

# Trade review from backtest results
from charts.models.serialization import load_trades_json
trades, summary = load_trades_json("backtest_results.json")
server = ChartServer(trades=trades, summary=summary)
server.serve()
```

## Performance Report Charts

```python
from charts.reports import ChartGenerator

chart_gen = ChartGenerator()
charts = chart_gen.generate_all_charts(results)
# charts["equity_curve"], charts["drawdown"], ... -> base64 PNG strings
```

## CLI

```bash
# Market data browser
charts serve --cache-dir data/cache

# Trade review
charts serve --trades results.json

# Static HTML export
charts export-html --trades results.json -o chart.html

# CSV export
charts export-csv --trades results.json -o trades.csv
```

## Features

- **Market Data Charts**: Multi-panel layouts, symbol browser, custom timeframes
- **Indicators**: SMA, EMA, WMA, DEMA, TEMA, HMA, RSI, CCI, ADX, MACD, Stochastic, WaveTrend
- **Trade Review**: Calendar navigation, entry/exit markers, P&L details, scoring breakdown
- **Global Sync**: Synchronized crosshair, time scale, symbol, and interval across panels
- **Report Charts**: Equity curve, drawdown, monthly heatmap, distribution, MAE/MFE, rolling Sharpe
- **Export**: Self-contained HTML files and CSV trade logs

## Install

```bash
pip install charts                    # core charting package
pip install charts[marketdata]        # + market data provider support
```
