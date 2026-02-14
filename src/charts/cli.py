"""CLI for the charts package.

Commands:
    charts serve            Start the chart server
    charts export-html      Generate self-contained HTML trade chart
    charts export-csv       Export trades to CSV
"""

from __future__ import annotations

import argparse
import sys


def _cmd_serve(args: argparse.Namespace) -> None:
    """Start the chart server."""
    from charts.server.app import ChartServer
    from charts.models.serialization import load_trades_json

    trades = None
    summary = None

    if args.trades:
        trades, summary = load_trades_json(args.trades)
        print(f"Loaded {len(trades)} trades from {args.trades}")

    market_data = None
    if not args.trades or args.cache_dir:
        # Try to create a MarketDataManager if marketdata package available
        try:
            from marketdata import create_manager_from_env
            market_data = create_manager_from_env()
        except (ImportError, Exception):
            pass

    server = ChartServer(
        market_data=market_data,
        trades=trades,
        summary=summary,
        cache_dir=args.cache_dir,
        port=args.port,
        auto_open=not args.no_browser,
    )
    server.serve()


def _cmd_export_html(args: argparse.Namespace) -> None:
    """Generate self-contained HTML trade chart."""
    from charts.models.serialization import load_trades_json
    from charts.export.html_export import generate_trade_html

    trades, summary = load_trades_json(args.trades)
    print(f"Loaded {len(trades)} trades from {args.trades}")

    generate_trade_html(
        trades=trades,
        summary=summary,
        output_path=args.output,
        cache_dir=args.cache_dir,
    )
    print(f"Exported to {args.output}")


def _cmd_export_csv(args: argparse.Namespace) -> None:
    """Export trades to CSV."""
    from charts.models.serialization import load_trades_json, save_trades_csv

    trades, _ = load_trades_json(args.trades)
    save_trades_csv(args.output, trades)
    print(f"Exported {len(trades)} trades to {args.output}")


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="charts",
        description="Interactive market data and trade review charts",
    )
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # -- serve --
    p_serve = subparsers.add_parser("serve", help="Start the chart server")
    p_serve.add_argument("--port", type=int, default=5555, help="Server port")
    p_serve.add_argument("--cache-dir", default=None, help="Parquet cache directory")
    p_serve.add_argument("--trades", default=None, help="Backtest results JSON file")
    p_serve.add_argument("--no-browser", action="store_true", help="Don't auto-open browser")
    p_serve.set_defaults(func=_cmd_serve)

    # -- export-html --
    p_html = subparsers.add_parser("export-html", help="Generate self-contained HTML")
    p_html.add_argument("--trades", required=True, help="Backtest results JSON file")
    p_html.add_argument("-o", "--output", required=True, help="Output HTML file path")
    p_html.add_argument("--cache-dir", default=None, help="Parquet cache directory")
    p_html.set_defaults(func=_cmd_export_html)

    # -- export-csv --
    p_csv = subparsers.add_parser("export-csv", help="Export trades to CSV")
    p_csv.add_argument("--trades", required=True, help="Backtest results JSON file")
    p_csv.add_argument("-o", "--output", required=True, help="Output CSV file path")
    p_csv.set_defaults(func=_cmd_export_csv)

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    args.func(args)
