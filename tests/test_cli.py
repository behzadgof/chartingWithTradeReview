"""Tests for the CLI."""

from __future__ import annotations

import argparse
import json
import os
import sys
import types
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from charts.models.trade import TradeRecord
from charts.models.results import BacktestSummary


@pytest.fixture()
def sample_json_file(tmp_path):
    """Create a sample charts-native JSON file."""
    trades = [
        TradeRecord(
            trade_id="T001", symbol="AAPL", direction="LONG", date="2024-01-15",
            entry_price=185.50, exit_price=187.00, quantity=100,
            net_pnl=150.0, gross_pnl=155.0, pnl_pct=0.81,
        ),
    ]
    summary = BacktestSummary.from_trades(trades, symbol="AAPL")
    path = tmp_path / "trades.json"
    data = {
        "trades": [t.to_dict() for t in trades],
        "summary": summary.to_dict(),
    }
    path.write_text(json.dumps(data))
    return path


class TestCLIParsing:
    """Test CLI argument parsing."""

    def test_no_command_exits(self):
        from charts.cli import main
        with pytest.raises(SystemExit) as exc_info:
            with patch("sys.argv", ["charts"]):
                main()
        assert exc_info.value.code == 1

    def test_export_csv(self, sample_json_file, tmp_path):
        from charts.cli import main
        output = tmp_path / "output.csv"
        with patch("sys.argv", [
            "charts", "export-csv",
            "--trades", str(sample_json_file),
            "-o", str(output),
        ]):
            main()
        assert output.exists()
        content = output.read_text()
        assert "T001" in content

    def test_export_html(self, sample_json_file, tmp_path):
        from charts.cli import main
        output = tmp_path / "output.html"
        with patch("sys.argv", [
            "charts", "export-html",
            "--trades", str(sample_json_file),
            "-o", str(output),
        ]):
            main()
        assert output.exists()
        html = output.read_text(encoding="utf-8")
        assert "T001" in html
        assert "LightweightCharts" in html

    def test_serve_default_cache_dir(self):
        from charts.cli import main

        captured = {}

        def _fake_cmd(args):
            captured["cache_dir"] = args.cache_dir

        with patch("charts.cli._cmd_serve", side_effect=_fake_cmd):
            with patch("sys.argv", ["charts", "serve"]):
                main()

        assert captured["cache_dir"] == "data/cache"

    def test_cmd_serve_sets_cache_env_defaults(self, monkeypatch):
        from charts.cli import _cmd_serve

        captured = {}

        class _DummyServer:
            def __init__(self, **kwargs):
                captured.update(kwargs)

            def serve(self):
                captured["served"] = True

        app_mod = types.ModuleType("charts.server.app")
        app_mod.ChartServer = _DummyServer

        serialization_mod = types.ModuleType("charts.models.serialization")
        serialization_mod.load_trades_json = lambda _path: ([], None)

        marketdata_mod = types.ModuleType("marketdata")
        marketdata_mod.create_manager_from_env = lambda: types.SimpleNamespace(providers=[])

        monkeypatch.delenv("MARKET_DATA_CACHE", raising=False)
        monkeypatch.delenv("MARKET_DATA_CACHE_DIR", raising=False)

        with patch.dict(
            sys.modules,
            {
                "charts.server.app": app_mod,
                "charts.models.serialization": serialization_mod,
                "marketdata": marketdata_mod,
            },
        ):
            _cmd_serve(
                argparse.Namespace(
                    trades=None,
                    cache_dir="data/cache",
                    state_dir=None,
                    port=5555,
                    no_browser=True,
                )
            )

        assert os.environ["MARKET_DATA_CACHE"] == "parquet"
        assert os.environ["MARKET_DATA_CACHE_DIR"] == "data/cache"
        assert captured["cache_dir"] == "data/cache"
        assert captured["served"] is True
