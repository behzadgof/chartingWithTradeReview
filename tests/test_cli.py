"""Tests for the CLI."""

from __future__ import annotations

import json
import sys
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
        html = output.read_text()
        assert "T001" in html
        assert "LightweightCharts" in html
