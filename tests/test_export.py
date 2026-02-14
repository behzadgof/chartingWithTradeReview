"""Tests for export functionality."""

from __future__ import annotations

from pathlib import Path

import pytest

from charts.models.trade import TradeRecord
from charts.models.results import BacktestSummary
from charts.export.html_export import generate_trade_html
from charts.export.csv_export import export_trades_csv


@pytest.fixture()
def sample_trades():
    return [
        TradeRecord(
            trade_id="T001", symbol="AAPL", direction="LONG", date="2024-01-15",
            entry_price=185.50, exit_price=187.00, quantity=100,
            net_pnl=150.0, gross_pnl=155.0, pnl_pct=0.81,
            or_high=186.0, or_low=184.0, stop_price=183.0,
        ),
    ]


@pytest.fixture()
def sample_summary(sample_trades):
    return BacktestSummary.from_trades(sample_trades, symbol="AAPL")


class TestHtmlExport:
    """Test HTML export generation."""

    def test_generate_html(self, sample_trades, sample_summary, tmp_path):
        output = tmp_path / "trades.html"
        generate_trade_html(
            trades=sample_trades,
            summary=sample_summary,
            output_path=output,
        )
        assert output.exists()
        html = output.read_text(encoding="utf-8")
        # Should contain trade data inlined
        assert "T001" in html
        assert "AAPL" in html
        # Should not have unresolved placeholders
        assert "{{TRADES_JSON}}" not in html
        assert "{{BARS_JSON}}" not in html
        assert "{{SUMMARY_JSON}}" not in html

    def test_generate_html_creates_directory(self, sample_trades, sample_summary, tmp_path):
        output = tmp_path / "subdir" / "deep" / "trades.html"
        generate_trade_html(
            trades=sample_trades,
            summary=sample_summary,
            output_path=output,
        )
        assert output.exists()

    def test_html_contains_chart_library(self, sample_trades, sample_summary, tmp_path):
        output = tmp_path / "trades.html"
        generate_trade_html(
            trades=sample_trades,
            summary=sample_summary,
            output_path=output,
        )
        html = output.read_text(encoding="utf-8")
        # Should contain the vendored lightweight-charts library
        assert "LightweightCharts" in html

    def test_html_contains_indicator_js(self, sample_trades, sample_summary, tmp_path):
        output = tmp_path / "trades.html"
        generate_trade_html(
            trades=sample_trades,
            summary=sample_summary,
            output_path=output,
        )
        html = output.read_text(encoding="utf-8")
        # Should contain shared indicator functions
        assert "calcRSI" in html
        assert "calcMACD" in html

    def test_html_contains_shared_css(self, sample_trades, sample_summary, tmp_path):
        output = tmp_path / "trades.html"
        generate_trade_html(
            trades=sample_trades,
            summary=sample_summary,
            output_path=output,
        )
        html = output.read_text(encoding="utf-8")
        # Should contain shared CSS (no unresolved placeholders)
        assert "{{SHARED_CSS}}" not in html
        assert ".tgl-item" in html

    def test_html_with_bars(self, sample_trades, sample_summary, tmp_path):
        bars = {
            "2024-01-15": [
                {"time": 1705323000, "open": 185.0, "high": 186.0, "low": 184.5, "close": 185.5, "volume": 1000},
            ]
        }
        output = tmp_path / "trades.html"
        generate_trade_html(
            trades=sample_trades,
            summary=sample_summary,
            output_path=output,
            bars_by_date=bars,
        )
        html = output.read_text(encoding="utf-8")
        assert "1705323000" in html

    def test_html_inlines_data(self, sample_trades, sample_summary, tmp_path):
        output = tmp_path / "trades.html"
        generate_trade_html(
            trades=sample_trades,
            summary=sample_summary,
            output_path=output,
        )
        html = output.read_text(encoding="utf-8")
        # Should have replaced null with actual data
        assert "var __TRADES_INLINE__ = null;" not in html
        assert "__TRADES_INLINE__" in html


class TestCsvExport:
    """Test CSV export."""

    def test_export_csv(self, sample_trades, tmp_path):
        output = tmp_path / "trades.csv"
        export_trades_csv(sample_trades, output)
        assert output.exists()
        content = output.read_text()
        assert "T001" in content
        assert "AAPL" in content

    def test_export_csv_empty(self, tmp_path):
        output = tmp_path / "empty.csv"
        export_trades_csv([], output)
        assert output.exists()
