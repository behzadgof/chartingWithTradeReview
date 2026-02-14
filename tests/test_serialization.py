"""Tests for JSON and CSV serialization."""

import json
import tempfile
from pathlib import Path

from charts.models.trade import TradeRecord
from charts.models.results import BacktestSummary
from charts.models.serialization import (
    load_trades_json,
    save_trades_json,
    load_trades_csv,
    save_trades_csv,
)


class TestJsonSerialization:
    def test_charts_native_round_trip(self, sample_trades):
        summary = BacktestSummary.from_trades(sample_trades, symbol="AAPL")

        with tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w") as f:
            path = Path(f.name)

        try:
            save_trades_json(path, sample_trades, summary)
            loaded_trades, loaded_summary = load_trades_json(path)

            assert len(loaded_trades) == 3
            assert loaded_trades[0].trade_id == "T001"
            assert loaded_trades[1].net_pnl == -152.0
            assert loaded_summary.total_trades == 3
            assert loaded_summary.symbol == "AAPL"
        finally:
            path.unlink(missing_ok=True)

    def test_orb_format_auto_detection(self, sample_orb_trade_dict):
        """Simulate ai_orb BacktestResults JSON format."""
        orb_data = {
            "config": {"strategy": "orb"},
            "trades": [sample_orb_trade_dict],
            "total_trades": 1,
            "winning_trades": 1,
            "losing_trades": 0,
            "win_rate": 1.0,
            "net_pnl": "160.50",
            "symbol": "MSFT",
        }

        with tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w") as f:
            json.dump(orb_data, f)
            path = Path(f.name)

        try:
            trades, summary = load_trades_json(path)
            assert len(trades) == 1
            assert trades[0].symbol == "MSFT"
            assert trades[0].entry_price == 420.50
            assert trades[0].scores["s1_rvol"] == 0.90
            assert summary.total_trades == 1
        finally:
            path.unlink(missing_ok=True)

    def test_empty_trades(self):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w") as f:
            json.dump({"trades": [], "summary": {}}, f)
            path = Path(f.name)

        try:
            trades, summary = load_trades_json(path)
            assert trades == []
            assert summary.total_trades == 0
        finally:
            path.unlink(missing_ok=True)


class TestCsvSerialization:
    def test_round_trip(self, sample_trades):
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False, mode="w") as f:
            path = Path(f.name)

        try:
            save_trades_csv(path, sample_trades)
            loaded = load_trades_csv(path)

            assert len(loaded) == 3
            assert loaded[0].trade_id == "T001"
            assert loaded[0].symbol == "AAPL"
            assert loaded[0].entry_price == 172.50
            assert loaded[1].direction == "SHORT"
            assert loaded[2].net_pnl == 178.0
        finally:
            path.unlink(missing_ok=True)

    def test_scores_flattened(self):
        trades = [
            TradeRecord(
                trade_id="S1", symbol="X", direction="LONG",
                date="2024-01-01",
                scores={"s1_rvol": 0.85, "s2_vwap": 0.72},
            ),
        ]

        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False, mode="w") as f:
            path = Path(f.name)

        try:
            save_trades_csv(path, trades)
            # Read raw CSV to verify score columns exist
            text = path.read_text()
            assert "s1_rvol" in text
            assert "s2_vwap" in text
            assert "0.85" in text
        finally:
            path.unlink(missing_ok=True)

    def test_empty_trades(self):
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False, mode="w") as f:
            path = Path(f.name)

        try:
            save_trades_csv(path, [])
            loaded = load_trades_csv(path)
            assert loaded == []
        finally:
            path.unlink(missing_ok=True)
