"""Tests for TradeRecord and BacktestSummary data models."""

from charts.models.trade import TradeRecord
from charts.models.results import BacktestSummary


class TestTradeRecord:
    def test_create(self, sample_trade):
        assert sample_trade.trade_id == "T001"
        assert sample_trade.symbol == "AAPL"
        assert sample_trade.direction == "LONG"
        assert sample_trade.entry_price == 172.50

    def test_is_winner(self, sample_trade):
        assert sample_trade.is_winner is True

    def test_is_loser(self):
        t = TradeRecord(
            trade_id="L001", symbol="X", direction="LONG",
            date="2024-01-01", net_pnl=-50.0,
        )
        assert t.is_winner is False

    def test_to_dict_round_trip(self, sample_trade):
        d = sample_trade.to_dict()
        restored = TradeRecord.from_dict(d)
        assert restored.trade_id == sample_trade.trade_id
        assert restored.symbol == sample_trade.symbol
        assert restored.entry_price == sample_trade.entry_price
        assert restored.net_pnl == sample_trade.net_pnl
        assert restored.scores == sample_trade.scores
        assert restored.targets == sample_trade.targets

    def test_to_json(self, sample_trade):
        import json
        j = sample_trade.to_json()
        d = json.loads(j)
        assert d["trade_id"] == "T001"
        assert d["entry_price"] == 172.50

    def test_from_orb_dict(self, sample_orb_trade_dict):
        t = TradeRecord.from_orb_dict(sample_orb_trade_dict)
        assert t.trade_id == "ORB-001"
        assert t.symbol == "MSFT"
        assert t.direction == "LONG"
        assert t.date == "2024-03-15"
        assert t.entry_price == 420.50
        assert t.exit_price == 423.75
        assert t.quantity == 50
        assert t.targets == [422.0, 424.0, 426.0]
        assert t.net_pnl == 160.50
        assert t.r_multiple == 1.30
        assert t.composite_score == 5.8

    def test_from_orb_dict_scores(self, sample_orb_trade_dict):
        t = TradeRecord.from_orb_dict(sample_orb_trade_dict)
        assert t.scores["s1_rvol"] == 0.90
        assert t.scores["s2_vwap"] == 0.65
        assert t.scores["s7_time"] == 0.75
        assert len(t.scores) == 7

    def test_from_orb_dict_metadata(self, sample_orb_trade_dict):
        t = TradeRecord.from_orb_dict(sample_orb_trade_dict)
        assert t.metadata["ema_9_at_entry"] == 421.30
        assert t.metadata["prior_day_close"] == 418.50
        assert t.metadata["gap_pct"] == 0.48

    def test_defaults(self):
        t = TradeRecord(
            trade_id="X", symbol="X", direction="LONG", date="2024-01-01",
        )
        assert t.entry_price == 0.0
        assert t.net_pnl == 0.0
        assert t.scores == {}
        assert t.metadata == {}
        assert t.targets == []


class TestBacktestSummary:
    def test_from_trades(self, sample_trades):
        s = BacktestSummary.from_trades(sample_trades, symbol="AAPL")
        assert s.total_trades == 3
        assert s.winning_trades == 2
        assert s.losing_trades == 1
        assert s.symbol == "AAPL"
        assert s.net_pnl == 173.0 + (-152.0) + 178.0  # 199.0
        assert s.profit_factor > 0

    def test_from_trades_empty(self):
        s = BacktestSummary.from_trades([])
        assert s.total_trades == 0
        assert s.win_rate == 0.0
        assert s.net_pnl == 0.0

    def test_from_trades_all_winners(self):
        trades = [
            TradeRecord(trade_id="W1", symbol="X", direction="LONG",
                        date="2024-01-01", net_pnl=100.0, gross_pnl=102.0),
            TradeRecord(trade_id="W2", symbol="X", direction="LONG",
                        date="2024-01-02", net_pnl=50.0, gross_pnl=52.0),
        ]
        s = BacktestSummary.from_trades(trades)
        assert s.win_rate == 1.0
        assert s.profit_factor == float("inf")

    def test_to_dict_round_trip(self, sample_trades):
        s = BacktestSummary.from_trades(sample_trades, symbol="AAPL")
        d = s.to_dict()
        restored = BacktestSummary.from_dict(d)
        assert restored.total_trades == s.total_trades
        assert restored.winning_trades == s.winning_trades
        assert restored.symbol == s.symbol

    def test_from_orb_results(self):
        data = {
            "symbol": "AAPL",
            "total_trades": 50,
            "winning_trades": 30,
            "losing_trades": 20,
            "win_rate": 0.60,
            "net_pnl": "5432.10",
            "sharpe_ratio": 1.85,
            "max_drawdown": 0.08,
            "profit_factor": 2.1,
        }
        s = BacktestSummary.from_orb_results(data)
        assert s.total_trades == 50
        assert s.win_rate == 0.60
        assert s.net_pnl == 5432.10
        assert s.sharpe_ratio == 1.85

    def test_avg_win_loss(self, sample_trades):
        s = BacktestSummary.from_trades(sample_trades)
        assert s.avg_win == (173.0 + 178.0) / 2  # 175.5
        assert s.avg_loss == -152.0
