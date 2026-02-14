"""Shared test fixtures for the charts package."""

import pytest

from charts.models.trade import TradeRecord
from charts.models.results import BacktestSummary


@pytest.fixture
def sample_trade() -> TradeRecord:
    return TradeRecord(
        trade_id="T001",
        symbol="AAPL",
        direction="LONG",
        date="2024-03-15",
        signal_time="2024-03-15T09:35:00-04:00",
        fill_time="2024-03-15T09:35:05-04:00",
        exit_time="2024-03-15T10:30:00-04:00",
        entry_price=172.50,
        exit_price=174.25,
        quantity=100,
        stop_price=171.00,
        targets=[173.50, 174.50, 175.50],
        gross_pnl=175.00,
        net_pnl=173.00,
        commissions=2.00,
        pnl_pct=1.01,
        r_multiple=1.17,
        mae=0.75,
        mfe=2.10,
        or_high=173.00,
        or_low=171.50,
        pm_high=172.80,
        pm_low=170.50,
        pit_pdh=173.50,
        pit_pdl=170.00,
        composite_score=6.2,
        confidence_level="HIGH",
        exit_reason="target_1",
        scores={"s1_rvol": 0.85, "s2_vwap": 0.72, "s3_atr": 0.65},
    )


@pytest.fixture
def sample_trades() -> list[TradeRecord]:
    return [
        TradeRecord(
            trade_id="T001", symbol="AAPL", direction="LONG",
            date="2024-03-15", entry_price=172.50, exit_price=174.25,
            quantity=100, net_pnl=173.00, gross_pnl=175.00,
            commissions=2.00, mae=0.75, mfe=2.10,
        ),
        TradeRecord(
            trade_id="T002", symbol="AAPL", direction="SHORT",
            date="2024-03-16", entry_price=175.00, exit_price=176.50,
            quantity=100, net_pnl=-152.00, gross_pnl=-150.00,
            commissions=2.00, mae=2.00, mfe=0.50,
        ),
        TradeRecord(
            trade_id="T003", symbol="AAPL", direction="LONG",
            date="2024-03-17", entry_price=174.00, exit_price=175.80,
            quantity=100, net_pnl=178.00, gross_pnl=180.00,
            commissions=2.00, mae=0.30, mfe=2.50,
        ),
    ]


@pytest.fixture
def sample_orb_trade_dict() -> dict:
    """A dictionary in ai_orb TradeLogRecord.to_dict() format."""
    return {
        "trade_id": "ORB-001",
        "symbol": "MSFT",
        "direction": "LONG",
        "signal_time": "2024-03-15T09:35:00-04:00",
        "fill_time": "2024-03-15T09:35:05-04:00",
        "exit_time": "2024-03-15T10:30:00-04:00",
        "entry_price": "420.50",
        "exit_price": "423.75",
        "quantity": 50,
        "stop_price": "418.00",
        "targets": ["422.00", "424.00", "426.00"],
        "gross_pnl": "162.50",
        "net_pnl": "160.50",
        "commissions": "2.00",
        "pnl_pct": "0.77",
        "r_multiple": "1.30",
        "mae": "0.50",
        "mfe": "3.75",
        "or_high": "421.00",
        "or_low": "419.50",
        "pit_pdh": "422.00",
        "pit_pdl": "417.50",
        "composite_score": 5.8,
        "confidence_level": "MEDIUM",
        "s1_rvol": 0.90,
        "s2_vwap": 0.65,
        "s3_atr": 0.70,
        "s4_trend": 0.55,
        "s5_gap": 0.80,
        "s6_levels": 0.60,
        "s7_time": 0.75,
        "exit_reason": "target_1",
        "gate_results": [
            {"ema_9_at_entry": "421.30", "ema_21_at_entry": "419.80",
             "prior_day_close": "418.50"},
        ],
        "is_earnings_reaction_day": False,
        "gap_pct": 0.48,
    }
