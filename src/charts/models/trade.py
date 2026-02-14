"""Portable trade record for charting and analysis.

Designed as an interchange format — any trading system can produce
these for visualization without coupling to ai_orb.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any


def _to_float(val: Any, default: float = 0.0) -> float:
    """Safely convert a value to float (handles Decimal strings, None, etc.)."""
    if val is None:
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


@dataclass
class TradeRecord:
    """Portable trade record for charting and analysis.

    All price/P&L fields are float (not Decimal) since this is a charting
    package where float precision is sufficient.  Timestamps are ISO 8601
    strings to avoid timezone complexity in the interchange format.
    """

    # Required identification
    trade_id: str
    symbol: str
    direction: str  # "LONG" or "SHORT"
    date: str  # "YYYY-MM-DD" trade date

    # Timestamps (ISO 8601 strings, optional)
    signal_time: str | None = None
    fill_time: str | None = None
    exit_time: str | None = None

    # Execution
    entry_price: float = 0.0
    exit_price: float = 0.0
    quantity: int = 0
    stop_price: float = 0.0
    targets: list[float] = field(default_factory=list)

    # P&L
    gross_pnl: float = 0.0
    net_pnl: float = 0.0
    commissions: float = 0.0
    pnl_pct: float = 0.0
    r_multiple: float = 0.0

    # Excursion
    mae: float = 0.0
    mfe: float = 0.0

    # Levels (OR, pre-market, prior day)
    or_high: float = 0.0
    or_low: float = 0.0
    pm_high: float = 0.0
    pm_low: float = 0.0
    pit_pdh: float = 0.0
    pit_pdl: float = 0.0

    # Scoring (generic — extensible via scores dict)
    composite_score: float = 0.0
    confidence_level: str = ""
    exit_reason: str = ""
    scores: dict[str, float] = field(default_factory=dict)

    # Extensible metadata
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def is_winner(self) -> bool:
        return self.net_pnl > 0

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "trade_id": self.trade_id,
            "symbol": self.symbol,
            "direction": self.direction,
            "date": self.date,
            "signal_time": self.signal_time,
            "fill_time": self.fill_time,
            "exit_time": self.exit_time,
            "entry_price": self.entry_price,
            "exit_price": self.exit_price,
            "quantity": self.quantity,
            "stop_price": self.stop_price,
            "targets": list(self.targets),
            "gross_pnl": self.gross_pnl,
            "net_pnl": self.net_pnl,
            "commissions": self.commissions,
            "pnl_pct": self.pnl_pct,
            "r_multiple": self.r_multiple,
            "mae": self.mae,
            "mfe": self.mfe,
            "or_high": self.or_high,
            "or_low": self.or_low,
            "pm_high": self.pm_high,
            "pm_low": self.pm_low,
            "pit_pdh": self.pit_pdh,
            "pit_pdl": self.pit_pdl,
            "composite_score": self.composite_score,
            "confidence_level": self.confidence_level,
            "exit_reason": self.exit_reason,
            "scores": dict(self.scores),
            "metadata": dict(self.metadata),
        }

    def to_json(self) -> str:
        """Serialize to JSON string."""
        return json.dumps(self.to_dict(), indent=2)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> TradeRecord:
        """Create TradeRecord from a charts-native dictionary."""
        return cls(
            trade_id=str(data.get("trade_id", "")),
            symbol=str(data.get("symbol", "")),
            direction=str(data.get("direction", "")),
            date=str(data.get("date", "")),
            signal_time=data.get("signal_time"),
            fill_time=data.get("fill_time"),
            exit_time=data.get("exit_time"),
            entry_price=_to_float(data.get("entry_price")),
            exit_price=_to_float(data.get("exit_price")),
            quantity=int(data.get("quantity", 0)),
            stop_price=_to_float(data.get("stop_price")),
            targets=[_to_float(t) for t in data.get("targets", [])],
            gross_pnl=_to_float(data.get("gross_pnl")),
            net_pnl=_to_float(data.get("net_pnl")),
            commissions=_to_float(data.get("commissions")),
            pnl_pct=_to_float(data.get("pnl_pct")),
            r_multiple=_to_float(data.get("r_multiple")),
            mae=_to_float(data.get("mae")),
            mfe=_to_float(data.get("mfe")),
            or_high=_to_float(data.get("or_high")),
            or_low=_to_float(data.get("or_low")),
            pm_high=_to_float(data.get("pm_high")),
            pm_low=_to_float(data.get("pm_low")),
            pit_pdh=_to_float(data.get("pit_pdh")),
            pit_pdl=_to_float(data.get("pit_pdl")),
            composite_score=_to_float(data.get("composite_score")),
            confidence_level=str(data.get("confidence_level", "")),
            exit_reason=str(data.get("exit_reason", "")),
            scores=data.get("scores", {}),
            metadata=data.get("metadata", {}),
        )

    @classmethod
    def from_orb_dict(cls, data: dict[str, Any]) -> TradeRecord:
        """Convert from ai_orb TradeLogRecord.to_dict() format.

        Handles Decimal-string prices, datetime ISO strings, and maps
        s1_rvol..s7_time into the scores dict.
        """
        # Extract trade date from fill_time or signal_time
        trade_date = ""
        for ts_key in ("fill_time", "signal_time", "order_time"):
            ts_val = data.get(ts_key)
            if ts_val and isinstance(ts_val, str):
                trade_date = ts_val[:10]  # "YYYY-MM-DD" from ISO 8601
                break

        # Map s1-s7 scores into generic scores dict
        scores: dict[str, float] = {}
        for key in ("s1_rvol", "s2_vwap", "s3_atr", "s4_trend",
                     "s5_gap", "s6_levels", "s7_time"):
            val = data.get(key)
            if val is not None:
                scores[key] = _to_float(val)

        # Collect extra metadata from gate_results
        metadata: dict[str, Any] = {}
        gate_results = data.get("gate_results", [])
        if gate_results:
            # Extract market context from gate results (EMA, SMA, prior_day_close)
            for gr in gate_results:
                if isinstance(gr, dict):
                    for ctx_key in ("ema_9_at_entry", "ema_21_at_entry",
                                    "sma_20_at_entry", "prior_day_close"):
                        if ctx_key in gr and gr[ctx_key] is not None:
                            metadata[ctx_key] = _to_float(gr[ctx_key])

        # Earnings context
        if data.get("is_earnings_reaction_day"):
            metadata["is_earnings_reaction_day"] = True
            metadata["earnings_call_time"] = data.get("earnings_call_time")
            metadata["days_since_earnings"] = data.get("days_since_earnings")
            metadata["earnings_gap_pct"] = _to_float(data.get("earnings_gap_pct"))
        if data.get("gap_pct") is not None:
            metadata["gap_pct"] = _to_float(data.get("gap_pct"))

        return cls(
            trade_id=str(data.get("trade_id", "")),
            symbol=str(data.get("symbol", "")),
            direction=str(data.get("direction", "")),
            date=trade_date,
            signal_time=data.get("signal_time"),
            fill_time=data.get("fill_time"),
            exit_time=data.get("exit_time"),
            entry_price=_to_float(data.get("entry_price")),
            exit_price=_to_float(data.get("exit_price")),
            quantity=int(data.get("quantity", 0)),
            stop_price=_to_float(data.get("stop_price")),
            targets=[_to_float(t) for t in data.get("targets", [])],
            gross_pnl=_to_float(data.get("gross_pnl")),
            net_pnl=_to_float(data.get("net_pnl")),
            commissions=_to_float(data.get("commissions")),
            pnl_pct=_to_float(data.get("pnl_pct")),
            r_multiple=_to_float(data.get("r_multiple")),
            mae=_to_float(data.get("mae")),
            mfe=_to_float(data.get("mfe")),
            or_high=_to_float(data.get("or_high")),
            or_low=_to_float(data.get("or_low")),
            pit_pdh=_to_float(data.get("pit_pdh")),
            pit_pdl=_to_float(data.get("pit_pdl")),
            composite_score=_to_float(data.get("composite_score")),
            confidence_level=str(data.get("confidence_level", "")),
            exit_reason=str(data.get("exit_reason", "")),
            scores=scores,
            metadata=metadata,
        )
