"""Lightweight backtest summary for charting.

Holds scalar summary statistics only (no DataFrames/equity curves).
Designed for the trade review UI sidebar and HTML export.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from charts.models.trade import TradeRecord, _to_float


@dataclass
class BacktestSummary:
    """Summary statistics for a set of trades."""

    symbol: str = ""
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    win_rate: float = 0.0
    net_pnl: float = 0.0
    gross_pnl: float = 0.0
    total_commissions: float = 0.0
    sharpe_ratio: float = 0.0
    max_drawdown: float = 0.0
    profit_factor: float = 0.0
    avg_win: float = 0.0
    avg_loss: float = 0.0
    expectancy: float = 0.0
    avg_mae: float = 0.0
    avg_mfe: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "symbol": self.symbol,
            "total_trades": self.total_trades,
            "winning_trades": self.winning_trades,
            "losing_trades": self.losing_trades,
            "win_rate": round(self.win_rate, 4),
            "net_pnl": round(self.net_pnl, 2),
            "gross_pnl": round(self.gross_pnl, 2),
            "total_commissions": round(self.total_commissions, 2),
            "sharpe_ratio": round(self.sharpe_ratio, 4),
            "max_drawdown": round(self.max_drawdown, 4),
            "profit_factor": round(self.profit_factor, 4),
            "avg_win": round(self.avg_win, 2),
            "avg_loss": round(self.avg_loss, 2),
            "expectancy": round(self.expectancy, 2),
            "avg_mae": round(self.avg_mae, 4),
            "avg_mfe": round(self.avg_mfe, 4),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> BacktestSummary:
        """Create from a charts-native dictionary."""
        return cls(
            symbol=str(data.get("symbol", "")),
            total_trades=int(data.get("total_trades", 0)),
            winning_trades=int(data.get("winning_trades", 0)),
            losing_trades=int(data.get("losing_trades", 0)),
            win_rate=_to_float(data.get("win_rate")),
            net_pnl=_to_float(data.get("net_pnl")),
            gross_pnl=_to_float(data.get("gross_pnl")),
            total_commissions=_to_float(data.get("total_commissions")),
            sharpe_ratio=_to_float(data.get("sharpe_ratio")),
            max_drawdown=_to_float(data.get("max_drawdown")),
            profit_factor=_to_float(data.get("profit_factor")),
            avg_win=_to_float(data.get("avg_win")),
            avg_loss=_to_float(data.get("avg_loss")),
            expectancy=_to_float(data.get("expectancy")),
            avg_mae=_to_float(data.get("avg_mae")),
            avg_mfe=_to_float(data.get("avg_mfe")),
        )

    @classmethod
    def from_trades(cls, trades: list[TradeRecord], symbol: str = "") -> BacktestSummary:
        """Compute summary statistics from a list of TradeRecords."""
        if not trades:
            return cls(symbol=symbol)

        winners = [t for t in trades if t.net_pnl > 0]
        losers = [t for t in trades if t.net_pnl <= 0]

        total = len(trades)
        n_win = len(winners)
        n_loss = len(losers)

        gross = sum(t.gross_pnl for t in trades)
        net = sum(t.net_pnl for t in trades)
        comms = sum(t.commissions for t in trades)

        avg_w = sum(t.net_pnl for t in winners) / n_win if n_win else 0.0
        avg_l = sum(t.net_pnl for t in losers) / n_loss if n_loss else 0.0

        total_wins = sum(t.net_pnl for t in winners)
        total_losses = abs(sum(t.net_pnl for t in losers))
        pf = total_wins / total_losses if total_losses > 0 else float("inf")

        win_rate = n_win / total if total else 0.0
        expectancy = (win_rate * avg_w) + ((1 - win_rate) * avg_l)

        avg_mae = sum(t.mae for t in trades) / total if total else 0.0
        avg_mfe = sum(t.mfe for t in trades) / total if total else 0.0

        sym = symbol or (trades[0].symbol if trades else "")

        return cls(
            symbol=sym,
            total_trades=total,
            winning_trades=n_win,
            losing_trades=n_loss,
            win_rate=win_rate,
            net_pnl=net,
            gross_pnl=gross,
            total_commissions=comms,
            profit_factor=pf,
            avg_win=avg_w,
            avg_loss=avg_l,
            expectancy=expectancy,
            avg_mae=avg_mae,
            avg_mfe=avg_mfe,
        )

    @classmethod
    def from_orb_results(cls, data: dict[str, Any]) -> BacktestSummary:
        """Convert from ai_orb BacktestResults.to_dict() format."""
        return cls(
            symbol=str(data.get("symbol", "")),
            total_trades=int(data.get("total_trades", 0)),
            winning_trades=int(data.get("winning_trades", 0)),
            losing_trades=int(data.get("losing_trades", 0)),
            win_rate=_to_float(data.get("win_rate")),
            net_pnl=_to_float(data.get("net_pnl")),
            gross_pnl=_to_float(data.get("gross_pnl")),
            total_commissions=_to_float(data.get("total_commissions")),
            sharpe_ratio=_to_float(data.get("sharpe_ratio")),
            max_drawdown=_to_float(data.get("max_drawdown")),
            profit_factor=_to_float(data.get("profit_factor")),
            avg_win=_to_float(data.get("avg_win")),
            avg_loss=_to_float(data.get("avg_loss")),
            expectancy=_to_float(data.get("expectancy")),
            avg_mae=_to_float(data.get("avg_mae")),
            avg_mfe=_to_float(data.get("avg_mfe")),
        )
