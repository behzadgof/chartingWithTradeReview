"""Matplotlib-based performance chart generation utilities.

This module provides ``ChartGenerator`` for creating report-ready PNG charts
(equity, drawdown, monthly heatmap, trade distribution, MAE/MFE, rolling Sharpe)
from backtest-style result objects.
"""

from __future__ import annotations

import base64
import io
import logging
from pathlib import Path
from typing import Protocol, Sequence

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

# Non-interactive backend for server-side rendering.
matplotlib.use("Agg")

logger = logging.getLogger(__name__)

_DEFAULT_FIGSIZE = (12, 6)
_DEFAULT_DPI = 100
_COLORS = {
    "equity": "#2196F3",
    "benchmark": "#9E9E9E",
    "drawdown": "#F44336",
    "win": "#4CAF50",
    "loss": "#F44336",
    "neutral": "#9E9E9E",
    "rolling": "#FF9800",
}


class TradeLike(Protocol):
    """Protocol for minimal trade attributes required by charting."""

    net_pnl: float
    mae: float
    mfe: float


class BacktestResultsLike(Protocol):
    """Protocol for minimal backtest results attributes required by charting."""

    equity_curve: pd.DataFrame
    daily_returns: pd.Series
    trades: Sequence[TradeLike]


class ChartGenerator:
    """Generates trading performance charts."""

    def __init__(
        self,
        figsize: tuple[int, int] = _DEFAULT_FIGSIZE,
        dpi: int = _DEFAULT_DPI,
    ) -> None:
        self.figsize = figsize
        self.dpi = dpi

    def equity_curve(
        self,
        results: BacktestResultsLike,
        benchmark: pd.Series | None = None,
    ) -> plt.Figure:
        """Generate equity curve chart."""
        fig, ax = plt.subplots(figsize=self.figsize)

        equity_df = getattr(results, "equity_curve", pd.DataFrame())
        if equity_df.empty or "equity" not in equity_df.columns:
            ax.text(
                0.5,
                0.5,
                "No equity data available",
                ha="center",
                va="center",
                transform=ax.transAxes,
            )
            ax.set_title("Equity Curve")
            fig.tight_layout()
            return fig

        dates = equity_df["date"] if "date" in equity_df.columns else equity_df.index
        equity = equity_df["equity"]

        ax.plot(dates, equity, color=_COLORS["equity"], linewidth=1.5, label="Strategy")

        if benchmark is not None and len(benchmark) > 0:
            ax.plot(
                benchmark.index,
                benchmark.values,
                color=_COLORS["benchmark"],
                linewidth=1.0,
                linestyle="--",
                label="Benchmark",
            )
            ax.legend(loc="upper left")

        ax.set_title("Equity Curve")
        ax.set_xlabel("Date")
        ax.set_ylabel("Equity ($)")
        ax.grid(True, alpha=0.3)
        ax.ticklabel_format(style="plain", axis="y")

        fig.tight_layout()
        return fig

    def drawdown_chart(self, results: BacktestResultsLike) -> plt.Figure:
        """Generate underwater (drawdown) chart."""
        fig, ax = plt.subplots(figsize=self.figsize)

        equity_df = getattr(results, "equity_curve", pd.DataFrame())
        if equity_df.empty or "drawdown" not in equity_df.columns:
            ax.text(
                0.5,
                0.5,
                "No drawdown data available",
                ha="center",
                va="center",
                transform=ax.transAxes,
            )
            ax.set_title("Drawdown (Underwater)")
            fig.tight_layout()
            return fig

        dates = equity_df["date"] if "date" in equity_df.columns else equity_df.index
        drawdown = equity_df["drawdown"]
        dd_values = -abs(drawdown)

        ax.fill_between(dates, 0, dd_values, color=_COLORS["drawdown"], alpha=0.4)
        ax.plot(dates, dd_values, color=_COLORS["drawdown"], linewidth=0.8)

        ax.set_title("Drawdown (Underwater)")
        ax.set_xlabel("Date")
        ax.set_ylabel("Drawdown")
        ax.yaxis.set_major_formatter(matplotlib.ticker.PercentFormatter(xmax=1.0))
        ax.grid(True, alpha=0.3)
        ax.set_ylim(top=0)

        fig.tight_layout()
        return fig

    def monthly_returns_heatmap(self, results: BacktestResultsLike) -> plt.Figure:
        """Generate monthly returns heatmap."""
        fig, ax = plt.subplots(figsize=self.figsize)

        daily_returns_raw = getattr(results, "daily_returns", None)
        daily_returns = pd.Series(dtype=float)
        if daily_returns_raw is not None:
            daily_returns = pd.Series(daily_returns_raw).dropna()

        if len(daily_returns) == 0:
            ax.text(
                0.5,
                0.5,
                "No return data available",
                ha="center",
                va="center",
                transform=ax.transAxes,
            )
            ax.set_title("Monthly Returns Heatmap")
            fig.tight_layout()
            return fig

        idx = pd.DatetimeIndex(pd.to_datetime(daily_returns.index))
        monthly = daily_returns.groupby([idx.year, idx.month]).sum()

        years = sorted({k[0] for k in monthly.index})
        month_names = [
            "Jan",
            "Feb",
            "Mar",
            "Apr",
            "May",
            "Jun",
            "Jul",
            "Aug",
            "Sep",
            "Oct",
            "Nov",
            "Dec",
        ]

        data = np.full((len(years), 12), np.nan)
        for (yr, mo), val in monthly.items():
            row = years.index(yr)
            col = mo - 1
            data[row, col] = val

        if np.all(np.isnan(data)):
            vmax = 0.1
        else:
            vmax = max(abs(np.nanmin(data)), abs(np.nanmax(data)))
            if vmax == 0:
                vmax = 0.1

        im = ax.imshow(
            data,
            cmap="RdYlGn",
            aspect="auto",
            vmin=-vmax,
            vmax=vmax,
        )

        ax.set_xticks(range(12))
        ax.set_xticklabels(month_names)
        ax.set_yticks(range(len(years)))
        ax.set_yticklabels([str(y) for y in years])

        for i in range(len(years)):
            for j in range(12):
                val = data[i, j]
                if not np.isnan(val):
                    ax.text(
                        j,
                        i,
                        f"{val:.1%}",
                        ha="center",
                        va="center",
                        fontsize=8,
                        color="black" if abs(val) < vmax * 0.6 else "white",
                    )

        ax.set_title("Monthly Returns Heatmap")
        fig.colorbar(im, ax=ax, format=matplotlib.ticker.PercentFormatter(xmax=1.0))
        fig.tight_layout()
        return fig

    def trade_distribution(self, trades: Sequence[TradeLike]) -> plt.Figure:
        """Generate trade P&L distribution histogram."""
        fig, ax = plt.subplots(figsize=self.figsize)

        if not trades:
            ax.text(
                0.5,
                0.5,
                "No trade data available",
                ha="center",
                va="center",
                transform=ax.transAxes,
            )
            ax.set_title("Trade P&L Distribution")
            fig.tight_layout()
            return fig

        pnls = [float(t.net_pnl) for t in trades]
        wins = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p <= 0]

        all_pnl = np.array(pnls)
        n_bins = min(50, max(10, len(pnls) // 3))
        pmin = float(all_pnl.min())
        pmax = float(all_pnl.max())
        if np.isclose(pmin, pmax):
            pad = max(1.0, abs(pmin) * 0.1)
            bin_edges = np.linspace(pmin - pad, pmax + pad, n_bins + 1)
        else:
            bin_edges = np.linspace(pmin, pmax, n_bins + 1)

        if wins:
            ax.hist(
                wins,
                bins=bin_edges,
                color=_COLORS["win"],
                alpha=0.7,
                label=f"Winners ({len(wins)})",
            )
        if losses:
            ax.hist(
                losses,
                bins=bin_edges,
                color=_COLORS["loss"],
                alpha=0.7,
                label=f"Losers ({len(losses)})",
            )

        ax.axvline(0, color="black", linewidth=0.8, linestyle="--")
        ax.set_title("Trade P&L Distribution")
        ax.set_xlabel("P&L ($)")
        ax.set_ylabel("Count")
        ax.legend()
        ax.grid(True, alpha=0.3)

        fig.tight_layout()
        return fig

    def mae_mfe_scatter(self, trades: Sequence[TradeLike]) -> plt.Figure:
        """Generate MAE/MFE scatter plot."""
        fig, ax = plt.subplots(figsize=(8, 8))

        if not trades:
            ax.text(
                0.5,
                0.5,
                "No trade data available",
                ha="center",
                va="center",
                transform=ax.transAxes,
            )
            ax.set_title("MAE vs MFE")
            fig.tight_layout()
            return fig

        winners = [t for t in trades if float(t.net_pnl) > 0]
        losers = [t for t in trades if float(t.net_pnl) <= 0]

        if winners:
            w_mae = [float(t.mae) for t in winners]
            w_mfe = [float(t.mfe) for t in winners]
            ax.scatter(
                w_mae,
                w_mfe,
                color=_COLORS["win"],
                alpha=0.6,
                label=f"Winners ({len(winners)})",
                edgecolors="none",
                s=40,
            )

        if losers:
            l_mae = [float(t.mae) for t in losers]
            l_mfe = [float(t.mfe) for t in losers]
            ax.scatter(
                l_mae,
                l_mfe,
                color=_COLORS["loss"],
                alpha=0.6,
                label=f"Losers ({len(losers)})",
                edgecolors="none",
                s=40,
            )

        all_mae = [float(t.mae) for t in trades]
        all_mfe = [float(t.mfe) for t in trades]
        max_val = max(max(all_mae, default=1.0), max(all_mfe, default=1.0))
        ax.plot(
            [0, max_val],
            [0, max_val],
            color=_COLORS["neutral"],
            linestyle="--",
            linewidth=0.8,
        )

        ax.set_title("MAE vs MFE")
        ax.set_xlabel("MAE (Max Adverse Excursion) $")
        ax.set_ylabel("MFE (Max Favorable Excursion) $")
        ax.legend()
        ax.grid(True, alpha=0.3)

        fig.tight_layout()
        return fig

    def rolling_sharpe(
        self,
        results: BacktestResultsLike,
        window: int = 63,
    ) -> plt.Figure:
        """Generate rolling Sharpe ratio chart."""
        fig, ax = plt.subplots(figsize=self.figsize)

        daily_returns_raw = getattr(results, "daily_returns", None)
        daily_returns = pd.Series(dtype=float)
        if daily_returns_raw is not None:
            daily_returns = pd.Series(daily_returns_raw).dropna()

        if len(daily_returns) < window:
            ax.text(
                0.5,
                0.5,
                f"Insufficient data (need {window} days)",
                ha="center",
                va="center",
                transform=ax.transAxes,
            )
            ax.set_title(f"Rolling Sharpe Ratio ({window}-day)")
            fig.tight_layout()
            return fig

        rolling_mean = daily_returns.rolling(window=window).mean()
        rolling_std = daily_returns.rolling(window=window).std().replace(0, np.nan)
        rolling_sharpe_values = (rolling_mean / rolling_std) * np.sqrt(252)
        valid = rolling_sharpe_values.dropna()

        ax.plot(valid.index, valid.values, color=_COLORS["rolling"], linewidth=1.2)
        ax.axhline(0, color="black", linewidth=0.5, linestyle="-")
        ax.axhline(1.0, color=_COLORS["win"], linewidth=0.5, linestyle="--", alpha=0.5)
        ax.axhline(-1.0, color=_COLORS["loss"], linewidth=0.5, linestyle="--", alpha=0.5)

        ax.fill_between(
            valid.index,
            valid.values,
            0,
            where=valid.values > 0,
            color=_COLORS["win"],
            alpha=0.1,
        )
        ax.fill_between(
            valid.index,
            valid.values,
            0,
            where=valid.values < 0,
            color=_COLORS["loss"],
            alpha=0.1,
        )

        ax.set_title(f"Rolling Sharpe Ratio ({window}-day)")
        ax.set_xlabel("Date")
        ax.set_ylabel("Sharpe Ratio")
        ax.grid(True, alpha=0.3)

        fig.tight_layout()
        return fig

    def figure_to_png(self, fig: plt.Figure, filepath: str) -> None:
        """Save a figure to a PNG file."""
        path = Path(filepath)
        path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(str(path), dpi=self.dpi, format="png", bbox_inches="tight")
        plt.close(fig)
        logger.info("Chart saved to %s", filepath)

    def figure_to_base64(self, fig: plt.Figure) -> str:
        """Convert a figure to a base64-encoded PNG string."""
        buf = io.BytesIO()
        fig.savefig(buf, dpi=self.dpi, format="png", bbox_inches="tight")
        plt.close(fig)
        buf.seek(0)
        return base64.b64encode(buf.read()).decode("utf-8")

    def generate_all_charts(
        self,
        results: BacktestResultsLike,
        benchmark: pd.Series | None = None,
        rolling_window: int = 63,
    ) -> dict[str, str]:
        """Generate the standard chart set as base64-encoded PNG strings."""
        charts: dict[str, str] = {}

        fig = self.equity_curve(results, benchmark=benchmark)
        charts["equity_curve"] = self.figure_to_base64(fig)

        fig = self.drawdown_chart(results)
        charts["drawdown"] = self.figure_to_base64(fig)

        fig = self.monthly_returns_heatmap(results)
        charts["monthly_returns"] = self.figure_to_base64(fig)

        trades = getattr(results, "trades", [])
        fig = self.trade_distribution(trades)
        charts["trade_distribution"] = self.figure_to_base64(fig)

        fig = self.mae_mfe_scatter(trades)
        charts["mae_mfe"] = self.figure_to_base64(fig)

        fig = self.rolling_sharpe(results, window=rolling_window)
        charts["rolling_sharpe"] = self.figure_to_base64(fig)

        return charts
