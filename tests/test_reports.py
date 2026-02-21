"""Tests for report-chart generation APIs."""

from __future__ import annotations

import base64
from dataclasses import dataclass

import numpy as np
import pandas as pd
import pytest

pytest.importorskip("matplotlib")
import matplotlib.pyplot as plt

from charts.reports import ChartGenerator


@dataclass
class _Trade:
    net_pnl: float
    mae: float
    mfe: float


@dataclass
class _Results:
    equity_curve: pd.DataFrame
    daily_returns: pd.Series
    trades: list[_Trade]


def _make_results(n_days: int = 100, trades: list[_Trade] | None = None) -> _Results:
    rng = np.random.default_rng(42)
    returns = rng.normal(0.001, 0.01, n_days)

    equity = [100_000.0]
    for r in returns:
        equity.append(equity[-1] * (1 + r))

    dates = pd.date_range("2024-01-02", periods=len(equity), freq="B")
    eq_series = pd.Series(equity)
    running_max = eq_series.cummax()
    drawdowns = (eq_series - running_max) / running_max

    equity_curve = pd.DataFrame(
        {
            "date": dates,
            "equity": equity,
            "drawdown": drawdowns.values,
        }
    )

    daily_returns = pd.Series(
        returns,
        index=pd.date_range("2024-01-02", periods=n_days, freq="B"),
        dtype=float,
    )

    return _Results(
        equity_curve=equity_curve,
        daily_returns=daily_returns,
        trades=trades or [],
    )


def test_chart_generator_defaults() -> None:
    gen = ChartGenerator()
    assert gen.figsize == (12, 6)
    assert gen.dpi == 100


def test_equity_curve_returns_figure() -> None:
    gen = ChartGenerator()
    fig = gen.equity_curve(_make_results())
    assert isinstance(fig, plt.Figure)
    plt.close(fig)


def test_monthly_returns_heatmap_has_colorbar() -> None:
    gen = ChartGenerator()
    fig = gen.monthly_returns_heatmap(_make_results(n_days=200))
    assert len(fig.axes) >= 2
    plt.close(fig)


def test_trade_distribution_handles_equal_pnl_values() -> None:
    gen = ChartGenerator()
    trades = [_Trade(net_pnl=10.0, mae=5.0, mfe=12.0) for _ in range(8)]
    fig = gen.trade_distribution(trades)
    assert isinstance(fig, plt.Figure)
    plt.close(fig)


def test_rolling_sharpe_handles_insufficient_data() -> None:
    gen = ChartGenerator()
    fig = gen.rolling_sharpe(_make_results(n_days=10), window=63)
    assert isinstance(fig, plt.Figure)
    plt.close(fig)


def test_generate_all_charts_returns_png_base64() -> None:
    gen = ChartGenerator()
    trades = [
        _Trade(net_pnl=120.0, mae=30.0, mfe=170.0),
        _Trade(net_pnl=-45.0, mae=40.0, mfe=20.0),
    ]
    charts = gen.generate_all_charts(_make_results(n_days=120, trades=trades), rolling_window=20)

    expected = {
        "equity_curve",
        "drawdown",
        "monthly_returns",
        "trade_distribution",
        "mae_mfe",
        "rolling_sharpe",
    }
    assert set(charts) == expected

    for payload in charts.values():
        raw = base64.b64decode(payload)
        assert raw[:4] == b"\x89PNG"
