"""
Momentum Signal - BTC 动量信号 (L1)

核心思路：5分钟窗口内 BTC 已经产生的价格方向和幅度
是最直接、最强的预测信号。

信号来源:
    - ctx.market.btc_price            (当前 Chainlink BTC/USD)
    - ctx.market.pm_window_start_price (窗口开始的基准价 PTB)
    - ctx.get("btc_price_history")     (最近 N 个 ticks 的价格序列)

计算维度:
    1. 窗口内变化率 (window_return)    - 主信号
    2. 短期趋势斜率 (trend_slope)      - 近 10~30 tick 的动量方向
    3. 加速度 (acceleration)           - 趋势在增强还是减弱
"""

from __future__ import annotations

import math
from collections import deque
from typing import Any

from src.core.context import Context
from src.strategy.signals.base import SignalProvider, SignalResult


class MomentumSignal(SignalProvider):
    """
    BTC 价格动量信号

    参数:
        short_window:  短期趋势 tick 数 (默认 10)
        long_window:   长期趋势 tick 数 (默认 30)
        max_return:    归一化用的最大变化率 (默认 0.10% = 0.001)
        accel_weight:  加速度权重 (默认 0.2)
    """

    def __init__(
        self,
        short_window: int = 10,
        long_window: int = 30,
        max_return: float = 0.001,
        accel_weight: float = 0.2,
    ) -> None:
        self._short_window = short_window
        self._long_window = long_window
        self._max_return = max_return
        self._accel_weight = accel_weight

        # 内部状态 — 价格历史环形缓冲区
        self._prices: deque[float] = deque(maxlen=long_window + 10)

    def name(self) -> str:
        return "momentum"

    def get_params(self) -> dict[str, Any]:
        return {
            "short_window": self._short_window,
            "long_window": self._long_window,
            "max_return": self._max_return,
            "accel_weight": self._accel_weight,
        }

    def reset(self) -> None:
        self._prices.clear()

    # ------------------------------------------------------------------ #
    #  核心计算
    # ------------------------------------------------------------------ #
    def calculate(self, ctx: Context) -> SignalResult:
        btc_price = ctx.market.btc_price
        ptb = ctx.market.pm_window_start_price  # Price-to-Beat

        if btc_price <= 0:
            return SignalResult(reason="BTC price not available")

        # 追加到内部缓冲
        self._prices.append(btc_price)

        # ---- 1) 窗口内收益率 ----
        if ptb > 0:
            window_return = (btc_price - ptb) / ptb
        else:
            window_return = 0.0

        # ---- 2) 短期趋势斜率 ----
        trend_slope = self._calc_slope(self._short_window)

        # ---- 3) 加速度: 短期斜率 vs 长期斜率 ----
        long_slope = self._calc_slope(self._long_window)
        acceleration = (trend_slope - long_slope) if long_slope != 0 else 0.0

        # ---- 综合打分 ----
        # window_return 是主分量，归一化到 [-1, 1]
        norm_return = _clamp(window_return / self._max_return, -1.0, 1.0)

        # 趋势斜率与加速度修正
        norm_slope = _clamp(trend_slope * 1e5, -1.0, 1.0)  # 斜率很小，放大后归一化
        norm_accel = _clamp(acceleration * 1e5, -1.0, 1.0)

        score = (
            0.6 * norm_return
            + 0.2 * norm_slope
            + self._accel_weight * norm_accel
        )
        score = _clamp(score, -1.0, 1.0)

        # 信心度: ptb 可用且有足够 tick 时信心高
        confidence = 0.3
        if ptb > 0:
            confidence += 0.4
        if len(self._prices) >= self._short_window:
            confidence += 0.2
        if len(self._prices) >= self._long_window:
            confidence += 0.1
        confidence = min(confidence, 1.0)

        # 构建原因描述
        direction_str = "UP" if score > 0 else "DOWN" if score < 0 else "FLAT"
        reason = (
            f"BTC {direction_str} | "
            f"窗口收益={window_return*100:+.4f}% | "
            f"趋势斜率={trend_slope*1e4:+.2f}bp/tick | "
            f"加速度={acceleration*1e4:+.2f}"
        )

        return SignalResult(
            score=score,
            confidence=confidence,
            reason=reason,
            details={
                "btc_price": btc_price,
                "ptb": ptb,
                "window_return": window_return,
                "trend_slope": trend_slope,
                "acceleration": acceleration,
                "norm_return": norm_return,
                "norm_slope": norm_slope,
                "tick_count": len(self._prices),
            },
        )

    # ------------------------------------------------------------------ #
    #  工具方法
    # ------------------------------------------------------------------ #
    def _calc_slope(self, window: int) -> float:
        """简单线性回归斜率 (least-squares)。"""
        n = min(window, len(self._prices))
        if n < 3:
            return 0.0

        prices = list(self._prices)[-n:]
        x_mean = (n - 1) / 2.0
        y_mean = sum(prices) / n

        numerator = 0.0
        denominator = 0.0
        for i, p in enumerate(prices):
            dx = i - x_mean
            numerator += dx * (p - y_mean)
            denominator += dx * dx

        if denominator == 0:
            return 0.0
        return numerator / denominator


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))
