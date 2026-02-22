"""
Technical Signal - 技术指标信号 (L3)

核心思路：基于 BTC 1-min K 线的经典技术指标做辅助过滤，
避免在极端超买/超卖区域追势，并捕捉短期 EMA 交叉。

信号来源:
    - ctx.get("aggregator") → MarketAggregator 内的 K 线 DataFrame
    - 直接用 ctx.get("btc_klines_1m") 如果聚合器已将其缓存

计算维度:
    1. RSI 极值信号 (rsi_signal)    — RSI < 30 超卖看涨 / RSI > 70 超买看跌
    2. EMA 交叉信号 (ema_signal)    — 快线>慢线 看涨 / 快线<慢线 看跌
    3. 波动率过滤 (volatility)      — ATR 太低时不交易（无利可图）
"""

from __future__ import annotations

from collections import deque
from typing import Any

from src.core.context import Context
from src.strategy.signals.base import SignalProvider, SignalResult


class TechnicalSignal(SignalProvider):
    """
    BTC 技术指标信号

    参数:
        rsi_period:     RSI 周期 (默认 14)
        rsi_overbought: RSI 超买阈值 (默认 70)
        rsi_oversold:   RSI 超卖阈值 (默认 30)
        ema_fast:       快速 EMA 周期 (默认 5)
        ema_slow:       慢速 EMA 周期 (默认 15)
        atr_period:     ATR 周期 (默认 14)
        atr_min:        最低 ATR 阈值 (默认 10 USD)
        rsi_weight:     RSI 分量权重 (默认 0.5)
        ema_weight:     EMA 分量权重 (默认 0.5)
    """

    def __init__(
        self,
        rsi_period: int = 14,
        rsi_overbought: float = 70,
        rsi_oversold: float = 30,
        ema_fast: int = 5,
        ema_slow: int = 15,
        atr_period: int = 14,
        atr_min: float = 10.0,
        rsi_weight: float = 0.5,
        ema_weight: float = 0.5,
    ) -> None:
        self._rsi_period = rsi_period
        self._rsi_overbought = rsi_overbought
        self._rsi_oversold = rsi_oversold
        self._ema_fast = ema_fast
        self._ema_slow = ema_slow
        self._atr_period = atr_period
        self._atr_min = atr_min
        self._rsi_weight = rsi_weight
        self._ema_weight = ema_weight

        # 内部缓冲: 收盘价序列 (用于自行计算指标)
        self._closes: deque[float] = deque(maxlen=max(rsi_period, ema_slow, atr_period) + 60)
        self._highs: deque[float] = deque(maxlen=atr_period + 5)
        self._lows: deque[float] = deque(maxlen=atr_period + 5)

        # EMA 状态
        self._ema_fast_val: float = 0.0
        self._ema_slow_val: float = 0.0
        self._ema_initialized: bool = False

    def name(self) -> str:
        return "technical"

    def get_params(self) -> dict[str, Any]:
        return {
            "rsi_period": self._rsi_period,
            "rsi_overbought": self._rsi_overbought,
            "rsi_oversold": self._rsi_oversold,
            "ema_fast": self._ema_fast,
            "ema_slow": self._ema_slow,
            "atr_period": self._atr_period,
            "atr_min": self._atr_min,
        }

    def reset(self) -> None:
        self._closes.clear()
        self._highs.clear()
        self._lows.clear()
        self._ema_fast_val = 0.0
        self._ema_slow_val = 0.0
        self._ema_initialized = False

    # ------------------------------------------------------------------ #
    #  喂数据 (每根 K 线调用一次)
    # ------------------------------------------------------------------ #
    def feed_bar(self, close: float, high: float = 0.0, low: float = 0.0) -> None:
        """
        喂入一根新 K 线数据。如果没有 high/low，自动复制 close。
        在策略的 on_market_data / on_timer 中调用。
        """
        self._closes.append(close)
        self._highs.append(high if high > 0 else close)
        self._lows.append(low if low > 0 else close)

        # 增量更新 EMA
        if not self._ema_initialized and len(self._closes) >= self._ema_slow:
            # 用前 ema_slow 根收盘价的 SMA 作为 EMA 起始值
            data = list(self._closes)
            self._ema_slow_val = sum(data[-self._ema_slow:]) / self._ema_slow
            self._ema_fast_val = sum(data[-self._ema_fast:]) / self._ema_fast
            self._ema_initialized = True
        elif self._ema_initialized:
            alpha_fast = 2.0 / (self._ema_fast + 1)
            alpha_slow = 2.0 / (self._ema_slow + 1)
            self._ema_fast_val = alpha_fast * close + (1 - alpha_fast) * self._ema_fast_val
            self._ema_slow_val = alpha_slow * close + (1 - alpha_slow) * self._ema_slow_val

    # ------------------------------------------------------------------ #
    #  核心计算
    # ------------------------------------------------------------------ #
    def calculate(self, ctx: Context) -> SignalResult:
        n = len(self._closes)
        min_required = max(self._rsi_period + 2, self._ema_slow + 2)

        if n < min_required:
            return SignalResult(
                reason=f"Insufficient data ({n}/{min_required} bars)",
                confidence=0.0,
            )

        closes = list(self._closes)

        # ---- 1) RSI ----
        rsi = self._calc_rsi(closes, self._rsi_period)
        rsi_signal = self._rsi_to_score(rsi)

        # ---- 2) EMA 交叉 ----
        ema_signal = 0.0
        if self._ema_initialized:
            # 快线在慢线上方 → 看涨，反之 → 看跌
            ema_diff = self._ema_fast_val - self._ema_slow_val
            # 归一化: 把差值除以慢线值，再放大
            if self._ema_slow_val > 0:
                ema_signal = _clamp(ema_diff / self._ema_slow_val * 500, -1.0, 1.0)

        # ---- 3) ATR (波动率过滤) ----
        atr = self._calc_atr()
        vol_ok = atr >= self._atr_min

        # ---- 综合打分 ----
        score = (
            self._rsi_weight * rsi_signal
            + self._ema_weight * ema_signal
        )
        score = _clamp(score, -1.0, 1.0)

        # 波动率不足 → 降低 confidence
        confidence = 0.5
        if n >= min_required + 20:
            confidence += 0.2
        if vol_ok:
            confidence += 0.2
        else:
            confidence *= 0.3  # ATR 太低时大幅压低信心

        # RSI 在中间区域 → 信号弱，降低信心
        if 40 < rsi < 60:
            confidence *= 0.7

        confidence = min(confidence, 1.0)

        direction_str = "UP" if score > 0 else "DOWN" if score < 0 else "FLAT"
        reason = (
            f"TA {direction_str} | "
            f"RSI={rsi:.1f} ({rsi_signal:+.2f}) | "
            f"EMA={ema_signal:+.2f} "
            f"(F={self._ema_fast_val:.1f}/S={self._ema_slow_val:.1f}) | "
            f"ATR={atr:.1f} {'OK' if vol_ok else 'LOW'}"
        )

        return SignalResult(
            score=score,
            confidence=confidence,
            reason=reason,
            details={
                "rsi": rsi,
                "rsi_signal": rsi_signal,
                "ema_fast_val": self._ema_fast_val,
                "ema_slow_val": self._ema_slow_val,
                "ema_signal": ema_signal,
                "atr": atr,
                "vol_ok": vol_ok,
                "bar_count": n,
            },
        )

    # ------------------------------------------------------------------ #
    #  指标计算工具
    # ------------------------------------------------------------------ #
    def _calc_rsi(self, closes: list[float], period: int) -> float:
        """标准 RSI (Wilder 平滑)"""
        if len(closes) < period + 1:
            return 50.0

        deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]

        # 初始 SMA
        gains = [d if d > 0 else 0 for d in deltas[:period]]
        losses = [-d if d < 0 else 0 for d in deltas[:period]]
        avg_gain = sum(gains) / period
        avg_loss = sum(losses) / period

        # Wilder 平滑
        for d in deltas[period:]:
            avg_gain = (avg_gain * (period - 1) + max(d, 0)) / period
            avg_loss = (avg_loss * (period - 1) + max(-d, 0)) / period

        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return 100.0 - (100.0 / (1.0 + rs))

    def _rsi_to_score(self, rsi: float) -> float:
        """
        RSI → 方向分。

        RSI < oversold → 看涨 (超卖反弹)
        RSI > overbought → 看跌 (超买回落)
        中间区域 → 按线性映射
        """
        if rsi <= self._rsi_oversold:
            # 超卖: RSI=30 → +0.3，RSI=20 → +0.6，RSI=10 → +1.0
            return _clamp((self._rsi_oversold - rsi) / (self._rsi_oversold - 10) * 1.0, 0.0, 1.0)
        elif rsi >= self._rsi_overbought:
            # 超买: RSI=70 → -0.3，RSI=80 → -0.6，RSI=90 → -1.0
            return -_clamp((rsi - self._rsi_overbought) / (90 - self._rsi_overbought) * 1.0, 0.0, 1.0)
        else:
            # 中间区域: 50 → 0，接近 30 → 正，接近 70 → 负
            mid = (self._rsi_overbought + self._rsi_oversold) / 2
            half_range = (self._rsi_overbought - self._rsi_oversold) / 2
            return -_clamp((rsi - mid) / half_range * 0.3, -0.3, 0.3)

    def _calc_atr(self) -> float:
        """计算 ATR (Average True Range)"""
        n = min(self._atr_period, len(self._closes) - 1)
        if n < 1:
            return 0.0

        closes = list(self._closes)
        highs = list(self._highs)
        lows = list(self._lows)

        trs = []
        for i in range(-n, 0):
            h = highs[i] if abs(i) <= len(highs) else closes[i]
            l = lows[i] if abs(i) <= len(lows) else closes[i]
            prev_c = closes[i - 1]
            tr = max(h - l, abs(h - prev_c), abs(l - prev_c))
            trs.append(tr)

        return sum(trs) / len(trs) if trs else 0.0


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))
