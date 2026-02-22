"""
OrderFlow Signal - PM 订单流信号 (L2)

核心思路：分析 UP/DOWN 订单簿的微观结构，
捕捉聪明钱方向和市场价格 vs 理论概率的偏离。

信号来源:
    - ctx.get("orderbook_up")   → OrderBook 对象
    - ctx.get("orderbook_down") → OrderBook 对象
    - ctx.market.pm_yes_price / pm_no_price
    - ctx.market.pm_yes_bid / pm_yes_ask / pm_no_bid / pm_no_ask

计算维度:
    1. 买卖深度不平衡 (depth_imbalance)  — UP bid_depth vs ask_depth 的倾斜
    2. 跨 token 信号 (cross_signal)     — UP 买盘强 ↔ DOWN 卖盘强 一致性
    3. 价格偏离 (price_dislocation)     — PM 价格 vs BTC 动量隐含概率的差距
    4. 流动性状态 (liquidity_score)     — spread/depth 过滤低质行情
"""

from __future__ import annotations

import math
from typing import Any

from src.core.context import Context
from src.strategy.signals.base import SignalProvider, SignalResult


class OrderFlowSignal(SignalProvider):
    """
    PM 订单流信号

    参数:
        depth_levels:       分析几档深度 (默认 10)
        imbalance_cap:      不平衡归一化上限 (默认 3.0)
        dislocation_weight: 价格偏离权重 (默认 0.3)
        cross_weight:       跨 token 权重 (默认 0.2)
    """

    def __init__(
        self,
        depth_levels: int = 10,
        imbalance_cap: float = 3.0,
        dislocation_weight: float = 0.3,
        cross_weight: float = 0.2,
    ) -> None:
        self._depth_levels = depth_levels
        self._imbalance_cap = imbalance_cap
        self._dislocation_weight = dislocation_weight
        self._cross_weight = cross_weight

    def name(self) -> str:
        return "orderflow"

    def get_params(self) -> dict[str, Any]:
        return {
            "depth_levels": self._depth_levels,
            "imbalance_cap": self._imbalance_cap,
            "dislocation_weight": self._dislocation_weight,
            "cross_weight": self._cross_weight,
        }

    # ------------------------------------------------------------------ #
    #  核心计算
    # ------------------------------------------------------------------ #
    def calculate(self, ctx: Context) -> SignalResult:
        ob_up = ctx.get("orderbook_up")
        ob_down = ctx.get("orderbook_down")

        if ob_up is None or ob_down is None:
            return SignalResult(reason="OrderBook not available")

        up_state = ob_up.get_state()
        down_state = ob_down.get_state()

        # ---- 1) UP token 买卖深度不平衡 ----
        up_bid_depth = up_state.total_bid_size(self._depth_levels)
        up_ask_depth = up_state.total_ask_size(self._depth_levels)
        up_imbalance = self._calc_imbalance(up_bid_depth, up_ask_depth)

        # ---- 2) DOWN token 买卖深度不平衡 ----
        down_bid_depth = down_state.total_bid_size(self._depth_levels)
        down_ask_depth = down_state.total_ask_size(self._depth_levels)
        down_imbalance = self._calc_imbalance(down_bid_depth, down_ask_depth)

        # ---- 3) 跨 token 一致性信号 ----
        # UP bid 强 + DOWN ask 强 → 市场看涨 (score > 0)
        # UP ask 强 + DOWN bid 强 → 市场看跌 (score < 0)
        cross_signal = (up_imbalance - down_imbalance) / 2.0

        # ---- 4) 价格偏离信号 ----
        # 如果 BTC 已经在涨 (ptb 可用)，理论 UP 概率应偏高
        # 实际 PM UP 报价如果低于理论值 → 买入机会 (dislocation > 0)
        dislocation = self._calc_dislocation(ctx)

        # ---- 5) 流动性质量 → 作为 confidence 而非 score ----
        liquidity = self._calc_liquidity_score(up_state, down_state)

        # ---- 综合打分 ----
        # 主分量: UP 深度不平衡 (UP bid 越强 → score 越高)
        score = (
            0.5 * up_imbalance
            + self._cross_weight * cross_signal
            + self._dislocation_weight * dislocation
        )
        score = _clamp(score, -1.0, 1.0)

        # 信心度: 基于流动性和数据可用性
        confidence = 0.2
        if up_bid_depth + up_ask_depth > 0:
            confidence += 0.3
        if down_bid_depth + down_ask_depth > 0:
            confidence += 0.2
        confidence += 0.3 * liquidity
        confidence = min(confidence, 1.0)

        direction_str = "UP" if score > 0 else "DOWN" if score < 0 else "FLAT"
        reason = (
            f"OF {direction_str} | "
            f"UP imb={up_imbalance:+.2f} "
            f"({up_bid_depth:.0f}/{up_ask_depth:.0f}) | "
            f"DOWN imb={down_imbalance:+.2f} | "
            f"disloc={dislocation:+.2f} | "
            f"liq={liquidity:.2f}"
        )

        return SignalResult(
            score=score,
            confidence=confidence,
            reason=reason,
            details={
                "up_bid_depth": up_bid_depth,
                "up_ask_depth": up_ask_depth,
                "up_imbalance": up_imbalance,
                "down_bid_depth": down_bid_depth,
                "down_ask_depth": down_ask_depth,
                "down_imbalance": down_imbalance,
                "cross_signal": cross_signal,
                "dislocation": dislocation,
                "liquidity": liquidity,
                "up_spread": up_state.spread,
                "down_spread": down_state.spread,
            },
        )

    # ------------------------------------------------------------------ #
    #  子计算
    # ------------------------------------------------------------------ #
    def _calc_imbalance(self, bid_depth: float, ask_depth: float) -> float:
        """
        买卖深度不平衡 → 归一化到 [-1, 1]。

        ratio = bid / ask （bid 多 → >1 → 看涨）
        归一化: tanh((ratio - 1) / cap * 2)
        """
        total = bid_depth + ask_depth
        if total <= 0:
            return 0.0
        ratio = bid_depth / ask_depth if ask_depth > 0 else self._imbalance_cap
        # 对称归一化：ratio=1 → 0，ratio>1 → 正，ratio<1 → 负
        raw = (ratio - 1.0) / self._imbalance_cap * 2.0
        return _clamp(math.tanh(raw), -1.0, 1.0)

    def _calc_dislocation(self, ctx: Context) -> float:
        """
        PM UP 价格 vs BTC 动量隐含概率的偏离。

        隐含概率粗略估计: 如果 BTC 已涨 0.05%，UP 「应该」值多少？
        简单模型: implied_up ≈ 0.5 + k * window_return
        如果实际 UP 价格 < implied → 被低估 → dislocation > 0 → 看涨信号
        """
        btc = ctx.market.btc_price
        ptb = ctx.market.pm_window_start_price
        up_mid = ctx.market.pm_yes_price

        if btc <= 0 or ptb <= 0 or up_mid <= 0:
            return 0.0

        window_return = (btc - ptb) / ptb
        # 粗略: 每涨 0.05% BTC，UP 概率偏移 10 个点
        sensitivity = 2000.0  # 0.05% → 0.10 概率偏移
        implied_up = 0.5 + sensitivity * window_return
        implied_up = _clamp(implied_up, 0.01, 0.99)

        # 偏离: implied 比 actual 高 → actual 被低估 → dislocation > 0
        dislocation = implied_up - up_mid
        # 归一化到 [-1, 1]：0.10 偏离 → 1.0
        return _clamp(dislocation / 0.10, -1.0, 1.0)

    def _calc_liquidity_score(self, up_state, down_state) -> float:
        """
        流动性评分 ∈ [0, 1]。

        考虑 spread 和深度。spread 过大或深度过浅 → 评分低。
        """
        score = 1.0

        # spread 惩罚: >5% 开始扣分
        for state in [up_state, down_state]:
            if state.spread_pct > 0.10:
                score -= 0.3
            elif state.spread_pct > 0.05:
                score -= 0.15

        # 深度惩罚: 任一侧 < 100 USDC
        for state in [up_state, down_state]:
            total = state.total_bid_size(5) + state.total_ask_size(5)
            if total < 100:
                score -= 0.2
            elif total < 500:
                score -= 0.1

        return max(0.0, score)


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))
