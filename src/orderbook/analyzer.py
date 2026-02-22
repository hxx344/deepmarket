"""
Order Book Analyzer - 订单簿深度分析

提供大单追踪、流动性分析、撤单检测等功能。
"""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass
from typing import Any

import pandas as pd
from loguru import logger

from src.orderbook.book import OrderBook, OrderBookState


@dataclass
class LargeOrder:
    """大单记录"""
    price: float
    size: float
    side: str          # 'bid' or 'ask'
    first_seen: float  # 首次发现时间
    last_seen: float   # 最后看到时间
    disappeared: bool = False  # 是否已消失（疑似撤单）


@dataclass
class TradeRecord:
    """成交记录"""
    price: float
    size: float
    side: str              # 'buy' or 'sell'
    timestamp: float
    is_aggressive: bool    # 是否主动成交


class OrderBookAnalyzer:
    """
    订单簿分析器

    - 大单追踪与撤单检测
    - 买卖压力分析
    - 成交流分析
    - 流动性评分
    """

    def __init__(
        self,
        large_order_threshold: float = 500,  # USDC
        max_trade_history: int = 5000,
    ) -> None:
        self.large_order_threshold = large_order_threshold
        self._large_orders: dict[str, LargeOrder] = {}  # key = f"{side}_{price}"
        self._trade_history: deque[TradeRecord] = deque(maxlen=max_trade_history)
        self._book_snapshots: deque[dict] = deque(maxlen=300)  # ~5min of 1s snapshots

    def update(self, book_state: OrderBookState) -> dict[str, Any]:
        """
        更新分析器状态

        Args:
            book_state: 当前订单簿状态

        Returns:
            分析结果 dict
        """
        self._detect_large_orders(book_state)
        self._record_snapshot(book_state)

        return {
            "spread": book_state.spread,
            "spread_pct": book_state.spread_pct,
            "bid_depth": book_state.total_bid_size(10),
            "ask_depth": book_state.total_ask_size(10),
            "book_pressure": self.book_pressure(book_state),
            "liquidity_score": self.liquidity_score(book_state),
            "large_orders": len([o for o in self._large_orders.values() if not o.disappeared]),
        }

    def record_trade(self, trade: TradeRecord) -> None:
        """记录成交"""
        self._trade_history.append(trade)

    def _detect_large_orders(self, state: OrderBookState) -> None:
        """检测大单和撤单行为"""
        current_keys: set[str] = set()

        for level in state.bids:
            if level.size * level.price >= self.large_order_threshold:
                key = f"bid_{level.price}"
                current_keys.add(key)
                if key not in self._large_orders:
                    self._large_orders[key] = LargeOrder(
                        price=level.price,
                        size=level.size,
                        side="bid",
                        first_seen=time.time(),
                        last_seen=time.time(),
                    )
                    logger.info(f"Large BID detected: {level.size:.0f} @ {level.price}")
                else:
                    self._large_orders[key].last_seen = time.time()
                    self._large_orders[key].size = level.size

        for level in state.asks:
            if level.size * level.price >= self.large_order_threshold:
                key = f"ask_{level.price}"
                current_keys.add(key)
                if key not in self._large_orders:
                    self._large_orders[key] = LargeOrder(
                        price=level.price,
                        size=level.size,
                        side="ask",
                        first_seen=time.time(),
                        last_seen=time.time(),
                    )
                    logger.info(f"Large ASK detected: {level.size:.0f} @ {level.price}")
                else:
                    self._large_orders[key].last_seen = time.time()
                    self._large_orders[key].size = level.size

        # 检测消失的大单（疑似撤单 / spoofing）
        for key, order in self._large_orders.items():
            if key not in current_keys and not order.disappeared:
                order.disappeared = True
                duration = time.time() - order.first_seen
                if duration < 10:  # 短暂挂单后撤单 → 可疑
                    logger.warning(
                        f"Suspicious cancel: {order.side.upper()} {order.size:.0f} "
                        f"@ {order.price} lasted {duration:.1f}s"
                    )

        # 清理过期记录 (> 5min)
        cutoff = time.time() - 300
        self._large_orders = {
            k: v for k, v in self._large_orders.items()
            if v.last_seen > cutoff
        }

    def _record_snapshot(self, state: OrderBookState) -> None:
        """记录订单簿快照（用于回放）"""
        self._book_snapshots.append(state.to_dict())

    def book_pressure(self, state: OrderBookState, depth: int = 5) -> float:
        """
        买卖盘压力比

        > 1 表示买方压力大，< 1 表示卖方压力大

        Returns:
            bid_total / ask_total
        """
        bid_sum = state.total_bid_size(depth)
        ask_sum = state.total_ask_size(depth)
        if ask_sum == 0:
            return float("inf") if bid_sum > 0 else 1.0
        return bid_sum / ask_sum

    def liquidity_score(self, state: OrderBookState) -> float:
        """
        流动性评分 (0-100)

        综合考虑：价差、深度、档位数
        """
        score = 100.0

        # 价差惩罚 (每 1% 扣 20 分)
        score -= min(state.spread_pct * 2000, 50)

        # 深度奖励
        total_depth = state.total_bid_size(10) + state.total_ask_size(10)
        if total_depth < 100:
            score -= 30
        elif total_depth < 500:
            score -= 15

        # 档位数奖励
        levels = len(state.bids) + len(state.asks)
        if levels < 5:
            score -= 20

        return max(0.0, min(100.0, score))

    def cvd(self, window: int | None = None) -> float:
        """
        累积成交量差值 (CVD)

        买方成交量 - 卖方成交量
        """
        trades = list(self._trade_history)
        if window:
            cutoff = time.time() - window
            trades = [t for t in trades if t.timestamp > cutoff]

        buy_vol = sum(t.size for t in trades if t.side == "buy")
        sell_vol = sum(t.size for t in trades if t.side == "sell")
        return buy_vol - sell_vol

    def order_flow_imbalance(self, window: int = 60) -> float:
        """
        订单流不平衡指标

        范围 [-1, 1]，正值表示买方主导
        """
        cutoff = time.time() - window
        trades = [t for t in self._trade_history if t.timestamp > cutoff]
        if not trades:
            return 0.0

        buy_vol = sum(t.size for t in trades if t.side == "buy")
        sell_vol = sum(t.size for t in trades if t.side == "sell")
        total = buy_vol + sell_vol
        if total == 0:
            return 0.0
        return (buy_vol - sell_vol) / total

    def aggressor_ratio(self, window: int = 60) -> float:
        """主动成交占比"""
        cutoff = time.time() - window
        trades = [t for t in self._trade_history if t.timestamp > cutoff]
        if not trades:
            return 0.5
        aggressive = sum(1 for t in trades if t.is_aggressive)
        return aggressive / len(trades)

    def trade_flow_summary(self, window: int = 300) -> dict[str, Any]:
        """成交流汇总 (默认 5 分钟)"""
        cutoff = time.time() - window
        trades = [t for t in self._trade_history if t.timestamp > cutoff]

        if not trades:
            return {"count": 0, "buy_vol": 0, "sell_vol": 0, "cvd": 0, "avg_size": 0}

        buy_vol = sum(t.size for t in trades if t.side == "buy")
        sell_vol = sum(t.size for t in trades if t.side == "sell")
        avg_size = sum(t.size for t in trades) / len(trades)

        return {
            "count": len(trades),
            "buy_vol": buy_vol,
            "sell_vol": sell_vol,
            "cvd": buy_vol - sell_vol,
            "avg_size": avg_size,
            "ofi": self.order_flow_imbalance(window),
        }

    def get_large_orders(self, active_only: bool = True) -> list[dict]:
        """获取大单列表"""
        orders = self._large_orders.values()
        if active_only:
            orders = [o for o in orders if not o.disappeared]
        return [
            {
                "price": o.price,
                "size": o.size,
                "side": o.side,
                "duration": time.time() - o.first_seen,
                "disappeared": o.disappeared,
            }
            for o in orders
        ]
