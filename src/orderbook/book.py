"""
Order Book - 订单簿管理

维护 Polymarket 市场的实时 L2 订单簿，支持增量更新和快照。
"""

from __future__ import annotations

import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

from loguru import logger


@dataclass
class PriceLevel:
    """价格档位"""
    price: float
    size: float
    order_count: int = 1
    timestamp: float = 0.0


@dataclass
class OrderBookState:
    """订单簿状态"""
    bids: list[PriceLevel] = field(default_factory=list)  # 买单 (价格降序)
    asks: list[PriceLevel] = field(default_factory=list)  # 卖单 (价格升序)
    timestamp: float = 0.0
    market_id: str = ""

    @property
    def best_bid(self) -> float:
        return self.bids[0].price if self.bids else 0.0

    @property
    def best_ask(self) -> float:
        return self.asks[0].price if self.asks else 1.0

    @property
    def mid_price(self) -> float:
        return (self.best_bid + self.best_ask) / 2 if self.bids and self.asks else 0.0

    @property
    def spread(self) -> float:
        return self.best_ask - self.best_bid if self.bids and self.asks else 0.0

    @property
    def spread_pct(self) -> float:
        mid = self.mid_price
        return self.spread / mid if mid > 0 else 0.0

    def total_bid_size(self, depth: int | None = None) -> float:
        levels = self.bids[:depth] if depth else self.bids
        return sum(lvl.size for lvl in levels)

    def total_ask_size(self, depth: int | None = None) -> float:
        levels = self.asks[:depth] if depth else self.asks
        return sum(lvl.size for lvl in levels)

    def to_dict(self) -> dict[str, Any]:
        return {
            "market_id": self.market_id,
            "timestamp": self.timestamp,
            "best_bid": self.best_bid,
            "best_ask": self.best_ask,
            "mid_price": self.mid_price,
            "spread": self.spread,
            "bids": [{"price": l.price, "size": l.size} for l in self.bids[:20]],
            "asks": [{"price": l.price, "size": l.size} for l in self.asks[:20]],
        }


class OrderBook:
    """
    订单簿管理器

    维护一个市场的完整 L2 订单簿。
    """

    def __init__(self, market_id: str) -> None:
        self.market_id = market_id
        self._bids: dict[float, PriceLevel] = {}  # price -> PriceLevel
        self._asks: dict[float, PriceLevel] = {}
        self._last_update: float = 0.0
        self._update_count: int = 0

    def apply_snapshot(self, bids: list[dict], asks: list[dict]) -> None:
        """
        应用完整快照（替换整个订单簿）

        Args:
            bids: [{"price": 0.65, "size": 100}, ...]
            asks: [{"price": 0.66, "size": 50}, ...]
        """
        self._bids.clear()
        self._asks.clear()

        for b in bids:
            price = float(b["price"])
            self._bids[price] = PriceLevel(
                price=price, size=float(b["size"]), timestamp=time.time()
            )

        for a in asks:
            price = float(a["price"])
            self._asks[price] = PriceLevel(
                price=price, size=float(a["size"]), timestamp=time.time()
            )

        self._last_update = time.time()
        self._update_count += 1

    def apply_update(self, side: str, price: float, size: float) -> None:
        """
        应用增量更新

        Args:
            side: 'bid' or 'ask'
            price: 价格
            size: 数量 (0 表示删除该档位)
        """
        book = self._bids if side == "bid" else self._asks

        if size <= 0:
            book.pop(price, None)
        else:
            book[price] = PriceLevel(price=price, size=size, timestamp=time.time())

        self._last_update = time.time()
        self._update_count += 1

    def get_state(self) -> OrderBookState:
        """获取当前订单簿状态"""
        bids = sorted(self._bids.values(), key=lambda x: x.price, reverse=True)
        asks = sorted(self._asks.values(), key=lambda x: x.price)
        return OrderBookState(
            bids=bids,
            asks=asks,
            timestamp=self._last_update,
            market_id=self.market_id,
        )

    def get_depth(self, levels: int = 10) -> dict[str, list[dict]]:
        """获取指定档位深度"""
        state = self.get_state()
        return {
            "bids": [{"price": l.price, "size": l.size} for l in state.bids[:levels]],
            "asks": [{"price": l.price, "size": l.size} for l in state.asks[:levels]],
        }

    def estimate_fill_price(self, side: str, size: float) -> tuple[float, float]:
        """
        估算成交价格和滑点

        Args:
            side: 'buy' (吃 ask) 或 'sell' (吃 bid)
            size: 成交数量

        Returns:
            (avg_price, slippage_pct)
        """
        levels = sorted(self._asks.values(), key=lambda x: x.price) if side == "buy" \
            else sorted(self._bids.values(), key=lambda x: x.price, reverse=True)

        if not levels:
            return 0.0, 0.0

        total_cost = 0.0
        remaining = size
        best_price = levels[0].price

        for level in levels:
            fill = min(remaining, level.size)
            total_cost += fill * level.price
            remaining -= fill
            if remaining <= 0:
                break

        if remaining > 0:
            return 0.0, float("inf")  # 深度不足

        avg_price = total_cost / size
        slippage = abs(avg_price - best_price) / best_price if best_price > 0 else 0.0
        return avg_price, slippage
