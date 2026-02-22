"""
Event Bus - 全局事件总线

基于 asyncio 的发布/订阅事件系统，作为所有模块间通信的核心。
支持同步和异步事件处理器，支持事件过滤和优先级。
"""

from __future__ import annotations

import asyncio
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Coroutine, Union

from loguru import logger


class EventType(str, Enum):
    """系统事件类型枚举"""

    # --- 行情事件 ---
    MARKET_TICK = "market.tick"                  # BTC 价格 tick
    MARKET_KLINE = "market.kline"                # K线更新
    MARKET_INDICATOR = "market.indicator"         # 指标更新
    CHAINLINK_PRICE = "chainlink.price"          # Chainlink BTC/USD 预言机价格
    PM_PRICE = "pm.price"                        # Polymarket 价格更新
    PM_TRADE = "pm.trade"                        # Polymarket 成交

    # --- 订单簿事件 ---
    ORDERBOOK_UPDATE = "orderbook.update"        # 订单簿更新
    ORDERBOOK_SNAPSHOT = "orderbook.snapshot"     # 订单簿快照
    ORDERBOOK_LARGE_ORDER = "orderbook.large"    # 大单检测

    # --- 交易事件 ---
    ORDER_CREATED = "order.created"              # 订单创建
    ORDER_SUBMITTED = "order.submitted"          # 订单提交
    ORDER_FILLED = "order.filled"                # 订单成交
    ORDER_PARTIAL_FILL = "order.partial_fill"    # 部分成交
    ORDER_CANCELLED = "order.cancelled"          # 订单取消
    ORDER_REJECTED = "order.rejected"            # 订单拒绝

    # --- 策略事件 ---
    SIGNAL_GENERATED = "signal.generated"        # 策略信号生成
    STRATEGY_START = "strategy.start"            # 策略启动
    STRATEGY_STOP = "strategy.stop"              # 策略停止
    STRATEGY_ERROR = "strategy.error"            # 策略异常

    # --- 风控事件 ---
    RISK_WARNING = "risk.warning"                # 风控警告
    RISK_ALERT = "risk.alert"                    # 风控告警
    RISK_BREAKER = "risk.breaker"                # 熔断触发

    # --- 系统事件 ---
    SYSTEM_START = "system.start"
    SYSTEM_STOP = "system.stop"
    SYSTEM_ERROR = "system.error"
    HEARTBEAT = "system.heartbeat"


@dataclass
class Event:
    """事件对象"""

    type: EventType
    data: dict[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)
    source: str = ""

    def __repr__(self) -> str:
        return f"Event(type={self.type.value}, source={self.source}, ts={self.timestamp:.3f})"


# 事件处理器类型
EventHandler = Union[Callable[[Event], Coroutine[Any, Any, None]], Callable[[Event], None]]


@dataclass
class _Subscription:
    handler: EventHandler
    priority: int = 0
    is_async: bool = False


class EventBus:
    """
    全局事件总线

    Usage:
        bus = EventBus()
        bus.subscribe(EventType.MARKET_TICK, on_tick_handler)
        await bus.publish(Event(type=EventType.MARKET_TICK, data={"price": 50000}))
    """

    def __init__(self) -> None:
        self._subscribers: dict[EventType, list[_Subscription]] = defaultdict(list)
        self._event_log: deque[Event] = deque(maxlen=10000)
        self._max_log_size = 10000
        self._running = False
        self._queue: asyncio.Queue[Event] = asyncio.Queue()
        self._stats: dict[str, int] = defaultdict(int)

    def subscribe(
        self,
        event_type: EventType,
        handler: EventHandler,
        priority: int = 0,
    ) -> None:
        """
        订阅事件

        Args:
            event_type: 事件类型
            handler: 处理函数（同步或异步）
            priority: 优先级（数值越大越先执行）
        """
        is_async = asyncio.iscoroutinefunction(handler)
        sub = _Subscription(handler=handler, priority=priority, is_async=is_async)
        self._subscribers[event_type].append(sub)
        # 按优先级排序（降序）
        self._subscribers[event_type].sort(key=lambda s: s.priority, reverse=True)
        logger.debug(f"Subscribed {handler.__name__} to {event_type.value} (priority={priority})")

    def unsubscribe(self, event_type: EventType, handler: EventHandler) -> None:
        """取消订阅"""
        subs = self._subscribers[event_type]
        self._subscribers[event_type] = [s for s in subs if s.handler != handler]

    async def publish(self, event: Event) -> None:
        """
        发布事件（立即分发给所有订阅者）

        Args:
            event: 事件对象
        """
        self._stats[event.type.value] += 1

        # 记录事件日志 (deque 自动截断, 无需手动 slice)
        self._event_log.append(event)

        subscribers = self._subscribers.get(event.type, [])
        if not subscribers:
            return

        for sub in subscribers:
            try:
                if sub.is_async:
                    await sub.handler(event)
                else:
                    sub.handler(event)
            except Exception as e:
                logger.error(
                    f"Event handler {sub.handler.__name__} failed for "
                    f"{event.type.value}: {e}"
                )

    async def publish_async(self, event: Event) -> None:
        """发布事件到队列（异步处理，不阻塞发布者）"""
        await self._queue.put(event)

    async def start(self) -> None:
        """启动事件总线后台处理循环"""
        self._running = True
        logger.info("EventBus started")
        while self._running:
            try:
                event = await asyncio.wait_for(self._queue.get(), timeout=1.0)
                await self.publish(event)
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"EventBus processing error: {e}")

    async def stop(self) -> None:
        """停止事件总线"""
        self._running = False
        # 处理队列中剩余事件
        while not self._queue.empty():
            event = self._queue.get_nowait()
            await self.publish(event)
        logger.info("EventBus stopped")

    def get_stats(self) -> dict[str, int]:
        """获取事件统计"""
        return dict(self._stats)

    def get_recent_events(self, event_type: EventType = None, limit: int = 50) -> list[Event]:
        """获取最近的事件"""
        events = list(self._event_log)
        if event_type:
            events = [e for e in events if e.type == event_type]
        return events[-limit:]


# 全局事件总线单例
_global_bus: EventBus | None = None


def get_event_bus() -> EventBus:
    """获取全局事件总线实例"""
    global _global_bus
    if _global_bus is None:
        _global_bus = EventBus()
    return _global_bus
