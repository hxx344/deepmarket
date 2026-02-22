"""
DataSource Base - 数据源适配器基类

所有外部数据源（Binance、Polymarket、链上等）都实现此接口。
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, AsyncIterator

from src.core.event_bus import EventBus


class DataSourceStatus(str, Enum):
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    ERROR = "error"


@dataclass
class Tick:
    """通用 Tick 数据"""
    symbol: str
    price: float
    volume: float = 0.0
    bid: float = 0.0
    ask: float = 0.0
    timestamp: float = 0.0
    source: str = ""
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass
class Kline:
    """K线数据"""
    symbol: str
    interval: str           # '1m', '5m', etc.
    open: float = 0.0
    high: float = 0.0
    low: float = 0.0
    close: float = 0.0
    volume: float = 0.0
    timestamp: float = 0.0  # 开盘时间
    closed: bool = False     # 是否已闭合
    source: str = ""


class DataSourceBase(ABC):
    """数据源适配器基类"""

    def __init__(self, event_bus: EventBus | None = None) -> None:
        self.event_bus = event_bus
        self.status = DataSourceStatus.DISCONNECTED
        self._reconnect_count = 0
        self._max_reconnect = 10

    @abstractmethod
    def name(self) -> str:
        """数据源名称"""
        ...

    @abstractmethod
    async def connect(self) -> None:
        """建立连接"""
        ...

    @abstractmethod
    async def disconnect(self) -> None:
        """断开连接"""
        ...

    @abstractmethod
    async def subscribe(self, symbols: list[str], channels: list[str]) -> None:
        """订阅数据流"""
        ...

    async def reconnect(self) -> None:
        """重连逻辑（指数退避）"""
        import asyncio
        self.status = DataSourceStatus.RECONNECTING
        self._reconnect_count += 1
        wait = min(2 ** self._reconnect_count, 30)
        from loguru import logger
        logger.warning(f"{self.name()} reconnecting in {wait}s (attempt {self._reconnect_count})")
        await asyncio.sleep(wait)
        await self.connect()

    def is_connected(self) -> bool:
        return self.status == DataSourceStatus.CONNECTED
