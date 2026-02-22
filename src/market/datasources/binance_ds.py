"""
Binance DataSource - 币安行情数据源

通过 WebSocket 接收 BTC 实时价格、K线数据。
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any

import aiohttp
from loguru import logger

from src.core.event_bus import Event, EventBus, EventType
from src.market.datasources.base import (
    DataSourceBase,
    DataSourceStatus,
    Kline,
    Tick,
)


class BinanceDataSource(DataSourceBase):
    """
    Binance WebSocket 数据源

    提供 BTC/USDT 的实时 tick 和 K线数据。
    """

    WS_BASE = "wss://stream.binance.com:9443/ws"
    REST_BASE = "https://api.binance.com/api/v3"

    def __init__(self, event_bus: EventBus | None = None) -> None:
        super().__init__(event_bus)
        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self._session: aiohttp.ClientSession | None = None
        self._running = False
        self._subscribed_streams: list[str] = []

    def name(self) -> str:
        return "binance"

    async def connect(self) -> None:
        """连接到 Binance WebSocket"""
        self.status = DataSourceStatus.CONNECTING
        try:
            self._session = aiohttp.ClientSession()
            streams = "/".join(self._subscribed_streams) if self._subscribed_streams else "btcusdt@trade"
            url = f"{self.WS_BASE}/{streams}"
            self._ws = await self._session.ws_connect(url)
            self.status = DataSourceStatus.CONNECTED
            self._reconnect_count = 0
            logger.info(f"Binance WebSocket connected: {streams}")
        except Exception as e:
            self.status = DataSourceStatus.ERROR
            logger.error(f"Binance connection failed: {e}")
            raise

    async def disconnect(self) -> None:
        self._running = False
        if self._ws:
            await self._ws.close()
        if self._session:
            await self._session.close()
        self.status = DataSourceStatus.DISCONNECTED
        logger.info("Binance WebSocket disconnected")

    async def subscribe(self, symbols: list[str], channels: list[str]) -> None:
        """
        订阅数据流

        Args:
            symbols: ['btcusdt']
            channels: ['trade', 'kline_1m', 'kline_5m']
        """
        self._subscribed_streams = []
        for symbol in symbols:
            for channel in channels:
                self._subscribed_streams.append(f"{symbol.lower()}@{channel}")

    async def start_streaming(self) -> None:
        """启动数据流接收循环"""
        self._running = True
        while self._running:
            try:
                if not self.is_connected():
                    await self.connect()

                async for msg in self._ws:
                    if not self._running:
                        break
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = json.loads(msg.data)
                        await self._handle_message(data)
                    elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                        logger.warning(f"Binance WS closed: {msg.type}")
                        break

            except Exception as e:
                logger.error(f"Binance stream error: {e}")
                if self._running:
                    await self.reconnect()

    async def _handle_message(self, data: dict[str, Any]) -> None:
        """处理 WebSocket 消息"""
        event_type = data.get("e")

        if event_type == "trade":
            tick = Tick(
                symbol=data["s"],
                price=float(data["p"]),
                volume=float(data["q"]),
                timestamp=data["T"] / 1000,
                source="binance",
            )
            if self.event_bus:
                await self.event_bus.publish(Event(
                    type=EventType.MARKET_TICK,
                    data={"tick": tick, "source": "binance"},
                    source="binance",
                ))

        elif event_type == "kline":
            k = data["k"]
            kline = Kline(
                symbol=k["s"],
                interval=k["i"],
                open=float(k["o"]),
                high=float(k["h"]),
                low=float(k["l"]),
                close=float(k["c"]),
                volume=float(k["v"]),
                timestamp=k["t"] / 1000,
                closed=k["x"],
                source="binance",
            )
            if self.event_bus:
                await self.event_bus.publish(Event(
                    type=EventType.MARKET_KLINE,
                    data={"kline": kline, "source": "binance"},
                    source="binance",
                ))

    async def fetch_klines(
        self,
        symbol: str = "BTCUSDT",
        interval: str = "1m",
        limit: int = 500,
        start_time: int | None = None,
        end_time: int | None = None,
    ) -> list[Kline]:
        """
        REST API 获取历史 K线

        Args:
            symbol: 交易对
            interval: K线周期
            limit: 条数
            start_time: 起始时间戳 (ms)
            end_time: 结束时间戳 (ms)
        """
        params: dict[str, Any] = {
            "symbol": symbol,
            "interval": interval,
            "limit": limit,
        }
        if start_time:
            params["startTime"] = start_time
        if end_time:
            params["endTime"] = end_time

        async with aiohttp.ClientSession() as session:
            async with session.get(f"{self.REST_BASE}/klines", params=params) as resp:
                data = await resp.json()

        klines = []
        for row in data:
            klines.append(Kline(
                symbol=symbol,
                interval=interval,
                open=float(row[1]),
                high=float(row[2]),
                low=float(row[3]),
                close=float(row[4]),
                volume=float(row[5]),
                timestamp=row[0] / 1000,
                closed=True,
                source="binance",
            ))
        return klines
