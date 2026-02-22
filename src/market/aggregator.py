"""
Market Aggregator - 行情聚合引擎

整合多数据源数据，驱动指标计算，维护统一行情视图。
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

import pandas as pd
from loguru import logger

from src.core.event_bus import Event, EventBus, EventType, get_event_bus
from src.core.storage import StorageManager
from src.market.datasources.base import Kline, Tick
from src.market.indicators.base import IndicatorPlugin
from src.market.indicators.technical import create_default_indicators


class MarketAggregator:
    """
    行情聚合器

    - 接收多数据源的 tick/kline 事件
    - 维护内存中的 K线序列
    - 驱动指标计算
    - 将结果写入存储
    """

    def __init__(
        self,
        event_bus: EventBus | None = None,
        storage: StorageManager | None = None,
        max_klines: int = 1000,
    ) -> None:
        self.event_bus = event_bus or get_event_bus()
        self.storage = storage
        self.max_klines = max_klines

        # 内存 K线缓存: {interval: DataFrame}
        self._klines: dict[str, pd.DataFrame] = {}
        # 行缓冲 (避免每条 K 线都做 pd.concat)
        self._kline_buffer: dict[str, list[dict]] = {}
        self._buffer_flush_size = 50  # 每 50 条 flush 一次到 DataFrame

        # 最新 tick 缓存
        self._latest_ticks: dict[str, Tick] = {}  # {source: Tick}

        # 指标引擎
        self._indicators: list[IndicatorPlugin] = []
        self._indicator_results: dict[str, pd.Series | pd.DataFrame] = {}

        # BTC 价格 tick 聚合 -> 1min K线
        self._current_candle: dict[str, Any] | None = None
        self._candle_interval = 60  # 1 min

    def add_indicator(self, indicator: IndicatorPlugin) -> None:
        """添加指标"""
        self._indicators.append(indicator)
        logger.info(f"Indicator added: {indicator.name()}")

    def load_default_indicators(self) -> None:
        """加载默认指标集"""
        for ind in create_default_indicators():
            self.add_indicator(ind)
        logger.info(f"Loaded {len(self._indicators)} default indicators")

    async def start(self) -> None:
        """启动聚合器，订阅事件"""
        self.event_bus.subscribe(EventType.MARKET_TICK, self._on_tick)
        self.event_bus.subscribe(EventType.MARKET_KLINE, self._on_kline)
        self.event_bus.subscribe(EventType.PM_PRICE, self._on_pm_price)
        logger.info("MarketAggregator started")

    async def stop(self) -> None:
        """停止并持久化"""
        self._flush_to_storage()
        logger.info("MarketAggregator stopped")

    async def _on_tick(self, event: Event) -> None:
        """处理 BTC tick"""
        tick: Tick = event.data.get("tick")
        if not tick:
            return

        self._latest_ticks[tick.source] = tick
        self._aggregate_tick_to_candle(tick)

    async def _on_kline(self, event: Event) -> None:
        """处理 K线更新"""
        kline: Kline = event.data.get("kline")
        if not kline or not kline.closed:
            return

        interval = kline.interval
        row = {
            "timestamp": kline.timestamp,
            "open": kline.open,
            "high": kline.high,
            "low": kline.low,
            "close": kline.close,
            "volume": kline.volume,
            "source": kline.source,
        }

        # 缓冲行, 批量 flush (避免逐条 pd.concat 的 O(n²) 开销)
        if interval not in self._kline_buffer:
            self._kline_buffer[interval] = []
        self._kline_buffer[interval].append(row)

        if len(self._kline_buffer[interval]) >= self._buffer_flush_size:
            self._flush_kline_buffer(interval)

        # 计算指标 (需要先 flush 让 DataFrame 可用)
        self._flush_kline_buffer(interval)
        await self._compute_indicators(interval)

    async def _on_pm_price(self, event: Event) -> None:
        """处理 Polymarket 价格更新"""
        tick: Tick = event.data.get("tick")
        if tick:
            self._latest_ticks["polymarket"] = tick

    def _aggregate_tick_to_candle(self, tick: Tick) -> None:
        """将 tick 聚合为 1min K线"""
        candle_ts = int(tick.timestamp // self._candle_interval) * self._candle_interval

        if self._current_candle is None or self._current_candle["timestamp"] != candle_ts:
            # 闭合旧 K线
            if self._current_candle is not None:
                self._close_candle()

            # 开新 K线
            self._current_candle = {
                "timestamp": candle_ts,
                "open": tick.price,
                "high": tick.price,
                "low": tick.price,
                "close": tick.price,
                "volume": tick.volume,
                "source": tick.source,
            }
        else:
            self._current_candle["high"] = max(self._current_candle["high"], tick.price)
            self._current_candle["low"] = min(self._current_candle["low"], tick.price)
            self._current_candle["close"] = tick.price
            self._current_candle["volume"] += tick.volume

    def _close_candle(self) -> None:
        """闭合当前 K线"""
        if self._current_candle is None:
            return

        interval = "1m"
        if interval not in self._kline_buffer:
            self._kline_buffer[interval] = []
        self._kline_buffer[interval].append(self._current_candle)

        if len(self._kline_buffer[interval]) >= self._buffer_flush_size:
            self._flush_kline_buffer(interval)

    def _flush_kline_buffer(self, interval: str) -> None:
        """将行缓冲 flush 到 DataFrame"""
        buf = self._kline_buffer.get(interval, [])
        if not buf:
            return

        new_df = pd.DataFrame(buf)
        self._kline_buffer[interval] = []

        if interval not in self._klines or self._klines[interval].empty:
            self._klines[interval] = new_df.tail(self.max_klines)
        else:
            self._klines[interval] = pd.concat(
                [self._klines[interval], new_df], ignore_index=True,
            ).tail(self.max_klines)

    async def _compute_indicators(self, interval: str) -> None:
        """计算所有已注册的指标"""
        df = self._klines.get(interval)
        if df is None or df.empty:
            return

        for indicator in self._indicators:
            try:
                if not indicator.validate(df):
                    continue
                result = indicator.calculate(df)
                key = f"{interval}_{indicator.name()}"
                self._indicator_results[key] = result

                # 发布指标更新事件
                latest_val = result.iloc[-1] if isinstance(result, pd.Series) else result.iloc[-1].to_dict()
                await self.event_bus.publish(Event(
                    type=EventType.MARKET_INDICATOR,
                    data={
                        "indicator": indicator.name(),
                        "interval": interval,
                        "value": latest_val,
                    },
                    source="aggregator",
                ))
            except Exception as e:
                logger.error(f"Indicator {indicator.name()} calc failed: {e}")

    def _flush_to_storage(self) -> None:
        """持久化 K线数据到 Parquet"""
        if not self.storage:
            return
        for interval, df in self._klines.items():
            if df.empty:
                continue
            self.storage.parquet.append(f"klines_{interval}", df)
            logger.debug(f"Flushed {len(df)} klines ({interval}) to storage")

    # ---- 公开查询接口 ----

    def get_latest_price(self, source: str = "chainlink") -> float:
        """获取最新价格"""
        tick = self._latest_ticks.get(source)
        return tick.price if tick else 0.0

    def get_klines(self, interval: str = "1m", limit: int = 100) -> pd.DataFrame:
        """获取 K线数据"""
        df = self._klines.get(interval, pd.DataFrame())
        return df.tail(limit).copy()

    def get_indicator(self, name: str, interval: str = "1m") -> pd.Series | pd.DataFrame | None:
        """获取指标值"""
        return self._indicator_results.get(f"{interval}_{name}")

    def get_market_snapshot(self) -> dict[str, Any]:
        """获取当前市场快照"""
        return {
            "btc_price": self.get_latest_price("chainlink"),
            "pm_price": self.get_latest_price("polymarket"),
            "ticks": {k: {"price": v.price, "ts": v.timestamp} for k, v in self._latest_ticks.items()},
            "kline_counts": {k: len(v) for k, v in self._klines.items()},
            "indicator_count": len(self._indicator_results),
        }
