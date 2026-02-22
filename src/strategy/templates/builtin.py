"""
Built-in Strategy Templates - 内置策略模板

提供可直接使用或作为参考的策略实现。
"""

from __future__ import annotations

from typing import Any

import pandas as pd
from loguru import logger

from src.strategy.base import Strategy
from src.core.context import Context


class MomentumStrategy(Strategy):
    """
    动量策略

    逻辑：
    - 计算 BTC 过去 N 根 K线的价格变化率
    - 如果动量为正 → 下注 YES (BTC 继续涨)
    - 如果动量为负 → 下注 NO  (BTC 继续跌)
    - 使用 RSI 过滤极端超买超卖

    参数：
    - lookback: 回看周期
    - threshold: 动量阈值（只在动量够强时下注）
    - rsi_upper: RSI 超买阈值
    - rsi_lower: RSI 超卖阈值
    - bet_size: 单笔下注金额 (USDC)
    """

    def __init__(
        self,
        lookback: int = 5,
        threshold: float = 0.001,
        rsi_upper: float = 70,
        rsi_lower: float = 30,
        bet_size: float = 10.0,
    ) -> None:
        self._lookback = lookback
        self._threshold = threshold
        self._rsi_upper = rsi_upper
        self._rsi_lower = rsi_lower
        self._bet_size = bet_size
        self._last_signal: str | None = None

    def name(self) -> str:
        return "momentum"

    def version(self) -> str:
        return "1.0"

    def description(self) -> str:
        return f"BTC price momentum strategy (lookback={self._lookback})"

    def get_params(self) -> dict[str, Any]:
        return {
            "lookback": self._lookback,
            "threshold": self._threshold,
            "rsi_upper": self._rsi_upper,
            "rsi_lower": self._rsi_lower,
            "bet_size": self._bet_size,
        }

    def on_init(self, context: Context) -> None:
        logger.info(f"MomentumStrategy init: {self.get_params()}")

    def on_market_data(self, context: Context, data: dict[str, Any]) -> None:
        history: pd.DataFrame = data.get("history", pd.DataFrame())

        if len(history) < self._lookback + 14:  # 需要足够数据算 RSI
            return

        closes = history["close"].values

        # 计算动量
        momentum = (closes[-1] - closes[-self._lookback]) / closes[-self._lookback]

        # 计算 RSI
        rsi = self._calc_rsi(closes, 14)

        # 信号逻辑
        signal = None
        if momentum > self._threshold and rsi < self._rsi_upper:
            signal = "YES"
        elif momentum < -self._threshold and rsi > self._rsi_lower:
            signal = "NO"

        # 防止重复下单
        if signal and signal != self._last_signal:
            self._last_signal = signal

            # 通过回测引擎下单
            if context.get("backtest_engine"):
                from src.backtest.engine import OrderSide
                side = OrderSide.YES if signal == "YES" else OrderSide.NO
                context.get("backtest_engine").submit_order(side, self._bet_size)
            else:
                # 实盘模式：发布信号事件
                import asyncio
                from src.core.event_bus import Event, EventType
                asyncio.create_task(context.event_bus.publish(Event(
                    type=EventType.SIGNAL_GENERATED,
                    data={
                        "strategy": self.name(),
                        "signal": signal,
                        "size": self._bet_size,
                        "momentum": momentum,
                        "rsi": rsi,
                    },
                    source=self.name(),
                )))

    def _calc_rsi(self, closes, period: int = 14) -> float:
        deltas = pd.Series(closes).diff()
        gain = deltas.clip(lower=0).rolling(period).mean().iloc[-1]
        loss = (-deltas.clip(upper=0)).rolling(period).mean().iloc[-1]
        if loss == 0:
            return 100.0
        rs = gain / loss
        return 100 - (100 / (1 + rs))


class MeanReversionStrategy(Strategy):
    """
    均值回归策略

    逻辑：
    - 当 PM YES 价格显著偏离理论公允价值时反向下注
    - 理论价值 = 基于 BTC 动量的概率估计
    - 如果 YES 价格过高 → 卖 YES (买 NO)
    - 如果 YES 价格过低 → 买 YES

    参数：
    - bb_period: 布林带周期
    - bb_std: 布林带标准差倍数
    - bet_size: 下注金额
    """

    def __init__(
        self,
        bb_period: int = 20,
        bb_std: float = 2.0,
        bet_size: float = 10.0,
    ) -> None:
        self._bb_period = bb_period
        self._bb_std = bb_std
        self._bet_size = bet_size

    def name(self) -> str:
        return "mean_reversion"

    def version(self) -> str:
        return "1.0"

    def description(self) -> str:
        return "Mean reversion strategy based on Bollinger Bands"

    def get_params(self) -> dict[str, Any]:
        return {
            "bb_period": self._bb_period,
            "bb_std": self._bb_std,
            "bet_size": self._bet_size,
        }

    def on_init(self, context: Context) -> None:
        logger.info(f"MeanReversionStrategy init: {self.get_params()}")

    def on_market_data(self, context: Context, data: dict[str, Any]) -> None:
        history: pd.DataFrame = data.get("history", pd.DataFrame())

        if len(history) < self._bb_period:
            return

        closes = history["close"].tail(self._bb_period)
        current_price = closes.iloc[-1]

        sma = closes.mean()
        std = closes.std()
        upper = sma + self._bb_std * std
        lower = sma - self._bb_std * std

        signal = None
        if current_price > upper:
            signal = "NO"   # 价格超买 → 预期回落
        elif current_price < lower:
            signal = "YES"  # 价格超卖 → 预期反弹

        if signal and context.get("backtest_engine"):
            from src.backtest.engine import OrderSide
            side = OrderSide.YES if signal == "YES" else OrderSide.NO
            context.get("backtest_engine").submit_order(side, self._bet_size)


class OrderFlowStrategy(Strategy):
    """
    订单流策略

    逻辑：
    - 分析买卖成交量不平衡
    - CVD + 买卖盘压力比判断方向
    - 成交量突增时加大仓位

    参数：
    - ofi_threshold: 订单流不平衡阈值
    - pressure_threshold: 买卖压力比阈值
    - bet_size: 下注金额
    """

    def __init__(
        self,
        ofi_threshold: float = 0.3,
        pressure_threshold: float = 1.5,
        bet_size: float = 10.0,
    ) -> None:
        self._ofi_threshold = ofi_threshold
        self._pressure_threshold = pressure_threshold
        self._bet_size = bet_size

    def name(self) -> str:
        return "order_flow"

    def version(self) -> str:
        return "1.0"

    def description(self) -> str:
        return "Order flow based strategy using CVD and book pressure"

    def get_params(self) -> dict[str, Any]:
        return {
            "ofi_threshold": self._ofi_threshold,
            "pressure_threshold": self._pressure_threshold,
            "bet_size": self._bet_size,
        }

    def on_init(self, context: Context) -> None:
        logger.info(f"OrderFlowStrategy init: {self.get_params()}")

    def on_market_data(self, context: Context, data: dict[str, Any]) -> None:
        # 订单流策略主要依赖 on_order_book，这里仅做基础处理
        pass

    def on_order_book(self, context: Context, book: dict[str, Any]) -> None:
        ofi = book.get("order_flow_imbalance", 0)
        pressure = book.get("book_pressure", 1.0)
        cvd = book.get("cvd", 0)

        signal = None
        if ofi > self._ofi_threshold and pressure > self._pressure_threshold:
            signal = "YES"   # 买方主导
        elif ofi < -self._ofi_threshold and pressure < 1 / self._pressure_threshold:
            signal = "NO"    # 卖方主导

        if signal and context.get("backtest_engine"):
            from src.backtest.engine import OrderSide
            side = OrderSide.YES if signal == "YES" else OrderSide.NO
            context.get("backtest_engine").submit_order(side, self._bet_size)


class HybridStrategy(Strategy):
    """
    混合策略

    融合多个子策略的信号，加权投票决定方向。

    参数：
    - strategies: 子策略列表
    - weights: 权重列表
    - threshold: 信号阈值 (加权分需超过此值才下单)
    - bet_size: 下注金额
    """

    def __init__(
        self,
        strategies: list[Strategy] | None = None,
        weights: list[float] | None = None,
        threshold: float = 0.5,
        bet_size: float = 10.0,
    ) -> None:
        self._strategies = strategies or []
        self._weights = weights or [1.0] * len(self._strategies)
        self._threshold = threshold
        self._bet_size = bet_size
        self._signals: dict[str, str] = {}

    def name(self) -> str:
        return "hybrid"

    def version(self) -> str:
        return "1.0"

    def get_params(self) -> dict[str, Any]:
        return {
            "sub_strategies": [s.name() for s in self._strategies],
            "weights": self._weights,
            "threshold": self._threshold,
            "bet_size": self._bet_size,
        }

    def on_init(self, context: Context) -> None:
        for s in self._strategies:
            s.on_init(context)
        logger.info(f"HybridStrategy init with {len(self._strategies)} sub-strategies")

    def on_market_data(self, context: Context, data: dict[str, Any]) -> None:
        # 驱动所有子策略
        for s in self._strategies:
            s.on_market_data(context, data)

        # TODO: 收集子策略信号后做加权投票
        # 需要子策略通过事件或上下文传递信号
