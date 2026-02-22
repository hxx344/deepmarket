"""
Strategy Base - 策略基类

所有交易策略必须继承此基类，提供统一的生命周期接口。
支持回测和实盘的同一套代码。
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from src.core.context import Context


class Strategy(ABC):
    """
    策略基类

    生命周期:
    1. on_init()          - 策略初始化（设置参数、订阅数据）
    2. on_market_data()   - 行情更新回调（主驱动）
    3. on_order_book()    - 订单簿更新回调（可选）
    4. on_trade()         - 成交回调（自己的订单成交）
    5. on_order_update()  - 订单状态更新
    6. on_timer()         - 定时器回调（可选）
    7. on_stop()          - 策略停止时清理

    策略可以通过 context 访问:
    - context.account     - 账户状态
    - context.market      - 市场状态
    - context.event_bus   - 发布事件
    - context.get/set()   - 自定义数据

    Usage (回测):
        class MyStrategy(Strategy):
            def name(self): return "my_strategy"
            def on_init(self, ctx): ...
            def on_market_data(self, ctx, data): ...

        engine = BacktestEngine(config)
        engine.set_strategy(MyStrategy())
        result = await engine.run()
    """

    @abstractmethod
    def name(self) -> str:
        """策略唯一名称"""
        ...

    def version(self) -> str:
        """策略版本"""
        return "1.0"

    def description(self) -> str:
        """策略描述"""
        return ""

    @abstractmethod
    def on_init(self, context: Context) -> None:
        """
        策略初始化

        在回测/实盘开始前调用一次。
        可在此设置参数、注册指标等。
        """
        ...

    @abstractmethod
    def on_market_data(self, context: Context, data: dict[str, Any]) -> None:
        """
        行情更新回调

        每根 K线闭合时调用（回测）或每次 tick 时调用（实盘）。

        Args:
            context: 上下文
            data: 行情数据
                - timestamp: float
                - open/high/low/close/volume: float
                - bar_index: int
                - history: DataFrame (历史 K线)
        """
        ...

    def on_order_book(self, context: Context, book: dict[str, Any]) -> None:
        """
        订单簿更新回调（可选覆写）

        Args:
            context: 上下文
            book: 订单簿数据
        """
        pass

    def on_trade(self, context: Context, trade: dict[str, Any]) -> None:
        """
        自己的订单成交回调（可选覆写）

        Args:
            context: 上下文
            trade: 成交数据
        """
        pass

    def on_order_update(self, context: Context, order: dict[str, Any]) -> None:
        """
        订单状态更新回调（可选覆写）

        Args:
            context: 上下文
            order: 订单状态
        """
        pass

    def on_timer(self, context: Context, timer_id: str) -> None:
        """
        定时器回调（可选覆写）

        Args:
            context: 上下文
            timer_id: 定时器 ID
        """
        pass

    def on_stop(self, context: Context) -> None:
        """
        策略停止时回调（可选覆写）

        可在此做清理、保存状态等。
        """
        pass

    def get_params(self) -> dict[str, Any]:
        """返回策略参数（用于序列化和优化）"""
        return {}
