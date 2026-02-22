"""
Context - 全局上下文管理

维护系统运行时状态，提供各模块间共享的上下文信息。
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from src.core.event_bus import EventBus, get_event_bus


class RunMode(str, Enum):
    """运行模式"""
    LIVE = "live"               # 实盘
    PAPER = "paper"             # 模拟盘
    BACKTEST = "backtest"       # 回测


class TradingMode(str, Enum):
    """交易模式"""
    AUTO = "auto"               # 全自动
    SEMI_AUTO = "semi_auto"     # 半自动（需确认）
    MANUAL = "manual"           # 纯手动
    PAPER = "paper"             # 模拟盘


@dataclass
class AccountState:
    """账户状态"""
    balance: float = 0.0                # USDC 余额
    available: float = 0.0              # 可用余额 (余额 - 冻结)
    total_equity: float = 0.0           # 总权益
    open_positions: dict[str, Any] = field(default_factory=dict)  # 持仓
    daily_pnl: float = 0.0             # 当日盈亏
    total_pnl: float = 0.0            # 总盈亏
    initial_balance: float = 0.0       # 初始余额


@dataclass
class MarketState:
    """市场状态快照"""
    btc_price: float = 0.0
    btc_price_updated_at: float = 0.0  # BTC 价格最后一次真正变化的时间戳
    pm_yes_price: float = 0.0
    pm_no_price: float = 0.0
    pm_yes_bid: float = 0.0
    pm_yes_ask: float = 0.0
    pm_no_bid: float = 0.0
    pm_no_ask: float = 0.0
    pm_volume_5m: float = 0.0
    pm_market_id: str = ""
    pm_condition_id: str = ""               # CTF conditionId (用于链上 redeem)
    pm_market_question: str = ""
    pm_market_active: bool = False
    pm_yes_token_id: str = ""               # CLOB YES(Up) token ID
    pm_no_token_id: str = ""                # CLOB NO(Down) token ID
    pm_neg_risk: bool = False               # 是否负风险市场 (BTC 5-min 通常是 False)
    pm_window_seconds_left: float = 0.0  # 当前 5-min 窗口剩余秒数
    pm_window_start_ts: int = 0            # 当前窗口开始 UTC unix ts
    pm_window_start_price: float = 0.0     # 窗口开始时 BTC 基准价格 (Price to Beat)
    binance_price: float = 0.0             # Binance BTC/USDT 实时价格 (领先指标)
    binance_price_ts: float = 0.0          # Binance 价格最后更新时间戳
    last_update: float = 0.0


class Context:
    """
    全局上下文

    在回测和实盘中共用同一套接口，模块通过 Context 访问共享状态。
    """

    def __init__(
        self,
        run_mode: RunMode = RunMode.PAPER,
        trading_mode: TradingMode = TradingMode.PAPER,
        event_bus: EventBus | None = None,
    ) -> None:
        self.run_mode = run_mode
        self.trading_mode = trading_mode
        self.event_bus = event_bus or get_event_bus()

        self.account = AccountState()
        self.market = MarketState()

        # 自定义数据存储（策略可用）
        self._data: dict[str, Any] = {}

        # 时间管理（回测时可覆盖）
        self._time_func = time.time
        self.start_time = self.now()

    def now(self) -> float:
        """当前时间戳（回测时返回模拟时间）"""
        return self._time_func()

    def set_time_func(self, func):
        """设置时间函数（回测引擎使用）"""
        self._time_func = func

    def get(self, key: str, default: Any = None) -> Any:
        """获取自定义数据"""
        return self._data.get(key, default)

    def set(self, key: str, value: Any) -> None:
        """设置自定义数据"""
        self._data[key] = value

    @property
    def is_live(self) -> bool:
        return self.run_mode == RunMode.LIVE

    @property
    def is_backtest(self) -> bool:
        return self.run_mode == RunMode.BACKTEST

    @property
    def is_paper(self) -> bool:
        return self.run_mode == RunMode.PAPER

    def snapshot(self) -> dict[str, Any]:
        """导出当前上下文快照"""
        return {
            "run_mode": self.run_mode.value,
            "trading_mode": self.trading_mode.value,
            "account": {
                "balance": self.account.balance,
                "total_equity": self.account.total_equity,
                "daily_pnl": self.account.daily_pnl,
                "total_pnl": self.account.total_pnl,
            },
            "market": {
                "btc_price": self.market.btc_price,
                "pm_yes_price": self.market.pm_yes_price,
                "pm_no_price": self.market.pm_no_price,
                "pm_yes_bid": self.market.pm_yes_bid,
                "pm_yes_ask": self.market.pm_yes_ask,
                "pm_no_bid": self.market.pm_no_bid,
                "pm_no_ask": self.market.pm_no_ask,
            },
            "timestamp": self.now(),
        }
