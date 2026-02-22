"""
Backtesting Engine - 回测引擎核心

事件驱动的回测框架，支持 K线级和 Tick 级回测。
数据来源: Polymarket 预测市场历史数据 (概率价格 0~1)。
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any

import pandas as pd
from loguru import logger

from src.core.context import AccountState, Context, MarketState, RunMode, TradingMode
from src.core.event_bus import Event, EventBus, EventType

if TYPE_CHECKING:
    from src.strategy.base import Strategy


class OrderSide(str, Enum):
    YES = "YES"
    NO = "NO"


class OrderStatus(str, Enum):
    PENDING = "pending"
    FILLED = "filled"
    PARTIAL = "partial"
    CANCELLED = "cancelled"
    REJECTED = "rejected"


@dataclass
class BacktestOrder:
    """回测订单"""
    id: str = ""
    side: OrderSide = OrderSide.YES
    price: float = 0.0
    size: float = 0.0
    filled_size: float = 0.0
    filled_price: float = 0.0
    fee: float = 0.0
    status: OrderStatus = OrderStatus.PENDING
    timestamp: float = 0.0
    fill_timestamp: float = 0.0
    strategy_id: str = ""

    def __post_init__(self):
        if not self.id:
            self.id = str(uuid.uuid4())[:8]


@dataclass
class BacktestTrade:
    """回测成交"""
    order_id: str
    price: float
    size: float
    fee: float
    pnl: float
    timestamp: float
    side: OrderSide


@dataclass
class BacktestConfig:
    """回测配置"""
    start_date: str = ""               # YYYY-MM-DD
    end_date: str = ""
    initial_balance: float = 1000.0    # USDC
    fee_rate: float = 0.002            # 0.2% (Polymarket)
    slippage_bps: float = 5.0          # 滑点 (基点)
    latency_ms: float = 100.0          # 模拟延迟
    data_interval: str = "5m"          # K线周期
    max_position_size: float = 500.0   # 单笔最大仓位

    # Polymarket 数据源配置
    token_id: str = ""                 # Polymarket token ID (YES/NO token)
    condition_id: str = ""             # Polymarket market condition ID
    market_question: str = ""          # 市场问题 (可选, 用于报告)
    data_source: str = "polymarket"    # 数据源: polymarket / local / chainlink
    data_dir: str = "data/polymarket"  # 历史数据缓存目录


class BacktestEngine:
    """
    回测引擎

    事件驱动架构：
    1. 从 Polymarket 拉取历史数据 (或加载本地缓存)
    2. 按时间顺序逐条推送给策略
    3. 策略产生订单 → 虚拟撮合
    4. 生成绩效报告

    数据说明:
    - 价格范围 [0, 1], 代表概率 (YES token 的价格)
    - 成交量单位: USDC
    - 市场结算: 到期后 YES token 价值为 1 或 0

    Usage:
        engine = BacktestEngine(config)
        # 自动从 Polymarket 拉取数据:
        engine.set_strategy(my_strategy)
        result = await engine.run()
        # 或手动加载数据:
        engine.load_data(kline_df)
        result = await engine.run()
    """

    def __init__(self, config: BacktestConfig | None = None) -> None:
        self.config = config or BacktestConfig()

        # 内部事件总线（回测独立）
        self._event_bus = EventBus()

        # 回测上下文
        self.context = Context(
            run_mode=RunMode.BACKTEST,
            trading_mode=TradingMode.AUTO,
            event_bus=self._event_bus,
        )
        self.context.account.balance = self.config.initial_balance
        self.context.account.initial_balance = self.config.initial_balance
        self.context.account.total_equity = self.config.initial_balance

        # 数据
        self._klines: pd.DataFrame = pd.DataFrame()
        self._orderbook_snapshots: pd.DataFrame = pd.DataFrame()

        # 策略
        self._strategy: Strategy | None = None

        # 交易记录
        self._orders: list[BacktestOrder] = []
        self._trades: list[BacktestTrade] = []
        self._equity_curve: list[dict] = []
        self._positions: dict[str, float] = {}  # side -> size

        # 状态
        self._current_time: float = 0.0
        self._bar_index: int = 0
        self._data_loaded: bool = False

    def load_data(
        self,
        klines: pd.DataFrame,
        orderbook: pd.DataFrame | None = None,
    ) -> None:
        """
        手动加载回测数据 (覆盖自动拉取)

        Args:
            klines: K线数据 (columns: timestamp, open, high, low, close, volume)
                    对于 Polymarket: 价格范围 [0, 1], 代表概率
            orderbook: 订单簿快照 (可选)
        """
        required_cols = {"timestamp", "open", "high", "low", "close", "volume"}
        if not required_cols.issubset(set(klines.columns)):
            raise ValueError(f"K线数据缺少必要列: {required_cols - set(klines.columns)}")

        self._klines = klines.sort_values("timestamp").reset_index(drop=True)
        if orderbook is not None:
            self._orderbook_snapshots = orderbook.sort_values("timestamp").reset_index(drop=True)

        self._data_loaded = True
        logger.info(f"手动加载 {len(self._klines)} 根 K 线, {len(self._orderbook_snapshots)} 条 OB 快照")

    async def fetch_polymarket_data(self) -> bool:
        """
        从 Polymarket 拉取历史数据。

        根据 config 中的 token_id / condition_id 自动拉取并缓存。

        Returns:
            是否成功获取数据
        """
        from src.market.datasources.polymarket_history import PolymarketHistoryFetcher

        if not self.config.token_id:
            logger.error("未设置 token_id, 无法从 Polymarket 拉取数据")
            return False

        fetcher = PolymarketHistoryFetcher(data_dir=self.config.data_dir)

        try:
            logger.info(
                f"从 Polymarket 拉取历史数据: "
                f"token={self.config.token_id[:16]}... "
                f"interval={self.config.data_interval} "
                f"range=[{self.config.start_date}, {self.config.end_date}]"
            )

            candles = await fetcher.fetch_candles_cached(
                token_id=self.config.token_id,
                interval=self.config.data_interval,
                start_date=self.config.start_date,
                end_date=self.config.end_date,
                condition_id=self.config.condition_id,
            )

            if candles.empty:
                logger.warning("Polymarket 无历史数据")
                return False

            # 确保必要列存在
            for col in ["timestamp", "open", "high", "low", "close", "volume"]:
                if col not in candles.columns:
                    if col == "volume":
                        candles[col] = 0.0
                    else:
                        logger.error(f"聚合数据缺少 '{col}' 列")
                        return False

            self._klines = candles.sort_values("timestamp").reset_index(drop=True)
            self._data_loaded = True

            logger.info(
                f"Polymarket 数据加载完成: {len(self._klines)} 根 K 线, "
                f"价格范围: [{self._klines['close'].min():.4f}, {self._klines['close'].max():.4f}]"
            )
            return True

        except Exception as e:
            logger.error(f"拉取 Polymarket 数据失败: {e}")
            return False
        finally:
            await fetcher.close()

    def set_strategy(self, strategy: Strategy) -> None:
        """设置回测策略"""
        self._strategy = strategy

    async def run(self) -> BacktestResult:
        """
        执行回测

        如果未手动 load_data(), 自动从 Polymarket 拉取历史数据。

        Returns:
            BacktestResult 绩效结果
        """
        if self._strategy is None:
            raise ValueError("No strategy set")

        # 自动拉取数据
        if not self._data_loaded:
            success = await self.fetch_polymarket_data()
            if not success:
                raise ValueError(
                    "无法获取 Polymarket 历史数据。"
                    "请确认 config 中设置了有效的 token_id, "
                    "或手动调用 load_data() 加载本地数据。"
                )

        if self._klines.empty:
            raise ValueError("No data loaded")

        logger.info(
            f"Backtest starting: {self.config.start_date} -> {self.config.end_date}, "
            f"balance={self.config.initial_balance} USDC, "
            f"data_source={self.config.data_source}, "
            f"bars={len(self._klines)}"
        )

        # 设置回测时间函数
        self.context.set_time_func(lambda: self._current_time)

        # 注册引擎到 context (供策略通过 context.get("backtest_engine") 访问)
        self.context.set("backtest_engine", self)

        # 初始化策略
        self._strategy.on_init(self.context)

        # 逐条推送数据
        for idx, row in self._klines.iterrows():
            self._bar_index = idx
            self._current_time = row["timestamp"]

            # 更新市场状态
            self.context.market.btc_price = row["close"]
            self.context.market.last_update = self._current_time

            # 构造 MarketData (Polymarket 概率 K 线)
            market_data = {
                "timestamp": row["timestamp"],
                "open": row["open"],
                "high": row["high"],
                "low": row["low"],
                "close": row["close"],
                "volume": row["volume"],
                "bar_index": idx,
                "history": self._klines.iloc[: idx + 1],
                # Polymarket 特有字段
                "source": self.config.data_source,
                "token_id": self.config.token_id,
                "condition_id": self.config.condition_id,
                "market_question": self.config.market_question,
                "probability": row["close"],  # close 即为当前概率
            }

            # 回调策略
            try:
                self._strategy.on_market_data(self.context, market_data)
            except Exception as e:
                logger.error(f"Strategy error at bar {idx}: {e}")

            # 处理策略产生的订单
            self._process_pending_orders(row)

            # 记录权益
            equity = self._calculate_equity(row["close"])
            self._equity_curve.append({
                "timestamp": self._current_time,
                "equity": equity,
                "balance": self.context.account.balance,
                "btc_price": row["close"],
            })

        # 策略结束
        logger.info(f"Backtest completed: {len(self._trades)} trades")

        return self._generate_result()

    def submit_order(self, side: OrderSide, size: float, price: float = 0.0) -> str:
        """
        提交回测订单 (策略调用)

        Args:
            side: YES or NO
            size: 下注金额 (USDC)
            price: 限价 (0 = 市价)

        Returns:
            订单 ID
        """
        if size > self.context.account.balance:
            logger.warning(f"Insufficient balance: {size} > {self.context.account.balance}")
            return ""

        if size > self.config.max_position_size:
            size = self.config.max_position_size

        order = BacktestOrder(
            side=side,
            price=price,
            size=size,
            timestamp=self._current_time,
            strategy_id=self._strategy.name() if self._strategy else "",
        )
        self._orders.append(order)
        return order.id

    def _process_pending_orders(self, bar: pd.Series) -> None:
        """处理挂单（模拟撮合）"""
        for order in self._orders:
            if order.status != OrderStatus.PENDING:
                continue

            # 模拟成交
            fill_price = self._simulate_fill_price(order, bar)
            if fill_price is None:
                continue

            # 计算手续费
            fee = order.size * self.config.fee_rate
            net_size = order.size - fee

            order.filled_size = order.size
            order.filled_price = fill_price
            order.fee = fee
            order.status = OrderStatus.FILLED
            order.fill_timestamp = self._current_time

            # 更新余额
            self.context.account.balance -= order.size

            # 记录成交
            trade = BacktestTrade(
                order_id=order.id,
                price=fill_price,
                size=net_size,
                fee=fee,
                pnl=0.0,  # PnL 在结算时计算
                timestamp=self._current_time,
                side=order.side,
            )
            self._trades.append(trade)

            # 更新持仓
            side_key = order.side.value
            self._positions[side_key] = self._positions.get(side_key, 0) + net_size

    def _simulate_fill_price(self, order: BacktestOrder, bar: pd.Series) -> float | None:
        """
        模拟成交价

        包含滑点模拟:
        - 市价单: close + 随机滑点
        - 限价单: 如果 bar 范围覆盖价格则成交
        """
        import random

        slippage = self.config.slippage_bps / 10000
        noise = random.uniform(0, slippage)

        if order.price == 0:  # 市价单
            # YES 买入: 价格略高; NO 买入: 价格略低
            base_price = bar["close"]
            fill_price = base_price * (1 + noise) if order.side == OrderSide.YES else base_price * (1 - noise)
            return max(0.01, min(0.99, fill_price))  # PM 价格范围 [0.01, 0.99]

        else:  # 限价单
            if order.side == OrderSide.YES and bar["low"] <= order.price:
                return order.price
            elif order.side == OrderSide.NO and bar["high"] >= order.price:
                return order.price
            return None  # 未触及限价

    def _calculate_equity(self, current_price: float) -> float:
        """计算当前权益"""
        position_value = 0.0
        for side, size in self._positions.items():
            if side == "YES":
                # YES token 价值 = 如果 BTC 上涨则值 1，否则 0
                # 简化：以当前 PM 价格估值
                position_value += size * current_price
            else:
                position_value += size * (1 - current_price)

        equity = self.context.account.balance + position_value
        self.context.account.total_equity = equity
        return equity

    def _generate_result(self) -> BacktestResult:
        """生成回测结果"""
        equity_df = pd.DataFrame(self._equity_curve)

        trades_data = [
            {
                "order_id": t.order_id,
                "side": t.side.value,
                "price": t.price,
                "size": t.size,
                "fee": t.fee,
                "pnl": t.pnl,
                "timestamp": t.timestamp,
            }
            for t in self._trades
        ]
        trades_df = pd.DataFrame(trades_data) if trades_data else pd.DataFrame()

        return BacktestResult(
            config=self.config,
            equity_curve=equity_df,
            trades=trades_df,
            orders=self._orders,
            final_equity=self.context.account.total_equity,
            initial_balance=self.config.initial_balance,
        )


@dataclass
class BacktestResult:
    """回测结果"""
    config: BacktestConfig
    equity_curve: pd.DataFrame
    trades: pd.DataFrame
    orders: list[BacktestOrder]
    final_equity: float
    initial_balance: float

    @property
    def total_return(self) -> float:
        """总收益率"""
        return (self.final_equity - self.initial_balance) / self.initial_balance

    @property
    def total_pnl(self) -> float:
        """总盈亏"""
        return self.final_equity - self.initial_balance

    @property
    def trade_count(self) -> int:
        return len(self.trades) if not self.trades.empty else 0

    @property
    def win_rate(self) -> float:
        """胜率"""
        if self.trades.empty or "pnl" not in self.trades.columns:
            return 0.0
        wins = (self.trades["pnl"] > 0).sum()
        total = len(self.trades)
        return wins / total if total > 0 else 0.0

    @property
    def max_drawdown(self) -> float:
        """最大回撤"""
        if self.equity_curve.empty:
            return 0.0
        equity = self.equity_curve["equity"]
        peak = equity.expanding().max()
        drawdown = (equity - peak) / peak
        return drawdown.min()

    @property
    def sharpe_ratio(self) -> float:
        """夏普比率 (假设无风险利率为 0)"""
        if self.equity_curve.empty or len(self.equity_curve) < 2:
            return 0.0
        returns = self.equity_curve["equity"].pct_change().dropna()
        if returns.std() == 0:
            return 0.0
        # 年化：假设每 5 分钟一条，一天 288 条
        return returns.mean() / returns.std() * (288 * 365) ** 0.5

    @property
    def sortino_ratio(self) -> float:
        """索提诺比率"""
        if self.equity_curve.empty or len(self.equity_curve) < 2:
            return 0.0
        returns = self.equity_curve["equity"].pct_change().dropna()
        downside = returns[returns < 0]
        if downside.empty or downside.std() == 0:
            return float("inf") if returns.mean() > 0 else 0.0
        return returns.mean() / downside.std() * (288 * 365) ** 0.5

    @property
    def profit_factor(self) -> float:
        """盈亏比"""
        if self.trades.empty or "pnl" not in self.trades.columns:
            return 0.0
        gross_profit = self.trades.loc[self.trades["pnl"] > 0, "pnl"].sum()
        gross_loss = abs(self.trades.loc[self.trades["pnl"] < 0, "pnl"].sum())
        return gross_profit / gross_loss if gross_loss > 0 else float("inf")

    def summary(self) -> dict[str, Any]:
        """生成摘要"""
        return {
            "total_return": f"{self.total_return:.2%}",
            "total_pnl": f"{self.total_pnl:.2f} USDC",
            "trade_count": self.trade_count,
            "win_rate": f"{self.win_rate:.2%}",
            "max_drawdown": f"{self.max_drawdown:.2%}",
            "sharpe_ratio": f"{self.sharpe_ratio:.2f}",
            "sortino_ratio": f"{self.sortino_ratio:.2f}",
            "profit_factor": f"{self.profit_factor:.2f}",
            "final_equity": f"{self.final_equity:.2f} USDC",
        }
