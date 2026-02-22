"""
BN-RTDS Spread Lead-Lag Strategy v1.0

核心思路:
    Binance BTC/USDT 是全球流动性最深的 BTC 现货交易所,
    价格反应速度领先于 Chainlink RTDS (PM 结算源) 约 0.5~3 秒。
    捕捉 BN-RTDS 差价的快速偏离与回归，产生交易信号：

    1) spread = BN_price - RTDS_price
    2) 计算 spread 的 fast EMA 和 slow EMA (均值)
    3) deviation = fast_ema - slow_ema
       - deviation 快速上升 → BN 领先上涨 → RTDS 将跟随 → 买 UP (逢低买入)
       - deviation 回归零值 → 价差回落 → 买 DOWN (逢高卖出, 平衡持仓)
       - deviation 快速下降 → BN 领先下跌 → RTDS 将跟随 → 买 DOWN
       - deviation 回归零值 → 价差回落 → 买 UP (平衡持仓)

    交易方向用 UP/DOWN 的持仓差作为权重:
       - 目标: 5-min 窗口结束时 UP/DOWN shares 大致均衡
       - 持仓差越大, 落后方的下单量权重越高

    按波动剧烈程度分段:
       - 小偏离: 基础仓位
       - 中偏离: 1.5x
       - 大偏离: 2x

手续费模型 (Polymarket):
    - Taker fee: 0.2% (按成交 USDC 计)
    - 持有至结算只付入场 1 次
"""

from __future__ import annotations

import asyncio
import math
import time
from collections import deque
from typing import Any

from loguru import logger

from src.core.context import Context
from src.core.event_bus import Event, EventType
from src.strategy.base import Strategy
from src.trading.executor import ExecutionStatus, OrderRequest, OrderResult, OrderType, Side


class BnRtdsSpreadStrategy(Strategy):
    """
    BN-RTDS Spread Lead-Lag Strategy v1.0

    利用 Binance (领先) 与 RTDS/Chainlink (滞后) 的价差信号
    驱动 Polymarket BTC 5-min Up/Down 双边下注。
    """

    def __init__(
        self,
        # ── 核心仓位 ──
        target_shares_per_side: float = 100.0,
        shares_per_order: float = 10.0,
        max_combined_cost: float = 500.0,
        # ── Spread 计算 ──
        spread_fast_halflife_s: float = 3.0,      # fast EMA 半衰期 (秒)
        spread_slow_halflife_s: float = 30.0,      # slow EMA 半衰期 (均值基准)
        spread_buffer_size: int = 1200,            # 原始 spread 缓冲区大小
        # ── 信号阈值 ──
        open_threshold: float = 2.0,               # 偏离触发阈值 ($)
        revert_threshold: float = 0.5,             # 回归触发阈值 ($)
        strong_deviation: float = 5.0,             # 强偏离 (加量)
        extreme_deviation: float = 8.0,            # 极端偏离 (最大加量)
        # ── 分段倍率 ──
        stage_multipliers: list[float] | None = None,  # [1.0, 1.5, 2.0] 对应小/中/大偏离
        # ── 冷却 ──
        signal_cooldown_s: float = 1.0,            # 同方向信号最小间隔
        burst_cooldown_s: float = 0.3,             # burst 内订单间隔
        post_trade_pause_s: float = 2.0,           # 交易后暂停
        # ── 时间窗口 ──
        entry_delay_s: float = 9.0,                # 窗口开始后延迟
        entry_cutoff_s: float = 15.0,              # 窗口结束前停止
        # ── Gap 平衡 ──
        gap_weight_factor: float = 0.8,            # 持仓差修正强度
        # ── 流动性 ──
        min_depth: float = 50.0,
        max_spread: float = 0.20,
        # ── 出场 ──
        hold_to_settlement: bool = True,
        # ── 手续费 ──
        fee_rate: float = 0.002,
    ) -> None:
        # 核心
        self._target_shares_per_side = target_shares_per_side
        self._shares_per_order = shares_per_order
        self._max_combined_cost = max_combined_cost

        # Spread 计算
        self._spread_fast_halflife = spread_fast_halflife_s
        self._spread_slow_halflife = spread_slow_halflife_s
        self._spread_buffer_size = spread_buffer_size

        # 信号阈值
        self._open_threshold = open_threshold
        self._revert_threshold = revert_threshold
        self._strong_deviation = strong_deviation
        self._extreme_deviation = extreme_deviation

        # 分段倍率
        self._stage_multipliers = stage_multipliers or [1.0, 1.5, 2.0]

        # 冷却
        self._signal_cooldown_s = signal_cooldown_s
        self._burst_cooldown_s = burst_cooldown_s
        self._post_trade_pause_s = post_trade_pause_s

        # 时间窗口
        self._entry_delay_s = entry_delay_s
        self._entry_cutoff_s = entry_cutoff_s

        # Gap 平衡
        self._gap_weight_factor = gap_weight_factor

        # 流动性
        self._min_depth = min_depth
        self._max_spread = max_spread

        # 出场
        self._hold_to_settlement = hold_to_settlement

        # 手续费
        self._fee_rate = fee_rate

        # ── Spread 跟踪 ──
        self._spread_buffer: deque[tuple[float, float]] = deque(
            maxlen=self._spread_buffer_size
        )
        self._spread_ema_fast: float = 0.0
        self._spread_ema_slow: float = 0.0
        self._spread_ema_initialized: bool = False
        self._last_spread_ts: float = 0.0

        # ── 信号状态机 ──
        # IDLE: 等待偏离
        # DIVERGED_UP: BN 领先上涨, 已买 UP, 等待回归买 DOWN
        # DIVERGED_DOWN: BN 领先下跌, 已买 DOWN, 等待回归买 UP
        self._signal_state: str = "IDLE"
        self._diverge_peak: float = 0.0       # 偏离峰值 (用于分段追踪)
        self._last_stage_level: int = 0       # 上次触发的分段等级

        # ── 冷却时间跟踪 ──
        self._last_trade_time: float = 0.0
        self._last_signal_time: float = 0.0

        # ── 双边 shares 跟踪 ──
        self._cum_up_shares: float = 0.0
        self._cum_dn_shares: float = 0.0
        self._cum_up_cost: float = 0.0
        self._cum_dn_cost: float = 0.0

        # ── 持仓列表 ──
        self._positions: list[_Position] = []

        # ── 窗口状态 ──
        self._last_window_ts: int = 0
        self._entries_this_window: int = 0
        self._window_ptb: float = 0.0

        # ── 统计 ──
        self._trade_count: int = 0
        self._win_count: int = 0
        self._loss_count: int = 0
        self._cumulative_pnl: float = 0.0
        self._trade_history: list[dict] = []
        self._max_history: int = 200
        self._diverge_trades_this_window: int = 0
        self._revert_trades_this_window: int = 0

        # ── 链上兑付 ──
        self._pending_redeems: list[dict] = []
        self._redeemer = None
        self._redeem_check_interval: float = 30.0
        self._last_redeem_check_ts: float = 0.0
        self._max_redeem_retries: int = 20
        self._redeem_results: list[dict] = []

        # ── tick 计数器 ──
        self._tick_counter: int = 0

        # ── 防重入锁 ──
        self._in_trade: bool = False

    # ================================================================
    #  Strategy 接口
    # ================================================================

    def name(self) -> str:
        return "bn_rtds_spread"

    def version(self) -> str:
        return "1.0"

    def description(self) -> str:
        return (
            f"BN-RTDS Spread Lead-Lag v1.0 ("
            f"target={self._target_shares_per_side}sh/side, "
            f"open_thr=${self._open_threshold}, "
            f"revert_thr=${self._revert_threshold}, "
            f"budget=${self._max_combined_cost})"
        )

    def get_params(self) -> dict[str, Any]:
        return {
            "target_shares_per_side": self._target_shares_per_side,
            "shares_per_order": self._shares_per_order,
            "max_combined_cost": self._max_combined_cost,
            "spread_fast_halflife_s": self._spread_fast_halflife,
            "spread_slow_halflife_s": self._spread_slow_halflife,
            "open_threshold": self._open_threshold,
            "revert_threshold": self._revert_threshold,
            "strong_deviation": self._strong_deviation,
            "extreme_deviation": self._extreme_deviation,
            "stage_multipliers": self._stage_multipliers,
            "signal_cooldown_s": self._signal_cooldown_s,
            "entry_delay_s": self._entry_delay_s,
            "entry_cutoff_s": self._entry_cutoff_s,
            "gap_weight_factor": self._gap_weight_factor,
            "min_depth": self._min_depth,
            "max_spread": self._max_spread,
            "hold_to_settlement": self._hold_to_settlement,
            "fee_rate": self._fee_rate,
        }

    def on_init(self, context: Context) -> None:
        logger.info(f"[{self.name()}] 策略初始化: {self.description()}")
        logger.info(f"[{self.name()}] 参数: {self.get_params()}")

    async def on_market_data(self, context: Context, data: dict[str, Any]) -> None:
        """主驱动入口 — 每个 RTDS tick 调用一次。"""
        # ── 0. 窗口切换检测 ──
        await self._check_window_switch(context)

        # ── 0.1. 待兑付 ──
        await self._process_pending_redeems(context)

        # ── 1. 读取双价格源 ──
        rtds_price = context.market.btc_price
        bn_price = getattr(context.market, "binance_price", 0.0)
        now = context.now()

        if rtds_price <= 0 or bn_price <= 0:
            return  # 数据不完整, 跳过

        # ── 2. 计算并缓存 spread ──
        spread = bn_price - rtds_price
        self._spread_buffer.append((now, spread))
        self._update_spread_ema(spread, now)

        # ── 3. 诊断日志 (每 30 tick) ──
        self._tick_counter += 1
        if self._tick_counter % 30 == 1:
            self._log_status(context, bn_price, rtds_price, spread)

        # ── 4. 信号驱动交易 ──
        await self._spread_signal_entry(context, spread, now)

        # ── 5. Dashboard ──
        self._publish_state(context, bn_price, spread)

    def on_order_book(self, context: Context, book: dict[str, Any]) -> None:
        pass

    def on_trade(self, context: Context, trade: dict[str, Any]) -> None:
        pass

    def on_timer(self, context: Context, timer_id: str) -> None:
        pass

    def on_stop(self, context: Context) -> None:
        total_cost = self._cum_up_cost + self._cum_dn_cost
        gap = self._cum_up_shares - self._cum_dn_shares
        logger.info(
            f"[{self.name()}] 策略停止 | "
            f"总交易={self._trade_count} | "
            f"UP={self._cum_up_shares:.1f}sh DN={self._cum_dn_shares:.1f}sh "
            f"gap={gap:+.1f} | cost=${total_cost:.2f} | "
            f"PnL={self._cumulative_pnl:+.4f}"
        )

    # ================================================================
    #  Spread EMA 计算
    # ================================================================

    def _update_spread_ema(self, spread: float, now: float) -> None:
        """
        更新 spread 的 fast/slow 指数移动平均。

        使用时间加权 EMA:
            alpha = 1 - exp(-dt / halflife)
            ema = alpha * value + (1 - alpha) * ema
        """
        if not self._spread_ema_initialized:
            self._spread_ema_fast = spread
            self._spread_ema_slow = spread
            self._spread_ema_initialized = True
            self._last_spread_ts = now
            return

        dt = max(now - self._last_spread_ts, 0.001)
        self._last_spread_ts = now

        # fast EMA
        alpha_fast = 1.0 - math.exp(-dt / max(self._spread_fast_halflife, 0.1))
        self._spread_ema_fast = (
            alpha_fast * spread + (1.0 - alpha_fast) * self._spread_ema_fast
        )

        # slow EMA (均值基准)
        alpha_slow = 1.0 - math.exp(-dt / max(self._spread_slow_halflife, 1.0))
        self._spread_ema_slow = (
            alpha_slow * spread + (1.0 - alpha_slow) * self._spread_ema_slow
        )

    @property
    def _deviation(self) -> float:
        """当前偏离值: fast_ema - slow_ema"""
        return self._spread_ema_fast - self._spread_ema_slow

    # ================================================================
    #  诊断日志
    # ================================================================

    def _log_status(
        self,
        ctx: Context,
        bn_price: float,
        rtds_price: float,
        spread: float,
    ) -> None:
        secs_left = ctx.market.pm_window_seconds_left
        dev = self._deviation
        gap = self._cum_up_shares - self._cum_dn_shares
        total = max(self._cum_up_shares, self._cum_dn_shares, 1)
        gap_pct = abs(gap) / total * 100

        logger.info(
            f"[{self.name()}] "
            f"BN=${bn_price:,.2f} RTDS=${rtds_price:,.2f} "
            f"spread=${spread:+.2f} | "
            f"EMA fast={self._spread_ema_fast:+.2f} "
            f"slow={self._spread_ema_slow:+.2f} "
            f"dev={dev:+.2f} | "
            f"state={self._signal_state} | "
            f"UP={self._cum_up_shares:.1f}sh "
            f"DN={self._cum_dn_shares:.1f}sh "
            f"gap={gap:+.1f}({gap_pct:.0f}%) | "
            f"#{self._entries_this_window} | "
            f"secs_left={secs_left:.0f}"
        )

    # ================================================================
    #  窗口切换
    # ================================================================

    async def _check_window_switch(self, context: Context) -> None:
        wst = context.market.pm_window_start_ts
        if wst > 0 and wst != self._last_window_ts:
            if self._last_window_ts > 0:
                await self._settle_position(context)
                logger.info(
                    f"[{self.name()}] 窗口切换 "
                    f"old_ts={self._last_window_ts} -> new_ts={wst}"
                )
            self._last_window_ts = wst
            self._entries_this_window = 0
            self._cum_up_shares = 0.0
            self._cum_dn_shares = 0.0
            self._cum_up_cost = 0.0
            self._cum_dn_cost = 0.0
            self._window_ptb = context.market.btc_price
            self._signal_state = "IDLE"
            self._diverge_peak = 0.0
            self._last_stage_level = 0
            self._diverge_trades_this_window = 0
            self._revert_trades_this_window = 0
            # 重置 spread EMA (新窗口重新收集)
            self._spread_ema_initialized = False
            logger.info(
                f"[{self.name()}] 新窗口 PTB: ${self._window_ptb:,.2f}"
            )

    # ================================================================
    #  核心: Spread 信号驱动交易
    # ================================================================

    async def _spread_signal_entry(
        self,
        ctx: Context,
        spread: float,
        now: float,
    ) -> None:
        """
        Spread 信号状态机驱动交易。

        状态转移:
          IDLE
            → deviation > open_threshold  → 买 UP  → DIVERGED_UP
            → deviation < -open_threshold → 买 DOWN → DIVERGED_DOWN

          DIVERGED_UP
            → deviation 继续扩大 → 分段追加买 UP
            → |deviation| < revert_threshold → 买 DOWN → IDLE
            → deviation < -open_threshold → 反向切换 → 买 DOWN → DIVERGED_DOWN

          DIVERGED_DOWN
            → deviation 继续扩大 → 分段追加买 DOWN
            → |deviation| < revert_threshold → 买 UP → IDLE
            → deviation > open_threshold → 反向切换 → 买 UP → DIVERGED_UP
        """
        secs_left = ctx.market.pm_window_seconds_left
        if secs_left <= 0:
            return

        # 防重入
        if self._in_trade:
            return

        # 时间窗口检查
        window_elapsed = 300 - secs_left
        if window_elapsed < self._entry_delay_s:
            return
        if secs_left < self._entry_cutoff_s:
            return

        # EMA 尚未稳定 (至少需要一个 slow halflife 的数据)
        if not self._spread_ema_initialized:
            return
        if len(self._spread_buffer) < 5:
            return

        # 注: target_shares_per_side 仅作为进度参考, 不限制交易

        # 预算耗尽
        total_invested = self._cum_up_cost + self._cum_dn_cost
        remaining_budget = self._max_combined_cost - total_invested
        if remaining_budget < 2.0:
            return

        # 交易后冷却 (已取消)
        # if self._last_trade_time > 0:
        #     since_trade = now - self._last_trade_time
        #     if since_trade < self._post_trade_pause_s:
        #         return

        dev = self._deviation
        abs_dev = abs(dev)

        # ── 状态机 ──

        if self._signal_state == "IDLE":
            # 等待偏离信号
            if abs_dev >= self._open_threshold:
                if dev > 0:
                    # BN 领先上涨 → 买 UP (逢低买入: RTDS 还没跟上)
                    direction = "UP"
                    self._signal_state = "DIVERGED_UP"
                    self._diverge_peak = dev
                    self._last_stage_level = 0
                else:
                    # BN 领先下跌 → 买 DOWN
                    direction = "DOWN"
                    self._signal_state = "DIVERGED_DOWN"
                    self._diverge_peak = dev
                    self._last_stage_level = 0

                stage_mult = self._get_stage_multiplier(abs_dev)
                order_shares = self._calc_order_shares(
                    direction, stage_mult, secs_left, remaining_budget
                )

                if order_shares >= 0.1:
                    logger.info(
                        f"[{self.name()}] 📊 偏离信号 | "
                        f"dev={dev:+.2f} → {direction} | "
                        f"stage_mult={stage_mult:.1f} "
                        f"shares={order_shares:.1f} | "
                        f"state→{self._signal_state}"
                    )
                    await self._execute_trade(
                        ctx, direction, order_shares, dev, secs_left,
                        remaining_budget, "DIVERGE"
                    )

        elif self._signal_state == "DIVERGED_UP":
            # BN 领先上涨中...
            if abs_dev < self._revert_threshold:
                # 回归均值 → 买 DOWN (逢高卖出/平衡持仓)
                stage_mult = 1.0
                order_shares = self._calc_order_shares(
                    "DOWN", stage_mult, secs_left, remaining_budget
                )
                if order_shares >= 0.1:
                    logger.info(
                        f"[{self.name()}] 🔄 回归信号 | "
                        f"dev={dev:+.2f} → DOWN (回归) | "
                        f"shares={order_shares:.1f} | state→IDLE"
                    )
                    await self._execute_trade(
                        ctx, "DOWN", order_shares, dev, secs_left,
                        remaining_budget, "REVERT"
                    )
                self._signal_state = "IDLE"
                self._diverge_peak = 0.0
                self._last_stage_level = 0

            elif dev < -self._open_threshold:
                # 反向切换: 从 UP 偏离直接转为 DOWN 偏离
                stage_mult = self._get_stage_multiplier(abs_dev)
                order_shares = self._calc_order_shares(
                    "DOWN", stage_mult, secs_left, remaining_budget
                )
                if order_shares >= 0.1:
                    logger.info(
                        f"[{self.name()}] ⚡ 反向切换 | "
                        f"dev={dev:+.2f} DIVERGED_UP→DIVERGED_DOWN | "
                        f"mult={stage_mult:.1f} shares={order_shares:.1f}"
                    )
                    await self._execute_trade(
                        ctx, "DOWN", order_shares, dev, secs_left,
                        remaining_budget, "REVERSE"
                    )
                self._signal_state = "DIVERGED_DOWN"
                self._diverge_peak = dev
                self._last_stage_level = self._get_current_stage(abs_dev)

            elif dev > self._diverge_peak:
                # 偏离继续扩大 → 检查是否跨越新分段
                new_stage = self._get_current_stage(abs_dev)
                if new_stage > self._last_stage_level:
                    stage_mult = self._get_stage_multiplier(abs_dev)
                    order_shares = self._calc_order_shares(
                        "UP", stage_mult, secs_left, remaining_budget
                    )
                    if order_shares >= 0.1:
                        logger.info(
                            f"[{self.name()}] 📈 追加偏离 | "
                            f"dev={dev:+.2f} → UP (stage {new_stage}) | "
                            f"mult={stage_mult:.1f} shares={order_shares:.1f}"
                        )
                        await self._execute_trade(
                            ctx, "UP", order_shares, dev, secs_left,
                            remaining_budget, "DIVERGE_ADD"
                        )
                    self._last_stage_level = new_stage
                self._diverge_peak = dev

        elif self._signal_state == "DIVERGED_DOWN":
            # BN 领先下跌中...
            if abs_dev < self._revert_threshold:
                # 回归均值 → 买 UP (平衡持仓)
                stage_mult = 1.0
                order_shares = self._calc_order_shares(
                    "UP", stage_mult, secs_left, remaining_budget
                )
                if order_shares >= 0.1:
                    logger.info(
                        f"[{self.name()}] 🔄 回归信号 | "
                        f"dev={dev:+.2f} → UP (回归) | "
                        f"shares={order_shares:.1f} | state→IDLE"
                    )
                    await self._execute_trade(
                        ctx, "UP", order_shares, dev, secs_left,
                        remaining_budget, "REVERT"
                    )
                self._signal_state = "IDLE"
                self._diverge_peak = 0.0
                self._last_stage_level = 0

            elif dev > self._open_threshold:
                # 反向切换: 从 DOWN 偏离直接转为 UP 偏离
                stage_mult = self._get_stage_multiplier(abs_dev)
                order_shares = self._calc_order_shares(
                    "UP", stage_mult, secs_left, remaining_budget
                )
                if order_shares >= 0.1:
                    logger.info(
                        f"[{self.name()}] ⚡ 反向切换 | "
                        f"dev={dev:+.2f} DIVERGED_DOWN→DIVERGED_UP | "
                        f"mult={stage_mult:.1f} shares={order_shares:.1f}"
                    )
                    await self._execute_trade(
                        ctx, "UP", order_shares, dev, secs_left,
                        remaining_budget, "REVERSE"
                    )
                self._signal_state = "DIVERGED_UP"
                self._diverge_peak = dev
                self._last_stage_level = self._get_current_stage(abs_dev)

            elif dev < self._diverge_peak:
                # 偏离继续扩大 (dev 更负)
                new_stage = self._get_current_stage(abs_dev)
                if new_stage > self._last_stage_level:
                    stage_mult = self._get_stage_multiplier(abs_dev)
                    order_shares = self._calc_order_shares(
                        "DOWN", stage_mult, secs_left, remaining_budget
                    )
                    if order_shares >= 0.1:
                        logger.info(
                            f"[{self.name()}] 📉 追加偏离 | "
                            f"dev={dev:+.2f} → DOWN (stage {new_stage}) | "
                            f"mult={stage_mult:.1f} shares={order_shares:.1f}"
                        )
                        await self._execute_trade(
                            ctx, "DOWN", order_shares, dev, secs_left,
                            remaining_budget, "DIVERGE_ADD"
                        )
                    self._last_stage_level = new_stage
                self._diverge_peak = dev

    # ================================================================
    #  分段逻辑
    # ================================================================

    def _get_current_stage(self, abs_deviation: float) -> int:
        """
        根据偏离绝对值计算当前分段等级。

        等级:
            0: |dev| < open_threshold (无信号)
            1: open_threshold <= |dev| < strong_deviation
            2: strong_deviation <= |dev| < extreme_deviation
            3: |dev| >= extreme_deviation
        """
        if abs_deviation >= self._extreme_deviation:
            return 3
        elif abs_deviation >= self._strong_deviation:
            return 2
        elif abs_deviation >= self._open_threshold:
            return 1
        return 0

    def _get_stage_multiplier(self, abs_deviation: float) -> float:
        """
        根据偏离绝对值返回下单倍率。

        映射 stage → multiplier:
            stage 1: multipliers[0] (默认 1.0)
            stage 2: multipliers[1] (默认 1.5)
            stage 3: multipliers[2] (默认 2.0)
        """
        stage = self._get_current_stage(abs_deviation)
        if stage <= 0:
            return 0.0
        idx = min(stage - 1, len(self._stage_multipliers) - 1)
        return self._stage_multipliers[idx]

    # ================================================================
    #  下单量计算 (含 gap 平衡权重)
    # ================================================================

    def _calc_order_shares(
        self,
        direction: str,
        stage_multiplier: float,
        secs_left: float,
        remaining_budget: float,
    ) -> float:
        """
        计算下单 shares 数, 考虑:
            1. 基础量 × 分段倍率
            2. Gap 权重: 落后方加量, 领先方减量
            3. 窗口尾部缩量
            4. 剩余预算限制
        """
        base = self._shares_per_order * stage_multiplier

        # ── Gap 权重 ──
        gap = self._cum_up_shares - self._cum_dn_shares
        total = max(self._cum_up_shares, self._cum_dn_shares, 1.0)
        gap_ratio = gap / total  # 正=UP多, 负=DN多

        if direction == "UP":
            if gap_ratio > 0:
                # UP 已多, 买 UP 减量
                base *= max(0.3, 1.0 - abs(gap_ratio) * self._gap_weight_factor)
            else:
                # DN 多, 买 UP 加量 (补差)
                base *= min(2.0, 1.0 + abs(gap_ratio) * self._gap_weight_factor)
        else:  # DOWN
            if gap_ratio < 0:
                # DN 已多, 买 DN 减量
                base *= max(0.3, 1.0 - abs(gap_ratio) * self._gap_weight_factor)
            else:
                # UP 多, 买 DN 加量 (补差)
                base *= min(2.0, 1.0 + abs(gap_ratio) * self._gap_weight_factor)

        # ── 窗口尾部缩量 (精细平衡) ──
        if secs_left < 45:
            base = min(base, max(1.0, self._shares_per_order * 0.3))

        # 注: 不设 shares 上限, 仅受 max_combined_cost 预算约束

        return max(0, base)

    # ================================================================
    #  执行交易
    # ================================================================

    async def _execute_trade(
        self,
        ctx: Context,
        direction: str,
        order_shares: float,
        deviation: float,
        secs_left: float,
        remaining_budget: float,
        signal_type: str,
    ) -> bool:
        """执行一笔交易, 成功返回 True。"""
        self._in_trade = True
        try:
            return await self._execute_single_order(
                ctx, direction, order_shares, deviation, secs_left,
                remaining_budget, signal_type
            )
        finally:
            self._in_trade = False

    async def _execute_single_order(
        self,
        ctx: Context,
        direction: str,
        order_shares: float,
        deviation: float,
        secs_left: float,
        remaining_budget: float,
        signal_type: str,
    ) -> bool:
        """执行一笔订单。"""
        order_side = Side.YES if direction == "UP" else Side.NO

        ask = self._get_ask_price(ctx, direction)
        if ask < 0.01 or ask > 0.99:
            return False

        cost = order_shares * ask

        # 限制成本
        if cost > remaining_budget:
            cost = remaining_budget
            order_shares = cost / ask
        if cost > ctx.account.available:
            cost = ctx.account.available
            order_shares = cost / ask
        if cost < 1.05:
            return False

        # 流动性检查
        if not self._check_liquidity(ctx, direction):
            return False

        # edge 计算
        other_dir = "DOWN" if direction == "UP" else "UP"
        other_ask = self._get_ask_price(ctx, other_dir)
        combined_price = ask + other_ask if 0.01 <= other_ask <= 0.99 else 1.0
        edge = 1.0 - combined_price

        gap = self._cum_up_shares - self._cum_dn_shares
        total = max(self._cum_up_shares, self._cum_dn_shares, 1)
        gap_pct = gap / total * 100

        now = ctx.now()

        logger.info(
            f"[{self.name()}] #{self._entries_this_window+1} "
            f"→ {direction} ({signal_type}) | "
            f"{order_shares:.1f}sh@{ask:.4f}=${cost:.2f} | "
            f"dev={deviation:+.2f} gap={gap:+.1f}sh ({gap_pct:+.1f}%) | "
            f"edge={edge:+.4f}"
        )

        result = await self._submit_order(ctx, order_side, ask, cost)

        if result and result.status == ExecutionStatus.FILLED:
            filled_shares = result.filled_size
            actual_cost = filled_shares * ask
            cid = ctx.market.pm_condition_id

            self._positions.append(_Position(
                side=direction,
                entry_price=ask,
                size=actual_cost,
                shares=filled_shares,
                entry_time=now,
                entry_score=deviation,
                condition_id=cid,
                signal_type=signal_type,
            ))

            if direction == "UP":
                self._cum_up_shares += filled_shares
                self._cum_up_cost += actual_cost
            else:
                self._cum_dn_shares += filled_shares
                self._cum_dn_cost += actual_cost

            ctx.account.available -= actual_cost
            self._trade_count += 1
            self._entries_this_window += 1
            self._last_trade_time = now

            if signal_type.startswith("DIVERGE"):
                self._diverge_trades_this_window += 1
            else:
                self._revert_trades_this_window += 1

            logger.info(
                f"[{self.name()}] #{self._entries_this_window} 成交 | "
                f"{direction}={filled_shares:.1f}sh@{ask:.4f}=${actual_cost:.2f} | "
                f"CumUP={self._cum_up_shares:.1f} CumDN={self._cum_dn_shares:.1f}"
            )

            self._record_trade(ctx, {
                "action": "ENTRY",
                "signal_type": signal_type,
                "side": direction,
                "entry_num": self._entries_this_window,
                "price": round(ask, 4),
                "cost": round(actual_cost, 2),
                "shares": round(filled_shares, 2),
                "size": round(actual_cost, 2),
                "deviation": round(deviation, 2),
                "spread_fast": round(self._spread_ema_fast, 2),
                "spread_slow": round(self._spread_ema_slow, 2),
                "edge": round(edge, 4),
                "gap_shares": round(self._cum_up_shares - self._cum_dn_shares, 2),
                "cum_up_shares": round(self._cum_up_shares, 2),
                "cum_dn_shares": round(self._cum_dn_shares, 2),
                "secs_left": round(secs_left),
            })
            return True

        else:
            error = result.error if result else "no result"
            logger.warning(
                f"[{self.name()}] {direction} 未成交: {error}"
            )
            return False

    # ================================================================
    #  辅助方法
    # ================================================================

    def _check_liquidity(self, ctx: Context, direction: str) -> bool:
        ob_key = f"orderbook_{'up' if direction == 'UP' else 'down'}"
        ob = ctx.get(ob_key)
        if ob is not None:
            state = ob.get_state()
            ask_depth = state.total_ask_size(10)
            if ask_depth < self._min_depth:
                logger.debug(
                    f"[{self.name()}] [SKIP] {direction} depth "
                    f"{ask_depth:.0f} < {self._min_depth}"
                )
                return False
            if state.spread_pct > self._max_spread:
                logger.debug(
                    f"[{self.name()}] [SKIP] {direction} spread "
                    f"{state.spread_pct:.2%} > {self._max_spread:.2%}"
                )
                return False
        return True

    def _get_ask_price(self, ctx: Context, direction: str) -> float:
        ob_key = f"orderbook_{'up' if direction == 'UP' else 'down'}"
        ob = ctx.get(ob_key)
        if ob is not None:
            state = ob.get_state()
            if state.best_ask and state.best_ask > 0:
                return state.best_ask
        if direction == "UP":
            return ctx.market.pm_yes_price
        else:
            return ctx.market.pm_no_price

    # ================================================================
    #  窗口结算
    # ================================================================

    async def _settle_position(self, ctx: Context) -> None:
        if not self._positions:
            return

        btc = ctx.market.btc_price
        ptb = (
            self._window_ptb
            if self._window_ptb > 0
            else ctx.market.pm_window_start_price
        )

        if ptb <= 0:
            logger.warning(f"[{self.name()}] 结算时 PTB 不可用")
            self._positions.clear()
            return

        settle_condition_id = ""
        for p in self._positions:
            if p.condition_id:
                settle_condition_id = p.condition_id
                break

        btc_up = btc > ptb
        winner_side = "UP" if btc_up else "DOWN"

        up_shares = self._cum_up_shares
        dn_shares = self._cum_dn_shares
        up_cost = self._cum_up_cost
        dn_cost = self._cum_dn_cost
        total_cost = up_cost + dn_cost

        payout = up_shares * 1.0 if winner_side == "UP" else dn_shares * 1.0

        fee = total_cost * self._fee_rate
        net_pnl = payout - total_cost - fee

        actual_edge = 0.0
        if up_shares > 0 and dn_shares > 0:
            avg_up_price = up_cost / up_shares
            avg_dn_price = dn_cost / dn_shares
            actual_edge = 1.0 - (avg_up_price + avg_dn_price)

        gap = up_shares - dn_shares
        gap_pct = abs(gap) / max(up_shares, dn_shares, 1) * 100

        won = net_pnl >= 0
        result_str = "WIN" if won else "LOSE"

        logger.info(
            f"[{self.name()}] 结算 {result_str} | "
            f"赢家={winner_side} | BTC={btc:,.2f} vs PTB={ptb:,.2f} | "
            f"UP: {up_shares:.1f}sh/${up_cost:.2f} | "
            f"DN: {dn_shares:.1f}sh/${dn_cost:.2f} | "
            f"Gap={gap:+.1f}sh ({gap_pct:.1f}%) | "
            f"Payout=${payout:.2f} Cost=${total_cost:.2f} Fee=${fee:.4f} | "
            f"Edge={actual_edge:+.4f} | "
            f"Net PnL={net_pnl:+.4f} USDC | "
            f"Diverge={self._diverge_trades_this_window} "
            f"Revert={self._revert_trades_this_window}"
        )

        ctx.account.balance += net_pnl
        ctx.account.available += total_cost + net_pnl
        ctx.account.total_pnl += net_pnl
        ctx.account.daily_pnl += net_pnl
        ctx.account.total_equity = ctx.account.balance
        self._cumulative_pnl += net_pnl
        if won:
            self._win_count += 1
        else:
            self._loss_count += 1

        self._record_trade(ctx, {
            "action": "SETTLE",
            "winner": winner_side,
            "up_shares": round(up_shares, 2),
            "dn_shares": round(dn_shares, 2),
            "up_cost": round(up_cost, 2),
            "dn_cost": round(dn_cost, 2),
            "gap_shares": round(gap, 2),
            "gap_pct": round(gap_pct, 1),
            "payout": round(payout, 4),
            "size": round(total_cost, 2),
            "pnl": round(net_pnl, 4),
            "fee": round(fee, 4),
            "edge": round(actual_edge, 4),
            "result": result_str,
            "btc": round(btc, 2),
            "ptb": round(ptb, 2),
            "entries": self._entries_this_window,
            "diverge_trades": self._diverge_trades_this_window,
            "revert_trades": self._revert_trades_this_window,
        })

        # ── TradeLogger: 记录结算详情 ──
        trade_logger = ctx.get("trade_logger")
        if trade_logger:
            try:
                avg_up_px = up_cost / up_shares if up_shares > 0 else 0
                avg_dn_px = dn_cost / dn_shares if dn_shares > 0 else 0
                trade_logger.log_settlement(ctx, {
                    "window_start_ts": self._last_window_ts,
                    "condition_id": settle_condition_id,
                    "btc_price": round(btc, 2),
                    "ptb": round(ptb, 2),
                    "winner_side": winner_side,
                    "up_shares": round(up_shares, 4),
                    "up_cost": round(up_cost, 4),
                    "up_avg_price": round(avg_up_px, 4),
                    "dn_shares": round(dn_shares, 4),
                    "dn_cost": round(dn_cost, 4),
                    "dn_avg_price": round(avg_dn_px, 4),
                    "payout": round(payout, 4),
                    "total_cost": round(total_cost, 4),
                    "fee": round(fee, 4),
                    "net_pnl": round(net_pnl, 4),
                    "actual_edge": round(actual_edge, 4),
                    "gap_shares": round(gap, 4),
                    "gap_pct": round(gap_pct, 2),
                    "entries": self._entries_this_window,
                    "bursts": 0,
                    "result": result_str,
                    "balance_after": round(ctx.account.balance, 4),
                    "cumulative_pnl": round(self._cumulative_pnl, 4),
                    "extra": {
                        "diverge_trades": self._diverge_trades_this_window,
                        "revert_trades": self._revert_trades_this_window,
                    },
                })
            except Exception as e:
                logger.debug(f"TradeLogger log_settlement 失败: {e}")

        self._positions.clear()

        if settle_condition_id and ctx.is_live:
            self._pending_redeems.append({
                "condition_id": settle_condition_id,
                "neg_risk": ctx.market.pm_neg_risk,
                "ts": time.time(),
                "retries": 0,
                "pnl": round(net_pnl, 4),
                "winner": winner_side,
            })
            logger.info(
                f"[{self.name()}] 已加入待兑付队列 | "
                f"conditionId={settle_condition_id[:18]}..."
            )

    # ================================================================
    #  链上兑付
    # ================================================================

    def _ensure_redeemer(self):
        if self._redeemer is not None:
            return
        try:
            from src.trading.redeemer import CtfRedeemer
            self._redeemer = CtfRedeemer()
            logger.info(f"[{self.name()}] CtfRedeemer 已初始化")
        except Exception as e:
            logger.error(f"[{self.name()}] CtfRedeemer 初始化失败: {e}")
            self._redeemer = None

    async def _process_pending_redeems(self, ctx: Context) -> None:
        if not ctx.is_live:
            return
        if not self._pending_redeems:
            return
        now = time.time()
        if now - self._last_redeem_check_ts < self._redeem_check_interval:
            return
        self._last_redeem_check_ts = now

        self._ensure_redeemer()
        if self._redeemer is None:
            return

        still_pending = []
        for item in self._pending_redeems:
            cid = item["condition_id"]
            neg_risk = item.get("neg_risk", False)
            retries = item.get("retries", 0)

            if retries >= self._max_redeem_retries:
                logger.warning(
                    f"[{self.name()}] 兑付超时放弃 | {cid[:18]}..."
                )
                self._redeem_results.append({
                    "condition_id": cid, "success": False,
                    "error": "max retries", "ts": now,
                })
                continue

            try:
                resolved = await self._redeemer.is_resolved(cid)
                if not resolved:
                    item["retries"] = retries + 1
                    still_pending.append(item)
                    continue

                result = await self._redeemer.redeem(cid, neg_risk=neg_risk)
                if result.success:
                    logger.info(
                        f"[{self.name()}] 兑付成功 | "
                        f"tx={result.tx_hash[:18]}... "
                        f"USDC={result.usdc_received:.2f}"
                    )
                    self._redeem_results.append({
                        "condition_id": cid, "success": True,
                        "tx_hash": result.tx_hash,
                        "usdc_received": result.usdc_received, "ts": now,
                    })
                else:
                    item["retries"] = retries + 1
                    still_pending.append(item)
            except Exception as e:
                logger.error(f"[{self.name()}] 兑付异常: {e}")
                item["retries"] = retries + 1
                still_pending.append(item)

        self._pending_redeems = still_pending

    # ================================================================
    #  记录 / Dashboard
    # ================================================================

    def _record_trade(self, ctx: Context, trade: dict) -> None:
        import datetime
        trade["ts"] = ctx.now()
        trade["time"] = datetime.datetime.fromtimestamp(
            trade["ts"], tz=datetime.timezone.utc
        ).strftime("%H:%M:%S")
        trade["balance_after"] = round(ctx.account.balance, 2)
        self._trade_history.append(trade)
        if len(self._trade_history) > self._max_history:
            self._trade_history = self._trade_history[-self._max_history:]

    async def _submit_order(
        self, ctx: Context, side: Side, price: float, size: float
    ) -> "OrderResult | None":
        if ctx.is_backtest:
            engine = ctx.get("backtest_engine")
            if engine:
                from src.backtest.engine import OrderSide
                bt_side = OrderSide.YES if side == Side.YES else OrderSide.NO
                engine.submit_order(bt_side, size, price)
            return OrderResult(
                order_id="bt_" + str(int(ctx.now())),
                status=ExecutionStatus.FILLED,
                filled_price=price,
                filled_size=size / price,
                fee=size * self._fee_rate,
            )

        executor = ctx.get("executor")
        if executor is None:
            logger.warning(f"[{self.name()}] Executor 不可用")
            return None

        if side == Side.YES:
            token_id = ctx.market.pm_yes_token_id
        else:
            token_id = ctx.market.pm_no_token_id

        request = OrderRequest(
            market_id=ctx.market.pm_market_id,
            token_id=token_id,
            side=side,
            order_type=OrderType.FOK,
            price=price,
            size=size,
            strategy_id=self.name(),
            meta={
                "deviation": self._deviation,
                "signal_state": self._signal_state,
                "spread_fast": self._spread_ema_fast,
                "spread_slow": self._spread_ema_slow,
                "clob_side": "BUY",
                "tick_size": "0.01",
                "neg_risk": ctx.market.pm_neg_risk,
                # ── 策略上下文 (供 TradeLogger 记录) ──
                "momentum": self._deviation,
                "edge": 1.0 - (
                    (ctx.market.pm_yes_ask or ctx.market.pm_yes_price or 0.5)
                    + (ctx.market.pm_no_ask or ctx.market.pm_no_price or 0.5)
                ),
                "gap_shares": round(self._cum_up_shares - self._cum_dn_shares, 4),
                "cum_up_shares": round(self._cum_up_shares, 4),
                "cum_dn_shares": round(self._cum_dn_shares, 4),
                "cum_up_cost": round(self._cum_up_cost, 4),
                "cum_dn_cost": round(self._cum_dn_cost, 4),
                "entry_num": self._entries_this_window,
                "burst_num": getattr(self, '_burst_count_this_window', 0),
            },
        )
        return await executor.submit_order(request)

    def _publish_state(
        self,
        ctx: Context,
        bn_price: float,
        spread: float,
    ) -> None:
        up_positions = [p for p in self._positions if p.side == "UP"]
        dn_positions = [p for p in self._positions if p.side == "DOWN"]

        up_dict = None
        dn_dict = None

        if up_positions:
            avg_entry = (
                self._cum_up_cost / self._cum_up_shares
                if self._cum_up_shares > 0 else 0
            )
            cur = ctx.market.pm_yes_bid or ctx.market.pm_yes_price
            upnl = (
                (cur - avg_entry) * self._cum_up_shares
                if cur and avg_entry > 0 else 0.0
            )
            up_dict = {
                "side": "UP",
                "entry_price": round(avg_entry, 4),
                "size": round(self._cum_up_cost, 2),
                "shares": round(self._cum_up_shares, 2),
                "entries": len(up_positions),
                "unrealized_pnl": round(upnl, 6),
            }

        if dn_positions:
            avg_entry = (
                self._cum_dn_cost / self._cum_dn_shares
                if self._cum_dn_shares > 0 else 0
            )
            cur = ctx.market.pm_no_bid or ctx.market.pm_no_price
            upnl = (
                (cur - avg_entry) * self._cum_dn_shares
                if cur and avg_entry > 0 else 0.0
            )
            dn_dict = {
                "side": "DOWN",
                "entry_price": round(avg_entry, 4),
                "size": round(self._cum_dn_cost, 2),
                "shares": round(self._cum_dn_shares, 2),
                "entries": len(dn_positions),
                "unrealized_pnl": round(upnl, 6),
            }

        has_pos = up_dict is not None or dn_dict is not None
        combined_pos = None
        if has_pos:
            total_upnl = (
                (up_dict.get("unrealized_pnl", 0) if up_dict else 0) +
                (dn_dict.get("unrealized_pnl", 0) if dn_dict else 0)
            )
            combined_pos = {
                "side": "DUAL",
                "entry_price": 0,
                "size": round(self._cum_up_cost + self._cum_dn_cost, 2),
                "unrealized_pnl": round(total_upnl, 6),
                "entries": self._entries_this_window,
            }

        gap = self._cum_up_shares - self._cum_dn_shares
        total = max(self._cum_up_shares, self._cum_dn_shares, 1)
        gap_pct = gap / total * 100

        dev = self._deviation
        has_signal = abs(dev) >= self._open_threshold

        state = {
            "name": self.name(),
            "version": self.version(),
            # ── Spread 数据 ──
            "binance_price": round(bn_price, 2),
            "rtds_price": round(ctx.market.btc_price, 2),
            "spread": round(spread, 2),
            "spread_ema_fast": round(self._spread_ema_fast, 3),
            "spread_ema_slow": round(self._spread_ema_slow, 3),
            "deviation": round(dev, 3),
            "signal_state": self._signal_state,
            "window_ptb": round(self._window_ptb, 2),
            # ── 持仓 ──
            "direction": "UP" if dev > 0 else ("DOWN" if dev < 0 else "NEUTRAL"),
            "has_signal": has_signal,
            "up_position": up_dict,
            "dn_position": dn_dict,
            "position": combined_pos,
            "cum_up_shares": round(self._cum_up_shares, 2),
            "cum_dn_shares": round(self._cum_dn_shares, 2),
            "shares_gap": round(gap, 2),
            "shares_gap_pct": round(gap_pct, 1),
            "entries_this_window": self._entries_this_window,
            "diverge_trades": self._diverge_trades_this_window,
            "revert_trades": self._revert_trades_this_window,
            "up_progress_pct": round(
                self._cum_up_shares / self._target_shares_per_side * 100, 1
            ),
            "dn_progress_pct": round(
                self._cum_dn_shares / self._target_shares_per_side * 100, 1
            ),
            "trade_count": self._trade_count,
            "win_count": self._win_count,
            "loss_count": self._loss_count,
            "cumulative_pnl": round(self._cumulative_pnl, 4),
            "win_rate": round(
                self._win_count / max(self._win_count + self._loss_count, 1) * 100, 1
            ),
            "account": {
                "balance": round(ctx.account.balance, 2),
                "available": round(ctx.account.available, 2),
                "total_equity": round(ctx.account.total_equity, 2),
                "daily_pnl": round(ctx.account.daily_pnl, 4),
            },
            "trade_history": self._trade_history,
            "params": self.get_params(),
        }
        ctx.set("strategy_state", state)

        try:
            asyncio.ensure_future(ctx.event_bus.publish(Event(
                type=EventType.SIGNAL_GENERATED,
                data=state,
                source=self.name(),
            )))
        except RuntimeError:
            pass


# ================================================================
#  内部数据结构
# ================================================================

class _Position:
    __slots__ = (
        "side", "entry_price", "size", "shares",
        "entry_time", "entry_score", "condition_id", "signal_type",
    )

    def __init__(
        self,
        side: str,
        entry_price: float,
        size: float,
        shares: float,
        entry_time: float,
        entry_score: float,
        condition_id: str = "",
        signal_type: str = "",
    ) -> None:
        self.side = side
        self.entry_price = entry_price
        self.size = size
        self.shares = shares
        self.entry_time = entry_time
        self.entry_score = entry_score
        self.condition_id = condition_id
        self.signal_type = signal_type

    def to_dict(self) -> dict[str, Any]:
        return {
            "side": self.side,
            "entry_price": self.entry_price,
            "size": self.size,
            "shares": self.shares,
            "entry_time": self.entry_time,
            "entry_score": self.entry_score,
            "condition_id": self.condition_id,
            "signal_type": self.signal_type,
        }
