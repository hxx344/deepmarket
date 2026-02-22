"""
BTC 5-min Dual-Side Event-Driven Strategy v5.1

核心变更 (vs v5.0 定时版):
    - 去掉固定 2s 定时器, 改为 event-driven (动量触发)
    - 放宽 lead-lag 阈值, 让更多价格波动触发入场
    - 大波动时 burst 多笔 (吃多档 orderbook), 小波动时仅 1 笔
    - gap 平衡从 "机械交替+暂停领先方" 改为 "自然偏权重" 方式
    - 整体节奏: 有信号就打, 没信号就等, 高度接近 0x1d 的 burst+pause 模式

0x1d 实际时间模式 (来自分析):
    - CV=3.10 (极高变异, 完全非定时)
    - 51% 间隔 <0.5s (burst: 同秒内多笔)
    - 31% 间隔 ~2s (线程交替间歇)
    - 10% 间隔 >5s (等待流动性恢复)
    - 最多同一秒内 15 笔

手续费模型 (Polymarket):
    - Taker fee: 0.2% (按成交 USDC 计)
    - 持有至结算只付入场 1 次
"""

from __future__ import annotations

import asyncio
import time
from collections import deque
from typing import Any

from loguru import logger

from src.core.context import Context
from src.core.event_bus import Event, EventType
from src.strategy.base import Strategy
from src.trading.executor import ExecutionStatus, OrderRequest, OrderResult, OrderType, Side


class BTC5minTakerStrategy(Strategy):
    """
    BTC 5-min Dual-Side Event-Driven Taker v5.1

    核心机制:
        1. 动量驱动: BTC 价格波动 → 计算动量 → 超过 (放宽的) 阈值即触发
        2. Burst 模式: 触发时根据动量强度决定 burst_count (1~max_burst)
        3. 双边同下: 每次 burst 同时下 UP 和 DOWN, 用 gap 权重偏移分配
        4. Gap 自然平衡: 不强制交替, 而是通过 shares 分配比例自动修正
        5. 持有至结算
    """

    def __init__(
        self,
        # ── 核心 ──
        target_shares_per_side: float = 100.0,
        shares_per_order: float = 10.0,
        max_combined_cost: float = 500.0,
        # ── 动量触发 (放宽后的 lead-lag) ──
        momentum_lookback_s: float = 5.0,
        momentum_threshold: float = 1.0,       # 放宽: $1 就触发 (原来 $3)
        strong_momentum: float = 5.0,           # 强动量阈值: burst 加量
        # ── Burst 模式 ──
        max_burst_orders: int = 5,              # 单次 burst 最大订单数
        burst_cooldown_s: float = 0.3,          # burst 内订单间隔 (模拟吃多档)
        post_burst_pause_s: float = 2.0,        # burst 后暂停 (等 ob 恢复)
        # ── 时间窗口 ──
        entry_delay_s: float = 9.0,
        entry_cutoff_s: float = 15.0,
        # ── Gap 平衡 (自然偏权重) ──
        gap_weight_factor: float = 0.7,         # gap 修正强度: 0=不修正, 1=全修正
        # ── 时间衰减 gap 管控 ──
        time_decay_enabled: bool = True,         # 启用时间衰减 gap 管控
        time_decay_start_pct: float = 0.3,       # 窗口进度 30% 后开始衰减 (≈90s)
        time_decay_power: float = 2.0,           # 衰减曲线指数 (>1=后期急剧收紧)
        time_decay_floor: float = 0.1,           # 扩gap方最低缩放比 (10%)
        time_decay_rebalance_boost: float = 1.5,  # 缩gap方尾部放大倍数 (最多1.5x)
        # ── 震荡防护 (Choppiness Guard) ──
        chop_guard_enabled: bool = True,           # 启用震荡市场过滤
        chop_lookback_s: float = 30.0,             # 回看窗口 (秒): 统计零轴穿越次数
        chop_max_crosses: int = 6,                 # 最多穿越次数: 超过=震荡市
        chop_cooldown_s: float = 15.0,             # 触发后冷却时间 (秒)
        max_entries_per_window: int = 20,           # 单窗口最大入场次数
        min_momentum_consistency: int = 3,          # 动量方向连续同符号最少 tick 数
        # ── 价格保护 (Price Guard) ──
        min_edge: float = -0.02,                 # 最低 edge: combined_price ≤ 1.02
        price_deterioration_pct: float = 0.08,    # 均价恶化上限: ask > avg*(1+8%) → 拒绝
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

        # 动量触发
        self._momentum_lookback_s = momentum_lookback_s
        self._momentum_threshold = momentum_threshold
        self._strong_momentum = strong_momentum

        # Burst
        self._max_burst_orders = max_burst_orders
        self._burst_cooldown_s = burst_cooldown_s
        self._post_burst_pause_s = post_burst_pause_s

        # 时间窗口
        self._entry_delay_s = entry_delay_s
        self._entry_cutoff_s = entry_cutoff_s

        # Gap 平衡
        self._gap_weight_factor = gap_weight_factor

        # 时间衰减 gap 管控
        self._time_decay_enabled = time_decay_enabled
        self._time_decay_start_pct = time_decay_start_pct
        self._time_decay_power = time_decay_power
        self._time_decay_floor = time_decay_floor
        self._time_decay_rebalance_boost = time_decay_rebalance_boost

        # 震荡防护
        self._chop_guard_enabled = chop_guard_enabled
        self._chop_lookback_s = chop_lookback_s
        self._chop_max_crosses = chop_max_crosses
        self._chop_cooldown_s = chop_cooldown_s
        self._max_entries_per_window = max_entries_per_window
        self._min_momentum_consistency = min_momentum_consistency

        # 价格保护
        self._min_edge = min_edge
        self._price_deterioration_pct = price_deterioration_pct

        # 流动性
        self._min_depth = min_depth
        self._max_spread = max_spread

        # 出场
        self._hold_to_settlement = hold_to_settlement

        # 手续费
        self._fee_rate = fee_rate

        # ── BTC 价格缓冲区 ──
        self._btc_buffer: deque[tuple[float, float]] = deque(maxlen=1200)

        # ── 运行时状态 ──
        self._positions: list[_Position] = []
        self._last_window_ts: int = 0
        self._entries_this_window: int = 0
        self._last_burst_end_time: float = 0.0     # 上次 burst 结束时间
        self._current_momentum: float = 0.0
        self._last_momentum: float = 0.0           # 上一个 tick 的动量
        self._trade_count: int = 0
        self._win_count: int = 0
        self._loss_count: int = 0
        self._cumulative_pnl: float = 0.0
        self._trade_history: list[dict] = []
        self._max_history: int = 200
        self._burst_count_this_window: int = 0      # 本窗口 burst 次数

        # ── 双边 shares 跟踪 ──
        self._cum_up_shares: float = 0.0
        self._cum_dn_shares: float = 0.0
        self._cum_up_cost: float = 0.0
        self._cum_dn_cost: float = 0.0

        # ── 窗口 PTB 跟踪 ──
        self._window_ptb: float = 0.0
        self._window_entry_btc: float = 0.0

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
        self._in_burst: bool = False

        # ── 震荡防护状态 ──
        self._momentum_sign_history: deque[tuple[float, int]] = deque(maxlen=600)
        self._chop_blocked_until: float = 0.0      # 震荡冷却截止时间
        self._chop_block_count: int = 0             # 本窗口被震荡拦截次数
        self._consecutive_same_sign: int = 0        # 连续同方向 tick 数
        self._last_momentum_sign: int = 0           # 上一个动量符号

    # ================================================================
    #  Strategy 接口
    # ================================================================

    def name(self) -> str:
        return "btc5min_taker"

    def version(self) -> str:
        return "5.3"

    def description(self) -> str:
        td = "ON" if self._time_decay_enabled else "OFF"
        cg = "ON" if self._chop_guard_enabled else "OFF"
        return (
            f"BTC 5-min Dual-Side Event-Driven v5.3 ("
            f"target={self._target_shares_per_side}sh/side, "
            f"threshold=${self._momentum_threshold}, "
            f"burst_max={self._max_burst_orders}, "
            f"budget=${self._max_combined_cost}, "
            f"time_decay={td}, chop_guard={cg})"
        )

    def get_params(self) -> dict[str, Any]:
        return {
            "target_shares_per_side": self._target_shares_per_side,
            "shares_per_order": self._shares_per_order,
            "max_combined_cost": self._max_combined_cost,
            "momentum_lookback_s": self._momentum_lookback_s,
            "momentum_threshold": self._momentum_threshold,
            "strong_momentum": self._strong_momentum,
            "max_burst_orders": self._max_burst_orders,
            "burst_cooldown_s": self._burst_cooldown_s,
            "post_burst_pause_s": self._post_burst_pause_s,
            "entry_delay_s": self._entry_delay_s,
            "entry_cutoff_s": self._entry_cutoff_s,
            "gap_weight_factor": self._gap_weight_factor,
            "time_decay_enabled": self._time_decay_enabled,
            "time_decay_start_pct": self._time_decay_start_pct,
            "time_decay_power": self._time_decay_power,
            "time_decay_floor": self._time_decay_floor,
            "time_decay_rebalance_boost": self._time_decay_rebalance_boost,
            "chop_guard_enabled": self._chop_guard_enabled,
            "chop_lookback_s": self._chop_lookback_s,
            "chop_max_crosses": self._chop_max_crosses,
            "chop_cooldown_s": self._chop_cooldown_s,
            "max_entries_per_window": self._max_entries_per_window,
            "min_momentum_consistency": self._min_momentum_consistency,
            "min_edge": self._min_edge,
            "price_deterioration_pct": self._price_deterioration_pct,
            "min_depth": self._min_depth,
            "max_spread": self._max_spread,
            "hold_to_settlement": self._hold_to_settlement,
            "fee_rate": self._fee_rate,
        }

    def on_init(self, context: Context) -> None:
        logger.info(f"[{self.name()}] 策略初始化: {self.description()}")
        logger.info(f"[{self.name()}] 参数: {self.get_params()}")

    async def on_market_data(self, context: Context, data: dict[str, Any]) -> None:
        """主驱动入口 — 每个 BTC tick 调用一次。"""
        # ── 0. 窗口切换检测 ──
        await self._check_window_switch(context)

        # ── 0.1. 待兑付 ──
        await self._process_pending_redeems(context)

        # ── 1. 记录 BTC 价格 ──
        btc = context.market.btc_price
        now = context.now()
        if btc > 0:
            self._btc_buffer.append((now, btc))

        # ── 2. 计算动量 ──
        self._last_momentum = self._current_momentum
        self._current_momentum = self._calc_btc_momentum(now)

        # ── 2.1 跟踪动量符号 (震荡检测用) ──
        self._track_momentum_sign(now, self._current_momentum)

        # ── 3. 诊断日志 (每 30 个 tick) ──
        self._tick_counter += 1
        if self._tick_counter % 30 == 1:
            self._log_status(context)

        # ── 4. 事件驱动入场 ──
        await self._event_driven_entry(context)

        # ── 5. Dashboard ──
        self._publish_state(context)

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
            f"总交易={self._trade_count} burstx{self._burst_count_this_window} | "
            f"UP={self._cum_up_shares:.1f}sh DN={self._cum_dn_shares:.1f}sh "
            f"gap={gap:+.1f} | cost=${total_cost:.2f}"
        )

    # ================================================================
    #  BTC 动量计算
    # ================================================================

    def _calc_btc_momentum(self, now: float) -> float:
        if len(self._btc_buffer) < 2:
            return 0.0
        current_price = self._btc_buffer[-1][1]
        target_ts = now - self._momentum_lookback_s
        past_price = None
        for ts, price in self._btc_buffer:
            if ts <= target_ts:
                past_price = price
            elif past_price is not None:
                break
        if past_price is None:
            oldest_ts = self._btc_buffer[0][0]
            if now - oldest_ts >= 1.0:
                past_price = self._btc_buffer[0][1]
            else:
                return 0.0
        return current_price - past_price

    # ================================================================
    #  诊断日志
    # ================================================================

    def _log_status(self, ctx: Context) -> None:
        btc = ctx.market.btc_price
        secs_left = ctx.market.pm_window_seconds_left
        mom = self._current_momentum
        gap = self._cum_up_shares - self._cum_dn_shares
        total = max(self._cum_up_shares, self._cum_dn_shares, 1)
        gap_pct = abs(gap) / total * 100
        up_pct = min(100, self._cum_up_shares / self._target_shares_per_side * 100)
        dn_pct = min(100, self._cum_dn_shares / self._target_shares_per_side * 100)

        crosses = self._count_zero_crosses(ctx.now()) if self._chop_guard_enabled else 0
        chop_tag = f" CHOP:{crosses}/{self._chop_max_crosses}" if crosses >= self._chop_max_crosses - 1 else ""

        logger.info(
            f"[{self.name()}] EventDrv | "
            f"BTC=${btc:,.2f} mom=${mom:+.2f} | "
            f"UP={self._cum_up_shares:.1f}sh({up_pct:.0f}%)/${self._cum_up_cost:.1f} "
            f"DN={self._cum_dn_shares:.1f}sh({dn_pct:.0f}%)/${self._cum_dn_cost:.1f} | "
            f"gap={gap:+.1f}sh ({gap_pct:.1f}%) | "
            f"#{self._entries_this_window}/{self._max_entries_per_window} "
            f"burstx{self._burst_count_this_window}{chop_tag} | "
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
            self._last_burst_end_time = 0.0
            self._burst_count_this_window = 0
            self._cum_up_shares = 0.0
            self._cum_dn_shares = 0.0
            self._cum_up_cost = 0.0
            self._cum_dn_cost = 0.0
            # 重置震荡防护状态
            self._chop_blocked_until = 0.0
            self._chop_block_count = 0
            self._consecutive_same_sign = 0
            self._last_momentum_sign = 0
            self._momentum_sign_history.clear()
            self._window_ptb = context.market.btc_price
            self._window_entry_btc = context.market.btc_price
            logger.info(
                f"[{self.name()}] 新窗口 PTB: ${self._window_ptb:,.2f}"
            )

    # ================================================================
    #  核心: 事件驱动入场 + burst 多档
    # ================================================================

    async def _event_driven_entry(self, ctx: Context) -> None:
        """
        事件驱动入场 — 非定时, 由动量变化触发。

        流程:
            1. 检查时间窗口 (delay / cutoff)
            2. 检查 post-burst 冷却
            3. 检查是否有动量触发信号
            4. 计算 burst 大小 (基于动量强度)
            5. 执行 burst: 同时下 UP+DOWN, gap 权重偏移
        """
        secs_left = ctx.market.pm_window_seconds_left
        if secs_left <= 0:
            return

        # 防重入: asyncio.sleep 会让出控制权, 新 tick 可能再次触发
        if self._in_burst:
            return

        # 时间窗口
        window_elapsed = 300 - secs_left
        if window_elapsed < self._entry_delay_s:
            return
        if secs_left < self._entry_cutoff_s:
            return

        # 目标已达
        if (self._cum_up_shares >= self._target_shares_per_side and
                self._cum_dn_shares >= self._target_shares_per_side):
            return

        # 预算耗尽
        total_invested = self._cum_up_cost + self._cum_dn_cost
        remaining_budget = self._max_combined_cost - total_invested
        if remaining_budget < 2.0:
            return

        now = ctx.now()

        # post-burst 冷却
        if self._last_burst_end_time > 0:
            since_burst = now - self._last_burst_end_time
            if since_burst < self._post_burst_pause_s:
                return

        # ── 动量触发判定 ──
        mom = self._current_momentum
        abs_mom = abs(mom)

        if abs_mom < self._momentum_threshold:
            return  # 没信号 → 不做

        # ── 震荡防护: 窗口入场上限 ──
        if self._entries_this_window >= self._max_entries_per_window:
            return

        # ── 震荡防护: 动量方向一致性 ──
        if self._consecutive_same_sign < self._min_momentum_consistency:
            return  # 动量刚变方向, 等几个 tick 确认

        # ── 震荡防护: 零轴穿越频率 ──
        if self._is_choppy(now):
            return

        # ── 计算 burst 大小 ──
        # |mom| 在 [threshold, strong_momentum] 之间线性映射到 [1, max_burst]
        if abs_mom >= self._strong_momentum:
            burst_count = self._max_burst_orders
        else:
            ratio = (abs_mom - self._momentum_threshold) / max(
                self._strong_momentum - self._momentum_threshold, 0.01
            )
            burst_count = max(1, int(1 + ratio * (self._max_burst_orders - 1)))

        logger.info(
            f"[{self.name()}] 🔥 动量触发 | "
            f"mom=${mom:+.2f} → burst={burst_count} | "
            f"secs_left={secs_left:.0f}"
        )

        # ── TradeLogger: 记录触发信号 ──
        trade_logger = ctx.get("trade_logger")
        if trade_logger:
            try:
                gap = self._cum_up_shares - self._cum_dn_shares
                total = max(self._cum_up_shares, self._cum_dn_shares, 1)
                trade_logger.log_signal(
                    ctx=ctx,
                    signal_type="momentum_trigger",
                    direction="UP" if mom > 0 else "DOWN",
                    strength=abs_mom,
                    executed=True,
                    extra={
                        "strategy_id": self.name(),
                        "momentum": round(mom, 4),
                        "gap_ratio": round(gap / total, 4),
                        "secs_left": round(secs_left, 1),
                        "burst_count": burst_count,
                        "cum_up_shares": round(self._cum_up_shares, 2),
                        "cum_dn_shares": round(self._cum_dn_shares, 2),
                        "remaining_budget": round(remaining_budget, 2),
                    },
                )
            except Exception:
                pass

        # ── 执行 burst (加锁防重入) ──
        self._in_burst = True
        filled_in_burst = 0
        try:
            for i in range(burst_count):
                # 再次检查预算/目标
                total_invested = self._cum_up_cost + self._cum_dn_cost
                remaining_budget = self._max_combined_cost - total_invested
                if remaining_budget < 2.0:
                    break
                both_done = (
                    self._cum_up_shares >= self._target_shares_per_side and
                    self._cum_dn_shares >= self._target_shares_per_side
                )
                if both_done:
                    break

                # 决定方向和数量 (gap 自然平衡)
                direction, order_shares = self._decide_direction_and_size(
                    mom, secs_left, remaining_budget, ctx
                )
                if order_shares < 0.1:
                    break

                # 下单
                ok = await self._execute_single_order(
                    ctx, direction, order_shares, mom, secs_left, remaining_budget
                )
                if ok:
                    filled_in_burst += 1
                else:
                    break  # 失败则终止该侧 burst, 但不阻止后续

                # burst 内冷却
                if i < burst_count - 1 and self._burst_cooldown_s > 0:
                    await asyncio.sleep(self._burst_cooldown_s)
        finally:
            self._in_burst = False

        if filled_in_burst > 0:
            self._last_burst_end_time = ctx.now()
            self._burst_count_this_window += 1
            logger.info(
                f"[{self.name()}] burst #{self._burst_count_this_window} 完成 | "
                f"成交={filled_in_burst}/{burst_count} | "
                f"CumUP={self._cum_up_shares:.1f} CumDN={self._cum_dn_shares:.1f}"
            )

    # ================================================================
    #  震荡防护 (Choppiness Guard)
    # ================================================================

    def _track_momentum_sign(self, now: float, momentum: float) -> None:
        """记录动量符号历史, 维护连续同符号计数。"""
        if abs(momentum) < 0.01:
            # 接近零 → 不更新连续计数, 但记录 sign=0
            self._momentum_sign_history.append((now, 0))
            return

        sign = 1 if momentum > 0 else -1
        self._momentum_sign_history.append((now, sign))

        if sign == self._last_momentum_sign:
            self._consecutive_same_sign += 1
        else:
            self._consecutive_same_sign = 1
            self._last_momentum_sign = sign

    def _count_zero_crosses(self, now: float) -> int:
        """统计回看窗口内动量穿越零轴的次数。"""
        cutoff = now - self._chop_lookback_s
        # 过滤 sign != 0 的记录 (忽略接近零的点)
        signs = [
            s for ts, s in self._momentum_sign_history
            if ts >= cutoff and s != 0
        ]
        if len(signs) < 2:
            return 0
        crosses = 0
        for i in range(1, len(signs)):
            if signs[i] != signs[i - 1]:
                crosses += 1
        return crosses

    def _is_choppy(self, now: float) -> bool:
        """
        判断当前是否处于震荡市场。

        判断标准:
            1. 回看窗口内零轴穿越次数 > 阈值 → 触发震荡保护
            2. 触发后进入冷却期, 冷却期内一律视为震荡
        """
        if not self._chop_guard_enabled:
            return False

        # 仍在冷却期
        if now < self._chop_blocked_until:
            return True

        # 统计穿越次数
        crosses = self._count_zero_crosses(now)
        if crosses >= self._chop_max_crosses:
            self._chop_blocked_until = now + self._chop_cooldown_s
            self._chop_block_count += 1
            logger.warning(
                f"[{self.name()}] ⚠️ 震荡防护触发 | "
                f"零轴穿越={crosses}次/{self._chop_lookback_s:.0f}s "
                f"(阈值={self._chop_max_crosses}) | "
                f"冷却{self._chop_cooldown_s:.0f}s | "
                f"本窗口已拦截{self._chop_block_count}次"
            )
            return True

        return False

    # ================================================================
    #  时间衰减 gap 管控
    # ================================================================

    def _time_gap_control(
        self, secs_left: float, widens_gap: bool,
    ) -> float:
        """
        基于窗口剩余时间的 gap 管控缩放因子。

        核心逻辑:
            5-min 窗口越到后面, BTC 翻转的概率越低, 因此:
            - 会 **扩大** UP/DN 差距的交易 → 随时间递减缩放 (收紧)
            - 会 **缩小** UP/DN 差距的交易 → 保持甚至放大 (宽松)

        返回:
            size 缩放因子 (乘到 order_shares 上)
            - 扩gap方: 1.0 → floor (随时间衰减)
            - 缩gap方: 1.0 → rebalance_boost (随时间增大)

        时间衰减曲线:
            progress = (300 - secs_left) / 300   (0→1)
            decay_progress = (progress - start_pct) / (1 - start_pct)  (0→1, 在 start_pct 后)
            衰减因子 = 1 - decay_progress ^ power
            → power=2: 前半段缓慢衰减, 后半段急剧收紧
        """
        if not self._time_decay_enabled:
            return 1.0

        progress = (300.0 - secs_left) / 300.0  # 0 → 1
        progress = max(0.0, min(1.0, progress))

        # 未到衰减起始点 → 不干预
        if progress < self._time_decay_start_pct:
            return 1.0

        # 归一化到衰减区间 [start_pct, 1.0] → [0, 1]
        decay_progress = (progress - self._time_decay_start_pct) / (
            1.0 - self._time_decay_start_pct
        )
        decay_progress = min(1.0, decay_progress)

        if widens_gap:
            # 扩 gap 方: 随时间从 1.0 衰减到 floor
            #   factor = floor + (1 - floor) * (1 - decay^power)
            decay = decay_progress ** self._time_decay_power
            factor = self._time_decay_floor + (
                1.0 - self._time_decay_floor
            ) * (1.0 - decay)
            return factor
        else:
            # 缩 gap 方: 随时间从 1.0 增长到 rebalance_boost
            #   factor = 1.0 + (boost - 1.0) * decay^0.5 (平方根, 前快后缓)
            boost_progress = decay_progress ** 0.5
            factor = 1.0 + (
                self._time_decay_rebalance_boost - 1.0
            ) * boost_progress
            return factor

    # ================================================================
    #  方向与分量决策 (gap 自然偏权重 + 时间衰减管控)
    # ================================================================

    def _decide_direction_and_size(
        self,
        momentum: float,
        secs_left: float,
        remaining_budget: float,
        ctx: Context,
    ) -> tuple[str, float]:
        """
        Gap 自然偏权重平衡 + 时间衰减 gap 管控:

        核心思想:
            1. gap 权重: 落后方拿更多 shares, 领先方更少
            2. 时间衰减: 窗口越晚, 扩大 gap 的单量越小, 缩小 gap 的单量适当放大
               — 因为越到后面 BTC 翻转概率越低, 应保护已有持仓平衡

        时间衰减效果:
            progress=30% 起生效, 到窗口尾部:
            - 扩gap方 size × 0.1 (几乎不允许扩gap)
            - 缩gap方 size × 1.5 (加速再平衡)
        """
        up_remaining = max(0, self._target_shares_per_side - self._cum_up_shares)
        dn_remaining = max(0, self._target_shares_per_side - self._cum_dn_shares)

        if up_remaining <= 0 and dn_remaining <= 0:
            return "UP", 0.0

        # ── gap 分析 ──
        gap = self._cum_up_shares - self._cum_dn_shares  # 正=UP多, 负=DN多

        # 只有一边还需要 → 判断是否扩gap, 应用时间衰减
        if up_remaining <= 0:
            widens = False  # 只买 DOWN, DOWN 是落后方(UP多), 缩gap
            scale = self._time_gap_control(secs_left, widens_gap=widens)
            shares = min(self._shares_per_order * scale, dn_remaining)
            return "DOWN", max(0, shares)
        if dn_remaining <= 0:
            widens = False  # 只买 UP, UP 是落后方(DN多), 缩gap
            scale = self._time_gap_control(secs_left, widens_gap=widens)
            shares = min(self._shares_per_order * scale, up_remaining)
            return "UP", max(0, shares)

        # ── 两边都需要 → gap 方向分析 ──
        total = max(self._cum_up_shares, self._cum_dn_shares, 1.0)
        gap_ratio = gap / total  # 正=UP多, 负=DN多

        base_shares = self._shares_per_order

        # 窗口尾部 (最后 45s): 缩小单量精细平衡 (原有逻辑保留)
        if secs_left < 45:
            base_shares = min(base_shares, max(1.0, self._shares_per_order * 0.3))

        # ── gap 偏权重决策 ──
        gap_bias = gap_ratio * self._gap_weight_factor

        if abs(gap_bias) > 0.15:
            # gap 较大 → 只买落后方 (缩gap)
            scale = self._time_gap_control(secs_left, widens_gap=False)
            if gap_bias > 0:
                # UP 多 → 买 DOWN (缩gap)
                shares = min(base_shares * scale, dn_remaining)
                return "DOWN", max(0, shares)
            else:
                # DN 多 → 买 UP (缩gap)
                shares = min(base_shares * scale, up_remaining)
                return "UP", max(0, shares)
        else:
            # gap 在合理范围 → 买需求更大的一边
            if up_remaining >= dn_remaining:
                direction = "UP"
                remaining = up_remaining
            else:
                direction = "DOWN"
                remaining = dn_remaining

            # 判断本次交易是否扩大 gap
            # gap>0=UP多: 买UP→扩gap, 买DN→缩gap
            # gap<0=DN多: 买DN→扩gap, 买UP→缩gap
            widens = (
                (direction == "UP" and gap > 0)
                or (direction == "DOWN" and gap < 0)
            )

            scale = self._time_gap_control(secs_left, widens_gap=widens)
            shares = min(base_shares * scale, remaining)

            # 时间衰减后量太小 → 跳过
            if shares < 0.5:
                return direction, 0.0

            return direction, shares

    # ================================================================
    #  执行单笔订单
    # ================================================================

    async def _execute_single_order(
        self,
        ctx: Context,
        direction: str,
        order_shares: float,
        momentum: float,
        secs_left: float,
        remaining_budget: float,
    ) -> bool:
        """执行一笔订单, 成功返回 True。"""
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

        # 流动性
        if not self._check_liquidity(ctx, direction):
            return False

        # edge 计算
        other_dir = "DOWN" if direction == "UP" else "UP"
        other_ask = self._get_ask_price(ctx, other_dir)
        combined_price = ask + other_ask if 0.01 <= other_ask <= 0.99 else 1.0
        edge = 1.0 - combined_price

        # ── 价格保护层 1: Edge 门控 ──
        if edge < self._min_edge:
            logger.debug(
                f"[{self.name()}] ✘ {direction} 拒绝: edge={edge:+.4f} "
                f"< min_edge={self._min_edge:+.4f} "
                f"(ask={ask:.4f} other={other_ask:.4f} combined={combined_price:.4f})"
            )
            return False

        # ── 价格保护层 2: 均价恶化检查 ──
        if direction == "UP" and self._cum_up_shares > 0:
            avg_up = self._cum_up_cost / self._cum_up_shares
            if ask > avg_up * (1.0 + self._price_deterioration_pct):
                logger.debug(
                    f"[{self.name()}] ✘ UP 拒绝: ask={ask:.4f} >> "
                    f"avg_up={avg_up:.4f}*(1+{self._price_deterioration_pct:.0%})"
                    f"={avg_up * (1 + self._price_deterioration_pct):.4f}"
                )
                return False
        elif direction == "DOWN" and self._cum_dn_shares > 0:
            avg_dn = self._cum_dn_cost / self._cum_dn_shares
            if ask > avg_dn * (1.0 + self._price_deterioration_pct):
                logger.debug(
                    f"[{self.name()}] ✘ DOWN 拒绝: ask={ask:.4f} >> "
                    f"avg_dn={avg_dn:.4f}*(1+{self._price_deterioration_pct:.0%})"
                    f"={avg_dn * (1 + self._price_deterioration_pct):.4f}"
                )
                return False

        gap = self._cum_up_shares - self._cum_dn_shares
        total = max(self._cum_up_shares, self._cum_dn_shares, 1)
        gap_pct = gap / total * 100

        # 计算时间衰减 scale (仅用于日志)
        widens_gap = (
            (direction == "UP" and gap > 0)
            or (direction == "DOWN" and gap < 0)
        )
        td_scale = self._time_gap_control(secs_left, widens_gap=widens_gap)

        now = ctx.now()

        logger.info(
            f"[{self.name()}] #{self._entries_this_window+1} → {direction} | "
            f"{order_shares:.1f}sh@{ask:.4f}=${cost:.2f} | "
            f"mom=${momentum:+.2f} gap={gap:+.1f}sh ({gap_pct:+.1f}%) | "
            f"edge={edge:+.4f} td_scale={td_scale:.2f}"
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
                entry_score=momentum,
                condition_id=cid,
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

            logger.info(
                f"[{self.name()}] #{self._entries_this_window} 成交 | "
                f"{direction}={filled_shares:.1f}sh@{ask:.4f}=${actual_cost:.2f} | "
                f"CumUP={self._cum_up_shares:.1f} CumDN={self._cum_dn_shares:.1f}"
            )

            self._record_trade(ctx, {
                "action": "ENTRY",
                "side": direction,
                "entry_num": self._entries_this_window,
                "price": round(ask, 4),
                "cost": round(actual_cost, 2),
                "shares": round(filled_shares, 2),
                "size": round(actual_cost, 2),
                "momentum": round(momentum, 2),
                "edge": round(edge, 4),
                "gap_shares": round(self._cum_up_shares - self._cum_dn_shares, 2),
                "cum_up_shares": round(self._cum_up_shares, 2),
                "cum_dn_shares": round(self._cum_dn_shares, 2),
                "secs_left": round(secs_left),
                "td_scale": round(td_scale, 3),
                "widens_gap": widens_gap,
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
        ptb = self._window_ptb if self._window_ptb > 0 else ctx.market.pm_window_start_price

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
            f"Entries={self._entries_this_window} Bursts={self._burst_count_this_window}"
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
            "bursts": self._burst_count_this_window,
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
                    "bursts": self._burst_count_this_window,
                    "result": result_str,
                    "balance_after": round(ctx.account.balance, 4),
                    "cumulative_pnl": round(self._cumulative_pnl, 4),
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
                logger.warning(f"[{self.name()}] 兑付超时放弃 | {cid[:18]}...")
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
                        f"tx={result.tx_hash[:18]}... USDC={result.usdc_received:.2f}"
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
                "momentum": self._current_momentum,
                "clob_side": "BUY",
                "tick_size": "0.01",
                "neg_risk": ctx.market.pm_neg_risk,
                # ── 策略上下文 (供 TradeLogger 记录) ──
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
                "burst_num": self._burst_count_this_window,
            },
        )
        return await executor.submit_order(request)

    def _publish_state(self, ctx: Context) -> None:
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

        mom = self._current_momentum
        has_signal = abs(mom) >= self._momentum_threshold
        now = ctx.now()
        is_choppy = self._is_choppy(now) if self._chop_guard_enabled else False

        state = {
            "name": self.name(),
            "version": self.version(),
            "momentum": round(self._current_momentum, 2),
            "direction": "UP" if mom > 0 else ("DOWN" if mom < 0 else "NEUTRAL"),
            "has_signal": has_signal,
            "is_choppy": is_choppy,
            "chop_block_count": self._chop_block_count,
            "up_position": up_dict,
            "dn_position": dn_dict,
            "position": combined_pos,
            "cum_up_shares": round(self._cum_up_shares, 2),
            "cum_dn_shares": round(self._cum_dn_shares, 2),
            "shares_gap": round(gap, 2),
            "shares_gap_pct": round(gap_pct, 1),
            "entries_this_window": self._entries_this_window,
            "bursts_this_window": self._burst_count_this_window,
            "up_progress_pct": round(
                min(100, self._cum_up_shares / self._target_shares_per_side * 100), 1
            ),
            "dn_progress_pct": round(
                min(100, self._cum_dn_shares / self._target_shares_per_side * 100), 1
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
        "entry_time", "entry_score", "condition_id",
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
    ) -> None:
        self.side = side
        self.entry_price = entry_price
        self.size = size
        self.shares = shares
        self.entry_time = entry_time
        self.entry_score = entry_score
        self.condition_id = condition_id

    def to_dict(self) -> dict[str, Any]:
        return {
            "side": self.side,
            "entry_price": self.entry_price,
            "size": self.size,
            "shares": self.shares,
            "entry_time": self.entry_time,
            "entry_score": self.entry_score,
            "condition_id": self.condition_id,
        }
