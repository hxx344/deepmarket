"""
BTC 5-min Tail Reversal Strategy v1.0

核心思路:
    窗口末段 (最后 60-15s), 市场价格已经反映了当前 BTC 方向:
    - BTC 涨 → UP 价格被推高 (如 0.92), DOWN 变便宜 (如 0.08)
    - 此时买入便宜那一侧, 赌结算前价格翻转
    - 成本极低 ($0.05-$0.15/share), 翻转则获 $0.85-$0.95 回报 (6-20x)

为什么有效:
    1. BTC 本身就是高波动资产, 5s 内 $10-50 波动很常见
    2. 尾盘扫单者或 0x1d 推高一侧价格 → 便宜侧 odds 被压到极端
    3. 如果最后 1-5s BTC 反向跳一下, 结算翻转 → 便宜侧收 $1
    4. 胜率低但盈亏比极高, 期望值可以为正

关键参数:
    - entry_window: 在窗口进度 80%-95% 之间寻找入场机会
    - price_threshold: 便宜侧价格 < 0.15 才入场 (至少 6.7x 赔率)
    - max_bet_per_window: 单窗口最大投入 (控制风险)
    - min_btc_volatility: 需要一定波动率才有翻转可能

风险:
    - 大部分时候会亏 (胜率可能只有 10-15%)
    - 但单次盈利是成本的 6-20 倍
    - 需要足够多的窗口来实现期望值
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


class TailReversalStrategy(Strategy):
    """
    BTC 5-min 尾盘反转策略

    在每个 5-min 窗口的末段, 当一侧被推到极端价格时,
    买入便宜的另一侧, 赌最后几秒 BTC 价格翻转.
    """

    def __init__(
        self,
        # ── 价格条件 (核心) ──
        target_ask_price: float = 0.01,        # ask = 0.01 时买入 (99x 赔率)
        # ── 下注大小 ──
        bet_size_usdc: float = 5.0,            # 每次下注金额 (USDC), 小注高赔率
        max_bets_per_window: int = 2,          # 单窗口最多下注次数 (UP+DOWN 各1次)
        max_cost_per_window: float = 20.0,     # 单窗口最大总投入
        # ── 方向确认 (已禁用, 保留接口兼容) ──
        require_btc_counter_move: bool = False,
        counter_move_lookback_s: float = 5.0,
        counter_move_threshold: float = 1.0,
        # ── 手续费 ──
        fee_rate: float = 0.002,
    ) -> None:
        # 价格条件
        self._target_ask = target_ask_price

        # 下注
        self._bet_size = bet_size_usdc
        self._max_bets_per_window = max_bets_per_window
        self._max_cost_per_window = max_cost_per_window

        # 方向确认
        self._require_counter_move = require_btc_counter_move
        self._counter_lookback_s = counter_move_lookback_s
        self._counter_threshold = counter_move_threshold

        # 手续费
        self._fee_rate = fee_rate

        # ── 运行时状态 ──
        self._btc_buffer: deque[tuple[float, float]] = deque(maxlen=1200)
        self._last_window_ts: int = 0
        self._bets_this_window: int = 0
        self._cost_this_window: float = 0.0
        self._window_ptb: float = 0.0
        self._bought_sides: set[str] = set()  # 本窗口已买入的方向 {"UP", "DOWN"}

        # ── 持仓 ──
        self._positions: list[dict] = []
        self._cum_up_shares: float = 0.0
        self._cum_dn_shares: float = 0.0
        self._cum_up_cost: float = 0.0
        self._cum_dn_cost: float = 0.0

        # ── 统计 ──
        self._trade_count: int = 0
        self._win_count: int = 0
        self._loss_count: int = 0
        self._cumulative_pnl: float = 0.0
        self._tick_counter: int = 0

        # ── RTDS偏离统计 (每笔下注时 BTC 偏离 PTB 的幅度) ──
        self._deviation_stats: list[dict] = []  # 全局: 记录每笔下注的偏离信息
        self._window_deviations: list[dict] = []  # 当前窗口

        # ── 下注冷却 (防止同tick多次下注) ──
        self._last_bet_time: float = 0.0
        self._bet_cooldown_s: float = 2.0

        # ── Dashboard 交易历史 (供面板显示) ──
        self._trade_history: list[dict] = []  # ENTRY / SETTLE 记录
        self._max_trade_history: int = 200

    # ================================================================
    #  Strategy 接口
    # ================================================================

    def name(self) -> str:
        return "tail_reversal"

    def version(self) -> str:
        return "1.0"

    def description(self) -> str:
        return (
            f"BTC 5-min Tail Reversal v1.0 ("
            f"ask={self._target_ask:.2f}, "
            f"bet=${self._bet_size:.0f}x{self._max_bets_per_window})"
        )

    def get_params(self) -> dict[str, Any]:
        return {
            "target_ask_price": self._target_ask,
            "bet_size_usdc": self._bet_size,
            "max_bets_per_window": self._max_bets_per_window,
            "max_cost_per_window": self._max_cost_per_window,
            "fee_rate": self._fee_rate,
        }

    def on_init(self, context: Context) -> None:
        logger.info(f"[{self.name()}] 策略初始化: {self.description()}")

    async def on_market_data(self, context: Context, data: dict[str, Any]) -> None:
        """主驱动: 每个 BTC tick 调用。"""
        # ── 窗口切换检测 ──
        await self._check_window_switch(context)

        # ── 记录 BTC 价格 ──
        btc = context.market.btc_price
        now = context.now()
        if btc > 0:
            self._btc_buffer.append((now, btc))

        # ── 诊断日志 (每 60 tick) ──
        self._tick_counter += 1
        if self._tick_counter % 60 == 1:
            self._log_status(context)

        # ── 尾盘入场逻辑 ──
        await self._tail_entry(context)

        # ── 推送状态到 Dashboard ──
        self._push_state(context)

    def on_stop(self, context: Context) -> None:
        self._log_deviation_summary()
        logger.info(
            f"[{self.name()}] 策略停止 | "
            f"总交易={self._trade_count} | "
            f"W={self._win_count} L={self._loss_count} | "
            f"累计PnL=${self._cumulative_pnl:+.2f}"
        )

    # ================================================================
    #  窗口切换
    # ================================================================

    async def _check_window_switch(self, context: Context) -> None:
        wst = context.market.pm_window_start_ts
        if wst > 0 and wst != self._last_window_ts:
            if self._last_window_ts > 0:
                await self._settle_position(context)
            self._last_window_ts = wst
            self._bets_this_window = 0
            self._cost_this_window = 0.0
            self._bought_sides.clear()
            self._cum_up_shares = 0.0
            self._cum_dn_shares = 0.0
            self._cum_up_cost = 0.0
            self._cum_dn_cost = 0.0
            self._positions.clear()
            self._window_deviations.clear()
            self._window_ptb = context.market.btc_price
            logger.info(
                f"[{self.name()}] 新窗口 PTB=${self._window_ptb:,.2f} | "
                f"等待 ask=0.01 买入机会"
            )

    # ================================================================
    #  核心: 尾盘反转入场
    # ================================================================

    async def _tail_entry(self, ctx: Context) -> None:
        """
        窗口内任意时刻, 只要任一方向 ask = target_ask (0.01) 就买入.

        规则:
            1. 每个方向每窗口只买一次 (UP 一次 + DOWN 一次 = 最多 2 笔)
            2. 不限制入场时间窗口
            3. 不要求波动率条件
            4. ask 必须精确等于 target_ask (0.01)
        """
        secs_left = ctx.market.pm_window_seconds_left
        if secs_left <= 0:
            return

        # ── 下注上限 ──
        if self._bets_this_window >= self._max_bets_per_window:
            return
        if self._cost_this_window >= self._max_cost_per_window:
            return

        # ── 冷却 ──
        now = ctx.now()
        if now - self._last_bet_time < self._bet_cooldown_s:
            return

        # ── 读取两侧 ask ──
        up_ask = ctx.market.pm_yes_ask or ctx.market.pm_yes_price
        dn_ask = ctx.market.pm_no_ask or ctx.market.pm_no_price
        if up_ask <= 0 or dn_ask <= 0:
            return

        btc = ctx.market.btc_price
        ptb = self._window_ptb if self._window_ptb > 0 else ctx.market.pm_window_start_price
        if ptb <= 0 or btc <= 0:
            return

        elapsed_pct = (300 - secs_left) / 300.0

        # ── 检查每个方向是否有 ask = target_ask 且未买过 ──
        candidates: list[tuple[str, float, Side]] = []
        if abs(up_ask - self._target_ask) < 0.001 and "UP" not in self._bought_sides:
            candidates.append(("UP", up_ask, Side.YES))
        if abs(dn_ask - self._target_ask) < 0.001 and "DOWN" not in self._bought_sides:
            candidates.append(("DOWN", dn_ask, Side.NO))

        if not candidates:
            return

        # ── 逐个买入 ──
        for cheap_side, cheap_ask, order_side in candidates:
            if self._bets_this_window >= self._max_bets_per_window:
                break
            if self._cost_this_window >= self._max_cost_per_window:
                break

            odds = (1.0 - cheap_ask) / cheap_ask if cheap_ask > 0 else 0

            remaining_budget = self._max_cost_per_window - self._cost_this_window
            bet = min(self._bet_size, remaining_budget, ctx.account.available * 0.05)
            if bet < 1.0:
                break

            potential_payout = bet / cheap_ask
            potential_profit = potential_payout - bet

            btc_diff = abs(btc - ptb)
            logger.info(
                f"[{self.name()}] 🎯 ask=0.01 触发! | "
                f"买{cheap_side}@{cheap_ask:.4f} ${bet:.2f} | "
                f"赔率={odds:.1f}x 潜在利润=${potential_profit:.2f} | "
                f"BTC={btc:,.2f} vs PTB={ptb:,.2f} diff=${btc_diff:.2f} | "
                f"elapsed={elapsed_pct:.1%} secs_left={secs_left:.0f}"
            )

            # ── 下单 ──
            result = await self._submit_order(ctx, order_side, cheap_ask, bet)

            if result and result.status == ExecutionStatus.FILLED:
                filled_shares = result.filled_size
                actual_cost = filled_shares * cheap_ask

                if cheap_side == "UP":
                    self._cum_up_shares += filled_shares
                    self._cum_up_cost += actual_cost
                else:
                    self._cum_dn_shares += filled_shares
                    self._cum_dn_cost += actual_cost

                self._bets_this_window += 1
                self._cost_this_window += actual_cost
                self._last_bet_time = now
                self._trade_count += 1
                self._bought_sides.add(cheap_side)

                # ── 记录RTDS偏离度 ──
                deviation = btc - ptb
                abs_dev = abs(deviation)
                dev_record = {
                    "side": cheap_side,
                    "deviation": deviation,
                    "abs_deviation": abs_dev,
                    "btc": btc,
                    "ptb": ptb,
                    "entry_time": now,
                }
                self._window_deviations.append(dev_record)
                self._deviation_stats.append(dev_record)

                self._positions.append({
                    "side": cheap_side,
                    "entry_price": cheap_ask,
                    "shares": filled_shares,
                    "cost": actual_cost,
                    "entry_time": now,
                    "odds": odds,
                    "btc_at_entry": btc,
                    "ptb": ptb,
                    "deviation": deviation,
                })

                ctx.account.balance -= actual_cost
                ctx.account.available -= actual_cost

                # ── 记录到 Dashboard 交易历史 ──
                import datetime as _dt
                self._trade_history.append({
                    "action": "ENTRY",
                    "time": _dt.datetime.fromtimestamp(now).strftime("%H:%M:%S"),
                    "side": cheap_side,
                    "price": round(cheap_ask, 4),
                    "shares": round(filled_shares, 1),
                    "cost": round(actual_cost, 2),
                    "odds": round(odds, 1),
                    "deviation": round(deviation, 2),
                    "btc": round(btc, 2),
                    "ptb": round(ptb, 2),
                    "balance_after": round(ctx.account.balance, 2),
                    "elapsed_pct": round(elapsed_pct * 100, 1),
                    "secs_left": round(secs_left, 0),
                })
                if len(self._trade_history) > self._max_trade_history:
                    self._trade_history = self._trade_history[-self._max_trade_history:]

                logger.info(
                    f"[{self.name()}] ✓ 成交 {cheap_side} | "
                    f"{filled_shares:.1f}sh@{cheap_ask:.4f}=${actual_cost:.2f} | "
                    f"RTDS偏离PTB=${deviation:+.2f} (|{abs_dev:.2f}|) | "
                    f"累计投入: ${self._cost_this_window:.2f}/{self._max_cost_per_window:.0f}"
                )

    # ================================================================
    #  结算
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

        btc_up = btc > ptb
        winner_side = "UP" if btc_up else "DOWN"

        up_shares = self._cum_up_shares
        dn_shares = self._cum_dn_shares
        up_cost = self._cum_up_cost
        dn_cost = self._cum_dn_cost
        total_cost = up_cost + dn_cost

        if total_cost == 0:
            return

        payout = up_shares * 1.0 if winner_side == "UP" else dn_shares * 1.0
        fee = total_cost * self._fee_rate
        net_pnl = payout - total_cost - fee

        won = net_pnl > 0
        result_str = "🎉 WIN" if won else "✗ LOSE"

        if won:
            self._win_count += 1
        else:
            self._loss_count += 1
        self._cumulative_pnl += net_pnl

        # 更新余额
        ctx.account.balance += payout - fee
        ctx.account.available += payout - fee

        # 每一注的详情
        for pos in self._positions:
            p_won = pos["side"] == winner_side
            p_pnl = (pos["shares"] * 1.0 - pos["cost"]) if p_won else (-pos["cost"])
            p_str = "WIN" if p_won else "LOSE"
            dev = pos.get("deviation", 0)
            logger.info(
                f"[{self.name()}]   {p_str} {pos['side']}@{pos['entry_price']:.4f} | "
                f"{pos['shares']:.1f}sh cost=${pos['cost']:.2f} | "
                f"PnL=${p_pnl:+.2f} odds={pos['odds']:.1f}x | "
                f"RTDS偏离=${dev:+.2f}"
            )

        # ── RTDS偏离统计 ──
        dev_str = ""
        if self._window_deviations:
            devs = [d["abs_deviation"] for d in self._window_deviations]
            avg_dev = sum(devs) / len(devs)
            max_dev = max(devs)
            dev_str = f" | 入场偏离: avg=${avg_dev:.2f} max=${max_dev:.2f}"

        logger.info(
            f"[{self.name()}] 结算 {result_str} | "
            f"赢家={winner_side} | BTC={btc:,.2f} vs PTB={ptb:,.2f} | "
            f"投入=${total_cost:.2f} 回收=${payout:.2f} PnL=${net_pnl:+.2f}{dev_str} | "
            f"累计: W={self._win_count} L={self._loss_count} PnL=${self._cumulative_pnl:+.2f} | "
            f"余额=${ctx.account.balance:.2f}"
        )

        # ── 记录到 Dashboard 交易历史 ──
        import datetime as _dt
        self._trade_history.append({
            "action": "SETTLE",
            "time": _dt.datetime.now().strftime("%H:%M:%S"),
            "winner": winner_side,
            "result": "WIN" if won else "LOSE",
            "up_shares": round(up_shares, 1),
            "dn_shares": round(dn_shares, 1),
            "size": round(total_cost, 2),
            "payout": round(payout, 2),
            "pnl": round(net_pnl, 4),
            "btc": round(btc, 2),
            "ptb": round(ptb, 2),
            "deviation_avg": round(sum(d["abs_deviation"] for d in self._window_deviations) / len(self._window_deviations), 2) if self._window_deviations else 0,
        })
        if len(self._trade_history) > self._max_trade_history:
            self._trade_history = self._trade_history[-self._max_trade_history:]

        # ── 全局偏离统计摘要 (每10次结算打一次) ──
        total_settles = self._win_count + self._loss_count
        if total_settles > 0 and total_settles % 10 == 0:
            self._log_deviation_summary()

        self._positions.clear()

    # ================================================================
    #  Dashboard 状态推送
    # ================================================================

    def _push_state(self, ctx: Context) -> None:
        """将策略状态推送到 Dashboard via context + EventBus."""
        secs_left = ctx.market.pm_window_seconds_left
        elapsed_pct = (300 - secs_left) / 300.0 if secs_left > 0 else 0
        btc = ctx.market.btc_price
        ptb = self._window_ptb if self._window_ptb > 0 else ctx.market.pm_window_start_price
        deviation = btc - ptb if ptb > 0 else 0
        abs_dev = abs(deviation)

        up_ask = ctx.market.pm_yes_ask or ctx.market.pm_yes_price
        dn_ask = ctx.market.pm_no_ask or ctx.market.pm_no_price

        # 判断哪侧便宜 (直接比较 ask 价格, 选更低的)
        if up_ask and dn_ask and up_ask > 0 and dn_ask > 0:
            if up_ask <= dn_ask:
                cheap_side = "UP"
                cheap_ask = up_ask
            else:
                cheap_side = "DOWN"
                cheap_ask = dn_ask
        else:
            # fallback: 根据 BTC 方向推断
            btc_up = btc > ptb if ptb > 0 else False
            cheap_side = "DOWN" if btc_up else "UP"
            cheap_ask = dn_ask if btc_up else up_ask

        # 入场状态判断
        up_ready = up_ask and abs(up_ask - self._target_ask) < 0.001 and "UP" not in self._bought_sides
        dn_ready = dn_ask and abs(dn_ask - self._target_ask) < 0.001 and "DOWN" not in self._bought_sides
        if up_ready or dn_ready:
            zone = "READY"
        elif self._bets_this_window >= self._max_bets_per_window:
            zone = "FULL"
        else:
            zone = "SCAN"

        total_games = self._win_count + self._loss_count

        state = {
            "name": self.name(),
            "version": self.version(),
            "strategy_type": "tail_reversal",
            # ── 市场数据 ──
            "rtds_price": round(btc, 2),
            "window_ptb": round(ptb, 2),
            "deviation": round(deviation, 2),
            "abs_deviation": round(abs_dev, 2),
            "up_ask": round(up_ask, 4) if up_ask else 0,
            "dn_ask": round(dn_ask, 4) if dn_ask else 0,
            "cheap_side": cheap_side,
            "cheap_ask": round(cheap_ask, 4) if cheap_ask else 0,
            # ── 窗口进度 ──
            "elapsed_pct": round(elapsed_pct * 100, 1),
            "secs_left": round(secs_left, 0),
            "entry_zone": zone,
            "target_ask": self._target_ask,
            "bought_sides": list(self._bought_sides),
            # ── 当前窗口下注 ──
            "bets_this_window": self._bets_this_window,
            "max_bets_per_window": self._max_bets_per_window,
            "cost_this_window": round(self._cost_this_window, 2),
            "max_cost_per_window": self._max_cost_per_window,
            # ── 持仓 ──
            "cum_up_shares": round(self._cum_up_shares, 1),
            "cum_dn_shares": round(self._cum_dn_shares, 1),
            "cum_up_cost": round(self._cum_up_cost, 2),
            "cum_dn_cost": round(self._cum_dn_cost, 2),
            "positions": self._positions[-10:],  # 最近10笔
            "has_position": len(self._positions) > 0,
            # ── 统计 ──
            "trade_count": self._trade_count,
            "win_count": self._win_count,
            "loss_count": self._loss_count,
            "cumulative_pnl": round(self._cumulative_pnl, 4),
            "win_rate": round(
                self._win_count / max(total_games, 1) * 100, 1
            ),
            # ── 波动率 ──
            "btc_vol_30s": round(self._calc_btc_vol(ctx.now(), 30.0), 2),
            # ── 账户 ──
            "account": {
                "balance": round(ctx.account.balance, 2),
                "available": round(ctx.account.available, 2),
                "total_equity": round(ctx.account.total_equity, 2),
                "daily_pnl": round(ctx.account.daily_pnl, 4),
            },
            # ── 历史 ──
            "trade_history": self._trade_history,
            # ── 参数 ──
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
    #  辅助函数
    # ================================================================

    def _log_deviation_summary(self) -> None:
        """输出全局 RTDS 偏离统计, 按 WIN/LOSE 分组分析."""
        if not self._deviation_stats:
            return

        # 关联 win/lose (简化: 按顺序与 _win_count/_loss_count 对应)
        all_devs = [d["abs_deviation"] for d in self._deviation_stats]
        avg_all = sum(all_devs) / len(all_devs)
        min_all = min(all_devs)
        max_all = max(all_devs)

        # 按偏离大小分桶
        small = [d for d in all_devs if d <= 10]
        med = [d for d in all_devs if 10 < d <= 30]
        large = [d for d in all_devs if d > 30]

        logger.info(
            f"[{self.name()}] 📊 RTDS偏离统计 (全局{len(all_devs)}笔) | "
            f"avg=${avg_all:.2f} min=${min_all:.2f} max=${max_all:.2f} | "
            f"≤$10: {len(small)}笔, $10-30: {len(med)}笔, >$30: {len(large)}笔"
        )

    def _calc_btc_vol(self, now: float, lookback_s: float = 30.0) -> float:
        """计算 BTC 在近 lookback_s 秒内的价格波动幅度 (high - low)."""
        cutoff = now - lookback_s
        prices = [p for t, p in self._btc_buffer if t >= cutoff]
        if len(prices) < 5:
            return 0.0
        return max(prices) - min(prices)

    def _check_counter_move(self, now: float, currently_btc_up: bool) -> bool:
        """
        检测 BTC 是否在短期出现了反向运动.
        如果当前 BTC > PTB (UP领先), 检测最近 5s 是否有回落 ≥ threshold.
        """
        cutoff = now - self._counter_lookback_s
        recent = [(t, p) for t, p in self._btc_buffer if t >= cutoff]
        if len(recent) < 3:
            return False

        if currently_btc_up:
            # BTC 在涨, 检测是否从近期高点有回落
            peak = max(p for _, p in recent)
            current = recent[-1][1]
            drop = peak - current
            return drop >= self._counter_threshold
        else:
            # BTC 在跌, 检测是否从近期低点有反弹
            trough = min(p for _, p in recent)
            current = recent[-1][1]
            bounce = current - trough
            return bounce >= self._counter_threshold

    async def _submit_order(
        self, ctx: Context, side: Side, price: float, size: float
    ) -> "OrderResult | None":
        """提交订单 (兼容纸交易和实盘)."""
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
                "strategy": "tail_reversal",
                "cheap_side_price": price,
                "odds": (1.0 - price) / price if price > 0 else 0,
                "clob_side": "BUY",
                "tick_size": "0.01",
                "neg_risk": ctx.market.pm_neg_risk,
            },
        )
        return await executor.submit_order(request)

    def _log_status(self, ctx: Context) -> None:
        secs_left = ctx.market.pm_window_seconds_left
        btc = ctx.market.btc_price
        ptb = self._window_ptb
        up_ask = ctx.market.pm_yes_ask or ctx.market.pm_yes_price
        dn_ask = ctx.market.pm_no_ask or ctx.market.pm_no_price

        elapsed_pct = (300 - secs_left) / 300.0 if secs_left > 0 else 0
        zone = "[等待]" if elapsed_pct < self._entry_start_pct else "[观察]"
        if self._bets_this_window > 0:
            zone = f"[已下{self._bets_this_window}注]"

        btc_diff = btc - ptb if ptb > 0 else 0
        logger.debug(
            f"[{self.name()}] {zone} | "
            f"BTC={btc:,.2f} PTB={ptb:,.2f} diff={btc_diff:+.2f} | "
            f"UP_ask={up_ask:.4f} DN_ask={dn_ask:.4f} | "
            f"elapsed={elapsed_pct:.1%} left={secs_left:.0f}s | "
            f"bets={self._bets_this_window}/{self._max_bets_per_window} "
            f"cost=${self._cost_this_window:.2f} | "
            f"cum PnL=${self._cumulative_pnl:+.2f}"
        )
