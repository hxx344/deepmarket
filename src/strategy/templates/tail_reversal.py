"""
Multi-Symbol 5-min Tail Reversal Strategy v2.0

核心思路:
    窗口末段, 市场价格已经反映了当前价格方向:
    - 价格涨 → UP 价格被推高 (如 0.92), DOWN 变便宜 (如 0.08)
    - 此时买入便宜那一侧, 赌结算前价格翻转
    - 成本极低 ($0.05-$0.15/share), 翻转则获 $0.85-$0.95 回报 (6-20x)

支持币种:
    - BTC / ETH / XRP 的 5-min Up/Down 市场
    - 每个币种独立管道, 独立统计

关键参数:
    - target_ask_price: ask = 0.01 时入场 (99x 赔率)
    - max_bets_per_window: 每币种每窗口最大下注次数
    - bet_size_usdc: 每注金额

风险:
    - 大部分时候会亏 (胜率可能只有 10-15%)
    - 但单次盈利是成本的 6-20 倍
    - 需要足够多的窗口来实现期望值
"""

from __future__ import annotations

import asyncio
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any

from loguru import logger

from src.core.context import Context, MarketState
from src.core.event_bus import Event, EventType
from src.strategy.base import Strategy
from src.trading.executor import ExecutionStatus, OrderRequest, OrderResult, OrderType, Side


# ────────────────────── 每币种运行时状态 ──────────────────────

@dataclass
class _SymbolState:
    """单个币种的独立运行时状态 (窗口/持仓/统计)"""

    symbol: str = ""

    # ── 窗口状态 ──
    last_window_ts: int = 0
    bets_this_window: int = 0
    cost_this_window: float = 0.0
    window_ptb: float = 0.0          # 窗口开始时的价格 (Price at Time Boundary)
    bought_sides: set = field(default_factory=set)

    # ── 持仓 ──
    positions: list = field(default_factory=list)
    cum_up_shares: float = 0.0
    cum_dn_shares: float = 0.0
    cum_up_cost: float = 0.0
    cum_dn_cost: float = 0.0

    # ── 价格缓冲 (计算波动率等) ──
    price_buffer: deque = field(default_factory=lambda: deque(maxlen=1200))

    # ── 统计 ──
    trade_count: int = 0
    win_count: int = 0
    loss_count: int = 0
    cumulative_pnl: float = 0.0
    tick_counter: int = 0

    # ── 偏离统计 ──
    deviation_stats: list = field(default_factory=list)
    window_deviations: list = field(default_factory=list)

    # ── 下注冷却 ──
    last_bet_time: float = 0.0

    # ── Dashboard 交易历史 ──
    trade_history: list = field(default_factory=list)


class TailReversalStrategy(Strategy):
    """
    Multi-Symbol 5-min 尾盘反转策略

    在每个 5-min 窗口中, 当任一方向被推到 ask = target_ask (0.01) 时,
    买入该方向, 赌结算前价格翻转.
    BTC / ETH / XRP 三个市场独立管道, 独立统计.
    """

    def __init__(
        self,
        # ── 价格条件 (核心) ──
        target_ask_price: float = 0.01,
        # ── 下注大小 ──
        bet_size_usdc: float = 5.0,
        max_bets_per_window: int = 2,
        max_cost_per_window: float = 20.0,
        # ── 方向确认 (已禁用, 保留接口兼容) ──
        require_btc_counter_move: bool = False,
        counter_move_lookback_s: float = 5.0,
        counter_move_threshold: float = 1.0,
        # ── 手续费 ──
        fee_rate: float = 0.002,
        # ── 币种 ──
        symbols: list[str] | None = None,
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

        # ── 币种管道 ──
        self._symbols: list[str] = symbols or ["btc"]
        self._sym_states: dict[str, _SymbolState] = {
            s: _SymbolState(symbol=s) for s in self._symbols
        }

        # ── 全局限制 ──
        self._bet_cooldown_s: float = 2.0
        self._max_trade_history: int = 200

    # ================================================================
    #  Strategy 接口
    # ================================================================

    def name(self) -> str:
        return "tail_reversal"

    def version(self) -> str:
        return "2.0"

    def description(self) -> str:
        syms = "/".join(s.upper() for s in self._symbols)
        return (
            f"{syms} 5-min Tail Reversal v2.0 ("
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
            "symbols": self._symbols,
        }

    def on_init(self, context: Context) -> None:
        logger.info(f"[{self.name()}] 策略初始化: {self.description()}")

    async def on_market_data(self, context: Context, data: dict[str, Any]) -> None:
        """
        主驱动: 每个 tick 调用, 遍历所有币种管道.

        data 中可能包含 symbol 用于指示哪个币种触发了本次 tick,
        但我们仍然检查所有币种的状态 (防止漏看窗口切换).
        """
        now = context.now()

        for sym in self._symbols:
            mkt = context.markets.get(sym)
            if not mkt:
                continue

            ss = self._sym_states[sym]
            price = mkt.price if mkt.price > 0 else mkt.btc_price

            # ── 记录价格 ──
            if price > 0:
                ss.price_buffer.append((now, price))

            # ── 窗口切换检测 ──
            await self._check_window_switch(context, mkt, ss)

            # ── 诊断日志 (每 60 tick) ──
            ss.tick_counter += 1
            if ss.tick_counter % 60 == 1:
                self._log_status(mkt, ss)

            # ── 尾盘入场逻辑 ──
            await self._tail_entry(context, mkt, ss)

        # ── 推送状态到 Dashboard ──
        self._push_state(context)

    def on_stop(self, context: Context) -> None:
        for sym, ss in self._sym_states.items():
            self._log_deviation_summary(ss)
            logger.info(
                f"[{self.name()}:{sym.upper()}] 策略停止 | "
                f"总交易={ss.trade_count} | "
                f"W={ss.win_count} L={ss.loss_count} | "
                f"累计PnL=${ss.cumulative_pnl:+.2f}"
            )

    # ================================================================
    #  窗口切换
    # ================================================================

    async def _check_window_switch(
        self, ctx: Context, mkt: MarketState, ss: _SymbolState
    ) -> None:
        wst = mkt.pm_window_start_ts
        if wst > 0 and wst != ss.last_window_ts:
            if ss.last_window_ts > 0:
                await self._settle_position(ctx, mkt, ss)
            ss.last_window_ts = wst
            ss.bets_this_window = 0
            ss.cost_this_window = 0.0
            ss.bought_sides.clear()
            ss.cum_up_shares = 0.0
            ss.cum_dn_shares = 0.0
            ss.cum_up_cost = 0.0
            ss.cum_dn_cost = 0.0
            ss.positions.clear()
            ss.window_deviations.clear()
            ss.window_ptb = mkt.price if mkt.price > 0 else mkt.btc_price
            logger.info(
                f"[{self.name()}:{ss.symbol.upper()}] 新窗口 PTB=${ss.window_ptb:,.2f} | "
                f"等待 ask=0.01 买入机会"
            )

    # ================================================================
    #  核心: 尾盘反转入场
    # ================================================================

    async def _tail_entry(
        self, ctx: Context, mkt: MarketState, ss: _SymbolState
    ) -> None:
        """
        窗口内任意时刻, 只要任一方向 ask = target_ask (0.01) 就买入.

        规则:
            1. 每个方向每窗口只买一次 (UP 一次 + DOWN 一次 = 最多 2 笔)
            2. 不限制入场时间窗口
            3. 不要求波动率条件
            4. ask 必须精确等于 target_ask (0.01)
        """
        secs_left = mkt.pm_window_seconds_left
        if secs_left <= 0:
            return

        # ── 窗口开始 5s 内不交易 (等价格稳定) ──
        elapsed = 300 - secs_left
        if elapsed < 5:
            return

        # ── 下注上限 ──
        if ss.bets_this_window >= self._max_bets_per_window:
            return
        if ss.cost_this_window >= self._max_cost_per_window:
            return

        # ── 冷却 ──
        now = ctx.now()
        if now - ss.last_bet_time < self._bet_cooldown_s:
            return

        # ── 读取两侧 ask ──
        up_ask = mkt.pm_yes_ask or mkt.pm_yes_price
        dn_ask = mkt.pm_no_ask or mkt.pm_no_price
        if up_ask <= 0 or dn_ask <= 0:
            return

        price = mkt.price if mkt.price > 0 else mkt.btc_price
        ptb = ss.window_ptb if ss.window_ptb > 0 else mkt.pm_window_start_price
        if ptb <= 0 or price <= 0:
            return

        elapsed_pct = (300 - secs_left) / 300.0

        # ── 检查每个方向是否有 ask = target_ask 且未买过 ──
        candidates: list[tuple[str, float, Side]] = []
        if abs(up_ask - self._target_ask) < 0.001 and "UP" not in ss.bought_sides:
            candidates.append(("UP", up_ask, Side.YES))
        if abs(dn_ask - self._target_ask) < 0.001 and "DOWN" not in ss.bought_sides:
            candidates.append(("DOWN", dn_ask, Side.NO))

        if not candidates:
            return

        # ── 逐个买入 ──
        for cheap_side, cheap_ask, order_side in candidates:
            if ss.bets_this_window >= self._max_bets_per_window:
                break
            if ss.cost_this_window >= self._max_cost_per_window:
                break

            odds = (1.0 - cheap_ask) / cheap_ask if cheap_ask > 0 else 0

            remaining_budget = self._max_cost_per_window - ss.cost_this_window
            bet = min(self._bet_size, remaining_budget, ctx.account.available * 0.05)
            if bet < 1.0:
                break

            potential_payout = bet / cheap_ask
            potential_profit = potential_payout - bet

            price_diff = abs(price - ptb)
            logger.info(
                f"[{self.name()}:{ss.symbol.upper()}] 🎯 ask=0.01 触发! | "
                f"买{cheap_side}@{cheap_ask:.4f} ${bet:.2f} | "
                f"赔率={odds:.1f}x 潜在利润=${potential_profit:.2f} | "
                f"price={price:,.2f} vs PTB={ptb:,.2f} diff=${price_diff:.2f} | "
                f"elapsed={elapsed_pct:.1%} secs_left={secs_left:.0f}"
            )

            # ── 下单 ──
            result = await self._submit_order(ctx, mkt, order_side, cheap_ask, bet)

            if result and result.status == ExecutionStatus.FILLED:
                filled_shares = result.filled_size
                actual_cost = filled_shares * cheap_ask

                if cheap_side == "UP":
                    ss.cum_up_shares += filled_shares
                    ss.cum_up_cost += actual_cost
                else:
                    ss.cum_dn_shares += filled_shares
                    ss.cum_dn_cost += actual_cost

                ss.bets_this_window += 1
                ss.cost_this_window += actual_cost
                ss.last_bet_time = now
                ss.trade_count += 1
                ss.bought_sides.add(cheap_side)

                # ── 记录偏离度 ──
                deviation = price - ptb
                abs_dev = abs(deviation)
                dev_record = {
                    "symbol": ss.symbol,
                    "side": cheap_side,
                    "deviation": deviation,
                    "abs_deviation": abs_dev,
                    "price": price,
                    "ptb": ptb,
                    "entry_time": now,
                }
                ss.window_deviations.append(dev_record)
                ss.deviation_stats.append(dev_record)

                ss.positions.append({
                    "side": cheap_side,
                    "entry_price": cheap_ask,
                    "shares": filled_shares,
                    "cost": actual_cost,
                    "entry_time": now,
                    "odds": odds,
                    "price_at_entry": price,
                    "ptb": ptb,
                    "deviation": deviation,
                })

                ctx.account.balance -= actual_cost
                ctx.account.available -= actual_cost

                # ── 记录到 Dashboard 交易历史 ──
                import datetime as _dt
                ss.trade_history.append({
                    "action": "ENTRY",
                    "symbol": ss.symbol.upper(),
                    "time": _dt.datetime.fromtimestamp(now).strftime("%H:%M:%S"),
                    "side": cheap_side,
                    "price": round(cheap_ask, 4),
                    "shares": round(filled_shares, 1),
                    "cost": round(actual_cost, 2),
                    "odds": round(odds, 1),
                    "deviation": round(deviation, 2),
                    "coin_price": round(price, 2),
                    "ptb": round(ptb, 2),
                    "balance_after": round(ctx.account.balance, 2),
                    "elapsed_pct": round(elapsed_pct * 100, 1),
                    "secs_left": round(secs_left, 0),
                })
                if len(ss.trade_history) > self._max_trade_history:
                    ss.trade_history = ss.trade_history[-self._max_trade_history:]

                logger.info(
                    f"[{self.name()}:{ss.symbol.upper()}] ✓ 成交 {cheap_side} | "
                    f"{filled_shares:.1f}sh@{cheap_ask:.4f}=${actual_cost:.2f} | "
                    f"偏离PTB=${deviation:+.2f} (|{abs_dev:.2f}|) | "
                    f"累计投入: ${ss.cost_this_window:.2f}/{self._max_cost_per_window:.0f}"
                )

    # ================================================================
    #  结算
    # ================================================================

    async def _settle_position(
        self, ctx: Context, mkt: MarketState, ss: _SymbolState
    ) -> None:
        if not ss.positions:
            return

        price = mkt.price if mkt.price > 0 else mkt.btc_price
        ptb = ss.window_ptb if ss.window_ptb > 0 else mkt.pm_window_start_price

        if ptb <= 0:
            logger.warning(f"[{self.name()}:{ss.symbol.upper()}] 结算时 PTB 不可用")
            ss.positions.clear()
            return

        price_up = price > ptb
        winner_side = "UP" if price_up else "DOWN"

        up_shares = ss.cum_up_shares
        dn_shares = ss.cum_dn_shares
        up_cost = ss.cum_up_cost
        dn_cost = ss.cum_dn_cost
        total_cost = up_cost + dn_cost

        if total_cost == 0:
            return

        payout = up_shares * 1.0 if winner_side == "UP" else dn_shares * 1.0
        fee = total_cost * self._fee_rate
        net_pnl = payout - total_cost - fee

        won = net_pnl > 0
        result_str = "🎉 WIN" if won else "✗ LOSE"

        if won:
            ss.win_count += 1
        else:
            ss.loss_count += 1
        ss.cumulative_pnl += net_pnl

        # 更新余额
        ctx.account.balance += payout - fee
        ctx.account.available += payout - fee

        # 每一注的详情
        for pos in ss.positions:
            p_won = pos["side"] == winner_side
            p_pnl = (pos["shares"] * 1.0 - pos["cost"]) if p_won else (-pos["cost"])
            p_str = "WIN" if p_won else "LOSE"
            dev = pos.get("deviation", 0)
            logger.info(
                f"[{self.name()}:{ss.symbol.upper()}]   {p_str} {pos['side']}@{pos['entry_price']:.4f} | "
                f"{pos['shares']:.1f}sh cost=${pos['cost']:.2f} | "
                f"PnL=${p_pnl:+.2f} odds={pos['odds']:.1f}x | "
                f"偏离=${dev:+.2f}"
            )

        # ── 偏离统计 ──
        dev_str = ""
        if ss.window_deviations:
            devs = [d["abs_deviation"] for d in ss.window_deviations]
            avg_dev = sum(devs) / len(devs)
            max_dev = max(devs)
            dev_str = f" | 入场偏离: avg=${avg_dev:.2f} max=${max_dev:.2f}"

        logger.info(
            f"[{self.name()}:{ss.symbol.upper()}] 结算 {result_str} | "
            f"赢家={winner_side} | price={price:,.2f} vs PTB={ptb:,.2f} | "
            f"投入=${total_cost:.2f} 回收=${payout:.2f} PnL=${net_pnl:+.2f}{dev_str} | "
            f"累计: W={ss.win_count} L={ss.loss_count} PnL=${ss.cumulative_pnl:+.2f} | "
            f"余额=${ctx.account.balance:.2f}"
        )

        # ── 记录到 Dashboard 交易历史 ──
        import datetime as _dt
        ss.trade_history.append({
            "action": "SETTLE",
            "symbol": ss.symbol.upper(),
            "time": _dt.datetime.now().strftime("%H:%M:%S"),
            "winner": winner_side,
            "result": "WIN" if won else "LOSE",
            "up_shares": round(up_shares, 1),
            "dn_shares": round(dn_shares, 1),
            "size": round(total_cost, 2),
            "payout": round(payout, 2),
            "pnl": round(net_pnl, 4),
            "coin_price": round(price, 2),
            "ptb": round(ptb, 2),
            "deviation_avg": round(
                sum(d["abs_deviation"] for d in ss.window_deviations)
                / len(ss.window_deviations), 2
            ) if ss.window_deviations else 0,
        })
        if len(ss.trade_history) > self._max_trade_history:
            ss.trade_history = ss.trade_history[-self._max_trade_history:]

        # ── 全局偏离统计摘要 (每10次结算打一次) ──
        total_settles = ss.win_count + ss.loss_count
        if total_settles > 0 and total_settles % 10 == 0:
            self._log_deviation_summary(ss)

        ss.positions.clear()

    # ================================================================
    #  Dashboard 状态推送
    # ================================================================

    def _push_state(self, ctx: Context) -> None:
        """
        将策略状态推送到 Dashboard via context + EventBus.

        输出格式:
        {
            "name": ...,
            "symbols": {...},   # 每币种独立数据
            "aggregate": {...}, # 汇总统计
            "account": {...},
            "params": {...},
        }
        """
        symbols_data: dict[str, dict] = {}

        total_trade_count = 0
        total_win_count = 0
        total_loss_count = 0
        total_pnl = 0.0
        all_trade_history: list[dict] = []

        for sym in self._symbols:
            mkt = ctx.markets.get(sym)
            ss = self._sym_states.get(sym)
            if not mkt or not ss:
                continue

            secs_left = mkt.pm_window_seconds_left
            elapsed_pct = (300 - secs_left) / 300.0 if secs_left > 0 else 0
            price = mkt.price if mkt.price > 0 else mkt.btc_price
            ptb = ss.window_ptb if ss.window_ptb > 0 else mkt.pm_window_start_price
            deviation = price - ptb if ptb > 0 else 0
            abs_dev = abs(deviation)

            up_ask = mkt.pm_yes_ask or mkt.pm_yes_price
            dn_ask = mkt.pm_no_ask or mkt.pm_no_price

            # 判断哪侧便宜
            if up_ask and dn_ask and up_ask > 0 and dn_ask > 0:
                if up_ask <= dn_ask:
                    cheap_side = "UP"
                    cheap_ask = up_ask
                else:
                    cheap_side = "DOWN"
                    cheap_ask = dn_ask
            else:
                price_up = price > ptb if ptb > 0 else False
                cheap_side = "DOWN" if price_up else "UP"
                cheap_ask = dn_ask if price_up else up_ask

            # 入场状态
            up_ready = up_ask and abs(up_ask - self._target_ask) < 0.001 and "UP" not in ss.bought_sides
            dn_ready = dn_ask and abs(dn_ask - self._target_ask) < 0.001 and "DOWN" not in ss.bought_sides
            if up_ready or dn_ready:
                zone = "READY"
            elif ss.bets_this_window >= self._max_bets_per_window:
                zone = "FULL"
            else:
                zone = "SCAN"

            total_games = ss.win_count + ss.loss_count
            total_trade_count += ss.trade_count
            total_win_count += ss.win_count
            total_loss_count += ss.loss_count
            total_pnl += ss.cumulative_pnl
            all_trade_history.extend(ss.trade_history)

            symbols_data[sym] = {
                "symbol": sym.upper(),
                # ── 市场数据 ──
                "rtds_price": round(price, 2),
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
                "bought_sides": list(ss.bought_sides),
                # ── 当前窗口下注 ──
                "bets_this_window": ss.bets_this_window,
                "max_bets_per_window": self._max_bets_per_window,
                "cost_this_window": round(ss.cost_this_window, 2),
                "max_cost_per_window": self._max_cost_per_window,
                # ── 持仓 ──
                "cum_up_shares": round(ss.cum_up_shares, 1),
                "cum_dn_shares": round(ss.cum_dn_shares, 1),
                "cum_up_cost": round(ss.cum_up_cost, 2),
                "cum_dn_cost": round(ss.cum_dn_cost, 2),
                "positions": ss.positions[-10:],
                "has_position": len(ss.positions) > 0,
                # ── 统计 ──
                "trade_count": ss.trade_count,
                "win_count": ss.win_count,
                "loss_count": ss.loss_count,
                "cumulative_pnl": round(ss.cumulative_pnl, 4),
                "win_rate": round(
                    ss.win_count / max(total_games, 1) * 100, 1
                ),
                # ── 波动率 ──
                "vol_30s": round(self._calc_vol(ss, ctx.now(), 30.0), 2),
                # ── 历史 ──
                "trade_history": ss.trade_history,
            }

        # 汇总交易历史 (按时间排序)
        all_trade_history.sort(key=lambda x: x.get("time", ""))

        total_total_games = total_win_count + total_loss_count
        state = {
            "name": self.name(),
            "version": self.version(),
            "strategy_type": "tail_reversal",
            # ── 每币种数据 ──
            "symbols": symbols_data,
            "symbol_list": self._symbols,
            # ── 汇总统计 ──
            "aggregate": {
                "trade_count": total_trade_count,
                "win_count": total_win_count,
                "loss_count": total_loss_count,
                "cumulative_pnl": round(total_pnl, 4),
                "win_rate": round(
                    total_win_count / max(total_total_games, 1) * 100, 1
                ),
            },
            # ── 账户 ──
            "account": {
                "balance": round(ctx.account.balance, 2),
                "available": round(ctx.account.available, 2),
                "total_equity": round(ctx.account.total_equity, 2),
                "daily_pnl": round(ctx.account.daily_pnl, 4),
            },
            # ── 参数 ──
            "params": self.get_params(),
            # ── Legacy (向后兼容 BTC 单币种面板) ──
            **(symbols_data.get("btc", {})),
            "trade_history": all_trade_history[-self._max_trade_history:],
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

    def _log_deviation_summary(self, ss: _SymbolState) -> None:
        """输出某币种的全局偏离统计."""
        if not ss.deviation_stats:
            return

        all_devs = [d["abs_deviation"] for d in ss.deviation_stats]
        avg_all = sum(all_devs) / len(all_devs)
        min_all = min(all_devs)
        max_all = max(all_devs)

        small = [d for d in all_devs if d <= 10]
        med = [d for d in all_devs if 10 < d <= 30]
        large = [d for d in all_devs if d > 30]

        logger.info(
            f"[{self.name()}:{ss.symbol.upper()}] 📊 偏离统计 (全局{len(all_devs)}笔) | "
            f"avg=${avg_all:.2f} min=${min_all:.2f} max=${max_all:.2f} | "
            f"≤$10: {len(small)}笔, $10-30: {len(med)}笔, >$30: {len(large)}笔"
        )

    def _calc_vol(
        self, ss: _SymbolState, now: float, lookback_s: float = 30.0
    ) -> float:
        """计算某币种在近 lookback_s 秒内的价格波动幅度."""
        cutoff = now - lookback_s
        prices = [p for t, p in ss.price_buffer if t >= cutoff]
        if len(prices) < 5:
            return 0.0
        return max(prices) - min(prices)

    def _check_counter_move(
        self, ss: _SymbolState, now: float, currently_up: bool
    ) -> bool:
        """检测价格是否在短期出现了反向运动."""
        cutoff = now - self._counter_lookback_s
        recent = [(t, p) for t, p in ss.price_buffer if t >= cutoff]
        if len(recent) < 3:
            return False

        if currently_up:
            peak = max(p for _, p in recent)
            current = recent[-1][1]
            return (peak - current) >= self._counter_threshold
        else:
            trough = min(p for _, p in recent)
            current = recent[-1][1]
            return (current - trough) >= self._counter_threshold

    async def _submit_order(
        self,
        ctx: Context,
        mkt: MarketState,
        side: Side,
        price: float,
        size: float,
    ) -> OrderResult | None:
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
            token_id = mkt.pm_yes_token_id
        else:
            token_id = mkt.pm_no_token_id

        request = OrderRequest(
            market_id=mkt.pm_market_id,
            token_id=token_id,
            side=side,
            order_type=OrderType.FOK,
            price=price,
            size=size,
            strategy_id=self.name(),
            meta={
                "strategy": "tail_reversal",
                "symbol": mkt.symbol,
                "cheap_side_price": price,
                "odds": (1.0 - price) / price if price > 0 else 0,
                "clob_side": "BUY",
                "tick_size": "0.01",
                "neg_risk": mkt.pm_neg_risk,
            },
        )
        return await executor.submit_order(request)

    def _log_status(self, mkt: MarketState, ss: _SymbolState) -> None:
        secs_left = mkt.pm_window_seconds_left
        price = mkt.price if mkt.price > 0 else mkt.btc_price
        ptb = ss.window_ptb
        up_ask = mkt.pm_yes_ask or mkt.pm_yes_price
        dn_ask = mkt.pm_no_ask or mkt.pm_no_price

        elapsed_pct = (300 - secs_left) / 300.0 if secs_left > 0 else 0
        zone = "[扫描]"
        if ss.bets_this_window > 0:
            sides = ",".join(ss.bought_sides) if ss.bought_sides else ""
            zone = f"[已买{sides}]"

        price_diff = price - ptb if ptb > 0 else 0
        logger.debug(
            f"[{self.name()}:{ss.symbol.upper()}] {zone} | "
            f"price={price:,.2f} PTB={ptb:,.2f} diff={price_diff:+.2f} | "
            f"UP_ask={up_ask:.4f} DN_ask={dn_ask:.4f} | "
            f"elapsed={elapsed_pct:.1%} left={secs_left:.0f}s | "
            f"bets={ss.bets_this_window}/{self._max_bets_per_window} "
            f"cost=${ss.cost_this_window:.2f} | "
            f"cum PnL=${ss.cumulative_pnl:+.2f}"
        )
