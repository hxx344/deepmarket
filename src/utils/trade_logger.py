"""
Trade Logger - 交易日志记录器

为每次 paper/live 运行记录详细的交易信息，用于后续策略优化分析。

记录内容:
    - 运行会话: run_id, 模式, 策略, 启动时间, 配置参数
    - 交易信号: 策略为什么决定交易 (动量, gap, edge 等)
    - 订单明细: 请求 + 结果 + 当时的完整市场上下文
    - 结算记录: 每个 5-min 窗口的盈亏明细
    - 运行摘要: 整体统计

存储:
    - SQLite (结构化, 可用 SQL 查询)
    - JSONL 文件 (完整上下文, 便于 pandas 分析)
"""

from __future__ import annotations

import json
import os
import sqlite3
import time
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, TYPE_CHECKING

from loguru import logger

if TYPE_CHECKING:
    from src.core.context import Context
    from src.trading.executor import OrderRequest, OrderResult


@dataclass
class MarketSnapshot:
    """下单时刻的市场快照"""
    btc_price: float = 0.0
    btc_price_updated_at: float = 0.0
    binance_price: float = 0.0
    pm_yes_price: float = 0.0
    pm_no_price: float = 0.0
    pm_yes_bid: float = 0.0
    pm_yes_ask: float = 0.0
    pm_no_bid: float = 0.0
    pm_no_ask: float = 0.0
    pm_window_seconds_left: float = 0.0
    pm_window_start_ts: int = 0
    pm_window_start_price: float = 0.0  # PTB
    pm_market_question: str = ""
    pm_market_active: bool = False
    pm_condition_id: str = ""
    pm_yes_token_id: str = ""
    pm_no_token_id: str = ""

    @classmethod
    def from_context(cls, ctx: "Context") -> "MarketSnapshot":
        m = ctx.market
        return cls(
            btc_price=m.btc_price,
            btc_price_updated_at=m.btc_price_updated_at,
            binance_price=m.binance_price,
            pm_yes_price=m.pm_yes_price,
            pm_no_price=m.pm_no_price,
            pm_yes_bid=m.pm_yes_bid,
            pm_yes_ask=m.pm_yes_ask,
            pm_no_bid=m.pm_no_bid,
            pm_no_ask=m.pm_no_ask,
            pm_window_seconds_left=m.pm_window_seconds_left,
            pm_window_start_ts=m.pm_window_start_ts,
            pm_window_start_price=m.pm_window_start_price,
            pm_market_question=m.pm_market_question,
            pm_market_active=m.pm_market_active,
            pm_condition_id=m.pm_condition_id,
            pm_yes_token_id=m.pm_yes_token_id,
            pm_no_token_id=m.pm_no_token_id,
        )


@dataclass
class OrderbookSnapshot:
    """下单时刻的订单簿快照"""
    up_best_bid: float = 0.0
    up_best_ask: float = 0.0
    up_spread: float = 0.0
    up_bid_depth_5: float = 0.0   # 前 5 档 bid 深度
    up_ask_depth_5: float = 0.0   # 前 5 档 ask 深度
    dn_best_bid: float = 0.0
    dn_best_ask: float = 0.0
    dn_spread: float = 0.0
    dn_bid_depth_5: float = 0.0
    dn_ask_depth_5: float = 0.0

    @classmethod
    def from_context(cls, ctx: "Context") -> "OrderbookSnapshot":
        snap = cls()
        ob_up = ctx.get("orderbook_up")
        ob_dn = ctx.get("orderbook_down")
        if ob_up:
            state = ob_up.get_state()
            snap.up_best_bid = state.best_bid or 0.0
            snap.up_best_ask = state.best_ask or 0.0
            snap.up_spread = state.spread or 0.0
            snap.up_bid_depth_5 = state.total_bid_size(5)
            snap.up_ask_depth_5 = state.total_ask_size(5)
        if ob_dn:
            state = ob_dn.get_state()
            snap.dn_best_bid = state.best_bid or 0.0
            snap.dn_best_ask = state.best_ask or 0.0
            snap.dn_spread = state.spread or 0.0
            snap.dn_bid_depth_5 = state.total_bid_size(5)
            snap.dn_ask_depth_5 = state.total_ask_size(5)
        return snap


class TradeLogger:
    """
    交易日志记录器

    每次 paper/live 运行创建一个实例, 自动记录所有交易活动。
    数据同时写入 SQLite (查询) 和 JSONL (分析)。
    """

    def __init__(
        self,
        data_dir: str | Path = "data",
        run_mode: str = "paper",
        strategy_name: str = "",
        config: dict | None = None,
    ) -> None:
        self.data_dir = Path(data_dir)
        self.run_id = f"{run_mode}_{time.strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
        self.run_mode = run_mode
        self.strategy_name = strategy_name
        self.start_time = time.time()
        self._config = config or {}

        # ── 初始化 JSONL 文件 ──
        self._log_dir = self.data_dir / "trade_logs"
        self._log_dir.mkdir(parents=True, exist_ok=True)
        self._jsonl_path = self._log_dir / f"{self.run_id}.jsonl"
        self._jsonl_file = open(self._jsonl_path, "a", encoding="utf-8")

        # ── 初始化 SQLite ──
        self._db_path = self.data_dir / "trade_logs.db"
        self._conn: sqlite3.Connection | None = None
        self._init_db()

        # ── 运行期统计 ──
        self._order_count = 0
        self._fill_count = 0
        self._reject_count = 0
        self._settle_count = 0
        self._total_volume = 0.0
        self._total_pnl = 0.0

        # 写入运行记录
        self._log_run_start()
        logger.info(
            f"TradeLogger 已启动 | run_id={self.run_id} | "
            f"JSONL={self._jsonl_path.name} | DB={self._db_path.name}"
        )

    # ================================================================
    #  SQLite 初始化
    # ================================================================

    def _init_db(self) -> None:
        self._conn = sqlite3.connect(str(self._db_path))
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript("""
            -- 运行会话表
            CREATE TABLE IF NOT EXISTS runs (
                run_id TEXT PRIMARY KEY,
                mode TEXT NOT NULL,
                strategy TEXT,
                start_time REAL NOT NULL,
                end_time REAL,
                initial_balance REAL,
                final_balance REAL,
                total_pnl REAL DEFAULT 0,
                total_orders INTEGER DEFAULT 0,
                total_fills INTEGER DEFAULT 0,
                total_rejects INTEGER DEFAULT 0,
                total_settlements INTEGER DEFAULT 0,
                total_volume REAL DEFAULT 0,
                win_count INTEGER DEFAULT 0,
                loss_count INTEGER DEFAULT 0,
                win_rate REAL DEFAULT 0,
                sharpe REAL,
                max_drawdown REAL,
                config TEXT,
                status TEXT DEFAULT 'running'
            );

            -- 详细订单日志 (包含市场上下文)
            CREATE TABLE IF NOT EXISTS order_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                timestamp REAL NOT NULL,
                -- 订单信息
                order_id TEXT NOT NULL,
                strategy_id TEXT,
                market_id TEXT,
                token_id TEXT,
                side TEXT,                  -- YES / NO
                direction TEXT,             -- UP / DOWN
                order_type TEXT,
                request_price REAL,
                request_size REAL,          -- USDC
                -- 结果
                status TEXT,                -- filled / rejected / failed / ...
                filled_price REAL,
                filled_shares REAL,
                fee REAL,
                tx_hash TEXT,
                error TEXT,
                -- 市场上下文快照
                btc_price REAL,
                binance_price REAL,
                pm_yes_price REAL,
                pm_no_price REAL,
                pm_yes_bid REAL,
                pm_yes_ask REAL,
                pm_no_bid REAL,
                pm_no_ask REAL,
                window_seconds_left REAL,
                window_start_ts INTEGER,
                ptb REAL,                   -- Price to Beat
                -- 订单簿上下文
                up_best_bid REAL,
                up_best_ask REAL,
                up_spread REAL,
                up_bid_depth REAL,
                up_ask_depth REAL,
                dn_best_bid REAL,
                dn_best_ask REAL,
                dn_spread REAL,
                dn_bid_depth REAL,
                dn_ask_depth REAL,
                -- 策略上下文
                momentum REAL,
                edge REAL,
                gap_shares REAL,
                cum_up_shares REAL,
                cum_dn_shares REAL,
                cum_up_cost REAL,
                cum_dn_cost REAL,
                entry_num INTEGER,
                burst_num INTEGER,
                -- 扩展 JSON
                meta TEXT,
                FOREIGN KEY (run_id) REFERENCES runs(run_id)
            );

            -- 结算日志
            CREATE TABLE IF NOT EXISTS settlement_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                timestamp REAL NOT NULL,
                -- 窗口信息
                window_start_ts INTEGER,
                window_question TEXT,
                condition_id TEXT,
                -- BTC 价格
                btc_price REAL,
                ptb REAL,
                btc_direction TEXT,         -- UP / DOWN
                -- 持仓明细
                up_shares REAL,
                up_cost REAL,
                up_avg_price REAL,
                dn_shares REAL,
                dn_cost REAL,
                dn_avg_price REAL,
                -- 结果
                winner_side TEXT,
                payout REAL,
                total_cost REAL,
                fee REAL,
                net_pnl REAL,
                -- 统计
                actual_edge REAL,
                gap_shares REAL,
                gap_pct REAL,
                entries INTEGER,
                bursts INTEGER,
                result TEXT,                -- WIN / LOSE
                -- 账户状态
                balance_after REAL,
                cumulative_pnl REAL,
                -- 扩展 JSON
                meta TEXT,
                FOREIGN KEY (run_id) REFERENCES runs(run_id)
            );

            -- 策略信号日志
            CREATE TABLE IF NOT EXISTS signal_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                timestamp REAL NOT NULL,
                strategy_id TEXT,
                signal_type TEXT,           -- momentum_trigger / gap_rebalance / ...
                direction TEXT,
                strength REAL,
                -- 决策相关
                momentum REAL,
                gap_ratio REAL,
                secs_left REAL,
                btc_price REAL,
                -- 是否被执行
                executed INTEGER DEFAULT 0,
                reject_reason TEXT,
                meta TEXT,
                FOREIGN KEY (run_id) REFERENCES runs(run_id)
            );

            -- 索引
            CREATE INDEX IF NOT EXISTS idx_ol_run ON order_logs(run_id);
            CREATE INDEX IF NOT EXISTS idx_ol_ts ON order_logs(timestamp);
            CREATE INDEX IF NOT EXISTS idx_ol_status ON order_logs(status);
            CREATE INDEX IF NOT EXISTS idx_sl_run ON settlement_logs(run_id);
            CREATE INDEX IF NOT EXISTS idx_sl_ts ON settlement_logs(timestamp);
            CREATE INDEX IF NOT EXISTS idx_sig_run ON signal_logs(run_id);
            CREATE INDEX IF NOT EXISTS idx_sig_ts ON signal_logs(timestamp);
        """)
        self._conn.commit()

    # ================================================================
    #  运行生命周期
    # ================================================================

    def _log_run_start(self) -> None:
        """记录运行开始"""
        self._conn.execute(
            "INSERT INTO runs (run_id, mode, strategy, start_time, config, status) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (
                self.run_id,
                self.run_mode,
                self.strategy_name,
                self.start_time,
                json.dumps(self._config, default=str, ensure_ascii=False),
                "running",
            ),
        )
        self._conn.commit()
        self._write_jsonl({
            "event": "run_start",
            "run_id": self.run_id,
            "mode": self.run_mode,
            "strategy": self.strategy_name,
            "start_time": self.start_time,
        })

    def finalize(
        self,
        final_balance: float = 0.0,
        initial_balance: float = 0.0,
        win_count: int = 0,
        loss_count: int = 0,
        extra_stats: dict | None = None,
    ) -> None:
        """运行结束时调用, 写入摘要"""
        end_time = time.time()
        total = win_count + loss_count
        win_rate = win_count / total if total > 0 else 0.0

        self._conn.execute(
            "UPDATE runs SET "
            "end_time=?, initial_balance=?, final_balance=?, "
            "total_pnl=?, total_orders=?, total_fills=?, "
            "total_rejects=?, total_settlements=?, "
            "total_volume=?, win_count=?, loss_count=?, "
            "win_rate=?, status=? "
            "WHERE run_id=?",
            (
                end_time,
                initial_balance,
                final_balance,
                self._total_pnl,
                self._order_count,
                self._fill_count,
                self._reject_count,
                self._settle_count,
                self._total_volume,
                win_count,
                loss_count,
                win_rate,
                "completed",
                self.run_id,
            ),
        )
        self._conn.commit()

        summary = {
            "event": "run_end",
            "run_id": self.run_id,
            "duration_s": round(end_time - self.start_time, 1),
            "initial_balance": initial_balance,
            "final_balance": final_balance,
            "total_pnl": self._total_pnl,
            "total_orders": self._order_count,
            "total_fills": self._fill_count,
            "total_rejects": self._reject_count,
            "total_settlements": self._settle_count,
            "win_count": win_count,
            "loss_count": loss_count,
            "win_rate": win_rate,
        }
        if extra_stats:
            summary.update(extra_stats)
        self._write_jsonl(summary)

        logger.info(
            f"TradeLogger 运行摘要 | run_id={self.run_id} | "
            f"时长={summary['duration_s']:.0f}s | "
            f"订单={self._order_count} 成交={self._fill_count} | "
            f"结算={self._settle_count} 胜率={win_rate:.1%} | "
            f"PnL={self._total_pnl:+.4f}"
        )

    # ================================================================
    #  订单日志
    # ================================================================

    def log_order(
        self,
        request: "OrderRequest",
        result: "OrderResult",
        ctx: "Context",
        strategy_context: dict | None = None,
    ) -> None:
        """
        记录一笔订单的完整信息。

        Args:
            request: 订单请求
            result: 执行结果
            ctx: Context (获取市场快照)
            strategy_context: 策略额外上下文 (momentum, gap, edge 等)
        """
        self._order_count += 1
        sc = strategy_context or {}

        market_snap = MarketSnapshot.from_context(ctx)
        ob_snap = OrderbookSnapshot.from_context(ctx)

        status_str = result.status.value if result else "unknown"
        if result and result.status.value == "filled":
            self._fill_count += 1
            self._total_volume += result.filled_size * result.filled_price
        elif result and result.status.value in ("rejected", "failed"):
            self._reject_count += 1

        # 方向: YES → UP, NO → DOWN
        direction = "UP" if request.side.value == "YES" else "DOWN"

        # ── SQLite ──
        try:
            self._conn.execute(
                "INSERT INTO order_logs ("
                "run_id, timestamp, order_id, strategy_id, market_id, "
                "token_id, side, direction, order_type, request_price, request_size, "
                "status, filled_price, filled_shares, fee, tx_hash, error, "
                "btc_price, binance_price, pm_yes_price, pm_no_price, "
                "pm_yes_bid, pm_yes_ask, pm_no_bid, pm_no_ask, "
                "window_seconds_left, window_start_ts, ptb, "
                "up_best_bid, up_best_ask, up_spread, up_bid_depth, up_ask_depth, "
                "dn_best_bid, dn_best_ask, dn_spread, dn_bid_depth, dn_ask_depth, "
                "momentum, edge, gap_shares, "
                "cum_up_shares, cum_dn_shares, cum_up_cost, cum_dn_cost, "
                "entry_num, burst_num, meta"
                ") VALUES ("
                "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    self.run_id,
                    result.timestamp if result else time.time(),
                    request.id,
                    request.strategy_id,
                    request.market_id,
                    request.token_id,
                    request.side.value,
                    direction,
                    request.order_type.value,
                    request.price,
                    request.size,
                    status_str,
                    result.filled_price if result else 0,
                    result.filled_size if result else 0,
                    result.fee if result else 0,
                    result.tx_hash if result else "",
                    result.error if result else "",
                    market_snap.btc_price,
                    market_snap.binance_price,
                    market_snap.pm_yes_price,
                    market_snap.pm_no_price,
                    market_snap.pm_yes_bid,
                    market_snap.pm_yes_ask,
                    market_snap.pm_no_bid,
                    market_snap.pm_no_ask,
                    market_snap.pm_window_seconds_left,
                    market_snap.pm_window_start_ts,
                    market_snap.pm_window_start_price,
                    ob_snap.up_best_bid,
                    ob_snap.up_best_ask,
                    ob_snap.up_spread,
                    ob_snap.up_bid_depth_5,
                    ob_snap.up_ask_depth_5,
                    ob_snap.dn_best_bid,
                    ob_snap.dn_best_ask,
                    ob_snap.dn_spread,
                    ob_snap.dn_bid_depth_5,
                    ob_snap.dn_ask_depth_5,
                    sc.get("momentum", 0),
                    sc.get("edge", 0),
                    sc.get("gap_shares", 0),
                    sc.get("cum_up_shares", 0),
                    sc.get("cum_dn_shares", 0),
                    sc.get("cum_up_cost", 0),
                    sc.get("cum_dn_cost", 0),
                    sc.get("entry_num", 0),
                    sc.get("burst_num", 0),
                    json.dumps(sc.get("extra", {}), default=str),
                ),
            )
            self._conn.commit()
        except Exception as e:
            logger.error(f"TradeLogger order_log 写入失败: {e}")

        # ── JSONL ──
        self._write_jsonl({
            "event": "order",
            "run_id": self.run_id,
            "ts": result.timestamp if result else time.time(),
            "order_id": request.id,
            "strategy_id": request.strategy_id,
            "side": request.side.value,
            "direction": direction,
            "order_type": request.order_type.value,
            "request_price": request.price,
            "request_size": request.size,
            "status": status_str,
            "filled_price": result.filled_price if result else 0,
            "filled_shares": result.filled_size if result else 0,
            "fee": result.fee if result else 0,
            "tx_hash": result.tx_hash if result else "",
            "error": result.error if result else "",
            "market": asdict(market_snap),
            "orderbook": asdict(ob_snap),
            "strategy": sc,
        })

    # ================================================================
    #  结算日志
    # ================================================================

    def log_settlement(
        self,
        ctx: "Context",
        settlement: dict,
    ) -> None:
        """
        记录一次 5-min 窗口结算。

        Args:
            ctx: Context
            settlement: 结算详情 dict, 至少包含:
                winner_side, btc_price, ptb, up_shares, dn_shares,
                up_cost, dn_cost, payout, total_cost, fee, net_pnl,
                actual_edge, gap_shares, gap_pct, entries, bursts,
                result (WIN/LOSE), balance_after, cumulative_pnl
        """
        self._settle_count += 1
        self._total_pnl += settlement.get("net_pnl", 0)

        ts = time.time()

        # ── SQLite ──
        try:
            self._conn.execute(
                "INSERT INTO settlement_logs ("
                "run_id, timestamp, window_start_ts, window_question, condition_id, "
                "btc_price, ptb, btc_direction, "
                "up_shares, up_cost, up_avg_price, dn_shares, dn_cost, dn_avg_price, "
                "winner_side, payout, total_cost, fee, net_pnl, "
                "actual_edge, gap_shares, gap_pct, entries, bursts, result, "
                "balance_after, cumulative_pnl, meta"
                ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    self.run_id,
                    ts,
                    settlement.get("window_start_ts", ctx.market.pm_window_start_ts),
                    settlement.get("window_question", ctx.market.pm_market_question),
                    settlement.get("condition_id", ctx.market.pm_condition_id),
                    settlement.get("btc_price", ctx.market.btc_price),
                    settlement.get("ptb", ctx.market.pm_window_start_price),
                    settlement.get("winner_side", ""),
                    settlement.get("up_shares", 0),
                    settlement.get("up_cost", 0),
                    settlement.get("up_avg_price", 0),
                    settlement.get("dn_shares", 0),
                    settlement.get("dn_cost", 0),
                    settlement.get("dn_avg_price", 0),
                    settlement.get("winner_side", ""),
                    settlement.get("payout", 0),
                    settlement.get("total_cost", 0),
                    settlement.get("fee", 0),
                    settlement.get("net_pnl", 0),
                    settlement.get("actual_edge", 0),
                    settlement.get("gap_shares", 0),
                    settlement.get("gap_pct", 0),
                    settlement.get("entries", 0),
                    settlement.get("bursts", 0),
                    settlement.get("result", ""),
                    settlement.get("balance_after", ctx.account.balance),
                    settlement.get("cumulative_pnl", ctx.account.total_pnl),
                    json.dumps(settlement.get("extra", {}), default=str),
                ),
            )
            self._conn.commit()
        except Exception as e:
            logger.error(f"TradeLogger settlement_log 写入失败: {e}")

        # ── JSONL ──
        self._write_jsonl({
            "event": "settlement",
            "run_id": self.run_id,
            "ts": ts,
            **settlement,
        })

    # ================================================================
    #  信号日志
    # ================================================================

    def log_signal(
        self,
        ctx: "Context",
        signal_type: str,
        direction: str,
        strength: float,
        executed: bool = False,
        reject_reason: str = "",
        extra: dict | None = None,
    ) -> None:
        """
        记录策略信号 (无论是否被执行)。

        用于分析: 哪些信号产生了利润, 哪些应该被过滤。
        """
        ts = time.time()
        sc = extra or {}

        try:
            self._conn.execute(
                "INSERT INTO signal_logs ("
                "run_id, timestamp, strategy_id, signal_type, direction, strength, "
                "momentum, gap_ratio, secs_left, btc_price, "
                "executed, reject_reason, meta"
                ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    self.run_id,
                    ts,
                    sc.get("strategy_id", self.strategy_name),
                    signal_type,
                    direction,
                    strength,
                    sc.get("momentum", 0),
                    sc.get("gap_ratio", 0),
                    sc.get("secs_left", 0),
                    ctx.market.btc_price,
                    1 if executed else 0,
                    reject_reason,
                    json.dumps(sc, default=str),
                ),
            )
            self._conn.commit()
        except Exception as e:
            logger.error(f"TradeLogger signal_log 写入失败: {e}")

        self._write_jsonl({
            "event": "signal",
            "run_id": self.run_id,
            "ts": ts,
            "signal_type": signal_type,
            "direction": direction,
            "strength": strength,
            "executed": executed,
            "reject_reason": reject_reason,
            **sc,
        })

    # ================================================================
    #  JSONL 写入
    # ================================================================

    def _write_jsonl(self, record: dict) -> None:
        """追加一条 JSONL 记录"""
        try:
            line = json.dumps(record, default=str, ensure_ascii=False)
            self._jsonl_file.write(line + "\n")
            self._jsonl_file.flush()
        except Exception as e:
            logger.error(f"TradeLogger JSONL 写入失败: {e}")

    # ================================================================
    #  关闭
    # ================================================================

    def close(self) -> None:
        """关闭文件和数据库连接"""
        try:
            if self._jsonl_file:
                self._jsonl_file.close()
        except Exception:
            pass
        try:
            if self._conn:
                self._conn.close()
        except Exception:
            pass
        logger.debug(f"TradeLogger 已关闭 | run_id={self.run_id}")

    def __del__(self):
        self.close()
