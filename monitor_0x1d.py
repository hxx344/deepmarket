"""
0x1d 实时监控面板 — 监控 BTC 行情 + PM 报价 + 0x1d 下单可视化

启动方式:
    python monitor_0x1d.py [--port 8888]

功能:
    1. Chainlink BTC/USD 实时价格 (PM RTDS WebSocket, 与 PM 结算一致)
       → Binance BTC/USDT 作为备用回退
    2. Polymarket UP/DOWN 实时报价 (CLOB midpoint + book)
    3. 5-min 基准价 (PTB) 基于 Chainlink observationsTimestamp % 300 精确捕获
    4. 0x1d 实时下单监控 (Activity API 轮询)
    5. Web 可视化 (Chart.js 实时更新)

架构:
    Backend:  aiohttp server + asyncio 数据采集
    Frontend: 单页 HTML, Chart.js + WebSocket 实时推送
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import time
import math
import sqlite3
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import aiohttp
import aiohttp.web

# ─────────────────────────────────────────────────────────
#  常量
# ─────────────────────────────────────────────────────────

ADDR_0X1D = "0x1d0034134e339a309700ff2d34e99fa2d48b0313"
ACTIVITY_API = "https://data-api.polymarket.com/activity"
GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_REST = "https://clob.polymarket.com"
# Binance 域名 (可通过环境变量覆盖, 应对地区封锁)
# 备选: stream.binance.us / data-stream.binance.vision
_BN_HOST = os.environ.get("BN_HOST", "data-stream.binance.vision")
_BN_API  = os.environ.get("BN_API",  "data-api.binance.vision")
BINANCE_WS = f"wss://{_BN_HOST}:9443/ws/btcusdt@trade"
BINANCE_REST = f"https://{_BN_API}/api/v3/ticker/price?symbol=BTCUSDT"
PM_RTDS_WS = "wss://ws-live-data.polymarket.com"
CLOB_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

SLUG_PREFIX = "btc-updown-5m-"
POLL_ACTIVITY_INTERVAL = 3.0       # 轮询 0x1d activity (秒)
POLL_PM_PRICE_INTERVAL = 2.0       # 轮询 PM 报价 (秒)
WS_PUSH_INTERVAL = 1.0             # WebSocket 推送间隔 (秒)
MAX_BTC_HISTORY = 600              # BTC 价格历史长度 (~10 分钟)
MAX_TRADE_HISTORY = 500            # 0x1d 交易历史长度
# 结算方式: RTDS 0秒价格即时结算 (不再依赖 Gamma API)
BTC_DOWNSAMPLE_INTERVAL = 0.5      # BTC 降采样间隔 (s)


# ─────────────────────────────────────────────────────────
#  数据持久化 (SQLite)
# ─────────────────────────────────────────────────────────

DB_PATH = Path(__file__).parent / "data" / "0x1d_data.db"


def _init_db():
    """初始化 SQLite 数据库"""
    DB_PATH.parent.mkdir(exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("""
        CREATE TABLE IF NOT EXISTS trade_snaps (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tx_hash TEXT UNIQUE,
            ts INTEGER,
            slug TEXT,
            side TEXT,
            shares REAL,
            price REAL,
            usdc REAL,
            features TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS settlements (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            slug TEXT UNIQUE,
            question TEXT,
            won TEXT,
            cost REAL,
            payout REAL,
            pnl REAL,
            trades INTEGER,
            ptb REAL,
            settle_src TEXT,
            settled_at REAL
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_snaps_slug ON trade_snaps(slug)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_snaps_ts ON trade_snaps(ts)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_settle_slug ON settlements(slug)")
    # ── 扩展 settlements 表: 新增窗口级分析字段 (安全 ALTER, 已存在则忽略) ──
    _new_cols = [
        ("up_shares", "REAL DEFAULT 0"),
        ("dn_shares", "REAL DEFAULT 0"),
        ("up_cost", "REAL DEFAULT 0"),
        ("dn_cost", "REAL DEFAULT 0"),
        ("avg_up_price", "REAL DEFAULT 0"),
        ("avg_dn_price", "REAL DEFAULT 0"),
        ("gap_shares", "REAL DEFAULT 0"),
        ("gap_pct", "REAL DEFAULT 0"),
        ("first_trade_elapsed", "REAL DEFAULT 0"),
        ("last_trade_elapsed", "REAL DEFAULT 0"),
        ("avg_trade_interval", "REAL DEFAULT 0"),
        ("burst_count", "INTEGER DEFAULT 0"),
        ("btc_start", "REAL DEFAULT 0"),
        ("btc_end", "REAL DEFAULT 0"),
        ("btc_move", "REAL DEFAULT 0"),
        ("btc_vol_window", "REAL DEFAULT 0"),
        ("window_summary", "TEXT DEFAULT '{}'"),
    ]
    existing_cols = {r[1] for r in conn.execute("PRAGMA table_info(settlements)").fetchall()}
    for col_name, col_type in _new_cols:
        if col_name not in existing_cols:
            try:
                conn.execute(f"ALTER TABLE settlements ADD COLUMN {col_name} {col_type}")
            except Exception:
                pass
    conn.commit()
    conn.close()
    print(f"[DB] 数据库就绪: {DB_PATH}")


def _db_save_trade_snap(tx_hash: str, ts: int, slug: str, side: str,
                        shares: float, price: float, usdc: float,
                        features: dict):
    """持久化单笔交易特征快照 (去重: tx_hash UNIQUE)"""
    try:
        conn = sqlite3.connect(str(DB_PATH))
        conn.execute(
            "INSERT OR IGNORE INTO trade_snaps "
            "(tx_hash, ts, slug, side, shares, price, usdc, features) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (tx_hash, ts, slug, side, shares, price, usdc,
             json.dumps(features, ensure_ascii=False))
        )
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[DB] 保存交易快照失败: {e}")


def _db_save_settlement(record: dict):
    """持久化结算记录 (去重: slug UNIQUE)"""
    try:
        conn = sqlite3.connect(str(DB_PATH))
        conn.execute(
            "INSERT OR REPLACE INTO settlements "
            "(slug, question, won, cost, payout, pnl, trades, ptb, settle_src, settled_at,"
            " up_shares, dn_shares, up_cost, dn_cost, avg_up_price, avg_dn_price,"
            " gap_shares, gap_pct, first_trade_elapsed, last_trade_elapsed,"
            " avg_trade_interval, burst_count, btc_start, btc_end, btc_move,"
            " btc_vol_window, window_summary) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (record["slug"], record.get("question", ""), record["won"],
             record["cost"], record["payout"], record["pnl"],
             record["trades"], record.get("ptb", 0),
             record.get("settle_src", ""), time.time(),
             record.get("up_shares", 0), record.get("dn_shares", 0),
             record.get("up_cost", 0), record.get("dn_cost", 0),
             record.get("avg_up_price", 0), record.get("avg_dn_price", 0),
             record.get("gap_shares", 0), record.get("gap_pct", 0),
             record.get("first_trade_elapsed", 0), record.get("last_trade_elapsed", 0),
             record.get("avg_trade_interval", 0), record.get("burst_count", 0),
             record.get("btc_start", 0), record.get("btc_end", 0),
             record.get("btc_move", 0), record.get("btc_vol_window", 0),
             json.dumps({k: v for k, v in record.items()
                        if k not in ("slug", "question", "won", "cost", "payout", "pnl",
                                     "trades", "ptb", "settle_src")},
                       ensure_ascii=False))
        )
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[DB] 保存结算记录失败: {e}")


def _db_load_trade_snaps(limit: int = 5000) -> list[dict]:
    """从数据库加载历史交易快照 (最近 N 条)"""
    try:
        if not DB_PATH.exists():
            return []
        conn = sqlite3.connect(str(DB_PATH))
        rows = conn.execute(
            "SELECT features FROM trade_snaps ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
        conn.close()
        return [json.loads(r[0]) for r in reversed(rows)]
    except Exception as e:
        print(f"[DB] 加载交易快照失败: {e}")
        return []


def _db_load_settlements() -> list[dict]:
    """从数据库加载所有结算记录"""
    try:
        if not DB_PATH.exists():
            return []
        conn = sqlite3.connect(str(DB_PATH))
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM settlements ORDER BY id"
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception as e:
        print(f"[DB] 加载结算记录失败: {e}")
        return []


def _db_get_stats() -> dict:
    """获取数据库统计"""
    try:
        if not DB_PATH.exists():
            return {"trade_snaps": 0, "settlements": 0}
        conn = sqlite3.connect(str(DB_PATH))
        n_snaps = conn.execute("SELECT COUNT(*) FROM trade_snaps").fetchone()[0]
        n_settle = conn.execute("SELECT COUNT(*) FROM settlements").fetchone()[0]
        conn.close()
        return {"trade_snaps": n_snaps, "settlements": n_settle}
    except Exception:
        return {"trade_snaps": 0, "settlements": 0}


# ─────────────────────────────────────────────────────────
#  全局状态
# ─────────────────────────────────────────────────────────

class MonitorState:
    """全局共享状态"""

    def __init__(self):
        # BTC 价格
        self.btc_price: float = 0.0
        self.bn_price: float = 0.0                                          # Binance 最新价 (始终更新)
        self.btc_history: deque[dict] = deque(maxlen=MAX_BTC_HISTORY)       # RTDS (主)
        self.bn_history: deque[dict] = deque(maxlen=MAX_BTC_HISTORY)        # Binance (始终记录, 用于对比)
        # BTC 价格缓冲区 (用于 PTB 回溯, 高精度 ~200ms 采样)
        self.btc_price_buffer: deque[tuple] = deque(maxlen=3000)  # (ts, price) — RTDS 源
        self.bn_price_buffer: deque[tuple] = deque(maxlen=3000)   # (ts, price) — Binance 源 (始终写)

        # PM 5-min 窗口
        self.window_slug: str = ""
        self.window_question: str = ""
        self.window_start_ts: int = 0
        self.window_end_ts: int = 0
        self.window_ptb: float = 0.0          # 基准价
        self.up_token_id: str = ""
        self.dn_token_id: str = ""
        self.condition_id: str = ""

        # PM 报价
        self.up_price: float = 0.0
        self.dn_price: float = 0.0
        self.up_bid: float = 0.0
        self.up_ask: float = 0.0
        self.dn_bid: float = 0.0
        self.dn_ask: float = 0.0
        self.combined_price: float = 0.0       # UP + DN
        self.edge: float = 0.0                 # 1 - (UP + DN)
        self.pm_price_history: deque[dict] = deque(maxlen=MAX_BTC_HISTORY)

        # 0x1d 交易
        self.trades_0x1d: deque[dict] = deque(maxlen=MAX_TRADE_HISTORY)
        self.last_trade_ts: str = ""           # 最新一笔交易 timestamp (用于去重)

        # CLOB WS 链下成交流 (用于修正入场时间戳)
        self.clob_trades: deque[dict] = deque(maxlen=500)  # {ts, price, size, side, asset_id}
        self.clob_ws_connected: bool = False
        self.cum_up_shares: float = 0.0
        self.cum_dn_shares: float = 0.0
        self.cum_up_cost: float = 0.0
        self.cum_dn_cost: float = 0.0
        self.trade_count_window: int = 0
        self.burst_events: list[dict] = []     # burst 事件

        # 盈亏
        self.pnl_history: deque[dict] = deque(maxlen=MAX_BTC_HISTORY)
        self.settled_windows: list[dict] = []       # 已结算窗口

        # 延迟结算
        self._pending_settle: dict | None = None    # {slug, question, end_ts, ptb, ...}
        # RTDS 边界缓存: 0秒价格到达时保存, 供窗口切换时即时结算
        self._rtds_boundary: dict | None = None     # {settle_price, old_ptb, obs_ts, at}

        # 动量快照 (BTC 行情 + 0x1d 下单关联)
        self.momentum_history: deque[dict] = deque(maxlen=MAX_BTC_HISTORY)
        # 每笔交易时的动量快照 (当前窗口)
        self.trade_momentum_snaps: list[dict] = []
        # 跨窗口累积交易动量 (用于计算更稳定的相关系数)
        self.trade_momentum_all: deque[dict] = deque(maxlen=5000)

        # Chainlink RTDS 状态
        self.rtds_connected: bool = False
        self.btc_source: str = "---"           # 当前 BTC 价源: "Chainlink" / "Binance"
        self.rtds_last_obs_ts: int = 0         # 最近 observationsTimestamp
        self.ptb_source: str = "---"           # PTB 来源: "Chainlink obs" / "Binance buffer"
        # RTDS 连接健康度追踪
        self.rtds_connect_ts: float = 0.0      # 本次连接建立时间
        self.rtds_last_pong_ts: float = 0.0    # 最近一次 pong 响应时间
        self.rtds_last_data_ts: float = 0.0    # 最近一次收到 BTC 数据的时间
        self.rtds_reconnect_count: int = 0     # 累计重连次数
        self.rtds_total_uptime: float = 0.0    # 累计在线时长 (秒)

        # 连接状态
        self.binance_connected: bool = False
        self.pm_connected: bool = False
        self.activity_connected: bool = False

        # 预热标志: 启动时若处于窗口中间, 等到下一个窗口再正式统计
        self.warmup: bool = True

        # WebSocket 客户端
        self.ws_clients: set[aiohttp.web.WebSocketResponse] = set()

        # ── 从数据库加载历史数据 ──
        _init_db()
        hist_snaps = _db_load_trade_snaps(5000)
        if hist_snaps:
            self.trade_momentum_all.extend(hist_snaps)
            print(f"[DB] 加载历史交易快照: {len(hist_snaps)} 条")
        hist_settle = _db_load_settlements()
        if hist_settle:
            self.settled_windows = hist_settle
            print(f"[DB] 加载历史结算记录: {len(hist_settle)} 条")

    def settle_window(self, won_side: str):
        """结算当前窗口并记录历史 (含窗口级汇总)"""
        total_cost = self.cum_up_cost + self.cum_dn_cost
        if total_cost > 0:
            if won_side == "UP":
                payout = self.cum_up_shares
            else:
                payout = self.cum_dn_shares
            pnl = payout - total_cost

            # ── 窗口级汇总特征 ──
            gap_shares = self.cum_up_shares - self.cum_dn_shares
            max_shares = max(self.cum_up_shares, self.cum_dn_shares, 1)
            gap_pct = gap_shares / max_shares * 100
            avg_up = self.cum_up_cost / self.cum_up_shares if self.cum_up_shares > 0 else 0
            avg_dn = self.cum_dn_cost / self.cum_dn_shares if self.cum_dn_shares > 0 else 0

            # 入场时序
            snaps = self.trade_momentum_snaps
            first_elapsed = snaps[0].get("elapsed", 0) if snaps else 0
            last_elapsed = snaps[-1].get("elapsed", 0) if snaps else 0

            # 平均交易间隔
            if len(snaps) >= 2:
                intervals = []
                for i in range(1, len(snaps)):
                    intervals.append(snaps[i].get("elapsed", 0) - snaps[i-1].get("elapsed", 0))
                avg_interval = sum(intervals) / len(intervals) if intervals else 0
            else:
                avg_interval = 0

            # Burst 次数
            burst_cnt = len(self.burst_events)

            # BTC 窗口波动
            btc_start = self.window_ptb
            btc_end = self.btc_price
            btc_move = btc_end - btc_start if btc_start > 0 else 0
            btc_vol_window = self.calc_btc_volatility(300.0)

            record = {
                "slug": self.window_slug,
                "question": self.window_question,
                "won": won_side,
                "cost": round(total_cost, 2),
                "payout": round(payout, 2),
                "pnl": round(pnl, 2),
                "trades": self.trade_count_window,
                "up_shares": round(self.cum_up_shares, 2),
                "dn_shares": round(self.cum_dn_shares, 2),
                "up_cost": round(self.cum_up_cost, 2),
                "dn_cost": round(self.cum_dn_cost, 2),
                "avg_up_price": round(avg_up, 4),
                "avg_dn_price": round(avg_dn, 4),
                "gap_shares": round(gap_shares, 2),
                "gap_pct": round(gap_pct, 1),
                "first_trade_elapsed": round(first_elapsed),
                "last_trade_elapsed": round(last_elapsed),
                "avg_trade_interval": round(avg_interval, 1),
                "burst_count": burst_cnt,
                "btc_start": round(btc_start, 2),
                "btc_end": round(btc_end, 2),
                "btc_move": round(btc_move, 2),
                "btc_vol_window": round(btc_vol_window, 2),
            }

            self.settled_windows.append(record)
            # 只保留最近 500 个窗口
            if len(self.settled_windows) > 500:
                self.settled_windows = self.settled_windows[-500:]
            _db_save_settlement(record)

    def lookup_price_at(self, target_ts: float) -> float:
        """在价格缓冲区中找到最接近 target_ts 的价格 (优先取 <= target_ts 的最新值)"""
        if not self.btc_price_buffer:
            return 0.0
        best_price = 0.0
        best_diff = float("inf")
        for ts, price in reversed(self.btc_price_buffer):
            diff = abs(ts - target_ts)
            if diff < best_diff:
                best_diff = diff
                best_price = price
            if ts < target_ts - 10:
                break
        return best_price

    @staticmethod
    def _calc_momentum_from(buf: deque, window_secs: float, ref_ts: float = 0.0) -> float:
        """通用动量: 从指定缓冲区计算指定时间窗口的价格变化 ($)
        ref_ts>0 时回溯到该时刻计算, 否则用缓冲区最新数据"""
        if len(buf) < 2:
            return 0.0
        if ref_ts > 0:
            cur_price = 0.0
            now_ts = ref_ts
            for ts, price in reversed(buf):
                if ts <= ref_ts:
                    cur_price = price
                    break
            if cur_price <= 0:
                return 0.0
        else:
            now_ts = buf[-1][0]
            cur_price = buf[-1][1]
        target_ts = now_ts - window_secs
        past_price = 0.0
        for ts, price in reversed(buf):
            if ts <= target_ts:
                past_price = price
                break
        return (cur_price - past_price) if past_price > 0 else 0.0

    def calc_momentum(self, window_secs: float, ref_ts: float = 0.0) -> float:
        """RTDS (Chainlink) BTC 动量"""
        return self._calc_momentum_from(self.btc_price_buffer, window_secs, ref_ts)

    def calc_bn_momentum(self, window_secs: float, ref_ts: float = 0.0) -> float:
        """Binance BTC 动量"""
        return self._calc_momentum_from(self.bn_price_buffer, window_secs, ref_ts)

    @staticmethod
    def _pearson(xs: list[float], ys: list[float]) -> float:
        """计算 Pearson 相关系数, 样本不足返回 0"""
        n = len(xs)
        if n < 3:
            return 0.0
        mx = sum(xs) / n
        my = sum(ys) / n
        sx = math.sqrt(sum((x - mx) ** 2 for x in xs) / n)
        sy = math.sqrt(sum((y - my) ** 2 for y in ys) / n)
        if sx == 0 or sy == 0:
            return 0.0
        cov = sum((xs[i] - mx) * (ys[i] - my) for i in range(n)) / n
        return cov / (sx * sy)

    @staticmethod
    def _calc_volatility_from(buf: deque, window_secs: float, ref_ts: float = 0.0) -> float:
        """通用波动率: 从指定缓冲区计算 max-min
        ref_ts>0 时回溯到该时刻计算"""
        if len(buf) < 2:
            return 0.0
        now_ts = ref_ts if ref_ts > 0 else buf[-1][0]
        cutoff = now_ts - window_secs
        if ref_ts > 0:
            prices = [p for ts, p in buf if cutoff <= ts <= now_ts]
        else:
            prices = [p for ts, p in buf if ts >= cutoff]
        if len(prices) < 2:
            return 0.0
        return max(prices) - min(prices)

    def calc_btc_volatility(self, window_secs: float, ref_ts: float = 0.0) -> float:
        """RTDS (Chainlink) BTC 波动率"""
        return self._calc_volatility_from(self.btc_price_buffer, window_secs, ref_ts)

    def calc_bn_volatility(self, window_secs: float, ref_ts: float = 0.0) -> float:
        """Binance BTC 波动率"""
        return self._calc_volatility_from(self.bn_price_buffer, window_secs, ref_ts)

    @staticmethod
    def _calc_trend_strength(buf: deque, window_secs: float, ref_ts: float = 0.0) -> float:
        """
        趋势强度: 终点位移 / 路径总长度, 范围 [-1, 1]
        +1 = 完美上涨, -1 = 完美下跌, 0 = 来回震荡
        ref_ts>0 时回溯到该时刻计算
        """
        if len(buf) < 3:
            return 0.0
        now_ts = ref_ts if ref_ts > 0 else buf[-1][0]
        cutoff = now_ts - window_secs
        if ref_ts > 0:
            prices = [p for ts, p in buf if cutoff <= ts <= now_ts]
        else:
            prices = [p for ts, p in buf if ts >= cutoff]
        if len(prices) < 3:
            return 0.0
        displacement = prices[-1] - prices[0]
        path_len = sum(abs(prices[i] - prices[i - 1]) for i in range(1, len(prices)))
        if path_len < 0.01:
            return 0.0
        return displacement / path_len

    def calc_cl_trend(self, window_secs: float, ref_ts: float = 0.0) -> float:
        return self._calc_trend_strength(self.btc_price_buffer, window_secs, ref_ts)

    def calc_bn_trend(self, window_secs: float, ref_ts: float = 0.0) -> float:
        return self._calc_trend_strength(self.bn_price_buffer, window_secs, ref_ts)

    @staticmethod
    def _calc_price_percentile(buf: deque, window_secs: float, ref_ts: float = 0.0) -> float:
        """
        当前价格在 window 内的百分位 (0=最低, 100=最高)。
        可以判断当前价格在近期波动中的相对位置。
        ref_ts>0 时回溯到该时刻计算
        """
        if len(buf) < 2:
            return 50.0
        now_ts = ref_ts if ref_ts > 0 else buf[-1][0]
        cutoff = now_ts - window_secs
        if ref_ts > 0:
            prices = [p for ts, p in buf if cutoff <= ts <= now_ts]
        else:
            prices = [p for ts, p in buf if ts >= cutoff]
        if len(prices) < 2:
            return 50.0
        cur = prices[-1]
        lo, hi = min(prices), max(prices)
        if hi - lo < 0.01:
            return 50.0
        return (cur - lo) / (hi - lo) * 100.0

    def calc_cl_percentile(self, window_secs: float, ref_ts: float = 0.0) -> float:
        return self._calc_price_percentile(self.btc_price_buffer, window_secs, ref_ts)

    def calc_bn_percentile(self, window_secs: float, ref_ts: float = 0.0) -> float:
        return self._calc_price_percentile(self.bn_price_buffer, window_secs, ref_ts)

    @staticmethod
    def _calc_momentum_zscore(buf: deque, short_s: float, long_s: float, ref_ts: float = 0.0) -> float:
        """
        动量 Z-score: (短期动量 - 长期均值) / 长期标准差。
        用于捕捉当前动量相对历史是否异常。
        ref_ts>0 时回溯到该时刻计算
        """
        if len(buf) < 5:
            return 0.0
        now_ts = ref_ts if ref_ts > 0 else buf[-1][0]
        # 收集长期窗口内所有相邻差
        cutoff = now_ts - long_s
        if ref_ts > 0:
            items = [p for ts, p in buf if cutoff <= ts <= now_ts]
        else:
            items = [p for ts, p in buf if ts >= cutoff]
        diffs = []
        for i in range(1, len(items)):
            diffs.append(items[i] - items[i - 1])
        if len(diffs) < 3:
            return 0.0
        mean_d = sum(diffs) / len(diffs)
        std_d = math.sqrt(sum((d - mean_d) ** 2 for d in diffs) / len(diffs))
        if std_d < 1e-6:
            return 0.0
        # 短期动量
        short_mom = MonitorState._calc_momentum_from(buf, short_s, ref_ts)
        return (short_mom - mean_d * (short_s * 2)) / (std_d * math.sqrt(max(1, len(diffs))))

    def calc_cl_mom_zscore(self, short_s: float, long_s: float, ref_ts: float = 0.0) -> float:
        return self._calc_momentum_zscore(self.btc_price_buffer, short_s, long_s, ref_ts)

    def calc_bn_mom_zscore(self, short_s: float, long_s: float, ref_ts: float = 0.0) -> float:
        return self._calc_momentum_zscore(self.bn_price_buffer, short_s, long_s, ref_ts)

    @staticmethod
    def _count_direction_changes(buf: deque, window_secs: float, ref_ts: float = 0.0) -> int:
        """统计窗口内价格方向反转次数 (震荡指标)
        ref_ts>0 时回溯到该时刻计算"""
        if len(buf) < 3:
            return 0
        now_ts = ref_ts if ref_ts > 0 else buf[-1][0]
        cutoff = now_ts - window_secs
        if ref_ts > 0:
            prices = [p for ts, p in buf if cutoff <= ts <= now_ts]
        else:
            prices = [p for ts, p in buf if ts >= cutoff]
        if len(prices) < 3:
            return 0
        changes = 0
        prev_dir = 0
        for i in range(1, len(prices)):
            d = prices[i] - prices[i - 1]
            if abs(d) < 0.01:
                continue
            cur_dir = 1 if d > 0 else -1
            if prev_dir != 0 and cur_dir != prev_dir:
                changes += 1
            prev_dir = cur_dir
        return changes

    # ── RTDS-PTB 比对特征方法 ──

    def calc_rtds_implied_prob(self, ref_ts: float = 0.0) -> float:
        """
        根据 RTDS 价格相对 PTB 的距离 + 当前波动率,
        用 logistic 函数估算 UP 的公允概率。

        逻辑: distance / volatility 越大, UP 概率越接近 1;
               distance / volatility 越负, UP 概率越接近 0。
        """
        if self.window_ptb <= 0:
            return 0.50
        btc_p = self._price_at(self.btc_price_buffer, ref_ts) if ref_ts > 0 else self.btc_price
        if btc_p <= 0:
            return 0.50
        delta = btc_p - self.window_ptb
        vol = self.calc_btc_volatility(60.0, ref_ts)
        if vol < 0.5:
            vol = 0.5  # 防止除零, 最小波动 $0.5
        z = delta / vol * 2.0  # 缩放因子, 使 ±1vol 大致对应 ±0.27 概率偏离
        prob = 1.0 / (1.0 + math.exp(-z))
        return max(0.01, min(0.99, prob))

    def calc_pm_rtds_prob_gap(self, ref_ts: float = 0.0) -> float:
        """
        PM 市场定价 vs RTDS 隐含概率的偏差。
        正值 = 市场高估 UP (相对 BTC 实际位置); 负值 = 市场低估 UP。
        这个偏差可能蕴含 alpha: 若 0x1d 在市场低估时买入, 说明他利用了定价滞后。
        """
        pm_up = self.up_price
        if ref_ts > 0:
            snap = self._pm_state_at(ref_ts)
            pm_up = snap.get("up", self.up_price)
        implied = self.calc_rtds_implied_prob(ref_ts)
        return round(pm_up - implied, 4) if pm_up > 0 else 0.0

    def calc_ptb_dist_vs_vol(self, ref_ts: float = 0.0) -> float:
        """
        PTB 距离 / 波动率 (归一化距离)。
        > 1 = BTC 明显高于 PTB (超过 1 个标准波动); < -1 = 明显低于。
        接近 0 = 距离不显著, 方向不确定。
        """
        if self.window_ptb <= 0:
            return 0.0
        btc_p = self._price_at(self.btc_price_buffer, ref_ts) if ref_ts > 0 else self.btc_price
        if btc_p <= 0:
            return 0.0
        vol = self.calc_btc_volatility(60.0, ref_ts)
        if vol < 0.5:
            vol = 0.5
        return (btc_p - self.window_ptb) / vol

    def calc_ptb_cross_count(self, ref_ts: float = 0.0) -> int:
        """
        在本窗口内, BTC 价格穿越 PTB 的次数。
        穿越次数多 = 方向不确定 / 高争议; 0 次 = 单边行情。
        """
        if self.window_ptb <= 0 or self.window_start_ts <= 0:
            return 0
        ptb = self.window_ptb
        now_ts = ref_ts if ref_ts > 0 else (self.btc_price_buffer[-1][0] if self.btc_price_buffer else 0)
        cutoff = float(self.window_start_ts)
        prices = [p for ts, p in self.btc_price_buffer if cutoff <= ts <= now_ts]
        if len(prices) < 2:
            return 0
        crosses = 0
        above = prices[0] > ptb
        for p in prices[1:]:
            new_above = p > ptb
            if new_above != above:
                crosses += 1
                above = new_above
        return crosses

    def calc_ptb_time_above_ratio(self, ref_ts: float = 0.0) -> float:
        """
        本窗口内 BTC 价格处于 PTB 之上的时间占比 (0~1)。
        0.9 = 几乎全程在 PTB 上方 (强 UP);
        0.1 = 几乎全程在下方 (强 DN);
        0.5 = 各半 (不确定)。
        """
        if self.window_ptb <= 0 or self.window_start_ts <= 0:
            return 0.5
        ptb = self.window_ptb
        now_ts = ref_ts if ref_ts > 0 else (self.btc_price_buffer[-1][0] if self.btc_price_buffer else 0)
        cutoff = float(self.window_start_ts)
        items = [(ts, p) for ts, p in self.btc_price_buffer if cutoff <= ts <= now_ts]
        if len(items) < 2:
            return 0.5
        time_above = 0.0
        total_time = 0.0
        for i in range(1, len(items)):
            dt = items[i][0] - items[i - 1][0]
            if dt <= 0:
                continue
            total_time += dt
            if items[i - 1][1] > ptb:
                time_above += dt
        return time_above / total_time if total_time > 0 else 0.5

    def calc_ptb_dist_velocity(self, window_secs: float = 5.0, ref_ts: float = 0.0) -> float:
        """
        PTB 距离变化速度 ($/s): 正值 = BTC 在远离 PTB 向上; 负值 = 远离向下/回归。
        衡量当前价格离开 PTB 的紧迫程度。
        """
        if self.window_ptb <= 0 or len(self.btc_price_buffer) < 2:
            return 0.0
        ptb = self.window_ptb
        now_ts = ref_ts if ref_ts > 0 else self.btc_price_buffer[-1][0]
        p_now = self._price_at(self.btc_price_buffer, now_ts) or self.btc_price
        p_past = self._price_at(self.btc_price_buffer, now_ts - window_secs)
        if p_now <= 0 or p_past <= 0:
            return 0.0
        dist_now = p_now - ptb
        dist_past = p_past - ptb
        return (dist_now - dist_past) / window_secs

    def calc_ptb_extreme_dist(self, ref_ts: float = 0.0) -> tuple[float, float]:
        """
        本窗口内 BTC 距 PTB 的最大正距离和最大负距离。
        返回 (max_above, max_below), 其中 max_below <= 0。
        用于衡量窗口内的极端行情。
        """
        if self.window_ptb <= 0 or self.window_start_ts <= 0:
            return (0.0, 0.0)
        ptb = self.window_ptb
        now_ts = ref_ts if ref_ts > 0 else (self.btc_price_buffer[-1][0] if self.btc_price_buffer else 0)
        cutoff = float(self.window_start_ts)
        dists = [p - ptb for ts, p in self.btc_price_buffer if cutoff <= ts <= now_ts]
        if not dists:
            return (0.0, 0.0)
        return (max(dists), min(dists))

    # ── 历史时刻查询辅助方法 ──

    @staticmethod
    def _price_at(buf: deque, ref_ts: float) -> float:
        """查找缓冲区中 ref_ts 时刻(或之前最近)的价格"""
        for ts, price in reversed(buf):
            if ts <= ref_ts:
                return price
        return 0.0

    def _pm_state_at(self, ref_ts: float) -> dict:
        """查找 ref_ts 时刻(或之前最近)的 PM 报价快照
        pm_price_history 包含 {ts, up, dn, combined, edge, up_bid, up_ask, dn_bid, dn_ask}
        """
        for snap in reversed(self.pm_price_history):
            if snap["ts"] <= ref_ts:
                return snap
        # fallback: 无历史数据时返回当前值
        return {
            "ts": 0, "up": self.up_price, "dn": self.dn_price,
            "combined": self.combined_price, "edge": self.edge,
            "up_bid": self.up_bid, "up_ask": self.up_ask,
            "dn_bid": self.dn_bid, "dn_ask": self.dn_ask,
        }

    # ── 跨窗口状态辅助方法 ──

    def _cross_window_features(self) -> dict:
        """从 settled_windows 计算跨窗口状态特征"""
        settled = self.settled_windows
        if not settled:
            return {"prev_won": 0, "prev_pnl": 0.0, "session_pnl": 0.0,
                    "session_wr": 50.0, "win_streak": 0}

        last = settled[-1]
        prev_won = 1.0 if last["pnl"] > 0 else -1.0
        prev_pnl = last["pnl"]

        # 最近 10 个窗口
        recent = settled[-10:]
        session_pnl = sum(w["pnl"] for w in recent)
        wins = sum(1 for w in recent if w["pnl"] > 0)
        session_wr = wins / len(recent) * 100.0

        # 连赢/连输
        streak = 0
        last_dir = 1 if settled[-1]["pnl"] > 0 else -1
        for w in reversed(settled):
            d = 1 if w["pnl"] > 0 else -1
            if d == last_dir:
                streak += 1
            else:
                break
        win_streak = streak * last_dir  # +N=连赢, -N=连输

        return {"prev_won": prev_won, "prev_pnl": round(prev_pnl, 2),
                "session_pnl": round(session_pnl, 2),
                "session_wr": round(session_wr, 1),
                "win_streak": win_streak}

    def calc_btc_range_pct(self, ref_ts: float = 0.0) -> float:
        """BTC 在当前窗口内的高低幅度 (bps)
        从 window_start_ts 到 ref_ts, 计算 (max - min) / ptb * 10000
        """
        if self.window_ptb <= 0 or self.window_start_ts <= 0:
            return 0.0
        now_ts = ref_ts if ref_ts > 0 else (
            self.btc_price_buffer[-1][0] if self.btc_price_buffer else 0)
        cutoff = float(self.window_start_ts)
        prices = [p for ts, p in self.btc_price_buffer if cutoff <= ts <= now_ts]
        if len(prices) < 2:
            return 0.0
        return (max(prices) - min(prices)) / self.window_ptb * 10000

    def calc_pm_mid_velocity(self, window_secs: float, ref_ts: float = 0.0) -> float:
        """PM 中间价 (combined/2) 在 window_secs 内的变化速率 (¢/s)
        反映市场对 BTC 走势的定价反应速度
        """
        if not self.pm_price_history:
            return 0.0
        now_ts = ref_ts if ref_ts > 0 else self.pm_price_history[-1]["ts"]
        cutoff = now_ts - window_secs
        # 找 cutoff 附近的起点
        start_snap = None
        end_snap = None
        for snap in self.pm_price_history:
            if snap["ts"] >= cutoff and start_snap is None:
                start_snap = snap
        for snap in reversed(self.pm_price_history):
            if snap["ts"] <= now_ts:
                end_snap = snap
                break
        if not start_snap or not end_snap or start_snap is end_snap:
            return 0.0
        dt = end_snap["ts"] - start_snap["ts"]
        if dt < 0.5:
            return 0.0
        # combined = UP + DN, 中间价 = UP price
        delta = end_snap["up"] - start_snap["up"]
        return delta / dt * 100  # 转为 ¢/s

    def calc_side_switch_rate(self) -> float:
        """当前窗口内方向切换次数 / 总交易数"""
        trades = list(self.trades_0x1d)
        if len(trades) < 2:
            return 0.0
        switches = sum(1 for i in range(1, len(trades))
                       if trades[i]["side"] != trades[i-1]["side"])
        return switches / len(trades)

    def calc_dominant_side_ratio(self) -> float:
        """份额加权的方向比: UP / (UP + DN), 范围 [0, 1]
        0.5 = 均衡, >0.5 = 偏UP, <0.5 = 偏DN
        """
        total = self.cum_up_shares + self.cum_dn_shares
        if total <= 0:
            return 0.5
        return self.cum_up_shares / total

    # ── 多特征相关性引擎 ──
    # 特征定义: (key_in_snap, display_name, category, causality)
    #   causality = "exo"  → 外生特征: 纯市场状态, 不受交易者行为影响
    #   causality = "endo" → 内生特征: 仓位/交易节奏, 由交易者自身决策产生
    #   分层用途:
    #     exo only  → 提取纯市场信号 ("什么行情下他会入场")
    #     exo + endo → 复制完整策略 ("信号 + 风险管理")
    FEATURE_DEFS: list[tuple[str, str, str, str]] = [
        # RTDS (Chainlink) 动量
        ("mom_1s",   "CL动量 1s",    "RTDS",  "exo"),
        ("mom_3s",   "CL动量 3s",    "RTDS",  "exo"),
        ("mom_5s",   "CL动量 5s",    "RTDS",  "exo"),
        ("mom_10s",  "CL动量 10s",   "RTDS",  "exo"),
        ("mom_15s",  "CL动量 15s",   "RTDS",  "exo"),
        ("mom_30s",  "CL动量 30s",   "RTDS",  "exo"),
        # Binance 动量
        ("bn_mom_1s",  "BN动量 1s",   "BN",   "exo"),
        ("bn_mom_3s",  "BN动量 3s",   "BN",   "exo"),
        ("bn_mom_5s",  "BN动量 5s",   "BN",   "exo"),
        ("bn_mom_10s", "BN动量 10s",  "BN",   "exo"),
        ("bn_mom_15s", "BN动量 15s",  "BN",   "exo"),
        ("bn_mom_30s", "BN动量 30s",  "BN",   "exo"),
        # BTC 状态 (Chainlink)
        ("btc_delta_ptb",  "CL-PTB差",     "RTDS",  "exo"),
        ("btc_delta_pct",  "CL-PTB差%",    "RTDS",  "exo"),
        ("btc_vol_10s",    "CL波动 10s",   "RTDS",  "exo"),
        ("btc_vol_30s",    "CL波动 30s",   "RTDS",  "exo"),
        # BTC 状态 (Binance)
        ("bn_delta_ptb",   "BN-PTB差",     "BN",   "exo"),
        ("bn_delta_pct",   "BN-PTB差%",    "BN",   "exo"),
        ("bn_vol_10s",     "BN波动 10s",   "BN",   "exo"),
        ("bn_vol_30s",     "BN波动 30s",   "BN",   "exo"),
        # RTDS vs Binance 差异
        ("cl_bn_spread",   "CL-BN价差",    "差异", "exo"),
        ("cl_bn_mom_diff_5s",  "动量差 5s",  "差异", "exo"),
        ("cl_bn_mom_diff_10s", "动量差 10s", "差异", "exo"),
        # PM 市场 (外生: 全市场报价, 非交易者个人持仓)
        ("up_price",       "UP报价",       "PM",   "exo"),
        ("dn_price",       "DN报价",       "PM",   "exo"),
        ("pm_edge",        "PM边际",       "PM",   "exo"),
        ("up_bid",         "UP Bid",       "PM",   "exo"),
        ("up_ask",         "UP Ask",       "PM",   "exo"),
        # 窗口时序 (外生: 时间进度是客观的)
        ("elapsed",        "窗口进度s",    "时序", "exo"),
        ("elapsed_pct",    "窗口进度%",    "时序", "exo"),
        # ── 内生特征: 仓位状态 (由交易者自身决策产生) ──
        ("pos_imbalance",  "仓位偏差",     "仓位", "endo"),
        ("cum_trades",     "累积单数",     "仓位", "endo"),
        ("trade_velocity", "下单速度/min", "仓位", "endo"),
        # ── 扩展特征 (策略蒸馏) ──
        # 长周期动量
        ("mom_60s",    "CL动量 60s",   "RTDS",  "exo"),
        ("mom_120s",   "CL动量 120s",  "RTDS",  "exo"),
        ("bn_mom_60s",  "BN动量 60s",  "BN",   "exo"),
        ("bn_mom_120s", "BN动量 120s", "BN",   "exo"),
        # 扩展波动率
        ("btc_vol_60s", "CL波动 60s",  "RTDS",  "exo"),
        ("bn_vol_60s",  "BN波动 60s",  "BN",   "exo"),
        # 动量加速度 (短期 - 长期动量差)
        ("mom_accel_5s",     "CL加速 5-10s",  "RTDS",  "exo"),
        ("mom_accel_10s",    "CL加速 10-30s", "RTDS",  "exo"),
        ("bn_mom_accel_5s",  "BN加速 5-10s",  "BN",   "exo"),
        # 盘口宽度
        ("up_ba_spread", "UP盘口宽度", "PM",   "exo"),
        ("dn_ba_spread", "DN盘口宽度", "PM",   "exo"),
        ("dn_bid",       "DN Bid",     "PM",   "exo"),
        ("dn_ask",       "DN Ask",     "PM",   "exo"),
        # 方向信号
        ("btc_above_ptb",    "BTC>PTB",       "信号", "exo"),
        # 交易节奏 (内生: 取决于交易者过往操作)
        ("time_since_last",  "距上笔间隔s",   "时序", "endo"),
        ("same_side_streak", "连续同向笔",    "仓位", "endo"),
        # 趋势强度
        ("cl_trend_30s",     "CL趋势 30s",   "RTDS",  "exo"),
        ("cl_trend_60s",     "CL趋势 60s",   "RTDS",  "exo"),
        ("cl_trend_120s",    "CL趋势 120s",  "RTDS",  "exo"),
        ("bn_trend_30s",     "BN趋势 30s",   "BN",   "exo"),
        ("bn_trend_60s",     "BN趋势 60s",   "BN",   "exo"),
        # 价格百分位
        ("cl_pctl_60s",      "CL百分位 60s", "RTDS", "exo"),
        ("cl_pctl_300s",     "CL百分位 300s","RTDS", "exo"),
        ("bn_pctl_60s",      "BN百分位 60s", "BN",  "exo"),
        # 动量 Z-score
        ("cl_mom_z_5s",      "CL Z-score 5s", "RTDS", "exo"),
        ("cl_mom_z_10s",     "CL Z-score 10s","RTDS", "exo"),
        ("bn_mom_z_5s",      "BN Z-score 5s", "BN",  "exo"),
        # 震荡指标
        ("cl_dir_changes_30s", "CL反转次数 30s", "RTDS", "exo"),
        ("cl_dir_changes_60s", "CL反转次数 60s", "RTDS", "exo"),
        # 长周期动量
        ("mom_180s",         "CL动量 180s",  "RTDS",  "exo"),
        ("mom_300s",         "CL动量 300s",  "RTDS",  "exo"),
        ("bn_mom_180s",      "BN动量 180s",  "BN",   "exo"),
        # 扩展波动率
        ("btc_vol_120s",     "CL波动 120s",  "RTDS",  "exo"),
        ("bn_vol_120s",      "BN波动 120s",  "BN",   "exo"),
        # 扩展加速度
        ("mom_accel_30s",    "CL加速 30-60s", "RTDS",  "exo"),
        ("bn_mom_accel_10s", "BN加速 10-30s", "BN",   "exo"),
        # ── 内生特征: 仓位细节 ──
        ("cum_up_shares",    "累UP量",        "仓位", "endo"),
        ("cum_dn_shares",    "累DN量",        "仓位", "endo"),
        ("cum_up_cost",      "累UP成本",      "仓位", "endo"),
        ("cum_dn_cost",      "累DN成本",      "仓位", "endo"),
        ("avg_up_price",     "UP均价",        "仓位", "endo"),
        ("avg_dn_price",     "DN均价",        "仓位", "endo"),
        # PM定价效率
        ("up_price_vs_fair", "UP偏离公允",    "PM",   "exo"),
        ("dn_price_vs_fair", "DN偏离公允",    "PM",   "exo"),
        # 跨源一致性
        ("cl_bn_trend_agree","CL-BN趋势一致","差异", "exo"),
        # Burst 特征 (内生: burst是交易者自己的连续下单行为)
        ("burst_seq",        "Burst内序号",   "时序", "endo"),
        ("is_burst",         "是否Burst",     "时序", "endo"),
        # RTDS-PTB 比对特征
        ("rtds_implied_prob",  "RTDS隐含UP概率",  "RTDS-PTB", "exo"),
        ("pm_rtds_prob_gap",   "PM-RTDS概率偏差", "RTDS-PTB", "exo"),
        ("ptb_dist_vs_vol",    "PTB距离/波动",    "RTDS-PTB", "exo"),
        ("ptb_cross_count",    "PTB穿越次数",     "RTDS-PTB", "exo"),
        ("ptb_time_above",     "PTB上方时间比",    "RTDS-PTB", "exo"),
        ("ptb_dist_vel_5s",    "PTB距离速度5s",   "RTDS-PTB", "exo"),
        ("ptb_max_above",      "PTB最大正距离",    "RTDS-PTB", "exo"),
        ("ptb_max_below",      "PTB最大负距离",    "RTDS-PTB", "exo"),
        # ── 时间特征 (UTC, 已在 trade snap 中计算) ──
        ("hour_utc",           "UTC时",            "时序",  "exo"),
        ("minute_utc",         "UTC分",            "时序",  "exo"),
        ("day_of_week",        "星期几",           "时序",  "exo"),
        ("us_session",         "美股盘",           "时序",  "exo"),
        ("asia_session",       "亚洲盘",           "时序",  "exo"),
        ("euro_session",       "欧洲盘",           "时序",  "exo"),
        # ── 跨窗口状态 (由历史结算结果驱动) ──
        ("prev_won",           "前窗口赢",         "跨窗口", "endo"),
        ("prev_pnl",           "前窗口PnL",        "跨窗口", "endo"),
        ("session_pnl",        "会话累积PnL",      "跨窗口", "endo"),
        ("session_wr",         "近期胜率%",        "跨窗口", "endo"),
        ("win_streak",         "连赢/连输",        "跨窗口", "endo"),
        # ── 窗口内交易强度 ──
        ("trade_density",      "交易密度/min",     "仓位",  "endo"),
        ("cum_volume",         "累积交易额",       "仓位",  "endo"),
        ("side_switch_rate",   "方向切换率",       "仓位",  "endo"),
        ("dominant_side_ratio","多空份额比",       "仓位",  "endo"),
        # ── BTC 窗口走势 ──
        ("btc_range_pct",      "BTC窗口幅bps",    "RTDS",  "exo"),
        # ── PM 报价速度 ──
        ("pm_mid_vel_10s",     "PM速度10s",       "PM",    "exo"),
        ("pm_mid_vel_30s",     "PM速度30s",       "PM",    "exo"),
    ]

    def calc_feature_correlations(self, snaps: list[dict], scope: str = "all") -> dict:
        """计算交易方向(UP=+1,DN=-1)与特征的 Pearson r + 方向一致率
        scope: "all"=全部特征, "exo"=仅外生, "endo"=仅内生
        返回: {features: [{key, name, cat, causality, r, abs_r, agree, rank}], n, best_key, best_name, scope}
        """
        n = len(snaps)
        if n < 3:
            return {"features": [], "n": n, "best_key": "--", "best_name": "--", "scope": scope}
        sides = [1.0 if s.get("side") == "UP" else -1.0 for s in snaps]
        results = []
        for key, name, cat, causality in self.FEATURE_DEFS:
            # 按因果性过滤
            if scope == "exo" and causality != "exo":
                continue
            if scope == "endo" and causality != "endo":
                continue
            vals = [s.get(key, 0.0) for s in snaps]
            # 跳过全零特征
            if all(v == 0 for v in vals):
                continue
            r = self._pearson(sides, vals)
            # 方向一致率
            matched = sum(1 for s, v in zip(sides, vals)
                          if (s > 0 and v > 0) or (s < 0 and v < 0))
            non_zero = sum(1 for v in vals if v != 0)
            agree = matched / non_zero * 100 if non_zero > 0 else 0.0
            results.append({
                "key": key, "name": name, "cat": cat, "causality": causality,
                "r": round(r, 4), "abs_r": round(abs(r), 4),
                "agree": round(agree, 1),
            })
        # 按 |r| 降序排名
        results.sort(key=lambda x: x["abs_r"], reverse=True)
        for i, f in enumerate(results):
            f["rank"] = i + 1
        best = results[0] if results else {"key": "--", "name": "--"}
        return {
            "features": results,
            "n": n,
            "best_key": best.get("key", "--"),
            "best_name": best.get("name", "--"),
            "scope": scope,
        }

    def _aligned_bn_history(self) -> list[dict]:
        """返回与 btc_history[-120:] 时间范围对齐的 Binance 历史"""
        btc_slice = list(self.btc_history)[-120:]
        if not btc_slice or not self.bn_history:
            return list(self.bn_history)[-120:]
        min_ts = btc_slice[0]["ts"]
        return [h for h in self.bn_history if h["ts"] >= min_ts]

    def reset_window(self):
        """窗口切换时重置"""
        # 当前窗口交易快照归入全局累积
        self.trade_momentum_all.extend(self.trade_momentum_snaps)
        self.cum_up_shares = 0.0
        self.cum_dn_shares = 0.0
        self.cum_up_cost = 0.0
        self.cum_dn_cost = 0.0
        self.trade_count_window = 0
        self.burst_events = []
        self.trades_0x1d.clear()
        self.last_trade_ts = ""
        self.clob_trades.clear()
        self.pnl_history.clear()
        self.momentum_history.clear()
        self.trade_momentum_snaps = []

    def snapshot(self) -> dict:
        """生成完整快照 (发送给前端)"""
        now = time.time()
        secs_left = max(0, self.window_end_ts - now) if self.window_end_ts > 0 else 0
        window_elapsed = max(0, 300 - secs_left)

        gap = self.cum_up_shares - self.cum_dn_shares
        total = max(self.cum_up_shares, self.cum_dn_shares, 1)
        gap_pct = gap / total * 100

        # BTC 动量 (1s/3s/5s)
        mom_1s = self.calc_momentum(1.0)
        mom_3s = self.calc_momentum(3.0)
        mom_5s = self.calc_momentum(5.0)
        momentum = mom_5s  # 保持兼容

        # 追加动量历史点
        if self.btc_price > 0:
            elapsed_sec = round(window_elapsed)
            self.momentum_history.append({
                "ts": now,
                "elapsed": elapsed_sec,
                "mom_1s": round(mom_1s, 2),
                "mom_3s": round(mom_3s, 2),
                "mom_5s": round(mom_5s, 2),
            })

        # ── 盈亏计算 ──
        total_cost = self.cum_up_cost + self.cum_dn_cost

        # Mark-to-Market PnL: 按当前市场报价估值
        mtm_value = (self.cum_up_shares * self.up_price
                     + self.cum_dn_shares * self.dn_price)
        pnl_mtm = mtm_value - total_cost if total_cost > 0 else 0.0

        # 结算 PnL: 赢方 $1/share, 输方 $0
        pnl_if_up = self.cum_up_shares - total_cost if total_cost > 0 else 0.0
        pnl_if_dn = self.cum_dn_shares - total_cost if total_cost > 0 else 0.0

        # 当前预期 PnL: 根据 BTC 方向
        btc_up = self.btc_price > self.window_ptb if self.window_ptb > 0 and self.btc_price > 0 else None
        if btc_up is True:
            pnl_expected = pnl_if_up
        elif btc_up is False:
            pnl_expected = pnl_if_dn
        else:
            pnl_expected = 0.0

        # 平均成本
        avg_up_price = (self.cum_up_cost / self.cum_up_shares) if self.cum_up_shares > 0 else 0.0
        avg_dn_price = (self.cum_dn_cost / self.cum_dn_shares) if self.cum_dn_shares > 0 else 0.0

        # ROI %
        pnl_mtm_pct = (pnl_mtm / total_cost * 100) if total_cost > 0 else 0.0
        pnl_expected_pct = (pnl_expected / total_cost * 100) if total_cost > 0 else 0.0

        # 历史结算汇总
        settled_total_pnl = sum(w["pnl"] for w in self.settled_windows)
        settled_wins = sum(1 for w in self.settled_windows if w["pnl"] > 0)
        settled_total = len(self.settled_windows)

        # 追加 PnL 历史点 (供图表使用)
        if total_cost > 0:
            self.pnl_history.append({
                "ts": now,
                "mtm": round(pnl_mtm, 2),
                "expected": round(pnl_expected, 2),
            })

        return {
            "ts": now,
            "time": datetime.fromtimestamp(now, tz=timezone.utc).strftime("%H:%M:%S"),

            # BTC
            "btc_price": round(self.btc_price, 2),
            "bn_price": round(self.bn_price, 2),
            "btc_momentum": round(momentum, 2),
            "btc_momentum_1s": round(mom_1s, 2),
            "btc_momentum_3s": round(mom_3s, 2),
            "btc_momentum_5s": round(mom_5s, 2),
            # BTC vs PTB (与 FEATURE_DEFS 命名一致)
            "btc_delta_ptb": round(self.btc_price - self.window_ptb, 2) if self.window_ptb > 0 else 0,
            "btc_delta_pct": round((self.btc_price - self.window_ptb) / self.window_ptb * 10000, 2) if self.window_ptb > 0 else 0,  # bps
            "bn_delta_ptb": round(self.bn_price - self.window_ptb, 2) if self.window_ptb > 0 and self.bn_price > 0 else 0,
            "bn_delta_pct": round((self.bn_price - self.window_ptb) / self.window_ptb * 10000, 2) if self.window_ptb > 0 and self.bn_price > 0 else 0,  # bps
            "btc_history": list(self.btc_history)[-120:],
            # Binance 历史: 对齐到 btc_history 的时间范围 (避免 X 轴不一致)
            "bn_history": self._aligned_bn_history(),

            # PM 窗口
            "window_slug": self.window_slug,
            "window_question": self.window_question,
            "window_ptb": round(self.window_ptb, 2),
            "secs_left": round(secs_left),
            "window_elapsed": round(window_elapsed),
            "window_progress_pct": round(window_elapsed / 300 * 100, 1),

            # PM 报价
            "up_price": round(self.up_price, 4),
            "dn_price": round(self.dn_price, 4),
            "up_bid": round(self.up_bid, 4),
            "up_ask": round(self.up_ask, 4),
            "dn_bid": round(self.dn_bid, 4),
            "dn_ask": round(self.dn_ask, 4),
            "combined_price": round(self.combined_price, 4),
            "edge": round(self.edge, 4),
            "pm_price_history": list(self.pm_price_history)[-120:],

            # 0x1d 交易
            "trades": list(self.trades_0x1d)[-100:],
            "cum_up_shares": round(self.cum_up_shares, 2),
            "cum_dn_shares": round(self.cum_dn_shares, 2),
            "cum_up_cost": round(self.cum_up_cost, 2),
            "cum_dn_cost": round(self.cum_dn_cost, 2),
            "gap_shares": round(gap, 2),
            "gap_pct": round(gap_pct, 1),
            "trade_count": self.trade_count_window,
            "total_cost": round(total_cost, 2),
            "bursts": self.burst_events[-20:],

            # 盈亏
            "pnl_mtm": round(pnl_mtm, 2),
            "pnl_mtm_pct": round(pnl_mtm_pct, 1),
            "pnl_if_up": round(pnl_if_up, 2),
            "pnl_if_dn": round(pnl_if_dn, 2),
            "pnl_expected": round(pnl_expected, 2),
            "pnl_expected_pct": round(pnl_expected_pct, 1),
            "avg_up_price": round(avg_up_price, 4),
            "avg_dn_price": round(avg_dn_price, 4),
            "pnl_history": list(self.pnl_history)[-120:],
            "settled_windows": self.settled_windows,
            "settled_total_pnl": round(settled_total_pnl, 2),
            "settled_wins": settled_wins,
            "settled_total": settled_total,

            # 跨窗口状态
            "cross_window": self._cross_window_features(),

            # 动量 + 下单关联
            "momentum_history": list(self.momentum_history)[-300:],
            "trade_momentum_snaps": self.trade_momentum_snaps[-200:],

            # 多特征相关性分析 (策略逆向)
            # all = 全部特征 (完整策略: 信号 + 风险管理)
            "feature_corr_window": self.calc_feature_correlations(self.trade_momentum_snaps),
            "feature_corr_global": self.calc_feature_correlations(
                list(self.trade_momentum_all) + self.trade_momentum_snaps
            ),
            # exo = 仅外生特征 (纯市场信号, 排除仓位状态的影响)
            "feature_corr_exo_window": self.calc_feature_correlations(self.trade_momentum_snaps, scope="exo"),
            "feature_corr_exo_global": self.calc_feature_correlations(
                list(self.trade_momentum_all) + self.trade_momentum_snaps, scope="exo"
            ),

            # ── RTDS-PTB 比对实时指标 ──
            "rtds_implied_prob": round(self.calc_rtds_implied_prob(), 4),
            "pm_rtds_prob_gap": round(self.calc_pm_rtds_prob_gap(), 4),
            "ptb_dist_vs_vol": round(self.calc_ptb_dist_vs_vol(), 3),
            "ptb_cross_count": self.calc_ptb_cross_count(),
            "ptb_time_above": round(self.calc_ptb_time_above_ratio(), 3),
            "ptb_dist_vel_5s": round(self.calc_ptb_dist_velocity(5.0), 2),
            "ptb_max_above": round(self.calc_ptb_extreme_dist()[0], 2),
            "ptb_max_below": round(self.calc_ptb_extreme_dist()[1], 2),

            # 状态
            "binance_ok": self.binance_connected,
            "pm_ok": self.pm_connected,
            "activity_ok": self.activity_connected,
            "rtds_ok": self.rtds_connected,
            "rtds_reconnects": self.rtds_reconnect_count,
            "rtds_uptime": round(self.rtds_total_uptime + (
                (time.time() - self.rtds_connect_ts) if self.rtds_connected and self.rtds_connect_ts > 0 else 0
            )),
            "rtds_last_data_age": round(time.time() - self.rtds_last_data_ts, 1) if self.rtds_last_data_ts > 0 else -1,
            "clob_ws_ok": self.clob_ws_connected,
            "btc_source": self.btc_source,
            "ptb_source": self.ptb_source,
            "clob_trades_buf": len(self.clob_trades),
            "warmup": self.warmup,
        }


state = MonitorState()


# ─────────────────────────────────────────────────────────
#  数据采集: Binance BTC/USDT WebSocket
# ─────────────────────────────────────────────────────────

async def binance_btc_stream():
    """连接 Binance WebSocket 获取 BTC 实时价格 (备用数据源)
    
    当 Chainlink RTDS 正常连接时, Binance 只作为连接保活,
    不更新 state.btc_price 和价格缓冲区 (避免覆盖 Chainlink 数据)。
    当 RTDS 断开时, Binance 自动接管。
    """
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                # 先 REST 拿初始价格
                try:
                    async with session.get(BINANCE_REST, timeout=aiohttp.ClientTimeout(total=10)) as r:
                        data = await r.json()
                        price = float(data["price"])
                        # Binance 始终记录 (图表对比 + 独立动量/波动计算)
                        state.bn_price = price
                        state.bn_price_buffer.append((time.time(), price))
                        state.bn_history.append({"ts": time.time(), "price": price})
                        if not state.rtds_connected:
                            state.btc_price = price
                            state.btc_price_buffer.append((time.time(), price))
                            state.btc_history.append({"ts": time.time(), "price": price})
                            state.btc_source = "Binance"
                            if state.window_ptb <= 0:
                                state.window_ptb = price
                                state.ptb_source = "Binance初始"
                        print(f"[Binance] 初始 BTC: ${price:,.2f} (备用)")
                except Exception as e:
                    print(f"[Binance] REST 失败: {e}")

                # WebSocket 实时 (备用: 仅当 RTDS 未连接时写入)
                async with session.ws_connect(BINANCE_WS) as ws:
                    state.binance_connected = True
                    print("[Binance] WebSocket 已连接 (备用)")
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            data = json.loads(msg.data)
                            price = float(data["p"])
                            now = time.time()
                            # Binance 始终记录 (图表对比 + 独立动量/波动)
                            state.bn_price = price
                            state.bn_price_buffer.append((now, price))
                            if not state.bn_history or now - state.bn_history[-1]["ts"] >= BTC_DOWNSAMPLE_INTERVAL:
                                state.bn_history.append({"ts": now, "price": price})
                            # 仅当 RTDS 未连接时, Binance 接管价格更新
                            if not state.rtds_connected:
                                state.btc_price = price
                                state.btc_source = "Binance"
                                # 写入高精度缓冲区
                                state.btc_price_buffer.append((now, price))
                                # 降采样 UI 历史
                                if not state.btc_history or now - state.btc_history[-1]["ts"] >= BTC_DOWNSAMPLE_INTERVAL:
                                    state.btc_history.append({"ts": now, "price": price})
                        elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                            break
        except Exception as e:
            print(f"[Binance] 连接异常: {e}")
        state.binance_connected = False
        print("[Binance] 5s 后重连...")
        await asyncio.sleep(5)


# ─────────────────────────────────────────────────────────
#  数据采集: Chainlink BTC/USD via PM RTDS WebSocket
#  (与 Polymarket 结算价一致)
# ─────────────────────────────────────────────────────────

def _parse_rtds_price(raw: str) -> list[tuple[float, float]]:
    """
    解析 PM RTDS 消息, 返回 [(price, obs_ts_sec), ...] 列表。
    只保留 btc/usd, 过滤其他币种。

    消息格式:
      1) 空字符串: 连接确认, 跳过
      2) type=subscribe: 历史快照 payload.data[{timestamp, value}]
      3) type=update:    实时推送 payload.{symbol, value, timestamp}
    """
    if not raw or not raw.strip():
        return []
    try:
        obj = json.loads(raw)
    except json.JSONDecodeError:
        return []

    results: list[tuple[float, float]] = []
    payload = obj.get("payload", {})
    if not isinstance(payload, dict):
        return []

    msg_type = obj.get("type", "")

    # type=update: 单值实时推送 (每秒 ~1 条/币种)
    if msg_type == "update":
        sym = payload.get("symbol", "").lower()
        if sym != "btc/usd":
            return []
        price = float(payload.get("value", 0))
        if price > 0:
            ts = payload.get("timestamp", time.time() * 1000)
            if ts > 1e12:
                ts = ts / 1000.0
            results.append((price, ts))
        return results

    # type=subscribe: 历史快照 (payload.data 数组)
    if msg_type == "subscribe":
        sym = payload.get("symbol", "").lower()
        if sym != "btc/usd":
            return []
        data_arr = payload.get("data")
        if isinstance(data_arr, list):
            for item in data_arr:
                price = float(item.get("value", 0))
                ts = item.get("timestamp", time.time() * 1000)
                if ts > 1e12:
                    ts = ts / 1000.0
                if price > 0:
                    results.append((price, ts))
        return results

    # 兼容: 其他未知格式
    price = float(payload.get("value", 0))
    if price > 0:
        sym = payload.get("symbol", "").lower()
        if sym and sym != "btc/usd":
            return []
        ts = payload.get("timestamp", time.time() * 1000)
        if ts > 1e12:
            ts = ts / 1000.0
        results.append((price, ts))
    return results


RTDS_PING_INTERVAL = 10       # 客户端主动 ping 间隔 (秒)
RTDS_DATA_TIMEOUT  = 15       # 无 BTC 数据超时 → 断开重连 (秒)
RTDS_PONG_TIMEOUT  = 8        # ping 后未收到 pong → 判定连接死亡 (秒)
RTDS_BOUNDARY_WARN = 20       # 边界前 N 秒若 RTDS 不健康则告警


async def _rtds_ping_loop(ws: aiohttp.ClientWebSocketResponse):
    """后台 ping 保活: 每 RTDS_PING_INTERVAL 秒发送 ping 帧,
    并检测 pong 响应是否超时。如果超时直接关闭 ws 触发重连。"""
    while not ws.closed:
        try:
            await ws.ping()
            await asyncio.sleep(RTDS_PING_INTERVAL)
            # 检查 pong 是否在超时时间内回复
            if state.rtds_last_pong_ts > 0:
                pong_age = time.time() - state.rtds_last_pong_ts
                if pong_age > RTDS_PING_INTERVAL + RTDS_PONG_TIMEOUT:
                    print(
                        f"[Chainlink RTDS] pong 超时 "
                        f"({pong_age:.1f}s > {RTDS_PING_INTERVAL + RTDS_PONG_TIMEOUT}s), "
                        f"关闭连接触发重连"
                    )
                    await ws.close()
                    return
        except (asyncio.CancelledError, Exception):
            return


async def _rtds_boundary_watchdog():
    """边界看门狗: 在每个 5-min 边界前 RTDS_BOUNDARY_WARN 秒检查 RTDS 健康度。
    如果此时 RTDS 不在线, 打印告警以便排查结算来源降级。"""
    while True:
        try:
            now = time.time()
            now_int = int(now)
            next_boundary = now_int - (now_int % 300) + 300
            secs_to_boundary = next_boundary - now
            # 睡到边界前 RTDS_BOUNDARY_WARN 秒
            if secs_to_boundary > RTDS_BOUNDARY_WARN:
                await asyncio.sleep(secs_to_boundary - RTDS_BOUNDARY_WARN)
            # 检查 RTDS 健康
            if not state.rtds_connected:
                print(
                    f"[RTDS 边界告警] 距下一个 5-min 边界仅 "
                    f"{RTDS_BOUNDARY_WARN}s, 但 RTDS 未连接! "
                    f"(重连次数={state.rtds_reconnect_count})"
                )
            elif state.rtds_last_data_ts > 0:
                data_age = time.time() - state.rtds_last_data_ts
                if data_age > 10:
                    print(
                        f"[RTDS 边界告警] 距下一个 5-min 边界仅 "
                        f"{RTDS_BOUNDARY_WARN}s, RTDS 已 {data_age:.0f}s 无数据!"
                    )
            # 睡到边界后再循环
            await asyncio.sleep(RTDS_BOUNDARY_WARN + 2)
        except asyncio.CancelledError:
            break
        except Exception:
            await asyncio.sleep(5)


async def chainlink_rtds_stream():
    """
    连接 PM RTDS WebSocket 获取 Chainlink BTC/USD 实时价格。
    这是 Polymarket 结算使用的价格源, 与 Binance BTC/USDT 有微小差异。

    PTB 捕获: 当 observationsTimestamp % 300 == 0 时, 该价格即为窗口基准价。

    保活机制:
      - aiohttp heartbeat=5s (库级 ping/pong)
      - 客户端主动 ping 每 10s + pong 超时检测
      - 数据看门狗 15s 无 BTC 数据 → 断开重连
      - 边界看门狗: 5-min 边界前 20s 检查健康度
    """
    fail_count = 0
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(
                    PM_RTDS_WS,
                    heartbeat=None,      # 不用 aiohttp 内置心跳, 用我们自己的 _rtds_ping_loop
                    timeout=aiohttp.ClientWSTimeout(ws_close=15),
                    autoping=False,      # 手动处理 PING/PONG, 否则 PONG 被吞导致超时误判
                    autoclose=False,     # 手动控制关闭, 避免静默断连
                ) as ws:
                    # 订阅 crypto_prices_chainlink (无 filters, 客户端过滤 btc/usd)
                    await ws.send_json({
                        "action": "subscribe",
                        "subscriptions": [
                            {
                                "topic": "crypto_prices_chainlink",
                                "type": "*",
                            }
                        ],
                    })
                    state.rtds_connected = True
                    state.btc_source = "Chainlink"
                    state.rtds_connect_ts = time.time()
                    state.rtds_last_pong_ts = time.time()  # 初始化
                    fail_count = 0
                    if state.rtds_reconnect_count > 0:
                        print(
                            f"[Chainlink RTDS] WebSocket 已重连 "
                            f"(累计重连 {state.rtds_reconnect_count} 次), "
                            f"订阅 crypto_prices_chainlink"
                        )
                    else:
                        print("[Chainlink RTDS] WebSocket 已连接, 订阅 crypto_prices_chainlink")

                    last_data_time = time.time()

                    # 启动后台 ping 保活协程
                    ping_task = asyncio.create_task(_rtds_ping_loop(ws))

                    try:
                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                prices = _parse_rtds_price(msg.data)
                                if prices:
                                    last_data_time = time.time()
                                    state.rtds_last_data_ts = time.time()
                                for price, obs_ts in prices:
                                    now = time.time()
                                    state.btc_price = price
                                    state.rtds_last_obs_ts = int(obs_ts)

                                    # 写入价格缓冲区 (用 observationsTimestamp)
                                    state.btc_price_buffer.append((obs_ts, price))
                                    # 降采样 UI 历史
                                    if (not state.btc_history
                                            or now - state.btc_history[-1]["ts"] >= BTC_DOWNSAMPLE_INTERVAL):
                                        state.btc_history.append({"ts": now, "price": price})

                                    # ━━ PTB 精确捕获 + RTDS 即时结算 ━━
                                    # Chainlink observationsTimestamp % 300 == 0 → 窗口基准价
                                    # 该价格同时是旧窗口的结算价 + 新窗口的开盘价
                                    obs_ts_int = int(obs_ts)
                                    if obs_ts_int % 300 == 0 and obs_ts_int > 0:
                                        old_ptb = state.window_ptb

                                        # ── 保存边界信息 (供延迟结算使用) ──
                                        # 必须在覆写 window_ptb 之前保存!
                                        state._rtds_boundary = {
                                            "settle_price": price,
                                            "old_ptb": old_ptb,
                                            "obs_ts": obs_ts_int,
                                            "at": time.time(),
                                        }
                                        # 注: 不在此处直接结算, 由 _settle_pending 的延迟任务统一处理
                                        # 这样可以等待 Activity API 迟到交易追补到 _pending_settle

                                        # ── 新窗口 PTB ──
                                        state.window_ptb = price
                                        state.ptb_source = "Chainlink obs"
                                        print(
                                            f"[Chainlink RTDS] PTB 捕获 "
                                            f"(observationsTimestamp={obs_ts_int}): "
                                            f"${price:,.2f} (旧=${old_ptb:,.2f})"
                                        )
                            elif msg.type == aiohttp.WSMsgType.PONG:
                                state.rtds_last_pong_ts = time.time()
                            elif msg.type == aiohttp.WSMsgType.PING:
                                # autoping=False → 手动回复服务器 PING
                                await ws.pong(msg.data)
                            elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                                print(
                                    f"[Chainlink RTDS] WS 收到 {msg.type.name}, "
                                    f"本次在线 {time.time() - state.rtds_connect_ts:.0f}s"
                                )
                                break

                            # 数据看门狗: RTDS_DATA_TIMEOUT 秒无 BTC 数据 → 断开重连
                            if time.time() - last_data_time > RTDS_DATA_TIMEOUT:
                                print(
                                    f"[Chainlink RTDS] {RTDS_DATA_TIMEOUT}s 无数据, "
                                    f"断开重连 (本次在线 {time.time() - state.rtds_connect_ts:.0f}s)"
                                )
                                break
                    finally:
                        ping_task.cancel()
                        try:
                            await ping_task
                        except asyncio.CancelledError:
                            pass

        except asyncio.CancelledError:
            break
        except Exception as e:
            fail_count += 1
            print(f"[Chainlink RTDS] 连接异常 ({fail_count}): {e}")

        # 记录本次连接时长
        if state.rtds_connect_ts > 0:
            session_dur = time.time() - state.rtds_connect_ts
            state.rtds_total_uptime += session_dur
        state.rtds_connected = False
        state.rtds_reconnect_count += 1
        # 降级: RTDS 断了, Binance 仍然在作为备用数据源运行
        if state.binance_connected:
            state.btc_source = "Binance"
        # 自适应重连: 首次 1s, 连续失败逐步增加到 10s
        backoff = min(1 + fail_count * 2, 10)
        print(f"[Chainlink RTDS] {backoff}s 后重连 (累计重连 {state.rtds_reconnect_count} 次)...")
        await asyncio.sleep(backoff)


# ─────────────────────────────────────────────────────────
#  数据采集: Polymarket 5-min 窗口发现 + 报价轮询
# ─────────────────────────────────────────────────────────

def _do_settle(settle_price: float, ptb: float, src: str = "RTDS"):
    """用指定价格即时结算 _pending_settle"""
    ps = state._pending_settle
    if not ps:
        return
    # PTB 恢复: 如果 ptb=0, 从价格缓冲区回溯旧窗口开始时间
    if ptb <= 0 and ps.get("end_ts"):
        ptb = state.lookup_price_at(float(ps["end_ts"] - 300))
        if ptb > 0:
            print(f"[结算] PTB恢复: 从缓冲区回溯得 PTB=${ptb:,.2f}")
    if ptb <= 0:
        print(f"[结算] ⚠ PTB=0 无法判定 UP/DOWN, 跳过结算")
        state._pending_settle = None
        return
    won = "UP" if settle_price > ptb else "DOWN"
    total_cost = ps["cum_up_cost"] + ps["cum_dn_cost"]
    if total_cost > 0:
        payout = ps["cum_up_shares"] if won == "UP" else ps["cum_dn_shares"]
        pnl = payout - total_cost
        state.settled_windows.append({
            "slug": ps["slug"],
            "question": ps["question"],
            "won": won,
            "cost": round(total_cost, 2),
            "payout": round(payout, 2),
            "pnl": round(pnl, 2),
            "trades": ps["trade_count"],
            "ptb": round(ptb, 2),
            "settle_price": round(settle_price, 2),
            "settle_src": src,
        })
        if len(state.settled_windows) > 500:
            state.settled_windows = state.settled_windows[-500:]
        _q = ps.get('question', '') or ps.get('slug', '')
        print(
            f"[结算] {won} 赢 | PnL=${pnl:.2f} | "
            f"结算价=${settle_price:,.2f} vs PTB=${ptb:,.2f} | "
            f"来源={src} | {_q}"
        )
        _db_save_settlement(state.settled_windows[-1])
    else:
        _q = ps.get('question', '') or ps.get('slug', '')
        print(
            f"[结算] 无交易 (cost=0), 跳过 | "
            f"结算价=${settle_price:,.2f} vs PTB=${ptb:,.2f} | {_q}"
        )
    state._pending_settle = None


def _resolve_ptb(end_ts: int) -> float:
    """获取旧窗口的 PTB, 多层回退确保不为 0。
    优先级: _rtds_boundary.old_ptb > state.window_ptb > buffer回溯(窗口开始)
    """
    bd = state._rtds_boundary
    if bd and time.time() - bd.get("at", 0) < 30:
        ptb = bd["old_ptb"]
        if ptb > 0:
            return ptb
    if state.window_ptb > 0:
        return state.window_ptb
    # 从缓冲区回溯旧窗口开始时间
    if end_ts:
        ptb = state.lookup_price_at(float(end_ts - 300))
        if ptb > 0:
            return ptb
    return 0.0


def _get_settle_price(end_ts: int) -> tuple[float, str]:
    """获取窗口结束时的结算价。
    优先级: RTDS boundary > buffer回溯(窗口结束) > 当前价
    返回 (settle_price, source_label)
    """
    # 1. RTDS 边界缓存 (最精确)
    bd = state._rtds_boundary
    if bd and time.time() - bd.get("at", 0) < 30:
        if end_ts and abs(bd["obs_ts"] - end_ts) <= 5:
            return bd["settle_price"], "RTDS"
    # 2. 缓冲区回溯到窗口结束时刻
    p = state.lookup_price_at(float(end_ts))
    if p > 0:
        return p, "Buffer回溯"
    # 3. 当前价
    if state.btc_price > 0:
        return state.btc_price, "当前价"
    return 0.0, "无"


SETTLE_LATE_GRACE = 20  # 结算宽限期 (秒): 等待 RTDS 边界价到达 + Activity API 迟到交易追补


def _settle_pending():
    """创建 _pending_settle 后, 始终等待 SETTLE_LATE_GRACE 秒再结算。
    宽限期内:
      - RTDS 边界价自然写入 _rtds_boundary (RTDS handler 不再直接结算)
      - Activity API 迟到交易自然追补到 _pending_settle
    宽限期结束后, _get_settle_price() 采用最优价格源结算。
    """
    ps = state._pending_settle
    if not ps:
        return

    print(f"[结算] 等待 RTDS 边界价 + 迟到交易追补 ({SETTLE_LATE_GRACE}s)...")

    async def _grace_settle():
        await asyncio.sleep(SETTLE_LATE_GRACE)
        if not state._pending_settle:
            return  # 已被其他路径结算 (不应发生)
        ps2 = state._pending_settle
        settle_price, src = _get_settle_price(ps2["end_ts"])
        if settle_price <= 0:
            settle_price = state.btc_price
            src = "当前价"
        if settle_price > 0:
            _do_settle(settle_price, ps2["ptb"] or 0.0, src)
        else:
            print("[结算] ⚠ 宽限期后仍无结算价, 等待超时回退")

    try:
        asyncio.get_event_loop().create_task(_grace_settle())
    except RuntimeError:
        # 无事件循环 (不应发生), 直接结算
        settle_price, src = _get_settle_price(ps["end_ts"])
        if settle_price > 0:
            _do_settle(settle_price, ps["ptb"] or 0.0, src)


def calc_window_ts() -> tuple[int, int, float]:
    """计算当前 5-min 窗口的起止时间戳"""
    now = time.time()
    now_int = int(now)
    window_start = now_int - (now_int % 300)
    window_end = window_start + 300
    secs_left = window_end - now
    return window_start, window_end, secs_left


async def discover_pm_window(session: aiohttp.ClientSession) -> bool:
    """发现当前 PM 5-min BTC 窗口"""
    ws, we, sl = calc_window_ts()
    slug = f"{SLUG_PREFIX}{ws}"

    if slug == state.window_slug:
        return True  # 已经是当前窗口

    def _fallback_switch():
        """Gamma API 不可用/返回空时, 至少基于时间切换窗口slug"""
        if state.warmup:
            # 预热期结束: 跳过首个不完整窗口的结算
            print(f"[预热] 预热结束 (丢弃不完整窗口: trades={state.trade_count_window}), 正式开始统计")
            state.warmup = False
        elif state.window_slug and state.trade_count_window > 0:
            saved_ptb = _resolve_ptb(state.window_end_ts)
            state._pending_settle = {
                "slug": state.window_slug,
                "question": state.window_question,
                "end_ts": state.window_end_ts,
                "ptb": saved_ptb,
                "cum_up_shares": state.cum_up_shares,
                "cum_dn_shares": state.cum_dn_shares,
                "cum_up_cost": state.cum_up_cost,
                "cum_dn_cost": state.cum_dn_cost,
                "trade_count": state.trade_count_window,
                "end_up_price": state.up_price,
                "end_dn_price": state.dn_price,
            }
            _settle_pending()
        state.window_slug = slug
        state.window_start_ts = ws
        state.window_end_ts = we
        ptb = state.lookup_price_at(float(ws))
        if ptb <= 0:
            ptb = state.btc_price
        state.window_ptb = ptb
        state.ptb_source = "时间回退"
        state.reset_window()

    try:
        url = f"{GAMMA_API}/events?slug={slug}"
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
            events = await r.json()
            if not events:
                print(f"[PM] Gamma API 返回空 (事件可能尚未创建), 回退切换: {slug}")
                _fallback_switch()
                return False

            ev = events[0]
            markets = ev.get("markets", [])
            if not markets:
                print(f"[PM] 事件无 market, 回退切换: {slug}")
                _fallback_switch()
                return False

            # 只有 1 个 market，包含 2 个 outcomes (Up / Down)
            mkt = markets[0]

            # 解析 outcomes 和 token IDs
            outcomes_raw = mkt.get("outcomes", "[]")
            tokens_raw = mkt.get("clobTokenIds", "[]")
            outcomes = json.loads(outcomes_raw) if isinstance(outcomes_raw, str) else outcomes_raw
            tokens = json.loads(tokens_raw) if isinstance(tokens_raw, str) else tokens_raw

            if len(outcomes) < 2 or len(tokens) < 2:
                _fallback_switch()
                return False

            # 找 UP 和 DOWN 在 outcomes 中的索引
            up_idx, dn_idx = -1, -1
            for i, o in enumerate(outcomes):
                ol = o.lower()
                if "up" in ol:
                    up_idx = i
                elif "down" in ol:
                    dn_idx = i

            if up_idx < 0 or dn_idx < 0:
                return False

            # ── 旧窗口结算 ──
            if state.window_slug and state.window_slug != slug:
                if state.warmup:
                    # 预热期结束: 跳过首个不完整窗口的结算
                    print(f"[预热] 预热结束 (丢弃不完整窗口: trades={state.trade_count_window}), 正式开始统计")
                    state.warmup = False
                elif state.trade_count_window > 0:
                    saved_ptb = _resolve_ptb(state.window_end_ts)
                    state._pending_settle = {
                        "slug": state.window_slug,
                        "question": state.window_question,
                        "end_ts": state.window_end_ts,
                        "ptb": saved_ptb,
                        "cum_up_shares": state.cum_up_shares,
                        "cum_dn_shares": state.cum_dn_shares,
                        "cum_up_cost": state.cum_up_cost,
                        "cum_dn_cost": state.cum_dn_cost,
                        "trade_count": state.trade_count_window,
                        "end_up_price": state.up_price,
                        "end_dn_price": state.dn_price,
                    }
                    _settle_pending()

            state.window_slug = slug
            state.window_question = mkt.get("question", "")
            state.window_start_ts = ws
            state.window_end_ts = we
            state.condition_id = mkt.get("conditionId", "")

            # token IDs
            state.up_token_id = tokens[up_idx]
            state.dn_token_id = tokens[dn_idx]

            # PTB: 优先由 Chainlink RTDS observationsTimestamp % 300 自动捕获
            # 此处仅作备用: 若 RTDS 尚未捕获过 PTB 则从缓冲区回溯
            if state.rtds_connected and state.window_ptb > 0:
                # RTDS 已在运行, PTB 已经或即将被 chainlink_rtds_stream 捕获
                ptb = state.window_ptb
            else:
                # 回退: 从价格缓冲区回溯
                ptb = state.lookup_price_at(float(ws))
                if ptb <= 0:
                    ptb = state.btc_price
                state.ptb_source = "Buffer回溯"
            state.window_ptb = ptb

            state.reset_window()
            print(f"[PM] 新窗口: {state.window_question} (PTB=${state.window_ptb:,.2f})")
            return True
    except Exception as e:
        print(f"[PM] 窗口发现失败: {e}")
        if slug != state.window_slug:
            print(f"[PM] 回退: 使用时间戳切换窗口 {slug}")
            _fallback_switch()
        return False


async def poll_pm_prices(session: aiohttp.ClientSession):
    """轮询 PM UP/DOWN 报价"""
    for token_id, side in [
        (state.up_token_id, "UP"),
        (state.dn_token_id, "DN"),
    ]:
        if not token_id:
            continue
        try:
            # Book (bid/ask)
            url = f"{CLOB_REST}/book"
            async with session.get(url, params={"token_id": token_id},
                                   timeout=aiohttp.ClientTimeout(total=8)) as r:
                book = await r.json()
                bids = book.get("bids", [])
                asks = book.get("asks", [])
                # CLOB 返回未排序, 需找最优: bid=max, ask=min
                best_bid = max((float(b["price"]) for b in bids), default=0)
                best_ask = min((float(a["price"]) for a in asks), default=0)
                mid = (best_bid + best_ask) / 2 if best_bid and best_ask else 0

                if side == "UP":
                    state.up_price = mid
                    state.up_bid = best_bid
                    state.up_ask = best_ask
                else:
                    state.dn_price = mid
                    state.dn_bid = best_bid
                    state.dn_ask = best_ask
        except Exception as e:
            print(f"[PM] {side} book 失败: {e}")

    if state.up_price > 0 and state.dn_price > 0:
        state.combined_price = state.up_price + state.dn_price
        state.edge = 1.0 - state.combined_price
        now = time.time()
        state.pm_price_history.append({
            "ts": now,
            "up": state.up_price,
            "dn": state.dn_price,
            "combined": state.combined_price,
            "edge": state.edge,
            # bid/ask 快照 (用于历史回溯)
            "up_bid": state.up_bid,
            "up_ask": state.up_ask,
            "dn_bid": state.dn_bid,
            "dn_ask": state.dn_ask,
        })
    state.pm_connected = True


SETTLE_TIMEOUT = 120        # 结算超时秒数 (正常应立即结算, 此处仅兜底)


async def pm_price_loop():
    """PM 报价 + 窗口轮换主循环"""
    # 等待至少一个价格源就绪, 避免首次窗口 PTB=$0
    for _ in range(50):  # 最多等 5s
        if state.btc_price > 0:
            break
        await asyncio.sleep(0.1)
    if state.btc_price <= 0:
        print("[PM] ⚠ 价格源未就绪, 继续启动 (PTB 可能不准)")

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                # ── 结算超时兜底 (正常在窗口切换时已立即结算) ──
                if state._pending_settle:
                    ps = state._pending_settle
                    now = time.time()
                    if now >= ps["end_ts"] + SETTLE_TIMEOUT:
                        settle_price, src = _get_settle_price(ps["end_ts"])
                        if settle_price <= 0:
                            settle_price = state.btc_price
                            src = "当前价(超时)"
                        else:
                            src = f"{src}(超时)"
                        elapsed_s = int(now - ps["end_ts"])
                        print(f"[PM] 结算超时兜底({elapsed_s}s), 来源={src}")
                        _do_settle(settle_price, ps["ptb"], src)

                # 窗口发现/轮换
                await discover_pm_window(session)

                # 报价轮询
                if state.up_token_id and state.dn_token_id:
                    await poll_pm_prices(session)
            except Exception as e:
                print(f"[PM] 循环异常: {e}")
                state.pm_connected = False

            await asyncio.sleep(POLL_PM_PRICE_INTERVAL)


# ─────────────────────────────────────────────────────────
#  数据采集: CLOB WebSocket 成交流 (链下撮合时间戳)
# ─────────────────────────────────────────────────────────

async def clob_trade_stream():
    """订阅 CLOB WS 实时成交, 记录链下撮合精确时间戳"""
    fail_count = 0
    while True:
        try:
            # 等待窗口 token 就绪
            while not state.up_token_id or not state.dn_token_id:
                await asyncio.sleep(2)

            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(
                    CLOB_WS,
                    heartbeat=10.0,
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as ws:
                    # 订阅当前窗口的 UP/DOWN token
                    subscribed_tokens = {state.up_token_id, state.dn_token_id}
                    for token_id in subscribed_tokens:
                        await ws.send_json({
                            "type": "subscribe",
                            "market": token_id,
                            "channel": "live-activity",
                        })
                    state.clob_ws_connected = True
                    fail_count = 0
                    print(f"[CLOB WS] 已连接, 订阅 {len(subscribed_tokens)} 个 token")

                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            if not msg.data or not msg.data.strip():
                                continue
                            try:
                                data = json.loads(msg.data)
                            except json.JSONDecodeError:
                                continue

                            # 解析成交消息
                            evt = data.get("event_type", data.get("type", ""))
                            if evt in ("trade", "last_trade_price"):
                                now = time.time()
                                t_price = float(data.get("price", 0))
                                t_size = float(data.get("size", 0))
                                t_side = data.get("side", "").upper()
                                asset_id = data.get("market", data.get("asset_id", ""))
                                # 判断 UP/DOWN
                                if asset_id == state.up_token_id:
                                    pm_side = "UP"
                                elif asset_id == state.dn_token_id:
                                    pm_side = "DOWN"
                                else:
                                    pm_side = t_side
                                state.clob_trades.append({
                                    "ts": now,
                                    "price": t_price,
                                    "size": t_size,
                                    "side": pm_side,
                                    "asset_id": asset_id,
                                })

                            # 检查 token 是否轮换 → 重新订阅
                            new_tokens = {state.up_token_id, state.dn_token_id}
                            if new_tokens != subscribed_tokens and all(new_tokens):
                                for token_id in new_tokens - subscribed_tokens:
                                    await ws.send_json({
                                        "type": "subscribe",
                                        "market": token_id,
                                        "channel": "live-activity",
                                    })
                                subscribed_tokens = new_tokens
                                print(f"[CLOB WS] 窗口轮换, 重新订阅")

                        elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                            break

        except asyncio.CancelledError:
            break
        except Exception as e:
            fail_count += 1
            state.clob_ws_connected = False
            wait = min(5 * fail_count, 30)
            print(f"[CLOB WS] 异常 ({fail_count}): {e}, {wait}s 后重连")
            await asyncio.sleep(wait)


def _match_clob_timestamp(
    price: float, size: float, side: str, chain_ts: int
) -> float | None:
    """
    在 clob_trades 缓冲区中查找与 Activity API 交易匹配的链下撮合记录。
    匹配条件: price 完全一致 + size 误差 <1% + side 一致 + 时间在上链前。
    返回链下时间戳 (float/Unix), 未找到返回 None。
    """
    best_ts = None
    best_diff = float("inf")
    for ct in reversed(state.clob_trades):
        # side 必须一致
        if ct["side"] != side:
            continue
        # price 一致 (浮点精度 4 位)
        if abs(ct["price"] - price) > 0.0002:
            continue
        # size 差异 < 1%
        if size > 0 and abs(ct["size"] - size) / size > 0.01:
            continue
        # 时间约束: 链下时间应在上链时间之前 (允许 30s 窗口)
        if ct["ts"] > chain_ts + 2:
            continue  # 链下不应晚于上链太多
        diff = abs(chain_ts - ct["ts"])
        if diff < best_diff and diff < 30:
            best_diff = diff
            best_ts = ct["ts"]
    return best_ts


# ─────────────────────────────────────────────────────────
#  数据采集: 0x1d Activity 轮询
# ─────────────────────────────────────────────────────────

async def poll_0x1d_activity():
    """轮询 0x1d 账号最新交易"""
    seen_txs: set[str] = set()
    last_seen_ts: int = 0
    _dropped_non_btc = 0  # 非 BTC 交易计数 (静默丢弃)

    # 等待初始窗口发现 (避免启动时丢交易)
    for _ in range(30):  # 最多等 15 秒
        if state.window_slug:
            break
        await asyncio.sleep(0.5)
    if not state.window_slug:
        # 仍未发现窗口 → 自行从时间戳计算
        ws = int(time.time()); ws = ws - ws % 300
        state.window_slug = f"{SLUG_PREFIX}{ws}"
        state.window_start_ts = ws
        state.window_end_ts = ws + 300
        ptb = state.btc_price or 0
        state.window_ptb = ptb
        print(f"[Activity] ⚠ 窗口超时, 自行初始化: {state.window_slug}")

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                url = f"{ACTIVITY_API}?user={ADDR_0X1D}&limit=200"
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                    if r.status == 200:
                        trades = await r.json()
                        state.activity_connected = True

                        if not isinstance(trades, list) or not trades:
                            await asyncio.sleep(POLL_ACTIVITY_INTERVAL)
                            continue

                        # 倒序处理 (最旧在前)
                        new_trades = []
                        for t in reversed(trades):
                          try:  # ── 单笔交易异常隔离 ──
                            # 用 transactionHash + outcomeIndex 做去重
                            tx = t.get("transactionHash", "") + "_" + str(t.get("outcomeIndex", 0))
                            if tx in seen_txs:
                                continue
                            seen_txs.add(tx)
                            # 太多就清理 (保留后半部分以免老tx重入)
                            if len(seen_txs) > 5000:
                                # 只保留最近加入的 (set无序, 但清空后老tx会重入seen但DB去重)
                                seen_txs.clear()

                            # 获取 slug
                            slug = t.get("eventSlug", "") or t.get("slug", "")
                            is_btc_5m = SLUG_PREFIX in slug

                            # ── 非 BTC 5-min 交易: 静默跳过 ──
                            if not is_btc_5m:
                                _dropped_non_btc += 1
                                continue

                            # ── BTC 5-min 交易, 但 slug 不匹配当前窗口 ──
                            if state.window_slug and slug != state.window_slug:
                                # Case 1: 迟到的旧窗口交易 → 追补到 _pending_settle
                                if (state._pending_settle
                                        and state._pending_settle.get("slug") == slug):
                                    outcome_late = t.get("outcome", "")
                                    side_late = "UP" if outcome_late.lower() == "up" else "DOWN"
                                    shares_late = float(t.get("size", 0))
                                    usdc_late = float(t.get("usdcSize", 0))
                                    if side_late == "UP":
                                        state._pending_settle["cum_up_shares"] += shares_late
                                        state._pending_settle["cum_up_cost"] += usdc_late
                                    else:
                                        state._pending_settle["cum_dn_shares"] += shares_late
                                        state._pending_settle["cum_dn_cost"] += usdc_late
                                    state._pending_settle["trade_count"] += 1
                                    print(f"[Activity] 迟到交易追补到待结算窗口: {side_late} {shares_late:.1f}sh ${usdc_late:.2f}")
                                    continue

                                # Case 2: 新窗口的交易 (discover_pm_window 还没更新)
                                # ── 热切换: 从交易 slug 中提取窗口时间, 立即切换 ──
                                try:
                                    ts_from_slug = int(slug.replace(SLUG_PREFIX, ""))
                                    if ts_from_slug > (state.window_start_ts or 0):
                                        print(f"[Activity] ⚡ 检测到新窗口 (交易驱动切换): {slug}")
                                        # 保存旧窗口到 pending settle
                                        if state.warmup:
                                            print(f"[预热] 预热结束 (丢弃不完整窗口: trades={state.trade_count_window}), 正式开始统计")
                                            state.warmup = False
                                        elif state.window_slug and state.trade_count_window > 0:
                                            saved_ptb = _resolve_ptb(state.window_end_ts)
                                            state._pending_settle = {
                                                "slug": state.window_slug,
                                                "question": state.window_question,
                                                "end_ts": state.window_end_ts,
                                                "ptb": saved_ptb,
                                                "cum_up_shares": state.cum_up_shares,
                                                "cum_dn_shares": state.cum_dn_shares,
                                                "cum_up_cost": state.cum_up_cost,
                                                "cum_dn_cost": state.cum_dn_cost,
                                                "trade_count": state.trade_count_window,
                                                "end_up_price": state.up_price,
                                                "end_dn_price": state.dn_price,
                                            }
                                            _settle_pending()
                                        # 切换到新窗口
                                        state.window_slug = slug
                                        state.window_start_ts = ts_from_slug
                                        state.window_end_ts = ts_from_slug + 300
                                        # PTB 估计: 从价格缓冲区回溯到窗口开始时间
                                        ptb = state.lookup_price_at(float(ts_from_slug))
                                        if ptb <= 0:
                                            ptb = state.btc_price
                                        state.window_ptb = ptb
                                        state.ptb_source = "交易驱动切换"
                                        state.reset_window()
                                        print(f"[Activity] 新窗口就绪 (PTB=${ptb:,.2f}), 继续处理交易...")
                                    else:
                                        # 更老的窗口, 无法追补 → 静默跳过
                                        continue
                                except (ValueError, TypeError):
                                    print(f"[Activity] ⚠ 无法解析BTC slug: {slug}")
                                    continue

                            # 解析
                            outcome = t.get("outcome", "")
                            side = "UP" if outcome.lower() == "up" else "DOWN"
                            shares = float(t.get("size", 0))
                            price = float(t.get("price", 0))
                            usdc = float(t.get("usdcSize", 0))
                            ts_int = int(t.get("timestamp", 0))

                            # 跳过无效交易 (如 CTF redemption, price=0)
                            if price <= 0 or shares <= 0:
                                continue

                            # 生成可读时间
                            from datetime import datetime as _dt, timezone as _tz
                            time_str = _dt.fromtimestamp(ts_int, tz=_tz.utc).strftime("%H:%M:%S") if ts_int > 0 else ""

                            # ── CLOB WS 时间戳修正: 用链下撮合时间代替上链时间 ──
                            ts_clob = _match_clob_timestamp(
                                price, shares, side, ts_int
                            )

                            trade_rec = {
                                "ts": ts_clob if ts_clob else ts_int,
                                "ts_chain": ts_int,
                                "ts_clob": ts_clob,
                                "time": time_str,
                                "side": side,
                                "shares": round(shares, 2),
                                "price": round(price, 4),
                                "usdc": round(usdc, 2),
                                "slug": slug,
                            }
                            # (CLOB 时间戳修正统计由定时摘要打印)

                            state.trades_0x1d.append(trade_rec)
                            state.trade_count_window += 1
                            new_trades.append(trade_rec)

                            if side == "UP":
                                state.cum_up_shares += shares
                                state.cum_up_cost += usdc
                            else:
                                state.cum_dn_shares += shares
                                state.cum_dn_cost += usdc

                            # ── 时间修正: 用 CLOB 撮合时间回溯特征 ──
                            # ts_clob = 链下撮合时间 (最接近 0x1d 决策时刻)
                            # ts_int  = 上链时间 (比决策滞后 1-30s)
                            # 不修正时 = 轮询时间 (比决策滞后 3-30s, 会导致模型学错信号)
                            ref_ts = ts_clob if ts_clob else float(ts_int)
                            _feature_latency = round(time.time() - ref_ts, 1)

                            # 记录下单时刻的全特征快照 (用于策略逆向)
                            trade_elapsed = ts_int - state.window_start_ts if state.window_start_ts > 0 else 0
                            elapsed_pct = trade_elapsed / 300 * 100 if trade_elapsed >= 0 else 0
                            # 仓位
                            net_shares = state.cum_up_shares - state.cum_dn_shares  # 原始差值
                            total_shares = max(state.cum_up_shares, state.cum_dn_shares, 1)
                            pos_imbalance = net_shares / total_shares
                            # 下单速度 (最近 30s 内的交易数 / 0.5min)
                            recent_cnt = sum(1 for t in state.trades_0x1d if t.get("ts", 0) > ts_int - 30)
                            trade_velocity = recent_cnt * 2  # per minute

                            # ── 回溯 BTC 价格到 ref_ts (决策时刻) ──
                            btc_p = state._price_at(state.btc_price_buffer, ref_ts) or state.btc_price
                            bn_p  = state._price_at(state.bn_price_buffer, ref_ts) or state.bn_price or btc_p

                            # ── 回溯 PM 报价到 ref_ts (决策时刻) ──
                            pm_snap = state._pm_state_at(ref_ts)
                            pm_up = pm_snap.get("up", state.up_price)
                            pm_dn = pm_snap.get("dn", state.dn_price)
                            pm_edge = pm_snap.get("edge", state.edge)
                            pm_up_bid = pm_snap.get("up_bid", state.up_bid)
                            pm_up_ask = pm_snap.get("up_ask", state.up_ask)
                            pm_dn_bid = pm_snap.get("dn_bid", state.dn_bid)
                            pm_dn_ask = pm_snap.get("dn_ask", state.dn_ask)

                            # RTDS (Chainlink) 动量 (回溯到 ref_ts)
                            cl_mom_5s  = state.calc_momentum(5.0, ref_ts)
                            cl_mom_10s = state.calc_momentum(10.0, ref_ts)
                            # Binance 动量 (回溯到 ref_ts)
                            bn_mom_5s  = state.calc_bn_momentum(5.0, ref_ts)
                            bn_mom_10s = state.calc_bn_momentum(10.0, ref_ts)

                            # ── 扩展特征计算 (回溯到 ref_ts) ──
                            cl_mom_30s = state.calc_momentum(30.0, ref_ts)
                            cl_mom_60s = state.calc_momentum(60.0, ref_ts)
                            cl_mom_120s = state.calc_momentum(120.0, ref_ts)
                            bn_mom_60s = state.calc_bn_momentum(60.0, ref_ts)
                            bn_mom_120s = state.calc_bn_momentum(120.0, ref_ts)
                            # 动量加速度 (短期 - 长期)
                            mom_accel_5s = cl_mom_5s - cl_mom_10s
                            mom_accel_10s = cl_mom_10s - cl_mom_30s
                            bn_mom_accel_5s = bn_mom_5s - bn_mom_10s
                            bn_mom_30s = state.calc_bn_momentum(30.0, ref_ts)
                            bn_mom_accel_10s = bn_mom_10s - bn_mom_30s
                            mom_accel_30s = cl_mom_30s - cl_mom_60s

                            # ── 长周期动量 (回溯到 ref_ts) ──
                            cl_mom_180s = state.calc_momentum(180.0, ref_ts)
                            cl_mom_300s = state.calc_momentum(300.0, ref_ts)
                            bn_mom_180s = state.calc_bn_momentum(180.0, ref_ts)

                            # ── 趋势强度 (回溯到 ref_ts) ──
                            cl_trend_30s  = state.calc_cl_trend(30.0, ref_ts)
                            cl_trend_60s  = state.calc_cl_trend(60.0, ref_ts)
                            cl_trend_120s = state.calc_cl_trend(120.0, ref_ts)
                            bn_trend_30s  = state.calc_bn_trend(30.0, ref_ts)
                            bn_trend_60s  = state.calc_bn_trend(60.0, ref_ts)

                            # ── 价格百分位 (回溯到 ref_ts) ──
                            cl_pctl_60s  = state.calc_cl_percentile(60.0, ref_ts)
                            cl_pctl_300s = state.calc_cl_percentile(300.0, ref_ts)
                            bn_pctl_60s  = state.calc_bn_percentile(60.0, ref_ts)

                            # ── 动量 Z-score (回溯到 ref_ts) ──
                            cl_mom_z_5s  = state.calc_cl_mom_zscore(5.0, 60.0, ref_ts)
                            cl_mom_z_10s = state.calc_cl_mom_zscore(10.0, 60.0, ref_ts)
                            bn_mom_z_5s  = state.calc_bn_mom_zscore(5.0, 60.0, ref_ts)

                            # ── 震荡指标 (回溯到 ref_ts) ──
                            cl_dir_changes_30s = state._count_direction_changes(
                                state.btc_price_buffer, 30.0, ref_ts)
                            cl_dir_changes_60s = state._count_direction_changes(
                                state.btc_price_buffer, 60.0, ref_ts)

                            # ── 扩展波动率 (回溯到 ref_ts) ──
                            btc_vol_120s = state.calc_btc_volatility(120.0, ref_ts)
                            bn_vol_120s  = state.calc_bn_volatility(120.0, ref_ts)

                            # ── 新增特征: 仓位均价 ──
                            avg_up_price = (state.cum_up_cost / state.cum_up_shares
                                           if state.cum_up_shares > 0 else 0.0)
                            avg_dn_price = (state.cum_dn_cost / state.cum_dn_shares
                                           if state.cum_dn_shares > 0 else 0.0)

                            # ── PM 定价效率 (使用回溯 BTC/PM 价格) ──
                            fair_up = 0.50
                            if state.window_ptb > 0 and btc_p > 0:
                                bps_from_ptb = (btc_p - state.window_ptb) / state.window_ptb * 10000
                                fair_up = 0.50 + bps_from_ptb * 0.001
                                fair_up = max(0.05, min(0.95, fair_up))
                            up_price_vs_fair = round(pm_up - fair_up, 4) if pm_up > 0 else 0.0
                            dn_price_vs_fair = round(pm_dn - (1.0 - fair_up), 4) if pm_dn > 0 else 0.0

                            # ── 新增特征: 跨源一致性 ──
                            cl_bn_trend_agree = 1.0 if (cl_trend_30s > 0.1 and bn_trend_30s > 0.1) or (cl_trend_30s < -0.1 and bn_trend_30s < -0.1) else (-1.0 if (cl_trend_30s > 0.1 and bn_trend_30s < -0.1) or (cl_trend_30s < -0.1 and bn_trend_30s > 0.1) else 0.0)

                            # 交易列表 (必须在 burst 计算前赋值)
                            _trades_list = list(state.trades_0x1d)
                            # ── Burst 内序号 ──
                            # 1s 内连续交易视为同一 burst
                            burst_seq = 0
                            is_burst = 0.0
                            if len(_trades_list) >= 2:
                                for _bi in range(len(_trades_list) - 2, -1, -1):
                                    _bt = _trades_list[_bi].get("ts_chain", _trades_list[_bi].get("ts", 0))
                                    if abs(ts_int - int(_bt)) <= 1:
                                        burst_seq += 1
                                    else:
                                        break
                                is_burst = 1.0 if burst_seq > 0 else 0.0
                            # BTC vs PTB 方向信号 (使用回溯价格)
                            btc_above_ptb = 1.0 if (btc_p > state.window_ptb and state.window_ptb > 0) else (-1.0 if state.window_ptb > 0 else 0.0)

                            # ── RTDS-PTB 比对特征 (回溯到 ref_ts) ──
                            rtds_implied_prob = state.calc_rtds_implied_prob(ref_ts)
                            pm_rtds_prob_gap = state.calc_pm_rtds_prob_gap(ref_ts)
                            ptb_dist_vs_vol = state.calc_ptb_dist_vs_vol(ref_ts)
                            ptb_cross_count = state.calc_ptb_cross_count(ref_ts)
                            ptb_time_above = state.calc_ptb_time_above_ratio(ref_ts)
                            ptb_dist_vel_5s = state.calc_ptb_dist_velocity(5.0, ref_ts)
                            ptb_max_above, ptb_max_below = state.calc_ptb_extreme_dist(ref_ts)

                            if len(_trades_list) >= 2:
                                _prev_ts = _trades_list[-2].get("ts_chain", _trades_list[-2].get("ts", 0))
                                time_since_last = ts_int - int(_prev_ts) if _prev_ts and int(_prev_ts) > 0 else 0
                            else:
                                time_since_last = 0
                            # 连续同向笔数
                            _streak = 0
                            for _tr in reversed(_trades_list):
                                if _tr["side"] == side:
                                    _streak += 1
                                else:
                                    break

                            # ── 跨窗口状态特征 ──
                            _cw = state._cross_window_features()

                            # ── 窗口内交易强度特征 ──
                            _elapsed_min = max(trade_elapsed / 60.0, 0.1)
                            _trade_density = round(state.trade_count_window / _elapsed_min, 1)
                            _cum_volume = round(state.cum_up_cost + state.cum_dn_cost, 2)
                            _side_switch_rate = round(state.calc_side_switch_rate(), 3)
                            _dominant_side_ratio = round(state.calc_dominant_side_ratio(), 3)

                            # ── BTC 窗口走势 ──
                            _btc_range_pct = round(state.calc_btc_range_pct(ref_ts), 2)

                            # ── PM 报价速度 ──
                            _pm_mid_vel_10s = round(state.calc_pm_mid_velocity(10.0, ref_ts), 4)
                            _pm_mid_vel_30s = round(state.calc_pm_mid_velocity(30.0, ref_ts), 4)

                            state.trade_momentum_snaps.append({
                                "elapsed": max(0, trade_elapsed),
                                "elapsed_pct": round(elapsed_pct, 1),
                                "side": side,
                                "shares": round(shares, 2),
                                # ── 交易类型分类 (用于分层蒸馏) ──
                                # signal    = 信号驱动 (仓位平衡或顺仓方向下单)
                                # rebalance = 仓位调整 (逆仓方向下单, 明显是在平衡仓位)
                                "trade_type": (
                                    "rebalance" if (
                                        abs(pos_imbalance) > 0.3 and  # 仓位偏差超30%
                                        ((pos_imbalance > 0 and side == "DN") or  # 偏UP → 买DN
                                         (pos_imbalance < 0 and side == "UP"))    # 偏DN → 买UP
                                    ) else "signal"
                                ),
                                # ── 时间修正元数据 ──
                                "ref_ts": round(ref_ts, 3),
                                "feature_latency": _feature_latency,
                                "ts_source": "clob" if ts_clob else "chain",
                                # RTDS (Chainlink) 动量 (回溯到 ref_ts)
                                "mom_1s":  round(state.calc_momentum(1.0, ref_ts), 2),
                                "mom_3s":  round(state.calc_momentum(3.0, ref_ts), 2),
                                "mom_5s":  round(cl_mom_5s, 2),
                                "mom_10s": round(cl_mom_10s, 2),
                                "mom_15s": round(state.calc_momentum(15.0, ref_ts), 2),
                                "mom_30s": round(state.calc_momentum(30.0, ref_ts), 2),
                                # Binance 动量 (回溯到 ref_ts)
                                "bn_mom_1s":  round(state.calc_bn_momentum(1.0, ref_ts), 2),
                                "bn_mom_3s":  round(state.calc_bn_momentum(3.0, ref_ts), 2),
                                "bn_mom_5s":  round(bn_mom_5s, 2),
                                "bn_mom_10s": round(bn_mom_10s, 2),
                                "bn_mom_15s": round(state.calc_bn_momentum(15.0, ref_ts), 2),
                                "bn_mom_30s": round(state.calc_bn_momentum(30.0, ref_ts), 2),
                                # RTDS BTC 状态 (回溯到 ref_ts)
                                "btc_price": round(btc_p, 2),
                                "btc_delta_ptb": round(btc_p - state.window_ptb, 2) if state.window_ptb > 0 else 0,
                                "btc_delta_pct": round((btc_p - state.window_ptb) / state.window_ptb * 10000, 2) if state.window_ptb > 0 else 0,  # bps
                                "btc_vol_10s":  round(state.calc_btc_volatility(10.0, ref_ts), 2),
                                "btc_vol_30s":  round(state.calc_btc_volatility(30.0, ref_ts), 2),
                                # Binance BTC 状态 (回溯到 ref_ts)
                                "bn_price": round(bn_p, 2),
                                "bn_delta_ptb": round(bn_p - state.window_ptb, 2) if state.window_ptb > 0 else 0,
                                "bn_delta_pct": round((bn_p - state.window_ptb) / state.window_ptb * 10000, 2) if state.window_ptb > 0 else 0,
                                "bn_vol_10s":  round(state.calc_bn_volatility(10.0, ref_ts), 2),
                                "bn_vol_30s":  round(state.calc_bn_volatility(30.0, ref_ts), 2),
                                # RTDS vs Binance 差异
                                "cl_bn_spread": round(btc_p - bn_p, 2),
                                "cl_bn_mom_diff_5s":  round(cl_mom_5s - bn_mom_5s, 2),
                                "cl_bn_mom_diff_10s": round(cl_mom_10s - bn_mom_10s, 2),
                                # PM 报价 (回溯到 ref_ts)
                                "up_price": round(pm_up, 4),
                                "dn_price": round(pm_dn, 4),
                                "pm_edge":  round(pm_edge, 4),
                                "up_bid":   round(pm_up_bid, 4),
                                "up_ask":   round(pm_up_ask, 4),
                                # 仓位状态
                                "pos_imbalance": round(pos_imbalance, 4),
                                "cum_trades": state.trade_count_window,
                                "trade_velocity": round(trade_velocity, 1),
                                # ── 扩展特征 (回溯到 ref_ts) ──
                                "mom_60s":  round(cl_mom_60s, 2),
                                "mom_120s": round(cl_mom_120s, 2),
                                "bn_mom_60s":  round(bn_mom_60s, 2),
                                "bn_mom_120s": round(bn_mom_120s, 2),
                                "btc_vol_60s": round(state.calc_btc_volatility(60.0, ref_ts), 2),
                                "bn_vol_60s":  round(state.calc_bn_volatility(60.0, ref_ts), 2),
                                "mom_accel_5s":    round(mom_accel_5s, 2),
                                "mom_accel_10s":   round(mom_accel_10s, 2),
                                "bn_mom_accel_5s": round(bn_mom_accel_5s, 2),
                                "up_ba_spread": round(pm_up_ask - pm_up_bid, 4) if pm_up_ask > 0 and pm_up_bid > 0 else 0,
                                "dn_ba_spread": round(pm_dn_ask - pm_dn_bid, 4) if pm_dn_ask > 0 and pm_dn_bid > 0 else 0,
                                "dn_bid": round(pm_dn_bid, 4),
                                "dn_ask": round(pm_dn_ask, 4),
                                "btc_above_ptb": btc_above_ptb,
                                "time_since_last": time_since_last,
                                "same_side_streak": _streak if side == "UP" else -_streak,
                                # ── 趋势/百分位/Z-score (回溯到 ref_ts) ──
                                "cl_trend_30s":  round(cl_trend_30s, 4),
                                "cl_trend_60s":  round(cl_trend_60s, 4),
                                "cl_trend_120s": round(cl_trend_120s, 4),
                                "bn_trend_30s":  round(bn_trend_30s, 4),
                                "bn_trend_60s":  round(bn_trend_60s, 4),
                                "cl_pctl_60s":  round(cl_pctl_60s, 1),
                                "cl_pctl_300s": round(cl_pctl_300s, 1),
                                "bn_pctl_60s":  round(bn_pctl_60s, 1),
                                "cl_mom_z_5s":  round(cl_mom_z_5s, 2),
                                "cl_mom_z_10s": round(cl_mom_z_10s, 2),
                                "bn_mom_z_5s":  round(bn_mom_z_5s, 2),
                                # 震荡指标
                                "cl_dir_changes_30s": cl_dir_changes_30s,
                                "cl_dir_changes_60s": cl_dir_changes_60s,
                                # 长周期动量
                                "mom_180s": round(cl_mom_180s, 2),
                                "mom_300s": round(cl_mom_300s, 2),
                                "bn_mom_180s": round(bn_mom_180s, 2),
                                # 扩展波动率
                                "btc_vol_120s": round(btc_vol_120s, 2),
                                "bn_vol_120s":  round(bn_vol_120s, 2),
                                # 扩展加速度
                                "mom_accel_30s": round(mom_accel_30s, 2),
                                "bn_mom_accel_10s": round(bn_mom_accel_10s, 2),
                                # 仓位细节
                                "net_shares": round(net_shares, 2),  # UP-DN 原始差值
                                "cum_up_shares": round(state.cum_up_shares, 2),
                                "cum_dn_shares": round(state.cum_dn_shares, 2),
                                "cum_up_cost":   round(state.cum_up_cost, 2),
                                "cum_dn_cost":   round(state.cum_dn_cost, 2),
                                "avg_up_price":  round(avg_up_price, 4),
                                "avg_dn_price":  round(avg_dn_price, 4),
                                # PM 定价效率
                                "up_price_vs_fair": up_price_vs_fair,
                                "dn_price_vs_fair": dn_price_vs_fair,
                                # 跨源一致性
                                "cl_bn_trend_agree": cl_bn_trend_agree,
                                # Burst 特征
                                "burst_seq": burst_seq,
                                "is_burst": is_burst,
                                # ── RTDS-PTB 比对特征 ──
                                "rtds_implied_prob": round(rtds_implied_prob, 4),
                                "pm_rtds_prob_gap": round(pm_rtds_prob_gap, 4),
                                "ptb_dist_vs_vol": round(ptb_dist_vs_vol, 3),
                                "ptb_cross_count": ptb_cross_count,
                                "ptb_time_above": round(ptb_time_above, 3),
                                "ptb_dist_vel_5s": round(ptb_dist_vel_5s, 2),
                                "ptb_max_above": round(ptb_max_above, 2),
                                "ptb_max_below": round(ptb_max_below, 2),
                                # ── 时间特征 (UTC) ──
                                "hour_utc": datetime.fromtimestamp(ref_ts, tz=timezone.utc).hour,
                                "minute_utc": datetime.fromtimestamp(ref_ts, tz=timezone.utc).minute,
                                "day_of_week": datetime.fromtimestamp(ref_ts, tz=timezone.utc).weekday(),  # 0=Mon
                                # 美股盘会话标记: pre=1, regular=2, after=3, closed=0
                                "us_session": (
                                    2 if 13*60+30 <= datetime.fromtimestamp(ref_ts, tz=timezone.utc).hour*60 + datetime.fromtimestamp(ref_ts, tz=timezone.utc).minute < 20*60
                                    else (1 if 8*60 <= datetime.fromtimestamp(ref_ts, tz=timezone.utc).hour*60 + datetime.fromtimestamp(ref_ts, tz=timezone.utc).minute < 13*60+30
                                    else (3 if 20*60 <= datetime.fromtimestamp(ref_ts, tz=timezone.utc).hour*60 + datetime.fromtimestamp(ref_ts, tz=timezone.utc).minute < 24*60
                                    else 0))
                                ),
                                # 亚盘/欧盘/美盘连续标记
                                "asia_session": 1 if 0 <= datetime.fromtimestamp(ref_ts, tz=timezone.utc).hour < 8 else 0,
                                "euro_session": 1 if 7 <= datetime.fromtimestamp(ref_ts, tz=timezone.utc).hour < 16 else 0,
                                # ── 跨窗口状态特征 ──
                                "prev_won": _cw["prev_won"],
                                "prev_pnl": _cw["prev_pnl"],
                                "session_pnl": _cw["session_pnl"],
                                "session_wr": _cw["session_wr"],
                                "win_streak": _cw["win_streak"],
                                # ── 窗口内交易强度 ──
                                "trade_density": _trade_density,
                                "cum_volume": _cum_volume,
                                "side_switch_rate": _side_switch_rate,
                                "dominant_side_ratio": _dominant_side_ratio,
                                # ── BTC 窗口走势 ──
                                "btc_range_pct": _btc_range_pct,
                                # ── PM 报价速度 ──
                                "pm_mid_vel_10s": _pm_mid_vel_10s,
                                "pm_mid_vel_30s": _pm_mid_vel_30s,
                            })
                            # 持久化到数据库
                            _db_save_trade_snap(
                                tx, ts_int, slug, side, shares,
                                price, usdc, state.trade_momentum_snaps[-1]
                            )
                            # (特征回溯延迟统计由定时摘要打印)

                          except Exception as _trade_err:
                            # 单笔交易处理失败 → 跳过本笔, 不影响后续交易
                            print(f"[Activity] ⚠ 单笔交易处理异常: {_trade_err}")

                        # Burst 检测: 同一秒内 >= 3 笔
                        if len(new_trades) >= 3:
                            state.burst_events.append({
                                "ts": time.time(),
                                "time": datetime.now(timezone.utc).strftime("%H:%M:%S"),
                                "count": len(new_trades),
                                "up": sum(1 for t in new_trades if t["side"] == "UP"),
                                "dn": sum(1 for t in new_trades if t["side"] == "DOWN"),
                                "total_usdc": round(sum(t["usdc"] for t in new_trades), 2),
                            })

                    else:
                        state.activity_connected = False
            except Exception as e:
                print(f"[Activity] 异常: {e}")
                state.activity_connected = False

            await asyncio.sleep(POLL_ACTIVITY_INTERVAL)


# ─────────────────────────────────────────────────────────
#  定时状态摘要
# ─────────────────────────────────────────────────────────

SUMMARY_INTERVAL = 60  # 每 60 秒打印一次摘要

async def periodic_summary():
    """定期打印数据收集摘要 + 连接状态"""
    await asyncio.sleep(10)  # 启动后等 10s, 让各连接建立
    while True:
        now = time.time()
        # ── 连接状态 ──
        conn_flags = []
        conn_flags.append(f"RTDS={'✓' if state.rtds_connected else '✗'}")
        conn_flags.append(f"Binance={'✓' if state.binance_connected else '✗'}")
        conn_flags.append(f"PM={'✓' if state.pm_connected else '✗'}")
        conn_flags.append(f"CLOB_WS={'✓' if state.clob_ws_connected else '✗'}")
        conn_flags.append(f"Activity={'✓' if state.activity_connected else '✗'}")
        conn_str = " | ".join(conn_flags)

        # ── 数据量 ──
        btc_n = len(state.btc_history)
        bn_n = len(state.bn_history)
        pm_n = len(state.pm_price_history)
        trades_n = len(state.trades_0x1d)
        clob_n = len(state.clob_trades)
        settled_n = len(state.settled_windows)
        win_trades = state.trade_count_window

        # ── RTDS 健康 ──
        rtds_uptime = ""
        if state.rtds_connected and state.rtds_connect_ts > 0:
            up_s = now - state.rtds_connect_ts
            rtds_uptime = f", RTDS在线{up_s:.0f}s"
        rtds_recon = f", 重连{state.rtds_reconnect_count}次" if state.rtds_reconnect_count else ""

        # ── BTC 价格 ──
        btc_str = f"${state.btc_price:,.2f}" if state.btc_price else "N/A"

        warmup_str = " [预热中]" if state.warmup else ""

        # ── PnL 统计 ──
        pnl_str = ""
        if state.settled_windows:
            pnls = [w["pnl"] for w in state.settled_windows]
            total_pnl = sum(pnls)
            wins = sum(1 for p in pnls if p > 0)
            win_rate = wins / len(pnls) * 100
            avg_pnl = total_pnl / len(pnls)
            pnl_str = f" | PnL=${total_pnl:+.2f} 胜率={win_rate:.0f}% 均值=${avg_pnl:+.2f}"

        print(
            f"[摘要]{warmup_str} 连接: {conn_str} | "
            f"BTC={btc_str} | "
            f"数据: RTDS={btc_n} BN={bn_n} PM={pm_n} 交易={trades_n} CLOB={clob_n} | "
            f"窗口交易={win_trades} 已结算={settled_n}{pnl_str}{rtds_uptime}{rtds_recon}"
        )

        await asyncio.sleep(SUMMARY_INTERVAL)


# ─────────────────────────────────────────────────────────
#  WebSocket 推送
# ─────────────────────────────────────────────────────────

async def ws_push_loop():
    """定期向所有 WebSocket 客户端推送状态"""
    while True:
        if state.ws_clients:
            snapshot = state.snapshot()
            msg = json.dumps(snapshot)
            dead = set()
            for ws in state.ws_clients:
                try:
                    await ws.send_str(msg)
                except Exception:
                    dead.add(ws)
            state.ws_clients -= dead
        await asyncio.sleep(WS_PUSH_INTERVAL)


# ─────────────────────────────────────────────────────────
#  Web Server
# ─────────────────────────────────────────────────────────

async def handle_ws(request: aiohttp.web.Request) -> aiohttp.web.WebSocketResponse:
    """WebSocket 端点"""
    ws = aiohttp.web.WebSocketResponse()
    await ws.prepare(request)
    state.ws_clients.add(ws)

    # 立即推送一次快照
    try:
        await ws.send_str(json.dumps(state.snapshot()))
    except Exception:
        pass

    try:
        async for msg in ws:
            pass  # 前端不发消息, 只接收
    finally:
        state.ws_clients.discard(ws)
    return ws


async def handle_index(request: aiohttp.web.Request) -> aiohttp.web.Response:
    """主页"""
    html_path = Path(__file__).parent / "monitor_0x1d_ui.html"
    if html_path.exists():
        return aiohttp.web.FileResponse(html_path)
    return aiohttp.web.Response(text="monitor_0x1d_ui.html not found", status=404)


async def handle_api_snapshot(request: aiohttp.web.Request) -> aiohttp.web.Response:
    """REST API: 获取快照"""
    return aiohttp.web.json_response(state.snapshot())


def create_app() -> aiohttp.web.Application:
    app = aiohttp.web.Application()
    app.router.add_get("/", handle_index)
    app.router.add_get("/ws", handle_ws)
    app.router.add_get("/api/snapshot", handle_api_snapshot)
    return app


async def start_server(port: int):
    """启动所有任务"""
    app = create_app()
    runner = aiohttp.web.AppRunner(app)
    await runner.setup()
    site = aiohttp.web.TCPSite(runner, "127.0.0.1", port)
    await site.start()
    print(f"\n{'='*60}")
    print(f"  0x1d 实时监控面板")
    print(f"  打开浏览器: http://localhost:{port}")
    print(f"  监控地址: {ADDR_0X1D[:18]}...")
    print(f"  BTC 价源: Chainlink RTDS (PM结算价) + Binance (备用)")
    print(f"  PTB 捕获: observationsTimestamp % 300 == 0")
    db_stats = _db_get_stats()
    print(f"  数据库: {db_stats['trade_snaps']} 条交易快照, {db_stats['settlements']} 条结算")
    print(f"  特征数: {len(MonitorState.FEATURE_DEFS)} 个 (策略蒸馏用)")
    # 计算距下一个 5-min 窗口的秒数
    _now_ts = int(time.time())
    _secs_to_next = 300 - (_now_ts % 300)
    print(f"  预热: 等待下一个5min窗口 ({_secs_to_next}s后) 再正式统计")
    print(f"{'='*60}\n")

    # 启动所有数据采集任务
    async def _safe(name, coro):
        try:
            await coro
        except asyncio.CancelledError:
            pass
        except Exception as e:
            print(f"[FATAL] 任务 {name} 异常退出: {e}")
            import traceback
            traceback.print_exc()

    tasks = [
        asyncio.create_task(_safe("chainlink_rtds", chainlink_rtds_stream())),
        asyncio.create_task(_safe("rtds_boundary_wd", _rtds_boundary_watchdog())),
        asyncio.create_task(_safe("binance_btc", binance_btc_stream())),
        asyncio.create_task(_safe("pm_price", pm_price_loop())),
        asyncio.create_task(_safe("clob_trade", clob_trade_stream())),
        asyncio.create_task(_safe("poll_activity", poll_0x1d_activity())),
        asyncio.create_task(_safe("ws_push", ws_push_loop())),
        asyncio.create_task(_safe("summary", periodic_summary())),
    ]

    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        pass
    finally:
        await runner.cleanup()


def main():
    parser = argparse.ArgumentParser(description="0x1d 实时监控面板")
    parser.add_argument("--port", type=int, default=8888, help="Web 端口 (默认 8888)")
    args = parser.parse_args()

    try:
        asyncio.run(start_server(args.port))
    except KeyboardInterrupt:
        print("\n已停止")


if __name__ == "__main__":
    main()
