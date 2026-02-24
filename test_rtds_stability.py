#!/usr/bin/env python3
"""
RTDS (Chainlink via Polymarket) 连通性 & 稳定性测试
====================================================

测试项:
  1. TCP/TLS 握手延迟
  2. WebSocket 升级 + 订阅延迟
  3. 首条 BTC/USD 数据到达延迟
  4. 持续监测: 数据频率、价格跳动、ping-pong RTT、丢包
  5. 5-min 边界 PTB 捕获准确性

用法:
  python test_rtds_stability.py                # 默认跑 5 分钟
  python test_rtds_stability.py --duration 60  # 跑 60 秒
  python test_rtds_stability.py --duration 0   # 无限跑 (Ctrl+C 停止)

依赖: aiohttp (pip install aiohttp)
"""

from __future__ import annotations

import argparse
import asyncio
import json
import statistics
import sys
import time
from collections import deque
from datetime import datetime, timezone

try:
    import aiohttp
except ImportError:
    print("ERROR: 需要 aiohttp — pip install aiohttp")
    sys.exit(1)


# ─── 常量 ───
RTDS_WS_URL = "wss://ws-live-data.polymarket.com"
PING_INTERVAL = 5          # ping 频率 (秒)
REPORT_INTERVAL = 10       # 统计报告间隔 (秒)
DEFAULT_DURATION = 300     # 默认测试时长 (秒)


# ─── 颜色输出 ───
def _c(text: str, code: int) -> str:
    return f"\033[{code}m{text}\033[0m"

GREEN  = lambda t: _c(t, 32)
RED    = lambda t: _c(t, 31)
YELLOW = lambda t: _c(t, 33)
CYAN   = lambda t: _c(t, 36)
BOLD   = lambda t: _c(t, 1)
DIM    = lambda t: _c(t, 2)


# ─── 解析 RTDS 消息 ───
def parse_rtds(raw: str) -> list[dict]:
    """解析 RTDS 消息, 返回 [{price, obs_ts, symbol, type}]"""
    if not raw or not raw.strip():
        return []
    try:
        obj = json.loads(raw)
    except json.JSONDecodeError:
        return []

    payload = obj.get("payload", {})
    if not isinstance(payload, dict):
        return []

    msg_type = obj.get("type", "")
    results = []

    if msg_type == "update":
        sym = payload.get("symbol", "").lower()
        price = float(payload.get("value", 0))
        ts = payload.get("timestamp", time.time() * 1000)
        if ts > 1e12:
            ts /= 1000.0
        if price > 0:
            results.append({"price": price, "obs_ts": ts, "symbol": sym, "type": "update"})

    elif msg_type == "subscribe":
        sym = payload.get("symbol", "").lower()
        data_arr = payload.get("data", [])
        if isinstance(data_arr, list):
            for item in data_arr:
                price = float(item.get("value", 0))
                ts = item.get("timestamp", time.time() * 1000)
                if ts > 1e12:
                    ts /= 1000.0
                if price > 0:
                    results.append({"price": price, "obs_ts": ts, "symbol": sym, "type": "snapshot"})

    return results


# ─── 统计收集器 ───
class Stats:
    def __init__(self):
        self.t_start = time.time()

        # 连接阶段 (毫秒)
        self.t_tcp_tls: float = 0.0
        self.t_ws_subscribe: float = 0.0
        self.t_subscribe_ack: float = 0.0
        self.t_first_btc: float = 0.0

        # 数据流
        self.msg_count: int = 0
        self.btc_count: int = 0
        self.other_count: int = 0
        self.last_btc_ts: float = 0.0
        self.btc_intervals: deque[float] = deque(maxlen=500)
        self.btc_prices: deque[float] = deque(maxlen=500)
        self.btc_obs_timestamps: deque[float] = deque(maxlen=500)
        self.btc_freshness: deque[float] = deque(maxlen=500)  # 接收时即刻算好的延迟(ms)

        # Ping-Pong
        self.ping_sent: int = 0
        self.pong_received: int = 0
        self.rtt_samples: deque[float] = deque(maxlen=200)
        self._pending_ping_ts: float = 0.0

        # PTB 边界
        self.ptb_events: list[dict] = []

        # 错误
        self.errors: list[str] = []

    def elapsed(self) -> float:
        return time.time() - self.t_start

    def record_btc(self, price: float, obs_ts: float):
        now = time.time()
        if self.last_btc_ts > 0:
            self.btc_intervals.append(now - self.last_btc_ts)
        self.last_btc_ts = now
        self.btc_count += 1
        self.btc_prices.append(price)
        self.btc_obs_timestamps.append(obs_ts)
        freshness = (now - obs_ts) * 1000  # 接收瞬间就算好延迟
        if 0 < freshness < 120_000:
            self.btc_freshness.append(freshness)

        # 检测 PTB 边界 (obs_ts % 300 == 0)
        obs_int = int(obs_ts)
        if obs_int > 0 and obs_int % 300 == 0:
            self.ptb_events.append({
                "obs_ts": obs_int,
                "price": price,
                "received_at": now,
                "latency_ms": (now - obs_ts) * 1000,
            })

    def record_ping(self):
        self.ping_sent += 1
        self._pending_ping_ts = time.time()

    def record_pong(self):
        self.pong_received += 1
        if self._pending_ping_ts > 0:
            rtt = (time.time() - self._pending_ping_ts) * 1000
            self.rtt_samples.append(rtt)
            self._pending_ping_ts = 0

    def summary(self) -> str:
        lines = []
        elapsed = self.elapsed()
        lines.append("")
        lines.append(BOLD("=" * 62))
        lines.append(BOLD(f"  RTDS 稳定性测试报告  ({elapsed:.0f}s)"))
        lines.append(BOLD("=" * 62))

        # 1. 连接延迟
        lines.append("")
        lines.append(CYAN("── 连接延迟 ──"))
        lines.append(f"  TCP+TLS 握手:        {self._fmt_ms(self.t_tcp_tls)}")
        lines.append(f"  WS 订阅发送:         {self._fmt_ms(self.t_ws_subscribe)}")
        lines.append(f"  订阅确认 (snapshot):  {self._fmt_ms(self.t_subscribe_ack)}")
        lines.append(f"  首条 BTC/USD 到达:    {self._fmt_ms(self.t_first_btc)}")
        total_connect = self.t_tcp_tls + self.t_ws_subscribe
        lines.append(f"  总连接延迟:           {self._fmt_ms(total_connect)}")

        # 2. 数据流统计
        lines.append("")
        lines.append(CYAN("── 数据流 ──"))
        lines.append(f"  总消息数:    {self.msg_count}")
        lines.append(f"  BTC/USD:     {self.btc_count}  ({self.btc_count / max(elapsed, 1):.2f}/s)")
        lines.append(f"  其他币种:    {self.other_count}")

        if self.btc_intervals:
            intervals = list(self.btc_intervals)
            avg_iv = statistics.mean(intervals)
            med_iv = statistics.median(intervals)
            max_iv = max(intervals)
            min_iv = min(intervals)
            lines.append(f"  BTC 间隔:    avg={avg_iv:.3f}s  med={med_iv:.3f}s  "
                         f"min={min_iv:.3f}s  max={max_iv:.3f}s")
            gaps = [iv for iv in intervals if iv > 3.0]
            if gaps:
                lines.append(f"  {RED(f'⚠ 大间隔 (>3s): {len(gaps)} 次, 最大 {max(gaps):.1f}s')}")
            else:
                lines.append(f"  {GREEN('✓ 无大间隔 (>3s)')}")

        # 3. 价格统计
        if self.btc_prices:
            prices = list(self.btc_prices)
            lines.append("")
            lines.append(CYAN("── BTC 价格 ──"))
            lines.append(f"  最新:  ${prices[-1]:,.2f}")
            lines.append(f"  范围:  ${min(prices):,.2f} ~ ${max(prices):,.2f}")
            if len(prices) >= 2:
                jumps = [abs(prices[i] - prices[i-1]) for i in range(1, len(prices))]
                avg_jump = statistics.mean(jumps)
                max_jump = max(jumps)
                lines.append(f"  跳动:  avg=${avg_jump:.2f}  max=${max_jump:.2f}")

        # 4. observationsTimestamp 延迟 (接收瞬间计算)
        if self.btc_freshness:
            delays = list(self.btc_freshness)
            lines.append("")
            lines.append(CYAN("── 数据新鲜度 (receive_time - observationTimestamp) ──"))
            lines.append(f"  样本: {len(delays)}条  avg={statistics.mean(delays):.0f}ms  "
                         f"med={statistics.median(delays):.0f}ms  "
                         f"max={max(delays):.0f}ms")
            if statistics.mean(delays) > 3000:
                lines.append(f"  {YELLOW('⚠ 数据新鲜度较差 (>3s), Chainlink 源可能滞后')}")
            else:
                lines.append(f"  {GREEN('✓ 数据新鲜度正常')}")

        # 5. Ping-Pong
        lines.append("")
        lines.append(CYAN("── Ping-Pong RTT ──"))
        lines.append(f"  发送: {self.ping_sent}  收到: {self.pong_received}  "
                     f"丢包: {self.ping_sent - self.pong_received}")
        if self.rtt_samples:
            rtt = list(self.rtt_samples)
            lines.append(f"  RTT:  avg={statistics.mean(rtt):.1f}ms  "
                         f"med={statistics.median(rtt):.1f}ms  "
                         f"p95={self._p95(rtt):.1f}ms  "
                         f"max={max(rtt):.1f}ms")
            if statistics.mean(rtt) > 200:
                lines.append(f"  {YELLOW('⚠ RTT 偏高 (>200ms), 可能影响结算时效')}")
            else:
                lines.append(f"  {GREEN('✓ RTT 正常')}")

        # 6. PTB 边界
        if self.ptb_events:
            lines.append("")
            lines.append(CYAN("── PTB 边界事件 (obs_ts % 300 == 0) ──"))
            for ev in self.ptb_events:
                t = datetime.fromtimestamp(ev["obs_ts"], tz=timezone.utc).strftime("%H:%M:%S")
                lat_ms = ev["latency_ms"]
                lat_color = GREEN if lat_ms < 2000 else YELLOW
                lines.append(f"  {t} UTC  ${ev['price']:,.2f}  "
                             f"接收延迟={lat_color(f'{lat_ms:.0f}ms')}")

        # 7. 错误
        if self.errors:
            lines.append("")
            lines.append(RED(f"── 错误 ({len(self.errors)}) ──"))
            for err in self.errors[-10:]:
                lines.append(f"  {err}")

        # 8. 综合评估
        lines.append("")
        lines.append(BOLD("── 综合评估 ──"))
        issues = []
        if total_connect > 2000:
            issues.append("连接慢 (>2s)")
        if self.btc_intervals and max(list(self.btc_intervals)) > 5.0:
            issues.append(f"数据中断 (最大间隔={max(list(self.btc_intervals)):.1f}s)")
        if self.rtt_samples and statistics.mean(list(self.rtt_samples)) > 200:
            issues.append(f"RTT高 (avg={statistics.mean(list(self.rtt_samples)):.0f}ms)")
        loss = self.ping_sent - self.pong_received
        if self.ping_sent > 0 and loss / self.ping_sent > 0.1:
            issues.append(f"丢包率高 ({loss / self.ping_sent:.0%})")
        if self.btc_count == 0:
            issues.append("未收到任何 BTC/USD 数据!")
        if self.errors:
            issues.append(f"{len(self.errors)} 个错误")

        if not issues:
            lines.append(f"  {GREEN(BOLD('✓ RTDS 连接正常, 适合生产使用'))}")
        else:
            for iss in issues:
                lines.append(f"  {RED('✗ ' + iss)}")
            if len(issues) <= 1 and "错误" not in str(issues):
                lines.append(f"  {YELLOW('△ 存在轻微问题, 建议观察')}")
            else:
                lines.append(f"  {RED('✗ 不建议在此网络环境下依赖 RTDS')}")

        lines.append("")
        return "\n".join(lines)

    @staticmethod
    def _fmt_ms(ms: float) -> str:
        if ms <= 0:
            return DIM("N/A")
        color = GREEN if ms < 500 else (YELLOW if ms < 1000 else RED)
        return color(f"{ms:.0f} ms")

    @staticmethod
    def _p95(data: list[float]) -> float:
        s = sorted(data)
        idx = int(len(s) * 0.95)
        return s[min(idx, len(s) - 1)]


# ─── 实时状态行 ───
def _print_status(stats: Stats):
    elapsed = stats.elapsed()
    price = f"${stats.btc_prices[-1]:,.2f}" if stats.btc_prices else "--"
    rate = f"{stats.btc_count / max(elapsed, 1):.1f}/s"
    rtt = f"{statistics.mean(list(stats.rtt_samples)):.0f}ms" if stats.rtt_samples else "--"
    gap = f"{max(list(stats.btc_intervals)):.1f}s" if stats.btc_intervals else "--"
    print(f"\r  ⏱ {elapsed:5.0f}s │ BTC {price} │ "
          f"msgs={stats.btc_count} ({rate}) │ RTT={rtt} │ maxGap={gap}   ",
          end="", flush=True)


# ─── 主测试流程 ───
async def run_test(duration: int):
    stats = Stats()

    print(BOLD(f"\n{'=' * 62}"))
    print(BOLD(f"  RTDS 连通性 & 稳定性测试"))
    print(f"  服务器: {CYAN(RTDS_WS_URL)}")
    dur_str = f"{duration}s" if duration > 0 else "无限 (Ctrl+C 停止)"
    print(f"  时长:   {dur_str}")
    ts_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"  开始:   {ts_str}")
    print(BOLD(f"{'=' * 62}\n"))

    # ── Phase 1: TCP+TLS 连接 (含 429 限流重试) ──
    print(f"  [1/4] TCP + TLS 握手 ... ", end="", flush=True)

    session = aiohttp.ClientSession()
    ws = None
    max_retries = 5

    for attempt in range(1, max_retries + 1):
        t0 = time.perf_counter()
        try:
            ws = await session.ws_connect(
                RTDS_WS_URL,
                heartbeat=None,
                timeout=aiohttp.ClientWSTimeout(ws_close=15),
                autoping=False,
                autoclose=False,
            )
            break
        except Exception as e:
            err_str = str(e)
            if "429" in err_str and attempt < max_retries:
                wait = 10 * attempt
                print(f"\n         429 限流, 等 {wait}s 后重试 ({attempt}/{max_retries}) ... ",
                      end="", flush=True)
                await asyncio.sleep(wait)
                continue
            print(RED(f"FAIL: {e}"))
            stats.errors.append(f"连接失败 (尝试{attempt}次): {e}")
            await session.close()
            print(stats.summary())
            return

    t_connected = time.perf_counter()
    stats.t_tcp_tls = (t_connected - t0) * 1000
    print(GREEN(f"{stats.t_tcp_tls:.0f}ms ✓"))

    # ── Phase 2: WebSocket 订阅 ──
    print(f"  [2/4] 发送订阅请求 ... ", end="", flush=True)
    t1 = time.perf_counter()
    await ws.send_json({
        "action": "subscribe",
        "subscriptions": [
            {"topic": "crypto_prices_chainlink", "type": "*"}
        ],
    })
    stats.t_ws_subscribe = (time.perf_counter() - t1) * 1000
    print(GREEN(f"{stats.t_ws_subscribe:.0f}ms ✓"))

    # ── Phase 3: 等待首条数据 ──
    print(f"  [3/4] 等待首条数据 ... ", end="", flush=True)
    t2 = time.perf_counter()
    got_first_data = False
    got_first_btc = False

    while (time.perf_counter() - t2) < 15.0:
        try:
            msg = await asyncio.wait_for(ws.receive(), timeout=15.0)
        except asyncio.TimeoutError:
            break
        if msg.type == aiohttp.WSMsgType.TEXT:
            stats.msg_count += 1
            items = parse_rtds(msg.data)
            # 首条有效数据 = 订阅确认
            if items and not got_first_data:
                stats.t_subscribe_ack = (time.perf_counter() - t2) * 1000
                n_btc = sum(1 for it in items if it["symbol"] == "btc/usd")
                n_other = len(items) - n_btc
                msg_type = items[0]["type"]
                print(GREEN(f"{stats.t_subscribe_ack:.0f}ms ✓") +
                      f"  (type={msg_type}, {n_btc} BTC + {n_other} 其他)")
                got_first_data = True
            for it in (items or []):
                if it["symbol"] == "btc/usd":
                    if not got_first_btc:
                        stats.t_first_btc = (time.perf_counter() - t0) * 1000
                        got_first_btc = True
                    stats.record_btc(it["price"], it["obs_ts"])
                else:
                    stats.other_count += 1
            # 收到 BTC 数据就可以结束等待
            if got_first_btc:
                break
        elif msg.type == aiohttp.WSMsgType.PONG:
            stats.record_pong()
        elif msg.type == aiohttp.WSMsgType.PING:
            await ws.pong(msg.data)

    if not got_first_data:
        print(YELLOW("超时 (15s), 继续等待 ..."))

    if got_first_btc:
        print(f"  [4/4] 首条 BTC/USD: {GREEN(f'{stats.t_first_btc:.0f}ms')}  "
              f"${stats.btc_prices[-1]:,.2f}")
    else:
        print(f"  [4/4] 等待首条 BTC/USD ... ", end="", flush=True)

    # ── Phase 4: 持续监测 ──
    print(f"\n  {DIM('持续监测中 ...')}\n")

    last_report = time.time()
    last_ping = time.time()

    try:
        while True:
            if duration > 0 and stats.elapsed() >= duration:
                break

            now = time.time()
            # 手动发 WS ping 帧 (autoping=False, 这样 PONG 会作为消息返回)
            if now - last_ping >= PING_INTERVAL:
                try:
                    await ws.ping()
                    stats.record_ping()
                    last_ping = now
                except Exception as e:
                    stats.errors.append(f"ping 失败: {e}")

            if now - last_report >= REPORT_INTERVAL:
                _print_status(stats)
                last_report = now

            try:
                msg = await asyncio.wait_for(ws.receive(), timeout=1.0)
            except asyncio.TimeoutError:
                continue

            if msg.type == aiohttp.WSMsgType.TEXT:
                stats.msg_count += 1
                items = parse_rtds(msg.data)
                for it in items:
                    if it["symbol"] == "btc/usd":
                        if not got_first_btc:
                            stats.t_first_btc = (time.perf_counter() - t0) * 1000
                            got_first_btc = True
                            print(GREEN(f"{stats.t_first_btc:.0f}ms ✓") +
                                  f"  ${it['price']:,.2f}")
                        stats.record_btc(it["price"], it["obs_ts"])
                    else:
                        stats.other_count += 1

            elif msg.type == aiohttp.WSMsgType.PONG:
                stats.record_pong()

            elif msg.type == aiohttp.WSMsgType.PING:
                await ws.pong(msg.data)

            elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                stats.errors.append(f"WS 异常关闭: {msg.type.name}")
                break

    except KeyboardInterrupt:
        print(f"\n\n  {YELLOW('Ctrl+C 手动停止')}")
    except Exception as e:
        stats.errors.append(f"运行异常: {e}")
    finally:
        try:
            await ws.close()
        except Exception:
            pass
        await session.close()

    print(stats.summary())


def main():
    parser = argparse.ArgumentParser(description="RTDS 连通性 & 稳定性测试")
    parser.add_argument("--duration", "-d", type=int, default=DEFAULT_DURATION,
                        help=f"测试时长 (秒), 0=无限 (默认 {DEFAULT_DURATION})")
    args = parser.parse_args()

    # Windows: 避免 "Event loop is closed" 异常
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    try:
        asyncio.run(run_test(args.duration))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
