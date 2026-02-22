"""
WebSocket Manager — 实时数据推送

职责:
- 管理浏览器 WebSocket 连接
- 订阅 EventBus 事件，转发到所有连接的客户端
- 支持客户端订阅/取消特定频道
- 心跳保活

推送频道:
- price    : BTC/USD 实时价格
- kline    : K 线更新
- orderbook: 订单簿变更
- trades   : 成交推送
- strategy : 策略信号
- risk     : 风控事件
- system   : 系统状态
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any

import aiohttp
import aiohttp.web
from loguru import logger

from src.core.event_bus import Event, EventBus, EventType
from src.core.context import Context


# 事件类型 -> 推送频道 映射
_EVENT_CHANNEL_MAP: dict[EventType, str] = {
    EventType.CHAINLINK_PRICE: "price",
    EventType.MARKET_TICK: "price",
    EventType.MARKET_KLINE: "kline",
    EventType.MARKET_INDICATOR: "indicator",
    EventType.ORDERBOOK_UPDATE: "orderbook",
    EventType.ORDERBOOK_LARGE_ORDER: "orderbook",
    EventType.PM_PRICE: "pm_price",
    EventType.PM_TRADE: "pm_trade",
    EventType.ORDER_CREATED: "trades",
    EventType.ORDER_FILLED: "trades",
    EventType.ORDER_PARTIAL_FILL: "trades",
    EventType.ORDER_CANCELLED: "trades",
    EventType.ORDER_REJECTED: "trades",
    EventType.SIGNAL_GENERATED: "strategy",
    EventType.STRATEGY_START: "strategy",
    EventType.STRATEGY_STOP: "strategy",
    EventType.RISK_WARNING: "risk",
    EventType.RISK_ALERT: "risk",
    EventType.RISK_BREAKER: "risk",
    EventType.HEARTBEAT: "system",
    EventType.SYSTEM_ERROR: "system",
}


class WebSocketManager:
    """管理所有 Dashboard WebSocket 连接。"""

    def __init__(
        self,
        event_bus: EventBus,
        context: Context,
        heartbeat_interval: float = 10.0,
    ) -> None:
        self._bus = event_bus
        self._ctx = context
        self._heartbeat_interval = heartbeat_interval

        # 活跃连接集
        self._connections: set[aiohttp.web.WebSocketResponse] = set()
        # 客户端订阅的频道 { ws: set("price", "kline", ...) }
        self._subscriptions: dict[aiohttp.web.WebSocketResponse, set[str]] = {}
        self._running = False
        self._heartbeat_task: asyncio.Task | None = None
        self._price_broadcast_task: asyncio.Task | None = None

        # 最近价格缓存 (供新连接回放 tick 图)
        self._recent_prices: list[dict] = []  # [{time, value}, ...]
        self._max_recent_prices = 300  # ~5分钟 @ 1/s
        self._last_broadcast_price: float = 0.0

    # ────────────────── 生命周期 ──────────────────

    async def start(self) -> None:
        """注册 EventBus 订阅，启动心跳。"""
        self._running = True

        # 订阅所有需要推送的事件
        for event_type in _EVENT_CHANNEL_MAP:
            self._bus.subscribe(event_type, self._on_event, priority=-10)

        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        self._price_broadcast_task = asyncio.create_task(self._price_broadcast_loop())
        logger.debug(f"WS Manager 启动, 监听 {len(_EVENT_CHANNEL_MAP)} 种事件")

    async def stop(self) -> None:
        """关闭所有连接。"""
        self._running = False
        if self._price_broadcast_task:
            self._price_broadcast_task.cancel()
            try:
                await self._price_broadcast_task
            except asyncio.CancelledError:
                pass
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass

        for ws in list(self._connections):
            try:
                await ws.close()
            except Exception:
                pass
        self._connections.clear()
        self._subscriptions.clear()

    # ────────────────── WebSocket 处理 ──────────────────

    async def handle(self, request: aiohttp.web.Request) -> aiohttp.web.WebSocketResponse:
        """处理新的 WebSocket 连接。"""
        ws = aiohttp.web.WebSocketResponse(heartbeat=30.0)
        await ws.prepare(request)

        self._connections.add(ws)
        # 默认订阅所有频道
        self._subscriptions[ws] = set(_EVENT_CHANNEL_MAP.values())

        peer = request.remote or "unknown"
        logger.info(f"Dashboard WS 连接: {peer} (当前 {len(self._connections)} 个)")

        # 发送初始状态快照
        await self._send_snapshot(ws)

        try:
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    await self._handle_client_message(ws, msg.data)
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    logger.warning(f"WS 错误: {ws.exception()}")
                    break
        except Exception as e:
            logger.warning(f"WS 异常: {e}")
        finally:
            self._connections.discard(ws)
            self._subscriptions.pop(ws, None)
            logger.info(f"Dashboard WS 断开: {peer} (剩余 {len(self._connections)} 个)")

        return ws

    async def _handle_client_message(
        self, ws: aiohttp.web.WebSocketResponse, raw: str
    ) -> None:
        """
        处理客户端消息。

        协议:
          {"action": "subscribe", "channels": ["price", "kline"]}
          {"action": "unsubscribe", "channels": ["orderbook"]}
          {"action": "ping"}
        """
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            return

        action = msg.get("action", "")
        channels = msg.get("channels", [])

        if action == "subscribe":
            subs = self._subscriptions.get(ws, set())
            subs.update(channels)
            self._subscriptions[ws] = subs
        elif action == "unsubscribe":
            subs = self._subscriptions.get(ws, set())
            subs -= set(channels)
        elif action == "ping":
            await self._send_json(ws, {"type": "pong", "ts": time.time()})

    # ────────────────── 事件路由 ──────────────────

    async def _on_event(self, event: Event) -> None:
        """EventBus 回调: 将事件转发给订阅了对应频道的客户端。"""
        channel = _EVENT_CHANNEL_MAP.get(event.type)
        if not channel:
            return

        # 价格事件: 不管有没有 WS 客户端，都缓存到 recent_prices
        if channel == "price" and event.data:
            price = event.data.get("price", 0)
            ts = event.data.get("timestamp", time.time())
            if price > 0:
                self._recent_prices.append({
                    "time": int(ts) if ts > 1e9 else int(time.time()),
                    "value": price,
                })
                if len(self._recent_prices) > self._max_recent_prices:
                    self._recent_prices = self._recent_prices[-self._max_recent_prices:]

        if not self._connections:
            return

        payload = self._serialize_event(channel, event)
        if not payload:
            return
        dead: list[aiohttp.web.WebSocketResponse] = []

        for ws in list(self._connections):
            subs = self._subscriptions.get(ws, set())
            if channel not in subs:
                continue
            try:
                await ws.send_str(payload)
            except (ConnectionResetError, RuntimeError):
                dead.append(ws)

        for ws in dead:
            self._connections.discard(ws)
            self._subscriptions.pop(ws, None)

    def _serialize_event(self, channel: str, event: Event) -> str:
        """将 Event 序列化为 JSON 字符串。"""
        data = event.data.copy() if event.data else {}

        # Tick 对象不能直接 JSON 序列化，提取字段
        tick = data.pop("tick", None)
        if tick and hasattr(tick, "price"):
            data["price"] = tick.price
            data["timestamp"] = tick.timestamp
            data["source"] = tick.source

        # 确保所有值可 JSON 序列化
        safe_data = {}
        for k, v in data.items():
            if isinstance(v, (str, int, float, bool, type(None), list, dict)):
                safe_data[k] = v
            else:
                safe_data[k] = str(v)

        msg = {
            "type": channel,
            "data": safe_data,
            "ts": event.timestamp,
        }
        return json.dumps(msg, default=str)

    # ────────────────── 快照推送 ──────────────────

    async def _send_snapshot(self, ws: aiohttp.web.WebSocketResponse) -> None:
        """发送当前系统状态快照给新连接的客户端。"""
        ctx = self._ctx
        snapshot = {
            "type": "snapshot",
            "data": {
                "account": {
                    "balance": ctx.account.balance,
                    "total_equity": ctx.account.total_equity,
                    "daily_pnl": ctx.account.daily_pnl,
                    "total_pnl": ctx.account.total_pnl,
                    "initial_balance": ctx.account.initial_balance,
                    "open_positions": len(ctx.account.open_positions),
                },
                "market": {
                    "btc_price": ctx.market.btc_price,
                    "btc_price_updated_at": ctx.market.btc_price_updated_at,
                    "pm_yes_price": ctx.market.pm_yes_price,
                    "pm_no_price": ctx.market.pm_no_price,
                    "pm_yes_bid": ctx.market.pm_yes_bid,
                    "pm_yes_ask": ctx.market.pm_yes_ask,
                    "pm_no_bid": ctx.market.pm_no_bid,
                    "pm_no_ask": ctx.market.pm_no_ask,
                    "pm_market_question": ctx.market.pm_market_question,
                    "pm_window_seconds_left": ctx.market.pm_window_seconds_left,
                    "pm_window_start_price": ctx.market.pm_window_start_price,
                },
                "system": {
                    "run_mode": ctx.run_mode.value,
                    "trading_mode": ctx.trading_mode.value,
                    "uptime": time.time() - ctx.start_time,
                },
                "strategy": ctx.get("strategy_state") or {},
            },
            "ts": time.time(),
        }
        await self._send_json(ws, snapshot)

        # 回放最近的 tick 数据，让新客户端图表立即有数据
        if self._recent_prices:
            replay = {
                "type": "price_history",
                "data": {"prices": self._recent_prices},
                "ts": time.time(),
            }
            await self._send_json(ws, replay)

    # ────────────────── 工具方法 ──────────────────

    async def _send_json(
        self, ws: aiohttp.web.WebSocketResponse, data: dict
    ) -> None:
        try:
            await ws.send_str(json.dumps(data, default=str))
        except (ConnectionResetError, RuntimeError):
            self._connections.discard(ws)

    async def _heartbeat_loop(self) -> None:
        """定期推送心跳和系统状态。"""
        while self._running:
            try:
                await asyncio.sleep(self._heartbeat_interval)
                if not self._connections:
                    continue

                ctx = self._ctx
                heartbeat = {
                    "type": "heartbeat",
                    "data": {
                        "btc_price": ctx.market.btc_price,
                        "btc_price_updated_at": ctx.market.btc_price_updated_at,
                        "balance": ctx.account.balance,
                        "equity": ctx.account.total_equity,
                        "daily_pnl": ctx.account.daily_pnl,
                        "uptime": time.time() - ctx.start_time,
                        "ws_clients": len(self._connections),
                        "pm_yes_price": ctx.market.pm_yes_price,
                        "pm_no_price": ctx.market.pm_no_price,
                        "pm_yes_bid": ctx.market.pm_yes_bid,
                        "pm_yes_ask": ctx.market.pm_yes_ask,
                        "pm_no_bid": ctx.market.pm_no_bid,
                        "pm_no_ask": ctx.market.pm_no_ask,
                        "pm_market_question": ctx.market.pm_market_question,
                        "pm_window_seconds_left": ctx.market.pm_window_seconds_left,
                        "pm_window_start_price": ctx.market.pm_window_start_price,
                        "strategy": ctx.get("strategy_state") or {},
                    },
                    "ts": time.time(),
                }
                payload = json.dumps(heartbeat, default=str)
                dead = []
                for ws in list(self._connections):
                    try:
                        await ws.send_str(payload)
                    except (ConnectionResetError, RuntimeError):
                        dead.append(ws)
                for ws in dead:
                    self._connections.discard(ws)
                    self._subscriptions.pop(ws, None)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"WS 心跳错误: {e}")

    async def _price_broadcast_loop(self) -> None:
        """
        每 2 秒广播当前 BTC 价格到所有连接的客户端。

        解决 RTDS 事件驱动间隙问题: RTDS 仅在价格变化时推送,
        可能数十秒无更新, 导致浏览器图表和价格看似冻结。
        此循环确保价格持续推送, 包括 tick 图数据。
        """
        while self._running:
            try:
                await asyncio.sleep(2.0)
                if not self._connections:
                    continue

                ctx = self._ctx
                price = ctx.market.btc_price
                if price <= 0:
                    continue

                now = time.time()

                # 如果最近 2s 内已有事件推送了相同价格，跳过广播避免重复
                if (self._recent_prices
                        and abs(self._recent_prices[-1]["value"] - price) < 0.01
                        and now - self._recent_prices[-1]["time"] < 2.5):
                    continue

                # 记录到价格缓存 (供新客户端回放)
                tick_point = {
                    "time": int(now),
                    "value": price,
                }
                self._recent_prices.append(tick_point)
                if len(self._recent_prices) > self._max_recent_prices:
                    self._recent_prices = self._recent_prices[-self._max_recent_prices:]

                # 构建 price 消息 (与 RTDS 事件推送格式一致)
                msg = {
                    "type": "price",
                    "data": {
                        "price": price,
                        "timestamp": now,
                        "source": "broadcast",
                    },
                    "ts": now,
                }
                payload = json.dumps(msg, default=str)

                dead = []
                for ws in list(self._connections):
                    subs = self._subscriptions.get(ws, set())
                    if "price" not in subs:
                        continue
                    try:
                        await ws.send_str(payload)
                    except (ConnectionResetError, RuntimeError):
                        dead.append(ws)
                for ws in dead:
                    self._connections.discard(ws)
                    self._subscriptions.pop(ws, None)

                self._last_broadcast_price = price

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"价格广播错误: {e}")

    @property
    def client_count(self) -> int:
        return len(self._connections)
