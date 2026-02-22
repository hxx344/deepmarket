"""
Polymarket DataSource - Polymarket 预测市场数据源

通过 Gamma API 自动发现当前 5 分钟 BTC Up/Down 预测市场，
通过 CLOB API 轮询 midpoint 价格，
每 5 分钟自动轮换到下一个窗口市场。

市场发现机制:
  slug 模式: btc-updown-5m-{窗口开始时间的UTC Unix时间戳}
  API: GET https://gamma-api.polymarket.com/events?slug={slug}
  示例: btc-updown-5m-1771159500 → "Bitcoin Up or Down - February 15, 7:45AM-7:50AM ET"
"""

from __future__ import annotations

import asyncio
import datetime
import json
import re
import time
from dataclasses import dataclass, field
from typing import Any

import aiohttp
from loguru import logger

from src.core.event_bus import Event, EventBus, EventType
from src.market.datasources.base import DataSourceBase, DataSourceStatus, Tick


# ────────────────────── 市场数据结构 ──────────────────────

@dataclass
class PMMarket:
    """Polymarket 5-min BTC Up/Down 市场快照"""
    question: str = ""
    condition_id: str = ""
    slug: str = ""
    end_date: str = ""          # ISO date (UTC)
    event_start_time: str = ""  # 窗口开始时间 (UTC ISO)
    outcomes: list[str] = field(default_factory=list)  # ["Up", "Down"]
    token_ids: list[str] = field(default_factory=list)  # clobTokenIds
    prices: list[float] = field(default_factory=list)   # [up_price, down_price]
    neg_risk: bool = False      # 是否负风险市场 (BTC 5-min 通常是 False)
    window_start_ts: int = 0    # 窗口开始 UTC unix ts
    window_end_ts: int = 0      # 窗口结束 UTC unix ts


class PolymarketDataSource(DataSourceBase):
    """
    Polymarket CLOB 数据源 — 5 分钟 BTC Up/Down 市场

    功能:
    - 基于 slug 模式精确定位当前 5-min 窗口市场 (Gamma API)
    - 轮询 Up/Down midpoint 价格 (CLOB midpoint API)
    - WebSocket 实时成交流 (CLOB WS)
    - 每 5 分钟边界自动轮换到下一个窗口
    """

    # Polymarket API endpoints
    GAMMA_API = "https://gamma-api.polymarket.com"
    CLOB_REST = "https://clob.polymarket.com"
    CLOB_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

    # 5-min 市场 slug 前缀
    SLUG_PREFIX = "btc-updown-5m-"

    def __init__(
        self,
        event_bus: EventBus | None = None,
        market_ids: list[str] | None = None,
        search_query: str = "Bitcoin",
        poll_interval: float = 5.0,
    ) -> None:
        super().__init__(event_bus)
        self.market_ids = market_ids or []
        self.search_query = search_query
        self.poll_interval = poll_interval
        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self._ws_book: aiohttp.ClientWebSocketResponse | None = None
        self._session: aiohttp.ClientSession | None = None
        self._running = False

        # 当前活跃的 5-min 窗口市场
        self._active_market: PMMarket | None = None
        # 当前窗口的 UTC unix 时间戳
        self._current_window_ts: int = 0

        # WS Book 频道回调: token_id → callable(bids, asks, is_snapshot)
        # 由 main.py 注入, 用于将 WS 实时 OB 数据直接推送到 OrderBook 对象
        self._book_callbacks: dict[str, Any] = {}
        # WS Book 连接状态 (供外部判断是否需要 REST 降级轮询)
        self.ws_book_connected: bool = False
        # 当前 WS Book 已订阅的 token IDs (用于窗口切换时取消旧订阅)
        self._ws_book_subscribed_ids: list[str] = []

    def name(self) -> str:
        return "polymarket"

    # ────────────────── 连接管理 ──────────────────

    async def connect(self) -> None:
        """连接并发现当前 5-min BTC 窗口市场"""
        self.status = DataSourceStatus.CONNECTING
        try:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=15),
            )

            # 发现当前 5-min 窗口
            await self._discover_current_window()

            if self._active_market:
                self.status = DataSourceStatus.CONNECTED
                self._reconnect_count = 0
                logger.info(
                    f"Polymarket 已连接 — 当前窗口: {self._active_market.question}"
                )
            else:
                self.status = DataSourceStatus.CONNECTED
                logger.warning("Polymarket 已连接, 但未找到当前 5-min 窗口市场")
        except Exception as e:
            self.status = DataSourceStatus.ERROR
            logger.error(f"Polymarket 连接失败: {e}")
            raise

    def register_book_callback(
        self,
        token_id: str,
        callback: Any,
    ) -> None:
        """注册 WS Book 数据回调 (由 main.py 调用)。

        callback 签名: callback(bids: list[dict], asks: list[dict], is_snapshot: bool)
        """
        self._book_callbacks[token_id] = callback

    async def disconnect(self) -> None:
        self._running = False
        if self._ws:
            await self._ws.close()
        if self._ws_book:
            await self._ws_book.close()
        if self._session:
            await self._session.close()
            self._session = None
        self.ws_book_connected = False
        self.status = DataSourceStatus.DISCONNECTED
        logger.info("Polymarket 已断开")

    async def subscribe(self, symbols: list[str], channels: list[str]) -> None:
        """订阅市场"""
        self.market_ids = symbols
        if self.is_connected() and self._ws:
            await self._subscribe_markets(symbols)

    async def start_streaming(self) -> None:
        """启动数据流: 价格轮询 + WS 成交流 + WS Book + 窗口轮换"""
        self._running = True
        tasks = []

        # 价格轮询任务 (核心)
        tasks.append(asyncio.create_task(self._price_poll_loop()))

        # WS 成交流 (可选, 需要 token_ids)
        if self._active_market and self._active_market.token_ids:
            tasks.append(asyncio.create_task(self._ws_stream_loop()))

        # WS Book 实时订单簿频道 (替代 REST 轮询 OB)
        if self._active_market and self._active_market.token_ids:
            tasks.append(asyncio.create_task(self._ws_book_loop()))

        # 5-min 窗口自动轮换
        tasks.append(asyncio.create_task(self._window_rotation_loop()))

        try:
            await asyncio.gather(*tasks, return_exceptions=True)
        except asyncio.CancelledError:
            pass
        finally:
            self._running = False

    # ────────────────── 5-min 窗口发现 ──────────────────

    @staticmethod
    def _calc_window_ts() -> tuple[int, int, float]:
        """
        计算当前 5-min 窗口的 UTC Unix 时间戳。

        返回: (window_start_ts, window_end_ts, seconds_until_next_window)
        """
        now_utc = datetime.datetime.utcnow()
        # 以 UTC 时间 floor 到最近的 5 分钟边界
        minute = now_utc.minute
        floored_min = (minute // 5) * 5
        window_start = now_utc.replace(minute=floored_min, second=0, microsecond=0)
        window_end = window_start + datetime.timedelta(minutes=5)

        ts_start = int(window_start.replace(tzinfo=datetime.timezone.utc).timestamp())
        ts_end = int(window_end.replace(tzinfo=datetime.timezone.utc).timestamp())

        seconds_left = (window_end - now_utc).total_seconds()
        return ts_start, ts_end, max(seconds_left, 0)

    async def _discover_current_window(self) -> None:
        """
        通过 slug 模式精确定位当前 5-min BTC Up/Down 市场。

        Slug 模式: btc-updown-5m-{窗口开始 UTC Unix 秒}
        API: GET /events?slug={slug} → 返回事件及嵌套市场
        """
        if not self._session:
            return

        ts_start, ts_end, secs_left = self._calc_window_ts()

        # 如果已经在当前窗口，跳过
        if ts_start == self._current_window_ts and self._active_market:
            return

        slug = f"{self.SLUG_PREFIX}{ts_start}"
        try:
            url = f"{self.GAMMA_API}/events"
            async with self._session.get(url, params={"slug": slug}) as resp:
                if resp.status != 200:
                    logger.warning(f"Gamma API 返回 {resp.status} (slug={slug})")
                    return
                events = await resp.json()

            if not events:
                logger.warning(f"未找到 5-min 窗口市场 (slug={slug})")
                return

            event = events[0]
            markets = event.get("markets", [])

            # 找到 5-min 子市场 (排除 15-min 等)
            target_market = None
            for m in markets:
                q = m.get("question", "")
                match = re.search(
                    r"(\d{1,2}:\d{2}(?:AM|PM))-(\d{1,2}:\d{2}(?:AM|PM))",
                    q,
                )
                if match:
                    t1 = datetime.datetime.strptime(match.group(1), "%I:%M%p")
                    t2 = datetime.datetime.strptime(match.group(2), "%I:%M%p")
                    delta_min = (t2 - t1).total_seconds() / 60
                    if delta_min == 5:
                        target_market = m
                        break

            # 如果只有一个市场，直接用
            if not target_market and len(markets) == 1:
                target_market = markets[0]

            if not target_market:
                logger.warning(f"事件 {slug} 中未找到 5-min 市场")
                return

            # 解析市场数据
            pm = self._parse_gamma_market(target_market, ts_start, ts_end)
            if pm:
                old_q = self._active_market.question if self._active_market else "无"
                self._active_market = pm
                self._current_window_ts = ts_start
                logger.info(
                    f"5-min 窗口切换: {old_q} → {pm.question} "
                    f"(Up={pm.prices[0]:.3f}, Down={pm.prices[1] if len(pm.prices) > 1 else 0:.3f}, "
                    f"剩余 {secs_left:.0f}s)"
                )

        except Exception as e:
            logger.error(f"发现 5-min 窗口失败: {e}")

    def _parse_gamma_market(
        self, raw: dict, ts_start: int = 0, ts_end: int = 0
    ) -> PMMarket | None:
        """解析 Gamma API 返回的市场数据"""
        try:
            outcomes_str = raw.get("outcomes", "[]")
            prices_str = raw.get("outcomePrices", "[]")
            tokens_str = raw.get("clobTokenIds", "[]")

            outcomes = (
                json.loads(outcomes_str)
                if isinstance(outcomes_str, str) else outcomes_str
            )
            prices = [
                float(p)
                for p in (
                    json.loads(prices_str)
                    if isinstance(prices_str, str) else prices_str
                )
            ]
            tokens = (
                json.loads(tokens_str)
                if isinstance(tokens_str, str) else tokens_str
            )

            if not outcomes or not tokens:
                return None

            return PMMarket(
                question=raw.get("question", ""),
                condition_id=raw.get("conditionId", ""),
                slug=raw.get("slug", ""),
                end_date=raw.get("endDate", ""),
                event_start_time=raw.get("eventStartTime", ""),
                outcomes=outcomes,
                token_ids=tokens,
                prices=prices,
                neg_risk=bool(raw.get("negRisk", False)),
                window_start_ts=ts_start,
                window_end_ts=ts_end,
            )
        except Exception as e:
            logger.debug(f"解析市场失败: {e}")
            return None

    async def _window_rotation_loop(self) -> None:
        """
        5-min 窗口自动轮换。

        在每个 5 分钟边界 + 2s 时切换到新窗口市场，
        并立即发起一次价格轮询。
        """
        while self._running:
            try:
                _, _, secs_left = self._calc_window_ts()
                # 在窗口结束后 2 秒切换 (等待新市场可用)
                wait = secs_left + 2.0
                logger.debug(f"PM 窗口轮换: {wait:.1f}s 后切换")
                await asyncio.sleep(wait)

                # 发现新窗口
                await self._discover_current_window()

                # 立即轮询新窗口价格
                if self._active_market:
                    await self._poll_midpoints()
                    # WS Book 重新订阅新窗口 token
                    await self._resubscribe_book()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"窗口轮换失败: {e}")
                await asyncio.sleep(10)

    # ────────────────── 价格轮询 ──────────────────

    async def _price_poll_loop(self) -> None:
        """
        定期轮询 CLOB midpoint 获取 YES/NO 价格。

        这是获取 PM 市场价格的主要方式。
        """
        while self._running:
            try:
                if self._active_market and self._session:
                    await self._poll_midpoints()
                await asyncio.sleep(self.poll_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"PM 价格轮询错误: {e}")
                await asyncio.sleep(self.poll_interval * 2)

    async def _fetch_midpoint(self, token_id: str) -> float:
        """获取单个 token 的 midpoint 价格"""
        try:
            url = f"{self.CLOB_REST}/midpoint"
            async with self._session.get(url, params={"token_id": token_id}) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return float(data.get("mid", 0))
        except Exception as e:
            logger.debug(f"Midpoint 获取失败 [{token_id[:12]}]: {e}")
        return 0.0

    async def _fetch_price(self, token_id: str, side: str) -> float:
        """获取单个 token 的 bid/ask 价格"""
        try:
            url = f"{self.CLOB_REST}/price"
            async with self._session.get(url, params={"token_id": token_id, "side": side}) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return float(data.get("price", 0))
        except Exception as e:
            logger.debug(f"{side} price 获取失败 [{token_id[:12]}]: {e}")
        return 0.0

    async def _poll_midpoints(self) -> None:
        """获取活跃市场的 YES/NO midpoint + bid/ask 价格并发布事件 (并发请求)"""
        mkt = self._active_market
        if not mkt or not mkt.token_ids or not self._session:
            return

        tokens = mkt.token_ids[:2]
        token_up = tokens[0] if len(tokens) >= 1 else None
        token_dn = tokens[1] if len(tokens) >= 2 else None

        # 构建并发任务列表: midpoint + bid + ask for each token
        tasks = []
        task_keys = []

        if token_up:
            tasks.extend([
                self._fetch_midpoint(token_up),
                self._fetch_price(token_up, "BUY"),
                self._fetch_price(token_up, "SELL"),
            ])
            task_keys.extend(["yes_mid", "yes_bid", "yes_ask"])

        if token_dn:
            tasks.extend([
                self._fetch_midpoint(token_dn),
                self._fetch_price(token_dn, "BUY"),
                self._fetch_price(token_dn, "SELL"),
            ])
            task_keys.extend(["no_mid", "no_bid", "no_ask"])

        # 并发执行所有请求 (原来串行 6 次 → 现在 1 次往返时间)
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # 解析结果
        price_map = {}
        for key, result in zip(task_keys, results):
            if isinstance(result, Exception):
                logger.debug(f"并发价格获取失败 [{key}]: {result}")
                price_map[key] = 0.0
            else:
                price_map[key] = result

        yes_price = price_map.get("yes_mid", 0.0)
        no_price = price_map.get("no_mid", 0.0)
        yes_bid = price_map.get("yes_bid", 0.0)
        yes_ask = price_map.get("yes_ask", 0.0)
        no_bid = price_map.get("no_bid", 0.0)
        no_ask = price_map.get("no_ask", 0.0)

        # 更新本地缓存
        if yes_price > 0:
            mkt.prices = [yes_price, no_price]

        # 发布 PM_PRICE 事件
        if self.event_bus and (yes_price > 0 or no_price > 0):
            _, _, secs_left = self._calc_window_ts()
            await self.event_bus.publish(Event(
                type=EventType.PM_PRICE,
                data={
                    "yes_price": yes_price,
                    "no_price": no_price,
                    "yes_bid": yes_bid,
                    "yes_ask": yes_ask,
                    "no_bid": no_bid,
                    "no_ask": no_ask,
                    "question": mkt.question,
                    "condition_id": mkt.condition_id,
                    "token_ids": list(mkt.token_ids),
                    "neg_risk": mkt.neg_risk,
                    "window_start_ts": mkt.window_start_ts,
                    "window_end_ts": mkt.window_end_ts,
                    "seconds_left": secs_left,
                    "end_date": mkt.end_date,
                },
                source="polymarket",
            ))

    # ────────────────── WebSocket Book 实时订单簿 ──────────────────

    async def _ws_book_loop(self) -> None:
        """
        WS Book 频道 — 实时推送订单簿 snapshot + 增量更新。

        Polymarket CLOB WS 协议:
          连接: wss://ws-subscriptions-clob.polymarket.com/ws/market
          订阅: {"type": "subscribe", "market": <token_id>, "channel": "book"}
          消息:
            - 首次连接后立即推送完整 snapshot (event_type="book")
            - 后续推送增量 price_change 事件 (event_type="price_change")
          格式:
            snapshot: {"event_type": "book", "market": "<token>",
                       "bids": [{"price": "0.55", "size": "100"}],
                       "asks": [{"price": "0.56", "size": "80"}],
                       "timestamp": "..."}
            delta:    {"event_type": "price_change", "market": "<token>",
                       "price_changes": [{"price": "0.55", "size": "0", "side": "BUY"},
                                          {"price": "0.57", "size": "50", "side": "SELL"}]}
        """
        reconnect_delay = 1.0
        max_reconnect_delay = 30.0

        while self._running:
            heartbeat_task = None
            timeout_task = None
            try:
                mkt = self._active_market
                if not mkt or not mkt.token_ids or not self._session:
                    await asyncio.sleep(5)
                    continue

                self._ws_book = await self._session.ws_connect(
                    self.CLOB_WS,
                    heartbeat=30,
                    timeout=aiohttp.ClientTimeout(total=15),
                )

                # 订阅 market 频道 (Polymarket 官方格式)
                # 一次订阅所有 token, 并启用 custom_feature_enabled 获取 best_bid_ask 事件
                subscribe_ids = mkt.token_ids[:2]
                await self._ws_book.send_json({
                    "type": "market",
                    "assets_ids": subscribe_ids,
                    "custom_feature_enabled": True,
                })
                self._ws_book_subscribed_ids = list(subscribe_ids)
                logger.debug(
                    f"PM WS Book 已订阅 {len(subscribe_ids)} 个 token: "
                    f"{[t[:20] for t in subscribe_ids]}"
                )

                reconnect_delay = 1.0  # 连接成功, 重置退避
                # 注意: 不在此处设置 ws_book_connected = True
                # 只有在实际收到第一条有效 book 数据后才标记
                # (防止 WS 连接成功但服务器不推送数据导致 REST 轮询被错误暂停)
                _got_first_data = False
                _connect_time = time.time()
                logger.info(
                    f"PM WS Book TCP 已连接, 等待首条 book 数据... "
                    f"(callbacks={list(k[:16] for k in self._book_callbacks)})"
                )

                # 启动心跳任务 (每 10s 发送 ping 保持连接)
                async def _heartbeat():
                    try:
                        while self._running and self._ws_book and not self._ws_book.closed:
                            await self._ws_book.ping()
                            await asyncio.sleep(10)
                    except Exception:
                        pass
                heartbeat_task = asyncio.create_task(_heartbeat())

                # 超时监控: 如果 15s 内未收到有效 book 数据, 对外关闭 WS 触发重连
                async def _timeout_watchdog():
                    await asyncio.sleep(15)
                    if not _got_first_data and self._ws_book and not self._ws_book.closed:
                        logger.warning(
                            "PM WS Book 15s 未收到有效 book 数据 — 断开重连"
                        )
                        await self._ws_book.close()
                timeout_task = asyncio.create_task(_timeout_watchdog())

                _msg_count = 0
                async for msg in self._ws_book:
                    if not self._running:
                        break

                    if msg.type == aiohttp.WSMsgType.TEXT:
                        if not msg.data or not msg.data.strip():
                            continue
                        try:
                            data = json.loads(msg.data)
                        except json.JSONDecodeError:
                            continue

                        # 诊断日志: 打印前 3 条原始消息帮助调试
                        _msg_count += 1
                        if _msg_count <= 3:
                            _preview = msg.data[:300] if len(msg.data) > 300 else msg.data
                            logger.info(
                                f"PM WS Book raw msg #{_msg_count}: {_preview}"
                            )

                        # Polymarket WS 可能推送单个 dict 或 list[dict]
                        if isinstance(data, list):
                            for item in data:
                                if isinstance(item, dict):
                                    self._handle_ws_book_message(item)
                        elif isinstance(data, dict):
                            self._handle_ws_book_message(data)
                        if not _got_first_data and self.ws_book_connected:
                            _got_first_data = True
                            timeout_task.cancel()  # 取消超时监控
                            logger.info(
                                "PM WS Book 首条数据已收到 — "
                                "订单簿实时推送已生效"
                            )
                    elif msg.type in (
                        aiohttp.WSMsgType.ERROR,
                        aiohttp.WSMsgType.CLOSED,
                    ):
                        logger.warning(f"PM WS Book 断开: {msg.type}")
                        break

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"PM WS Book 错误: {e}")
            finally:
                self.ws_book_connected = False
                # 取消心跳 + 超时任务
                for _t in (heartbeat_task, timeout_task):
                    if _t and not _t.done():
                        _t.cancel()

            if self._running:
                logger.info(
                    f"PM WS Book 将在 {reconnect_delay:.0f}s 后重连 "
                    f"(期间 REST 轮询降级生效)"
                )
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)

    def _handle_ws_book_message(self, data: dict[str, Any]) -> None:
        """
        处理 WS Book 消息 — snapshot 或 price_change 增量。

        将解析后的数据转发给已注册的 book_callback (由 main.py 注入)。
        """
        event_type = data.get("event_type", data.get("type", ""))
        # WS "market" 字段是 condition_id (0x...), "asset_id" 才是 token_id
        # _book_callbacks 的 key 是 token_id, 所以优先用 asset_id
        token_id = data.get("asset_id", "") or data.get("market", "")

        if not token_id or token_id not in self._book_callbacks:
            # 诊断: 打印为什么被过滤
            if not token_id:
                logger.debug(
                    f"WS Book msg 无 token_id, keys={list(data.keys())[:8]}, "
                    f"event={event_type}"
                )
            elif token_id not in self._book_callbacks:
                logger.debug(
                    f"WS Book token_id 未注册: {token_id[:20]}..., "
                    f"registered={[k[:16] for k in self._book_callbacks]}, "
                    f"event={event_type}"
                )
            return

        cb = self._book_callbacks[token_id]

        # 收到有效 book 数据后, 标记 WS Book 可用 (此时才暂停 REST 轮询)
        if not self.ws_book_connected:
            self.ws_book_connected = True

        if event_type == "book":
            # 完整 snapshot — 替换整个订单簿
            bids = data.get("bids", [])
            asks = data.get("asks", [])
            if bids or asks:
                cb(bids, asks, True)  # is_snapshot=True
                logger.debug(
                    f"WS Book snapshot [{token_id[:12]}]: "
                    f"{len(bids)} bids, {len(asks)} asks"
                )

        elif event_type == "price_change":
            # 增量更新 — 逐条应用
            # Polymarket 官方字段名: price_changes (非 changes)
            changes = data.get("price_changes", data.get("changes", []))
            if changes:
                cb(changes, [], False)  # is_snapshot=False, changes in bids arg
                logger.debug(
                    f"WS Book delta [{token_id[:12]}]: "
                    f"{len(changes)} changes"
                )

    async def _resubscribe_book(self) -> None:
        """窗口轮换后重新订阅新 token 的 book 频道 (Polymarket 动态订阅格式)。"""
        if not self._ws_book or self._ws_book.closed:
            return
        mkt = self._active_market
        if not mkt or not mkt.token_ids:
            return
        new_ids = mkt.token_ids[:2]
        old_ids = getattr(self, '_ws_book_subscribed_ids', [])

        try:
            # 先取消订阅旧 token
            if old_ids:
                await self._ws_book.send_json({
                    "assets_ids": old_ids,
                    "operation": "unsubscribe",
                })
                logger.debug(f"PM WS Book 取消订阅: {[t[:20] for t in old_ids]}")

            # 订阅新 token
            await self._ws_book.send_json({
                "assets_ids": new_ids,
                "operation": "subscribe",
            })
            self._ws_book_subscribed_ids = list(new_ids)
            # 新窗口需要重新等待首条数据
            self.ws_book_connected = False
            logger.debug(f"PM WS Book 重新订阅: {[t[:20] for t in new_ids]}")
        except Exception as e:
            logger.warning(f"WS Book 重新订阅失败: {e}")

    # ────────────────── WebSocket 成交流 ──────────────────

    async def _ws_stream_loop(self) -> None:
        """WS 实时成交流 (可选增强)"""
        while self._running:
            try:
                mkt = self._active_market
                if not mkt or not self._session:
                    await asyncio.sleep(10)
                    continue

                self._ws = await self._session.ws_connect(self.CLOB_WS)
                # 订阅活跃市场的 token
                for token_id in mkt.token_ids:
                    await self._ws.send_json({
                        "type": "subscribe",
                        "market": token_id,
                        "channel": "live-activity",
                    })
                    logger.debug(f"PM WS 已订阅: {token_id[:30]}...")

                async for msg in self._ws:
                    if not self._running:
                        break
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        if not msg.data or not msg.data.strip():
                            continue  # 空消息 (CLOB WS 常见)
                        try:
                            data = json.loads(msg.data)
                        except json.JSONDecodeError:
                            continue
                        await self._handle_ws_message(data)
                    elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                        logger.warning(f"PM WS 断开: {msg.type}")
                        break

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"PM WS 流错误: {e}")
                if self._running:
                    await asyncio.sleep(5)

    async def _subscribe_markets(self, market_ids: list[str]) -> None:
        """发送 WS 订阅消息"""
        if not self._ws:
            return
        for market_id in market_ids:
            await self._ws.send_json({
                "type": "subscribe",
                "market": market_id,
                "channel": "live-activity",
            })
            logger.debug(f"PM WS 已订阅: {market_id[:30]}...")

    async def _handle_ws_message(self, data: dict[str, Any]) -> None:
        """处理 WS 成交消息"""
        msg_type = data.get("event_type", data.get("type", ""))

        if msg_type == "trade" and self.event_bus:
            await self.event_bus.publish(Event(
                type=EventType.PM_TRADE,
                data={
                    "market_id": data.get("market", ""),
                    "price": float(data.get("price", 0)),
                    "size": float(data.get("size", 0)),
                    "side": data.get("side", ""),
                    "timestamp": time.time(),
                    "raw": data,
                },
                source="polymarket",
            ))

    # ────────────────── REST API ──────────────────

    async def get_market(self, condition_id: str) -> dict[str, Any]:
        """获取市场信息"""
        async with aiohttp.ClientSession() as session:
            url = f"{self.CLOB_REST}/markets/{condition_id}"
            async with session.get(url) as resp:
                return await resp.json()

    async def get_orderbook(self, token_id: str) -> dict[str, Any]:
        """获取订单簿（复用已有 session）"""
        session = self._session or aiohttp.ClientSession()
        close_after = self._session is None
        try:
            url = f"{self.CLOB_REST}/book"
            params = {"token_id": token_id}
            async with session.get(url, params=params) as resp:
                return await resp.json()
        finally:
            if close_after:
                await session.close()

    async def get_trades(
        self,
        condition_id: str,
        limit: int = 100,
    ) -> list[dict]:
        """获取历史成交"""
        async with aiohttp.ClientSession() as session:
            url = f"{self.CLOB_REST}/trades"
            params = {"condition_id": condition_id, "limit": limit}
            async with session.get(url, params=params) as resp:
                data = await resp.json()
                return data if isinstance(data, list) else data.get("trades", [])

    async def get_midpoint(self, token_id: str) -> float:
        """获取中间价"""
        async with aiohttp.ClientSession() as session:
            url = f"{self.CLOB_REST}/midpoint"
            params = {"token_id": token_id}
            async with session.get(url, params=params) as resp:
                data = await resp.json()
                return float(data.get("mid", 0))

    async def search_markets(self, query: str = "Bitcoin", active: bool = True) -> list[dict]:
        """搜索市场"""
        async with aiohttp.ClientSession() as session:
            url = f"{self.CLOB_REST}/markets"
            params = {"next_cursor": "MA=="}
            async with session.get(url, params=params) as resp:
                data = await resp.json()
                markets = data if isinstance(data, list) else data.get("data", [])
                return [
                    m for m in markets
                    if query.lower() in m.get("question", "").lower()
                    and (not active or m.get("active", False))
                ]

    async def get_prices_history(
        self,
        token_id: str,
        fidelity: int = 5,
        start_ts: int | None = None,
        end_ts: int | None = None,
    ) -> list[dict]:
        """获取价格历史时间序列"""
        async with aiohttp.ClientSession() as session:
            url = f"{self.CLOB_REST}/prices-history"
            params: dict[str, Any] = {
                "market": token_id,
                "interval": "max",
                "fidelity": fidelity,
            }
            if start_ts:
                params["startTs"] = start_ts
            if end_ts:
                params["endTs"] = end_ts
            async with session.get(url, params=params) as resp:
                data = await resp.json()
                return data if isinstance(data, list) else data.get("history", [])
