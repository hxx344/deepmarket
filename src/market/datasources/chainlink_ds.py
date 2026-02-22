"""
Chainlink DataSource - BTC/USD 价格数据源 (与 Polymarket 结算对齐)

Polymarket BTC 5-min 市场结算使用 Chainlink Data Streams BTC/USD。
本模块提供与 PM 结算一致的 BTC/USD 价格流。

数据源优先级 (auto 模式):
  1. PM RTDS WebSocket          — 免费! PM 自己转发的 Chainlink 价格, 无需任何 key
  2. Chainlink Data Streams API — 精确匹配 PM 结算, 需要 API key
  3. Binance BTC/USDT           — Data Streams 底层聚合源, 差异极小
  4. Chainlink 链上 Price Feed  — 最后兜底, 延迟 ~30s

参考:
  https://data.chain.link/streams/btc-usd
  https://docs.polymarket.com/developers/RTDS/RTDS-crypto-prices

环境变量:
  CHAINLINK_CLIENT_ID      — Data Streams API client ID (可选)
  CHAINLINK_CLIENT_SECRET  — Data Streams API client secret (可选)
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import os
import time
from dataclasses import dataclass, field
from typing import Any

import json

import aiohttp
from loguru import logger

from src.core.event_bus import Event, EventBus, EventType
from src.market.datasources.base import (
    DataSourceBase,
    DataSourceStatus,
    Kline,
    Tick,
)


# ================================================================
#  配置
# ================================================================

@dataclass
class ChainlinkConfig:
    """BTC/USD 数据源配置 (多级优先级)"""

    # ── PM RTDS WebSocket (免费! PM 转发的 Chainlink 价格) ──
    # 文档: https://docs.polymarket.com/developers/RTDS/RTDS-crypto-prices
    # topic "crypto_prices_chainlink" 推送与 PM 结算一致的 Chainlink BTC/USD
    rtds_ws: str = "wss://ws-live-data.polymarket.com"
    rtds_ping_interval: float = 5.0  # RTDS 要求 ~5s 发送一次 ping

    # ── Chainlink Data Streams API (需要 API 密钥, 与 PM 结算完全一致) ──
    # 申请: https://chain.link/data-streams
    # 赞助通道 (通过 PM): https://pm-ds-request.streams.chain.link/
    # 也可以通过环境变量: CHAINLINK_CLIENT_ID, CHAINLINK_CLIENT_SECRET
    api_base: str = "https://api.chain.link"
    client_id: str = ""
    client_secret: str = ""

    # ── BTC/USD Feed ID (Data Streams v0.3) ──
    feed_id: str = (
        "0x00027bbaff688c906a3e20a34fe951715d1018d262a5b66e38edd64e0b0c69"
    )

    # ── Binance 兜底 (免费, 与 Data Streams 差异极小) ──
    binance_ws: str = "wss://stream.binance.com:9443/ws/btcusdt@trade"
    binance_rest: str = "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT"

    # ── Chainlink 链上 Price Feed (最后兜底) ──
    polygon_rpc: str = "https://polygon-bor-rpc.publicnode.com"
    polygon_rpc_fallbacks: list[str] = field(default_factory=lambda: [
        "https://polygon-rpc.com",
        "https://rpc.ankr.com/polygon",
    ])
    price_feed_address: str = "0xc907E116054Ad103354f2D350FD2514433D57F6f"

    # ── 轮询与 K线 ──
    poll_interval: float = 2.0   # REST 轮询间隔 (秒)
    kline_interval: str = "5m"   # 聚合 K 线周期

    # ── 连接模式 ──
    # "auto"     = RTDS → Data Streams → Binance → 链上 (推荐)
    # "rtds"     = 仅 PM RTDS WebSocket
    # "streams"  = 仅 Chainlink Data Streams API
    # "binance"  = 仅 Binance
    # "onchain"  = 仅链上 Price Feed
    mode: str = "auto"


# ================================================================
#  数据源实现
# ================================================================

class ChainlinkDataSource(DataSourceBase):
    """
    BTC/USD 价格数据源 (与 Polymarket 结算对齐)

    四级优先级:
      1. PM RTDS WebSocket           — 免费! PM 转发的 Chainlink BTC/USD 价格
      2. Chainlink Data Streams API  — 与 PM 结算价格完全一致, 需 API key
      3. Binance BTC/USDT            — Data Streams 底层聚合源, 差异极小
      4. Chainlink 链上 Price Feed    — 最后手段, 有 ~30s 延迟

    默认 auto 模式下, 无需任何 API key 即可通过 PM RTDS 获取
    与结算一致的 Chainlink BTC/USD 实时价格。

    用法::

        ds = ChainlinkDataSource(event_bus=bus)  # 零配置!
        await ds.connect()
        await ds.start_streaming()
    """

    def __init__(
        self,
        event_bus: EventBus | None = None,
        config: ChainlinkConfig | None = None,
    ) -> None:
        super().__init__(event_bus)
        self.config = config or ChainlinkConfig()

        # 从环境变量读取凭证 (优先级: 显式配置 > 环境变量)
        if not self.config.client_id:
            self.config.client_id = os.environ.get("CHAINLINK_CLIENT_ID", "")
        if not self.config.client_secret:
            self.config.client_secret = os.environ.get("CHAINLINK_CLIENT_SECRET", "")

        self._session: aiohttp.ClientSession | None = None
        self._running = False

        # 当前活跃的价格源: "rtds" | "streams" | "binance" | "onchain"
        self._active_source: str = ""

        # K线聚合状态
        self._current_candle: dict[str, Any] | None = None
        self._candle_start: float = 0.0
        self._candle_interval_sec = self._parse_interval(self.config.kline_interval)

        # 最新价格缓存
        self._last_price: float = 0.0
        self._last_price_ts: float = 0.0

        # RPC 缓存 (链上兜底)
        self._last_good_rpc: str | None = None

    def name(self) -> str:
        return "chainlink"

    @property
    def active_source(self) -> str:
        """当前活跃的价格来源"""
        return self._active_source

    # ================================================================
    #  连接管理
    # ================================================================

    async def connect(self) -> None:
        """
        按优先级尝试连接价格源。

        auto 模式依次尝试: PM RTDS → Data Streams → Binance → 链上
        """
        self.status = DataSourceStatus.CONNECTING
        self._session = aiohttp.ClientSession()
        mode = self.config.mode

        # 1. 尝试 PM RTDS WebSocket (免费 Chainlink 价格)
        if mode in ("rtds", "auto"):
            try:
                price = await self._test_rtds_connection()
                self._active_source = "rtds"
                self._last_price = price
                self._last_price_ts = time.time()
                self.status = DataSourceStatus.CONNECTED
                logger.info(
                    f"[PM RTDS] 连接成功, Chainlink BTC/USD: ${price:,.2f} "
                    f"(免费, 与 PM 结算一致)"
                )
                return
            except Exception as e:
                logger.warning(f"PM RTDS 不可用: {e}")
                if mode == "rtds":
                    self.status = DataSourceStatus.ERROR
                    raise

        # 2. 尝试 Chainlink Data Streams API
        if mode in ("streams", "auto") and self.config.client_id:
            try:
                price, ts = await self._fetch_streams_price()
                self._active_source = "streams"
                self._last_price = price
                self._last_price_ts = ts
                self.status = DataSourceStatus.CONNECTED
                logger.info(
                    f"[Chainlink Data Streams] 连接成功, "
                    f"BTC/USD: ${price:,.2f} (与 PM 结算一致)"
                )
                return
            except Exception as e:
                logger.warning(f"Data Streams API 不可用: {e}")
                if mode == "streams":
                    self.status = DataSourceStatus.ERROR
                    raise

        # 3. 尝试 Binance
        if mode in ("binance", "auto"):
            try:
                price = await self._fetch_binance_rest()
                self._active_source = "binance"
                self._last_price = price
                self._last_price_ts = time.time()
                self.status = DataSourceStatus.CONNECTED
                logger.info(
                    f"[Binance 兜底] 连接成功, BTC/USDT: ${price:,.2f} "
                    f"(Data Streams 聚合源, 差异极小)"
                )
                return
            except Exception as e:
                logger.warning(f"Binance 不可用: {e}")
                if mode == "binance":
                    self.status = DataSourceStatus.ERROR
                    raise

        # 4. 尝试链上 Price Feed
        if mode in ("onchain", "auto"):
            try:
                price = await self._read_onchain_price()
                self._active_source = "onchain"
                self._last_price = price
                self._last_price_ts = time.time()
                self.status = DataSourceStatus.CONNECTED
                logger.info(
                    f"[Chainlink 链上] 连接成功, BTC/USD: ${price:,.2f} "
                    f"(注意: 延迟 ~30s, 与 Data Streams 可能有小差异)"
                )
                return
            except Exception as e:
                logger.error(f"链上 Price Feed 失败: {e}")

        self.status = DataSourceStatus.ERROR
        raise ConnectionError("所有价格源均不可用")

    async def disconnect(self) -> None:
        self._running = False
        if self._session:
            await self._session.close()
            self._session = None
        self.status = DataSourceStatus.DISCONNECTED
        logger.info(f"数据源已断开 (source={self._active_source})")

    async def subscribe(self, symbols: list[str], channels: list[str]) -> None:
        """接口兼容, BTC/USD 为唯一标的"""
        pass

    # ================================================================
    #  数据流主循环
    # ================================================================

    async def start_streaming(self) -> None:
        """启动价格轮询/推送循环"""
        self._running = True
        logger.info(
            f"BTC/USD 数据流启动 "
            f"(source={self._active_source}, "
            f"poll={self.config.poll_interval}s, "
            f"kline={self.config.kline_interval})"
        )

        # RTDS 和 Binance 使用 WebSocket 推送
        if self._active_source == "rtds":
            await self._stream_rtds_ws()
        elif self._active_source == "binance":
            await self._stream_binance_ws()
        else:
            await self._poll_price_loop()

    async def _poll_price_loop(self) -> None:
        """轮询方式获取价格 (Data Streams / 链上)"""
        consecutive_errors = 0
        while self._running:
            try:
                price, timestamp = await self._fetch_price_by_source()
                if price > 0:
                    await self._on_price_update(price, timestamp)
                    consecutive_errors = 0
            except Exception as e:
                consecutive_errors += 1
                logger.error(
                    f"价格获取失败 ({self._active_source}): {e} "
                    f"(连续 {consecutive_errors} 次)"
                )
                # 连续 3 次失败, 尝试自动降级
                if consecutive_errors >= 3 and self.config.mode == "auto":
                    logger.warning(
                        f"{self._active_source} 连续失败, 尝试降级..."
                    )
                    if await self._try_fallback():
                        return

            await asyncio.sleep(self.config.poll_interval)

    async def _stream_binance_ws(self) -> None:
        """通过 Binance WebSocket 接收 BTC/USDT 实时成交价"""
        while self._running:
            try:
                async with self._session.ws_connect(
                    self.config.binance_ws,
                    heartbeat=30,
                ) as ws:
                    logger.debug("Binance WebSocket 已连接")
                    async for msg in ws:
                        if not self._running:
                            break
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            data = msg.json()
                            price = float(data.get("p", 0))
                            ts = data.get("T", time.time() * 1000) / 1000.0
                            if price > 0:
                                await self._on_price_update(price, ts)
                        elif msg.type in (
                            aiohttp.WSMsgType.ERROR,
                            aiohttp.WSMsgType.CLOSED,
                        ):
                            break
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"Binance WS 断开: {e}, 5s 后重连...")
                await asyncio.sleep(5)

    async def _try_fallback(self) -> bool:
        """尝试降级到下一个可用数据源, 成功返回 True"""
        fallback_order = ["binance", "onchain"]
        for source in fallback_order:
            if source == self._active_source:
                continue
            try:
                if source == "binance":
                    price = await self._fetch_binance_rest()
                    self._active_source = "binance"
                    logger.info(f"已降级到 Binance, BTC/USDT: ${price:,.2f}")
                    await self._on_price_update(price, time.time())
                    await self._stream_binance_ws()
                    return True
                elif source == "onchain":
                    price = await self._read_onchain_price()
                    self._active_source = "onchain"
                    logger.info(f"已降级到链上, BTC/USD: ${price:,.2f}")
                    await self._on_price_update(price, time.time())
                    return False  # 继续 poll loop
            except Exception as e:
                logger.warning(f"{source} 降级失败: {e}")
        return False

    async def _fetch_price_by_source(self) -> tuple[float, float]:
        """根据当前 active_source 获取价格"""
        if self._active_source == "streams":
            return await self._fetch_streams_price()
        elif self._active_source == "binance":
            price = await self._fetch_binance_rest()
            return price, time.time()
        else:
            price = await self._read_onchain_price()
            return price, time.time()

    # ================================================================
    #  PM RTDS WebSocket (免费 Chainlink 价格)
    # ================================================================

    def _parse_rtds_price(
        self, raw: str, symbol_filter: str = "btc/usd"
    ) -> list[tuple[float, float]]:
        """
        解析 RTDS 消息, 返回 [(price, ts_sec), ...] 列表。

        RTDS 消息格式:
          1) 订阅确认 (type=subscribe): {"payload":{"data":[...], "symbol":"btc/usd"}} — 含历史快照
          2) 实时更新 (type=update):   {"payload":{"value":price, "symbol":"btc/usd", ...}} — 每秒推送
          3) 空字符串: 连接确认, 跳过

        注意: 不带 filters 订阅时, 会收到所有币种 (btc/eth/sol/xrp),
              需通过 payload.symbol 过滤。
        """
        if not raw or not raw.strip():
            return []
        try:
            obj = json.loads(raw)
        except json.JSONDecodeError:
            logger.debug(f"RTDS 非 JSON 消息, 跳过: {raw[:120]}")
            return []

        results: list[tuple[float, float]] = []
        payload = obj.get("payload", {})
        if not isinstance(payload, dict):
            return []

        msg_type = obj.get("type", "")

        # ── 格式 1: type=update, 单值实时推送 (每秒 ~1 条/币种) ──
        if msg_type == "update":
            sym = payload.get("symbol", "").lower()
            if symbol_filter and sym != symbol_filter:
                return []  # 非目标币种, 跳过
            price = float(payload.get("value", 0))
            if price > 0:
                ts = payload.get("timestamp", time.time() * 1000)
                if ts > 1e12:
                    ts = ts / 1000.0
                results.append((price, ts))
            return results

        # ── 格式 2: type=subscribe, 历史快照 (payload.data 数组) ──
        if msg_type == "subscribe":
            sym = payload.get("symbol", "").lower()
            if symbol_filter and sym != symbol_filter:
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

        # ── 兼容: 其他未知格式, 尝试提取 value ──
        price = float(payload.get("value", 0))
        if price > 0:
            sym = payload.get("symbol", "").lower()
            if symbol_filter and sym and sym != symbol_filter:
                return []
            ts = payload.get("timestamp", time.time() * 1000)
            if ts > 1e12:
                ts = ts / 1000.0
            results.append((price, ts))
        return results

    async def _test_rtds_connection(self) -> float:
        """
        测试 PM RTDS WebSocket 连通性, 返回首个 BTC/USD 价格。

        订阅 topic: crypto_prices_chainlink (不带 filters 以获取流式推送)
        用 symbol_filter 在客户端过滤 btc/usd。
        """
        try:
            async with self._session.ws_connect(
                self.config.rtds_ws,
                heartbeat=self.config.rtds_ping_interval,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as ws:
                # 不带 filters 订阅全部币种
                await ws.send_json({
                    "action": "subscribe",
                    "subscriptions": [
                        {
                            "topic": "crypto_prices_chainlink",
                            "type": "*",
                        }
                    ],
                })

                # 等待首个 BTC 价格 (最多 10s)
                start = time.time()
                async for msg in ws:
                    if time.time() - start > 10:
                        break
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        prices = self._parse_rtds_price(msg.data)
                        if prices:
                            return prices[-1][0]  # 返回最新价格
                    elif msg.type in (
                        aiohttp.WSMsgType.ERROR,
                        aiohttp.WSMsgType.CLOSED,
                    ):
                        break
        except Exception as e:
            raise ConnectionError(f"RTDS 连接失败: {e}") from e

        raise ConnectionError("RTDS 未在 10s 内返回 BTC/USD 价格")

    async def _stream_rtds_ws(self) -> None:
        """
        通过 PM RTDS WebSocket 接收 Chainlink BTC/USD 实时价格。

        topic: crypto_prices_chainlink (不加 filters 订阅全部币种以获取流式推送)
        消息格式:
          1) 空字符串 → 连接确认
          2) type=subscribe → 历史快照 payload.data[{timestamp, value}]
          3) type=update   → 实时推送 payload.{symbol, value, timestamp} (每秒/每币种)

        客户端通过 _parse_rtds_price(symbol_filter="btc/usd") 过滤非 BTC 消息。
        看门狗: 30s 无 BTC 价格数据则断开重连 (正常情况每秒有 ~1 条 BTC 推送)。
        """
        rtds_fail_count = 0
        max_rtds_fails = 3  # 连续失败 N 次后降级
        silence_timeout = 30.0  # 现在有每秒推送, 30s 无数据就是真的断了

        while self._running:
            try:
                async with self._session.ws_connect(
                    self.config.rtds_ws,
                    heartbeat=self.config.rtds_ping_interval,
                ) as ws:
                    # 订阅 Chainlink 全部币种（不加 filters 才能收到流式推送）
                    # 客户端通过 _parse_rtds_price() 的 symbol_filter 过滤 btc/usd
                    await ws.send_json({
                        "action": "subscribe",
                        "subscriptions": [
                            {
                                "topic": "crypto_prices_chainlink",
                                "type": "*",
                            }
                        ],
                    })
                    logger.debug("PM RTDS WebSocket 已连接, 订阅 crypto_prices_chainlink (无filter, 客户端过滤btc/usd)")
                    rtds_fail_count = 0  # 连接成功, 重置计数

                    # 数据静默看门狗: 跟踪最后一次收到 BTC 价格的时间
                    last_data_time = time.time()

                    async def _rtds_data_watchdog():
                        """
                        RTDS 正常时每秒推送 ~1 条 BTC 价格。
                        30s 无数据说明连接异常, 主动断开重连。
                        """
                        nonlocal last_data_time
                        while self._running:
                            await asyncio.sleep(10)
                            silence = time.time() - last_data_time
                            if silence > silence_timeout:
                                logger.warning(
                                    f"RTDS 数据静默 {silence:.0f}s > "
                                    f"{silence_timeout:.0f}s, 主动断开重连..."
                                )
                                await ws.close()
                                return

                    watchdog_task = asyncio.create_task(_rtds_data_watchdog())

                    try:
                        async for msg in ws:
                            if not self._running:
                                break
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                prices = self._parse_rtds_price(msg.data)
                                if prices:
                                    last_data_time = time.time()
                                for price, ts in prices:
                                    await self._on_price_update(price, ts)
                            elif msg.type in (
                                aiohttp.WSMsgType.ERROR,
                                aiohttp.WSMsgType.CLOSED,
                            ):
                                break
                    finally:
                        watchdog_task.cancel()
                        try:
                            await watchdog_task
                        except asyncio.CancelledError:
                            pass

                    # 正常退出循环 = WS 被关闭, 需要重连
                    if self._running:
                        logger.info("RTDS WS 连接已断开, 2s 后重连...")
                        await asyncio.sleep(2)

            except asyncio.CancelledError:
                break
            except Exception as e:
                rtds_fail_count += 1
                logger.warning(
                    f"PM RTDS 断开: {e}, "
                    f"连续失败 {rtds_fail_count}/{max_rtds_fails}, "
                    f"5s 后重连..."
                )
                # auto 模式下如果 RTDS 持续失败, 降级到 Binance
                if self.config.mode == "auto" and rtds_fail_count >= max_rtds_fails:
                    logger.warning("RTDS 连续失败, 尝试降级...")
                    if await self._try_fallback():
                        return
                await asyncio.sleep(5)

    # ================================================================
    #  Chainlink Data Streams API (与 PM 结算一致)
    # ================================================================

    async def _fetch_streams_price(self) -> tuple[float, float]:
        """
        从 Chainlink Data Streams REST API 获取最新 BTC/USD 报告。

        这是 PM 结算直接引用的数据源。
        API: https://docs.chain.link/data-streams
        """
        path = "/api/v1/reports/latest"
        url = f"{self.config.api_base}{path}"
        params = {"feedID": self.config.feed_id}
        headers = self._make_hmac_headers("GET", path)

        async with self._session.get(
            url,
            params=params,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=10),
        ) as resp:
            if resp.status == 401:
                raise ConnectionError(
                    "Data Streams 认证失败, 请检查 client_id / client_secret"
                )
            resp.raise_for_status()
            data = await resp.json()

        report = data.get("report", data)

        # Data Streams v0.3: benchmarkPrice 可能是 18 位精度整数字符串
        raw_price = report.get("benchmarkPrice", report.get("price", "0"))
        price = float(raw_price)

        # 根据数值大小判断精度
        if price > 1e15:
            price = price / 1e18   # 18 位精度 (wei)
        elif price > 1e6:
            price = price / 1e8    # 8 位精度 (与链上一致)

        timestamp = float(report.get("observationsTimestamp", time.time()))
        return price, timestamp

    def _make_hmac_headers(self, method: str, path: str) -> dict[str, str]:
        """
        构造 Chainlink Data Streams HMAC-SHA256 认证头。

        签名消息格式 (空格分隔):
          "{method} {path} {sha256(body)} {client_id} {timestamp}"

        Header:
          Authorization: hmac {client_id}:{signature}
          X-Authorization-Timestamp: {timestamp}
          X-Authorization-Signature-SHA256: {signature}

        参考: github.com/smartcontractkit/data-streams-sdk
        """
        ts = str(int(time.time()))
        body = b""  # GET 请求无 body

        # body 的 SHA-256 哈希 (空字节串)
        body_hash = hashlib.sha256(body).hexdigest()

        # 签名消息
        sign_msg = f"{method} {path} {body_hash} {self.config.client_id} {ts}"
        signature = hmac.new(
            self.config.client_secret.encode("utf-8"),
            sign_msg.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

        return {
            "Authorization": f"hmac {self.config.client_id}:{signature}",
            "X-Authorization-Timestamp": ts,
            "X-Authorization-Signature-SHA256": signature,
        }

    # ================================================================
    #  Binance 兜底
    # ================================================================

    async def _fetch_binance_rest(self) -> float:
        """从 Binance REST API 获取 BTC/USDT 最新价格"""
        async with self._session.get(
            self.config.binance_rest,
            timeout=aiohttp.ClientTimeout(total=5),
        ) as resp:
            resp.raise_for_status()
            data = await resp.json()
        return float(data["price"])

    # ================================================================
    #  Chainlink 链上 Price Feed (最后兜底)
    # ================================================================

    async def _read_onchain_price(self) -> float:
        """
        Polygon JSON-RPC 读取 Chainlink BTC/USD Aggregator。

        调用 latestRoundData() → answer (8 位小数精度)
        """
        call_data = "0xfeaf968c"  # latestRoundData() selector

        payload = {
            "jsonrpc": "2.0",
            "method": "eth_call",
            "params": [
                {"to": self.config.price_feed_address, "data": call_data},
                "latest",
            ],
            "id": 1,
        }

        # 优先使用上次成功的 RPC
        rpcs = [self.config.polygon_rpc] + self.config.polygon_rpc_fallbacks
        if self._last_good_rpc and self._last_good_rpc in rpcs:
            rpcs.remove(self._last_good_rpc)
            rpcs.insert(0, self._last_good_rpc)

        last_err = None
        for rpc in rpcs:
            try:
                async with self._session.post(
                    rpc,
                    json=payload,
                    headers={"Content-Type": "application/json"},
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    resp.raise_for_status()
                    result = await resp.json()

                if "error" in result:
                    raise ValueError(f"RPC error: {result['error']}")

                hex_result = result.get("result", "0x")
                if hex_result == "0x" or len(hex_result) < 66:
                    raise ValueError(f"无效返回: {hex_result[:60]}")

                # answer = 第二个返回值 (offset 32-64 bytes)
                answer = int(hex_result[2 + 64: 2 + 128], 16)
                if answer >= 2**255:
                    answer -= 2**256

                price = answer / 1e8  # 8 位小数
                if price > 0:
                    self._last_good_rpc = rpc
                    return price
                raise ValueError(f"价格异常: {price}")

            except Exception as e:
                last_err = e
                logger.debug(f"RPC {rpc} 失败: {e}")
                continue

        raise ConnectionError(f"所有 Polygon RPC 均失败: {last_err}")

    # ================================================================
    #  价格处理与 K线聚合
    # ================================================================

    async def _on_price_update(self, price: float, timestamp: float) -> None:
        """处理价格更新: 发布事件 + K线聚合"""
        self._last_price = price
        self._last_price_ts = timestamp

        tick = Tick(
            symbol="BTC/USD",
            price=price,
            timestamp=timestamp,
            source=f"chainlink:{self._active_source}",
        )

        if self.event_bus:
            # Chainlink 专用事件 (包含数据源层级信息)
            await self.event_bus.publish(Event(
                type=EventType.CHAINLINK_PRICE,
                data={
                    "tick": tick,
                    "price": price,
                    "source": self._active_source,
                    "timestamp": timestamp,
                },
                source="chainlink",
            ))
            # 通用行情 tick (供聚合器消费)
            await self.event_bus.publish(Event(
                type=EventType.MARKET_TICK,
                data={"tick": tick, "source": "chainlink"},
                source="chainlink",
            ))

        # K线聚合
        self._aggregate_kline(price, timestamp)

    def _aggregate_kline(self, price: float, timestamp: float) -> None:
        """tick → K线聚合, 闭合时发布事件"""
        candle_start = (
            int(timestamp) // self._candle_interval_sec * self._candle_interval_sec
        )

        if self._current_candle is None or candle_start > self._candle_start:
            # 闭合旧 K线
            if self._current_candle is not None:
                self._emit_kline(closed=True)
            # 新 K线
            self._candle_start = candle_start
            self._current_candle = {
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume": 0.0,
                "timestamp": candle_start,
            }
        else:
            self._current_candle["high"] = max(self._current_candle["high"], price)
            self._current_candle["low"] = min(self._current_candle["low"], price)
            self._current_candle["close"] = price

    def _emit_kline(self, closed: bool = False) -> None:
        """发布 K线事件"""
        if not self._current_candle or not self.event_bus:
            return

        c = self._current_candle
        kline = Kline(
            symbol="BTC/USD",
            interval=self.config.kline_interval,
            open=c["open"],
            high=c["high"],
            low=c["low"],
            close=c["close"],
            volume=c["volume"],
            timestamp=c["timestamp"],
            closed=closed,
            source=f"chainlink:{self._active_source}",
        )

        asyncio.get_event_loop().create_task(
            self.event_bus.publish(Event(
                type=EventType.MARKET_KLINE,
                data={"kline": kline, "source": "chainlink"},
                source="chainlink",
            ))
        )

    # ================================================================
    #  公开 API
    # ================================================================

    async def get_current_price(self) -> float:
        """获取当前 BTC/USD 价格 (带缓存)"""
        if self._last_price > 0 and (time.time() - self._last_price_ts) < 30:
            return self._last_price

        if not self._session:
            self._session = aiohttp.ClientSession()
        try:
            price, _ = await self._fetch_price_by_source()
            self._last_price = price
            self._last_price_ts = time.time()
            return price
        except Exception as e:
            logger.error(f"获取 BTC/USD 价格失败: {e}")
            return self._last_price  # 过期缓存总比 0 好

    async def fetch_price_history(
        self,
        start_ts: int | None = None,
        end_ts: int | None = None,
    ) -> list[dict]:
        """获取历史价格 (仅 Data Streams 模式支持)"""
        if self._active_source != "streams":
            logger.warning(
                f"当前数据源 ({self._active_source}) 不支持历史查询, "
                f"需要 Data Streams API"
            )
            return []

        path = "/api/v1/reports/bulk"
        url = f"{self.config.api_base}{path}"
        params: dict[str, Any] = {"feedID": self.config.feed_id}
        if start_ts:
            params["startTimestamp"] = start_ts
        if end_ts:
            params["endTimestamp"] = end_ts

        headers = self._make_hmac_headers("GET", path)

        async with self._session.get(url, params=params, headers=headers) as resp:
            if resp.status != 200:
                return []
            data = await resp.json()

        reports = data if isinstance(data, list) else data.get("reports", [])
        history = []
        for r in reports:
            p = float(r.get("benchmarkPrice", r.get("price", 0)))
            if p > 1e15:
                p /= 1e18
            history.append({
                "timestamp": float(r.get("observationsTimestamp", 0)),
                "price": p,
            })
        return history

    # ================================================================
    #  工具
    # ================================================================

    @staticmethod
    def _parse_interval(interval: str) -> int:
        """'5m' → 300, '1h' → 3600"""
        unit = interval[-1]
        value = int(interval[:-1])
        return value * {"s": 1, "m": 60, "h": 3600, "d": 86400}.get(unit, 60)
