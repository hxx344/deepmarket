"""
CLOB API Client - Polymarket CLOB 交易接口

封装 py-clob-client SDK，提供:
- 自动 API Key 创建 / 派生 (Level 2 认证)
- 异步下单 / 撤单 / 查询
- 订单状态轮询
- 错误重试与日志
"""

from __future__ import annotations

import asyncio
import os
import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from loguru import logger

# Polymarket CLOB SDK
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import (
    ApiCreds,
    OrderArgs,
    OrderType as ClobOrderType,
    PartialCreateOrderOptions,
    TickSize,
)

# Polymarket 代理钱包签名类型
SIG_EOA = 0           # 直接用 EOA 钱包交易
SIG_POLY_PROXY = 1    # 通过 Polymarket 工厂部署的 Proxy Wallet 交易
SIG_GNOSIS_SAFE = 2   # Gnosis Safe 钱包 (非工厂部署的代理钱包)
POLYGON_CHAIN_ID = 137

# 默认 CLOB 服务器
DEFAULT_CLOB_HOST = "https://clob.polymarket.com"

# API Creds 本地缓存路径
_CREDS_CACHE = Path("data/.clob_creds.json")


@dataclass
class ClobOrderResult:
    """CLOB 下单结果"""
    success: bool
    order_id: str = ""
    error: str = ""
    raw: dict[str, Any] | None = None


@dataclass
class ClobOrderStatus:
    """CLOB 订单状态"""
    order_id: str
    status: str = ""        # LIVE, MATCHED, CANCELLED, ...
    size_matched: float = 0.0
    price: float = 0.0
    raw: dict[str, Any] | None = None


class ClobApiClient:
    """
    Polymarket CLOB 交易客户端

    使用流程:
        client = ClobApiClient(private_key=key)
        await client.initialize()
        result = await client.place_order(token_id, price, size, side)
        status = await client.get_order_status(result.order_id)
        await client.cancel_order(result.order_id)
    """

    def __init__(
        self,
        private_key: str,
        host: str = "",
        chain_id: int = POLYGON_CHAIN_ID,
        funder: str = "",
        signature_type: int | None = None,
    ) -> None:
        self._private_key = private_key
        self._host = host or os.environ.get("CLOB_HOST", DEFAULT_CLOB_HOST)
        self._chain_id = chain_id
        self._funder = funder or os.environ.get("PM_PROXY_ADDRESS", "")
        # 签名类型: 有 proxy 地址 → GNOSIS_SAFE(2), 否则 EOA(0)
        # 注意: POLY_PROXY(1) 仅适用于通过 Polymarket 工厂部署的代理, 大部分情况用 GNOSIS_SAFE(2)
        if signature_type is not None:
            self._sig_type = signature_type
        else:
            self._sig_type = SIG_GNOSIS_SAFE if self._funder else SIG_EOA
        self._client: ClobClient | None = None
        self._creds: ApiCreds | None = None
        self._initialized = False

    # ----------------------------------------------------------
    #  初始化
    # ----------------------------------------------------------
    async def initialize(self) -> bool:
        """
        初始化 CLOB 客户端 (Level 2 认证)

        1. 创建 Level-1 client (private key)
        2. 尝试加载或派生 API Key → Level-2
        3. 如失败则创建新 API Key
        """
        try:
            # Level 1：私钥签名
            self._client = ClobClient(
                host=self._host,
                chain_id=self._chain_id,
                key=self._private_key,
                signature_type=self._sig_type,
                funder=self._funder if self._funder else None,  # type: ignore[arg-type]
            )
            # 增加 SDK 内部 httpx 超时 (默认 5s 太短, 直接猴补丁模块级 _http_client)
            try:
                import httpx as _httpx
                import py_clob_client.http_helpers.helpers as _sdk_helpers
                old_timeout = getattr(_sdk_helpers._http_client, '_timeout', None)
                _sdk_helpers._http_client = _httpx.Client(timeout=30.0)
                logger.debug(f"SDK httpx timeout: {old_timeout} → 30s")
            except Exception as _te:
                logger.warning(f"无法设置 SDK httpx timeout: {_te}")
            sig_label = {0: "EOA", 1: "POLY_PROXY", 2: "GNOSIS_SAFE"}.get(self._sig_type, str(self._sig_type))
            logger.info(f"CLOB signature_type={sig_label}, funder={self._funder[:10]}..." if self._funder else f"CLOB signature_type={sig_label}")

            # 尝试加载缓存的 creds
            creds = self._load_cached_creds()
            if creds:
                self._client.set_api_creds(creds)
                self._creds = creds
                logger.info("CLOB API creds loaded from cache")
            else:
                # 尝试 derive (复用已有 key)
                try:
                    creds = await asyncio.to_thread(self._client.derive_api_key)
                except Exception as e:
                    logger.debug(f"derive_api_key failed (expected for first use): {e}")
                    creds = None

                if creds:
                    self._client.set_api_creds(creds)
                    self._creds = creds
                    self._save_cached_creds(creds)
                    logger.info("CLOB API key derived successfully")
                else:
                    # 创建新 key
                    try:
                        creds = await asyncio.to_thread(self._client.create_api_key)
                    except Exception as e:
                        logger.error(f"create_api_key failed: {e}")
                        creds = None

                    if not creds:
                        logger.error("Failed to create CLOB API key")
                        return False
                    self._client.set_api_creds(creds)
                    self._creds = creds
                    self._save_cached_creds(creds)
                    logger.info("CLOB API key created successfully")

            # --- 刷新服务端余额/授权缓存 ---
            try:
                from py_clob_client.clob_types import BalanceAllowanceParams
                await asyncio.to_thread(
                    self._client.update_balance_allowance,
                    BalanceAllowanceParams(asset_type="COLLATERAL", signature_type=self._sig_type),
                )
                logger.debug("CLOB balance/allowance cache refreshed")
            except Exception as e_ba:
                logger.debug(f"update_balance_allowance skipped: {e_ba}")

            self._initialized = True
            logger.info(f"CLOB client initialized (host={self._host})")
            return True

        except Exception as e:
            logger.error(f"CLOB initialization failed: {e}")
            return False

    async def warmup_cache(self, token_ids: list[str], neg_risk: bool = True) -> None:
        """
        预热 SDK 内部缓存 (tick_size / neg_risk), 避免首笔下单的隐式网络调用。

        SDK 的 create_order 内部会隐式调用 get_tick_size / get_neg_risk,
        每次约 100-200ms 网络开销。预热后这些调用直接命中内存缓存。
        """
        if not self.is_ready or not self._client:
            return

        for token_id in token_ids:
            try:
                # 预填充 neg_risk 缓存
                self._client._ClobClient__neg_risk[token_id] = neg_risk  # type: ignore[attr-defined]
                logger.debug(f"Cache warmup: neg_risk[{token_id[:12]}...] = {neg_risk}")
            except Exception as e:
                logger.debug(f"Cache warmup neg_risk failed: {e}")

            try:
                # 预热 tick_size: 触发 SDK 内部缓存填充
                await asyncio.to_thread(self._client.get_tick_size, token_id)
                logger.debug(f"Cache warmup: tick_size[{token_id[:12]}...] cached")
            except Exception as e:
                logger.debug(f"Cache warmup tick_size failed: {e}")

    @property
    def is_ready(self) -> bool:
        return self._initialized and self._client is not None

    # ----------------------------------------------------------
    #  下单
    # ----------------------------------------------------------
    async def place_order(
        self,
        token_id: str,
        price: float,
        size: float,
        side: str,                # "BUY" 或 "SELL"
        order_type: str = "FOK",  # FOK (Taker) / GTC / GTD
        tick_size: str = "0.01",
        neg_risk: bool = True,    # BTC 市场通常是 neg_risk
    ) -> ClobOrderResult:
        """
        提交限价单到 CLOB

        Args:
            token_id: 市场 token ID (YES 或 NO token)
            price: 价格 (0-1)
            size: 份数 (不是 USDC 金额, 是 token 数量)
            side: "BUY" 或 "SELL"
            order_type: GTC / FOK / GTD
            tick_size: 最小价格步长
            neg_risk: 是否为负风险市场 (大多数 BTC 市场是)

        Returns:
            ClobOrderResult
        """
        if not self.is_ready:
            return ClobOrderResult(success=False, error="CLOB client not initialized")

        try:
            order_args = OrderArgs(
                token_id=token_id,
                price=price,
                size=size,
                side=side,
            )

            options = PartialCreateOrderOptions(
                tick_size=tick_size,  # type: ignore[arg-type]
                neg_risk=neg_risk,
            )

            # 映射 order type
            clob_ot = {
                "GTC": ClobOrderType.GTC,
                "FOK": ClobOrderType.FOK,
                "GTD": ClobOrderType.GTD,
            }.get(order_type.upper(), ClobOrderType.GTC)

            assert self._client is not None

            # 预填充 SDK 内部缓存, 避免 create_order 中的隐式网络调用:
            # - neg_risk=False 因 falsy 会被 SDK 忽略, 导致额外 GET 请求
            # - tick_size / fee_rate 首次也需要网络获取
            # 这里直接写入缓存, 后续 create_order 直接命中
            self._client._ClobClient__neg_risk[token_id] = neg_risk  # type: ignore[attr-defined]

            # 全流程重试 (create_order + post_order)
            # create_order 内部的 get_tick_size / get_fee_rate 也走网络, 都可能超时
            max_retries = 3
            last_err: Exception | None = None
            resp = None

            for attempt in range(1, max_retries + 1):
                try:
                    # 构建签名订单 (含网络调用: tick_size, fee_rate 查询)
                    signed_order = await asyncio.to_thread(
                        self._client.create_order, order_args, options
                    )

                    # 提交到 CLOB
                    resp = await asyncio.to_thread(
                        self._client.post_order, signed_order, clob_ot  # type: ignore[arg-type]
                    )
                    last_err = None
                    break

                except Exception as attempt_e:
                    last_err = attempt_e
                    err_str = str(attempt_e)

                    # 400/401 业务错误 (invalid signature 等) 不重试
                    if "status_code=400" in err_str or "status_code=401" in err_str:
                        raise

                    # 网络级 "Request exception!" 可重试 (指数退避)
                    backoff = 1.0 * (2 ** (attempt - 1))  # 1s, 2s, 4s
                    logger.warning(
                        f"CLOB order attempt {attempt}/{max_retries} failed: {attempt_e} | "
                        f"retry in {backoff:.0f}s"
                    )
                    if attempt < max_retries:
                        await asyncio.sleep(backoff)

            if last_err is not None:
                raise last_err

            # 解析响应
            if isinstance(resp, dict):
                order_id = resp.get("orderID", "") or resp.get("id", "")
                if resp.get("success") is False or resp.get("errorMsg"):
                    err = resp.get("errorMsg", str(resp))
                    logger.error(f"CLOB order rejected: {err}")
                    return ClobOrderResult(
                        success=False, order_id=order_id, error=err, raw=resp,
                    )
                logger.info(
                    f"CLOB order posted: {side} {size}@{price} "
                    f"token={token_id[:10]}... → orderID={order_id}"
                )
                return ClobOrderResult(
                    success=True, order_id=order_id, raw=resp,
                )
            else:
                # 非 dict 响应 (可能是字符串)
                logger.warning(f"Unexpected CLOB response type: {type(resp)}: {resp}")
                return ClobOrderResult(
                    success=False, error=f"Unexpected response: {resp}",
                )

        except Exception as e:
            logger.error(f"CLOB place_order failed: {e}")
            return ClobOrderResult(success=False, error=str(e))

    # ----------------------------------------------------------
    #  查询订单状态
    # ----------------------------------------------------------
    async def get_order_status(self, order_id: str) -> ClobOrderStatus:
        """查询单个订单状态"""
        if not self.is_ready:
            return ClobOrderStatus(order_id=order_id, status="UNKNOWN")

        try:
            assert self._client is not None
            resp = await asyncio.to_thread(self._client.get_order, order_id)
            if isinstance(resp, dict):
                return ClobOrderStatus(
                    order_id=order_id,
                    status=resp.get("status", "UNKNOWN"),
                    size_matched=float(resp.get("size_matched", 0)),
                    price=float(resp.get("price", 0)),
                    raw=resp,
                )
            return ClobOrderStatus(order_id=order_id, status="UNKNOWN")
        except Exception as e:
            logger.error(f"CLOB get_order failed: {e}")
            return ClobOrderStatus(order_id=order_id, status="ERROR")

    async def poll_until_terminal(
        self,
        order_id: str,
        timeout: float = 30.0,
        interval: float = 1.0,
        immediate_first: bool = False,
    ) -> ClobOrderStatus:
        """
        轮询订单状态直到最终状态

        最终状态: MATCHED, CANCELLED
        非最终: LIVE (挂单中)

        Args:
            immediate_first: 若 True, 先立即查询一次再进入 interval 轮询
                             适用于 FOK 等几乎立即有结果的订单类型
        """
        deadline = time.monotonic() + timeout
        last_status = ClobOrderStatus(order_id=order_id)
        first_check = True

        while time.monotonic() < deadline:
            # FOK 优化: 首次查询零延迟, 后续按 interval 轮询
            if first_check and immediate_first:
                first_check = False
                # FOK 提交后极短时间内就有结果, 等 50ms 让服务端处理完
                await asyncio.sleep(0.05)
            else:
                await asyncio.sleep(interval)

            last_status = await self.get_order_status(order_id)
            st = last_status.status.upper()
            if st in ("MATCHED", "CANCELLED", "ERROR", "UNKNOWN"):
                return last_status

        logger.warning(f"Poll timeout for order {order_id}, last status={last_status.status}")
        return last_status

    # ----------------------------------------------------------
    #  撤单
    # ----------------------------------------------------------
    async def cancel_order(self, order_id: str) -> bool:
        """撤销单个订单"""
        if not self.is_ready:
            return False
        try:
            assert self._client is not None
            resp = await asyncio.to_thread(self._client.cancel, order_id)
            ok = False
            if isinstance(resp, dict):
                ok = resp.get("canceled") is not None or resp.get("success", False)
            logger.info(f"CLOB cancel order {order_id}: {'OK' if ok else resp}")
            return ok
        except Exception as e:
            logger.error(f"CLOB cancel failed: {e}")
            return False

    async def cancel_all_orders(self) -> bool:
        """撤销所有挂单"""
        if not self.is_ready:
            return False
        try:
            assert self._client is not None
            resp = await asyncio.to_thread(self._client.cancel_all)
            logger.info(f"CLOB cancel all orders: {resp}")
            return True
        except Exception as e:
            logger.error(f"CLOB cancel_all failed: {e}")
            return False

    # ----------------------------------------------------------
    #  辅助: Creds 缓存
    # ----------------------------------------------------------
    @staticmethod
    def _load_cached_creds() -> ApiCreds | None:
        try:
            if _CREDS_CACHE.exists():
                raw = json.loads(_CREDS_CACHE.read_text())
                return ApiCreds(
                    api_key=raw["api_key"],
                    api_secret=raw["api_secret"],
                    api_passphrase=raw["api_passphrase"],
                )
        except Exception:
            pass
        return None

    @staticmethod
    def _save_cached_creds(creds: ApiCreds) -> None:
        try:
            _CREDS_CACHE.parent.mkdir(parents=True, exist_ok=True)
            _CREDS_CACHE.write_text(json.dumps({
                "api_key": creds.api_key,
                "api_secret": creds.api_secret,
                "api_passphrase": creds.api_passphrase,
            }))
            logger.debug("CLOB API creds cached to disk")
        except Exception as e:
            logger.warning(f"Failed to cache CLOB creds: {e}")
