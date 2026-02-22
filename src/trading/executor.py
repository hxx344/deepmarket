"""
Order Executor - 订单执行器

将策略信号转化为 Polymarket 订单，并管理订单生命周期。
"""

from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, TYPE_CHECKING

from loguru import logger

from src.core.context import Context, TradingMode
from src.core.event_bus import Event, EventBus, EventType, get_event_bus
from src.core.storage import StorageManager

if TYPE_CHECKING:
    from src.trading.wallet import WalletManager
    from src.trading.clob_api import ClobApiClient


class OrderType(str, Enum):
    MARKET = "market"
    LIMIT = "limit"
    FOK = "fok"          # Fill-or-Kill
    GTC = "gtc"          # Good-Till-Cancel
    GTD = "gtd"          # Good-Till-Date


class Side(str, Enum):
    YES = "YES"
    NO = "NO"


class ExecutionStatus(str, Enum):
    PENDING = "pending"
    SUBMITTED = "submitted"
    FILLED = "filled"
    PARTIAL_FILL = "partial_fill"
    CANCELLED = "cancelled"
    REJECTED = "rejected"
    FAILED = "failed"


@dataclass
class OrderRequest:
    """订单请求"""
    id: str = ""
    market_id: str = ""
    token_id: str = ""
    side: Side = Side.YES
    order_type: OrderType = OrderType.LIMIT
    price: float = 0.0
    size: float = 0.0            # USDC amount
    strategy_id: str = ""
    meta: dict[str, Any] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)

    def __post_init__(self):
        if not self.id:
            self.id = str(uuid.uuid4())[:12]


@dataclass
class OrderResult:
    """订单执行结果"""
    order_id: str
    status: ExecutionStatus
    filled_price: float = 0.0
    filled_size: float = 0.0
    fee: float = 0.0
    tx_hash: str = ""
    error: str = ""
    timestamp: float = field(default_factory=time.time)


class OrderExecutor:
    """
    订单执行器

    - 风控预检 → 订单提交 → 状态追踪 → 结果回调
    - 支持全自动/半自动/模拟盘模式
    - 内置执行质量分析
    """

    def __init__(
        self,
        context: Context,
        event_bus: EventBus | None = None,
        storage: StorageManager | None = None,
        risk_checker: Callable[[OrderRequest], tuple[bool, str]] | None = None,
        wallet: WalletManager | None = None,
    ) -> None:
        self.context = context
        self.event_bus = event_bus or get_event_bus()
        self.storage = storage
        self.risk_checker = risk_checker
        self.wallet = wallet

        self._pending_orders: dict[str, OrderRequest] = {}
        self._order_history: list[OrderResult] = []
        self._clob: ClobApiClient | None = None   # 实盘 CLOB 客户端

        # 执行统计
        self._stats = {
            "submitted": 0,
            "filled": 0,
            "rejected": 0,
            "total_volume": 0.0,
            "total_fees": 0.0,
        }

    async def initialize(self, api_key: str = "", api_secret: str = "") -> None:
        """初始化 API 连接"""
        if self.context.is_live or self.context.trading_mode in (
            TradingMode.AUTO, TradingMode.SEMI_AUTO,
        ):
            if self.wallet and self.wallet.can_sign():
                from src.trading.clob_api import ClobApiClient
                import os
                # proxy_addr: Polymarket 代理钱包地址 (Gnosis Safe)
                # 如果设置了 PM_PROXY_ADDRESS → 使用 GNOSIS_SAFE 签名 (sig_type=2)
                # 如果没有 → 使用 EOA 直接签名 (sig_type=0)
                proxy_addr = os.environ.get("PM_PROXY_ADDRESS", "")
                self._clob = ClobApiClient(
                    private_key=self.wallet._private_key,
                    funder=proxy_addr,
                )
                ok = await self._clob.initialize()
                if ok:
                    logger.info("Executor → CLOB client ready (Level-2 auth)")
                else:
                    logger.error("CLOB client init failed — live orders will FAIL")
                    self._clob = None
            else:
                logger.warning("No wallet/signing capability — live orders disabled")
        logger.info(f"Executor initialized in {self.context.trading_mode.value} mode")

    async def submit_order(self, request: OrderRequest) -> OrderResult:
        """
        提交订单

        流程: 风控预检 → 模式判断 → 提交/模拟 → 结果通知

        Args:
            request: 订单请求

        Returns:
            OrderResult
        """
        logger.info(
            f"Order submit: {request.side.value} {request.size} USDC "
            f"@ {request.price} ({request.order_type.value})"
        )

        # 1. 风控预检
        if self.risk_checker:
            passed, reason = self.risk_checker(request)
            if not passed:
                result = OrderResult(
                    order_id=request.id,
                    status=ExecutionStatus.REJECTED,
                    error=f"Risk check failed: {reason}",
                )
                await self._on_order_result(result, request, request.meta)
                return result

        # 2. 根据交易模式执行
        if self.context.trading_mode == TradingMode.PAPER:
            result = await self._execute_paper(request)
        elif self.context.trading_mode == TradingMode.SEMI_AUTO:
            result = await self._execute_semi_auto(request)
        elif self.context.trading_mode == TradingMode.AUTO:
            result = await self._execute_live(request)
        else:
            # MANUAL 模式只记录信号
            logger.info(f"Manual mode - signal logged: {request}")
            result = OrderResult(
                order_id=request.id,
                status=ExecutionStatus.PENDING,
            )

        await self._on_order_result(result, request, request.meta)
        return result

    async def cancel_order(self, order_id: str) -> bool:
        """取消订单 (本地 + CLOB API)"""
        # 如果有 CLOB 客户端，先发 API 撤单
        if self._clob and self._clob.is_ready:
            api_ok = await self._clob.cancel_order(order_id)
            if not api_ok:
                logger.warning(f"CLOB cancel API failed for {order_id}, may already be filled/cancelled")

        if order_id in self._pending_orders:
            del self._pending_orders[order_id]
        logger.info(f"Order cancelled: {order_id}")
        await self.event_bus.publish(Event(
            type=EventType.ORDER_CANCELLED,
            data={"order_id": order_id},
            source="executor",
        ))
        return True

    async def cancel_all(self) -> int:
        """取消所有挂单 (CLOB + 本地)"""
        # 先通过 CLOB API 全部撤单
        if self._clob and self._clob.is_ready:
            await self._clob.cancel_all_orders()

        count = len(self._pending_orders)
        for order_id in list(self._pending_orders.keys()):
            await self.cancel_order(order_id)
        logger.warning(f"All orders cancelled ({count})")
        return count

    async def _execute_paper(self, request: OrderRequest) -> OrderResult:
        """模拟盘执行（不实际下单）"""
        await asyncio.sleep(0.05)  # 模拟延迟

        # 模拟成交
        fill_price = request.price if request.price > 0 else 0.50  # 默认中间价
        fee = request.size * 0.002  # 0.2% 手续费
        fill_shares = request.size / fill_price if fill_price > 0 else 0  # shares = USDC / price

        result = OrderResult(
            order_id=request.id,
            status=ExecutionStatus.FILLED,
            filled_price=fill_price,
            filled_size=fill_shares,  # 返回 shares 数量, 与 live 一致
            fee=fee,
        )

        self._stats["filled"] += 1
        self._stats["total_volume"] += request.size  # USDC volume
        self._stats["total_fees"] += fee

        logger.info(f"[PAPER] Filled: {request.side.value} {fill_shares:.2f} shares @ {fill_price} (${request.size:.2f})")
        return result

    async def _execute_semi_auto(self, request: OrderRequest) -> OrderResult:
        """半自动模式（记录信号，等待确认）"""
        self._pending_orders[request.id] = request
        logger.info(f"[SEMI-AUTO] Order pending confirmation: {request.id}")

        # 发布等待确认事件
        await self.event_bus.publish(Event(
            type=EventType.ORDER_CREATED,
            data={"order": request, "requires_confirmation": True},
            source="executor",
        ))

        return OrderResult(
            order_id=request.id,
            status=ExecutionStatus.PENDING,
        )

    async def confirm_order(self, order_id: str) -> OrderResult:
        """确认半自动订单"""
        request = self._pending_orders.pop(order_id, None)
        if not request:
            return OrderResult(order_id=order_id, status=ExecutionStatus.FAILED, error="Order not found")

        return await self._execute_live(request)

    async def _execute_live(self, request: OrderRequest) -> OrderResult:
        """
        实盘执行 — 通过 Polymarket CLOB API 下单

        流程:
        1. 校验 CLOB 客户端就绪
        2. 映射参数 (side/price/size → CLOB OrderArgs)
        3. 签名 + POST 到 CLOB
        4. 轮询订单状态 (FOK 立即返回; GTC 等待最多 30s)
        5. 返回 OrderResult
        """
        self._stats["submitted"] += 1

        # 0. 校验
        if not self._clob or not self._clob.is_ready:
            logger.error("[LIVE] CLOB client not ready — cannot execute live order")
            return OrderResult(
                order_id=request.id,
                status=ExecutionStatus.FAILED,
                error="CLOB client not initialized",
            )

        try:
            # 1. 映射 side → CLOB BUY/SELL
            #    策略给出 YES/NO，在 CLOB 中:
            #    - 买 YES token → BUY (使用 yes token_id)
            #    - 买 NO token  → BUY (使用 no token_id)
            #    token_id 已在 OrderRequest 中指定
            clob_side = "BUY"  # 默认买入
            if request.meta.get("clob_side"):
                clob_side = request.meta["clob_side"]  # 策略可直接指定

            # 2. 计算 size: 策略给的 size 是 USDC 金额，CLOB 需要份数
            #    份数 = USDC / price
            if request.price <= 0 or request.price >= 1:
                logger.error(f"[LIVE] Invalid price {request.price}, must be (0,1)")
                return OrderResult(
                    order_id=request.id,
                    status=ExecutionStatus.REJECTED,
                    error=f"Invalid price: {request.price}",
                )

            shares = request.size / request.price

            # Polymarket API 精度要求 (tick_size=0.01):
            #   - taker amount (shares): max 2 位小数
            #   - maker amount (shares * price, USDC): max 2 位小数
            # 例: 1.65 * 0.44 = 0.726 (3位) → 被 API 拒绝
            # 修正: 用整数算术保证 shares_cents * price_cents 能被 100 整除
            import math
            price_cents = round(request.price * 100)
            g = math.gcd(price_cents, 100)
            step = 100 // g  # shares_cents 必须是 step 的倍数
            shares_cents = math.floor(shares * 100)
            shares_cents = (shares_cents // step) * step
            shares = shares_cents / 100.0

            if shares <= 0:
                logger.error(
                    f"[LIVE] Cannot find valid share size: "
                    f"{request.size} USDC @ {request.price} → shares=0"
                )
                return OrderResult(
                    order_id=request.id,
                    status=ExecutionStatus.REJECTED,
                    error=f"Order too small after rounding: {request.size} USDC @ {request.price}",
                )

            # Polymarket FOK/marketable 最低 maker amount = $1.00
            maker_usdc = shares_cents * price_cents / 10000
            if maker_usdc < 1.0:
                logger.warning(
                    f"[LIVE] maker_usdc=${maker_usdc:.2f} < $1 minimum | "
                    f"shares={shares} @ {request.price}"
                )
                return OrderResult(
                    order_id=request.id,
                    status=ExecutionStatus.REJECTED,
                    error=f"Order below $1 minimum: ${maker_usdc:.2f}",
                )

            # 3. 映射 order type — 默认 FOK (Taker 吃单)
            ot_map = {
                OrderType.GTC: "GTC",
                OrderType.FOK: "FOK",
                OrderType.GTD: "GTD",
                OrderType.LIMIT: "GTC",
                OrderType.MARKET: "FOK",
            }
            clob_ot = ot_map.get(request.order_type, "FOK")

            # 4. 从 meta 获取市场属性
            tick_size = request.meta.get("tick_size", "0.01")
            neg_risk = request.meta.get("neg_risk", True)

            logger.info(
                f"[LIVE] Submitting: {clob_side} {shares:.2f} shares "
                f"@ {request.price} ({clob_ot}) token={request.token_id[:12]}..."
            )

            # 5. 提交到 CLOB
            clob_result = await self._clob.place_order(
                token_id=request.token_id,
                price=request.price,
                size=shares,
                side=clob_side,
                order_type=clob_ot,
                tick_size=tick_size,
                neg_risk=neg_risk,
            )

            if not clob_result.success:
                logger.error(f"[LIVE] CLOB order rejected: {clob_result.error}")
                return OrderResult(
                    order_id=request.id,
                    status=ExecutionStatus.REJECTED,
                    error=clob_result.error,
                )

            # CLOB 返回的 orderID 作为 tx_hash 保存
            clob_order_id = clob_result.order_id

            # 6. 轮询最终状态
            #    FOK/MARKET → Taker 吃单, 几乎立即有结果 (首次查询零延迟)
            #    GTC → Maker 挂单, 可能等较久
            if clob_ot in ("FOK", "FAK"):
                final = await self._clob.poll_until_terminal(
                    clob_order_id, timeout=5.0, interval=0.3,
                    immediate_first=True,
                )
            else:
                final = await self._clob.poll_until_terminal(
                    clob_order_id, timeout=30.0, interval=2.0,
                )

            # 7. 映射结果
            matched = final.size_matched
            fee = matched * request.price * 0.002  # 估算 0.2% 手续费

            if final.status.upper() == "MATCHED":
                status = ExecutionStatus.FILLED
                self._stats["filled"] += 1
                self._stats["total_volume"] += matched * request.price
                self._stats["total_fees"] += fee
            elif final.status.upper() == "LIVE":
                # 仍在挂单中 (超时后仍 LIVE)
                status = ExecutionStatus.SUBMITTED
                self._pending_orders[clob_order_id] = request
            elif final.status.upper() == "CANCELLED":
                status = ExecutionStatus.CANCELLED
            elif matched > 0:
                status = ExecutionStatus.PARTIAL_FILL
                self._pending_orders[clob_order_id] = request
            else:
                status = ExecutionStatus.FAILED

            result = OrderResult(
                order_id=request.id,
                status=status,
                filled_price=request.price,
                filled_size=matched,
                fee=fee,
                tx_hash=clob_order_id,
            )

            logger.info(
                f"[LIVE] Result: {status.value} | filled={matched:.2f} "
                f"| clob_id={clob_order_id}"
            )
            return result

        except Exception as e:
            logger.error(f"[LIVE] Order execution failed: {e}", exc_info=True)
            return OrderResult(
                order_id=request.id,
                status=ExecutionStatus.FAILED,
                error=str(e),
            )

    async def _on_order_result(
        self,
        result: OrderResult,
        request: OrderRequest = None,
        strategy_context: dict | None = None,
    ) -> None:
        """处理订单结果：记录、发布事件、写入 TradeLogger"""
        self._order_history.append(result)

        # 持久化 (原有 SQLite orders 表)
        if self.storage:
            self.storage.db.insert("orders", {
                "id": result.order_id,
                "strategy_id": request.strategy_id if request else "",
                "market_id": request.market_id if request else "",
                "side": request.side.value if request else "",
                "order_type": request.order_type.value if request else "",
                "price": result.filled_price,
                "size": request.size if request else result.filled_size,
                "filled_size": result.filled_size,
                "status": result.status.value,
                "created_at": request.created_at if request else result.timestamp,
                "updated_at": result.timestamp,
            })

        # ── TradeLogger: 记录完整交易日志 (含市场上下文) ──
        trade_logger = self.context.get("trade_logger") if self.context else None
        if trade_logger and request:
            try:
                trade_logger.log_order(
                    request=request,
                    result=result,
                    ctx=self.context,
                    strategy_context=strategy_context,
                )
            except Exception as e:
                logger.debug(f"TradeLogger log_order 失败: {e}")

        # 发布事件
        event_type_map = {
            ExecutionStatus.FILLED: EventType.ORDER_FILLED,
            ExecutionStatus.PARTIAL_FILL: EventType.ORDER_PARTIAL_FILL,
            ExecutionStatus.REJECTED: EventType.ORDER_REJECTED,
            ExecutionStatus.SUBMITTED: EventType.ORDER_SUBMITTED,
        }
        et = event_type_map.get(result.status)
        if et:
            await self.event_bus.publish(Event(
                type=et,
                data={"result": result},
                source="executor",
            ))

    def get_stats(self) -> dict:
        return dict(self._stats)

    def get_order_history(self, limit: int = 50) -> list[OrderResult]:
        return self._order_history[-limit:]
