"""
CTF Position Redeemer — 链上兑付 Polymarket 已结算头寸

在 5-min BTC 市场 resolve 后, 调用 ConditionalTokens.redeemPositions()
将赢方 CTF token 兑换回 USDC。

由于 CTF token 存放在 Gnosis Safe 代理钱包中, 需要通过 Safe 的
execTransaction 来执行 redeemPositions 调用。

使用流程:
    redeemer = CtfRedeemer(private_key, proxy_address)
    result = await redeemer.redeem(condition_id)
"""

from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from typing import Any

from loguru import logger

# ────────────── 合约地址 (Polygon Mainnet) ──────────────

USDC = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
CTF = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
CTF_EXCHANGE = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"
NEG_RISK_EXCHANGE = "0xC5d563A36AE78145C45a50134d48A1215220f80a"
NEG_RISK_ADAPTER = "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296"

# ────────────── ABI 片段 ──────────────

# ConditionalTokens.redeemPositions
CTF_REDEEM_ABI = [
    {
        "inputs": [
            {"name": "collateralToken", "type": "address"},
            {"name": "parentCollectionId", "type": "bytes32"},
            {"name": "conditionId", "type": "bytes32"},
            {"name": "indexSets", "type": "uint256[]"},
        ],
        "name": "redeemPositions",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function",
    },
    {
        "inputs": [{"name": "conditionId", "type": "bytes32"}],
        "name": "payoutDenominator",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [
            {"name": "conditionId", "type": "bytes32"},
            {"name": "index", "type": "uint256"},
        ],
        "name": "payoutNumerators",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "constant": True,
        "inputs": [
            {"name": "account", "type": "address"},
            {"name": "id", "type": "uint256"},
        ],
        "name": "balanceOf",
        "outputs": [{"name": "", "type": "uint256"}],
        "type": "function",
    },
]

# Gnosis Safe (最小化 ABI 用于 execTransaction)
SAFE_ABI = [
    {
        "inputs": [
            {"name": "to", "type": "address"},
            {"name": "value", "type": "uint256"},
            {"name": "data", "type": "bytes"},
            {"name": "operation", "type": "uint8"},
            {"name": "safeTxGas", "type": "uint256"},
            {"name": "baseGas", "type": "uint256"},
            {"name": "gasPrice", "type": "uint256"},
            {"name": "gasToken", "type": "address"},
            {"name": "refundReceiver", "type": "address"},
            {"name": "signatures", "type": "bytes"},
        ],
        "name": "execTransaction",
        "outputs": [{"name": "success", "type": "bool"}],
        "stateMutability": "payable",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "nonce",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [
            {"name": "to", "type": "address"},
            {"name": "value", "type": "uint256"},
            {"name": "data", "type": "bytes"},
            {"name": "operation", "type": "uint8"},
            {"name": "safeTxGas", "type": "uint256"},
            {"name": "baseGas", "type": "uint256"},
            {"name": "gasPrice", "type": "uint256"},
            {"name": "gasToken", "type": "address"},
            {"name": "refundReceiver", "type": "address"},
            {"name": "_nonce", "type": "uint256"},
        ],
        "name": "getTransactionHash",
        "outputs": [{"name": "", "type": "bytes32"}],
        "stateMutability": "view",
        "type": "function",
    },
]

ZERO_ADDR = "0x0000000000000000000000000000000000000000"
ZERO_BYTES32 = b"\x00" * 32


@dataclass
class RedeemResult:
    """兑付结果"""
    success: bool
    tx_hash: str = ""
    usdc_received: float = 0.0
    error: str = ""
    condition_id: str = ""


class CtfRedeemer:
    """
    CTF 头寸兑付器

    通过 Gnosis Safe execTransaction 调用 ConditionalTokens.redeemPositions(),
    将已 resolve 市场的 winning token 兑换回 USDC。
    """

    def __init__(
        self,
        private_key: str = "",
        proxy_address: str = "",
        rpc_url: str = "",
    ) -> None:
        self._private_key = private_key or os.environ.get("PM_PRIVATE_KEY", "")
        self._proxy = proxy_address or os.environ.get("PM_PROXY_ADDRESS", "")
        self._rpc_url = rpc_url or os.environ.get("POLYGON_RPC", "https://polygon-rpc.com")
        self._w3 = None
        self._ctf = None
        self._safe = None

    def _ensure_web3(self):
        """懒初始化 web3"""
        if self._w3 is not None:
            return
        from web3 import Web3
        self._w3 = Web3(Web3.HTTPProvider(self._rpc_url, request_kwargs={"timeout": 30}))
        self._ctf = self._w3.eth.contract(
            address=Web3.to_checksum_address(CTF),
            abi=CTF_REDEEM_ABI,
        )
        self._safe = self._w3.eth.contract(
            address=Web3.to_checksum_address(self._proxy),
            abi=SAFE_ABI,
        )

    # ────────────── 查询方法 ──────────────

    async def is_resolved(self, condition_id: str) -> bool:
        """检查市场是否已 resolve"""
        self._ensure_web3()
        cid_bytes = bytes.fromhex(condition_id.replace("0x", ""))
        denom = await asyncio.to_thread(
            self._ctf.functions.payoutDenominator(cid_bytes).call
        )
        return denom > 0

    async def get_payout_info(self, condition_id: str) -> dict:
        """获取 payout 信息: {resolved, winner_index, numerators}"""
        self._ensure_web3()
        cid_bytes = bytes.fromhex(condition_id.replace("0x", ""))
        denom = await asyncio.to_thread(
            self._ctf.functions.payoutDenominator(cid_bytes).call
        )
        if denom == 0:
            return {"resolved": False, "winner_index": -1, "numerators": []}

        nums = []
        for i in range(2):  # 二元市场
            n = await asyncio.to_thread(
                self._ctf.functions.payoutNumerators(cid_bytes, i).call
            )
            nums.append(n)

        winner = -1
        for i, n in enumerate(nums):
            if n > 0:
                winner = i
                break

        return {"resolved": True, "winner_index": winner, "numerators": nums}

    async def get_ctf_balance(
        self, condition_id: str, token_index: int = 0
    ) -> int:
        """
        查询 proxy 钱包中的 CTF token 余额 (raw, 6 decimals)

        token_index: 0 = Up/Yes, 1 = Down/No
        """
        self._ensure_web3()
        from web3 import Web3

        cid_bytes = bytes.fromhex(condition_id.replace("0x", ""))
        # CTF position ID = keccak256(collateralToken, collectionId)
        # collectionId = keccak256(conditionId, indexSet)
        # 但更简单地: 直接用 tokenId 查 (tokenId 由 datasource 提供)
        # 这里用 Gnosis Safe 的 balanceOf(proxy, positionId)
        index_set = 1 << token_index  # 1 for index 0, 2 for index 1

        # 计算 positionId: CTF.getPositionId(collateralToken, collectionId)
        # collectionId = CTF.getCollectionId(parentCollectionId, conditionId, indexSet)
        # 直接用低层级计算
        parent = ZERO_BYTES32
        collection_id = Web3.solidity_keccak(
            ["bytes32", "bytes32", "uint256"],
            [parent, cid_bytes, index_set],
        )
        position_id = int.from_bytes(
            Web3.solidity_keccak(
                ["address", "bytes32"],
                [Web3.to_checksum_address(USDC), collection_id],
            ),
            "big",
        )

        balance = await asyncio.to_thread(
            self._ctf.functions.balanceOf(
                Web3.to_checksum_address(self._proxy), position_id
            ).call
        )
        return balance

    # ────────────── 核心兑付 ──────────────

    async def redeem(
        self,
        condition_id: str,
        neg_risk: bool = False,
    ) -> RedeemResult:
        """
        兑付已 resolve 市场的头寸

        1. 检查市场是否已 resolve
        2. 检查 proxy 是否有 CTF balance
        3. 构造 redeemPositions calldata
        4. 通过 Gnosis Safe execTransaction 执行
        5. 返回结果

        Args:
            condition_id: 市场 conditionId (0x...)
            neg_risk: 是否 negRisk 市场 (BTC 5-min = False)
        """
        self._ensure_web3()
        from web3 import Web3
        from eth_account import Account

        cid = condition_id
        cid_bytes = bytes.fromhex(cid.replace("0x", ""))

        # 1. 检查 resolve 状态
        payout = await self.get_payout_info(cid)
        if not payout["resolved"]:
            return RedeemResult(
                success=False, condition_id=cid,
                error="Market not yet resolved",
            )

        # 2. 检查 CTF balance (两个 outcome)
        bal0 = await self.get_ctf_balance(cid, 0)
        bal1 = await self.get_ctf_balance(cid, 1)

        if bal0 == 0 and bal1 == 0:
            logger.debug(f"[Redeemer] conditionId={cid[:18]}... 无持仓, 跳过")
            return RedeemResult(
                success=True, condition_id=cid,
                error="No position to redeem (balance=0)",
            )

        winner_idx = payout["winner_index"]
        winner_side = "Up" if winner_idx == 0 else "Down"
        winning_bal = bal0 if winner_idx == 0 else bal1
        usdc_amount = winning_bal / 1e6

        logger.info(
            f"[Redeemer] 兑付 conditionId={cid[:18]}... | "
            f"赢方={winner_side} | 余额: Up={bal0/1e6:.2f} Down={bal1/1e6:.2f} | "
            f"预期 USDC={usdc_amount:.2f}"
        )

        # 3. 构造 redeemPositions calldata
        # indexSets: [1, 2] 兑付两个 outcome (输方余额=0, 无操作)
        redeem_data = self._ctf.encodeABI(
            fn_name="redeemPositions",
            args=[
                Web3.to_checksum_address(USDC),
                ZERO_BYTES32,
                cid_bytes,
                [1, 2],  # 二元市场的两个 indexSet
            ],
        )

        # 4. 通过 Gnosis Safe 执行
        tx_hash = await self._exec_safe_tx(
            to=CTF,
            data=redeem_data,
        )

        if not tx_hash:
            return RedeemResult(
                success=False, condition_id=cid,
                error="Safe execTransaction failed",
            )

        # 5. 等待交易确认
        try:
            receipt = await asyncio.to_thread(
                self._w3.eth.wait_for_transaction_receipt, tx_hash, timeout=60
            )
            if receipt["status"] == 1:
                logger.info(
                    f"[Redeemer] ✅ 兑付成功 | tx={tx_hash.hex()} | "
                    f"USDC≈{usdc_amount:.2f}"
                )
                return RedeemResult(
                    success=True,
                    tx_hash=tx_hash.hex(),
                    usdc_received=usdc_amount,
                    condition_id=cid,
                )
            else:
                logger.error(f"[Redeemer] ❌ 交易 revert | tx={tx_hash.hex()}")
                return RedeemResult(
                    success=False,
                    tx_hash=tx_hash.hex(),
                    condition_id=cid,
                    error="Transaction reverted",
                )
        except Exception as e:
            logger.error(f"[Redeemer] 等待交易确认失败: {e}")
            return RedeemResult(
                success=False, condition_id=cid,
                error=f"Wait for receipt failed: {e}",
            )

    async def _exec_safe_tx(self, to: str, data: str | bytes) -> bytes | None:
        """
        通过 Gnosis Safe execTransaction 执行一笔交易

        由于 Safe threshold=1, 单个 owner 签名即可。

        流程:
        1. 获取 Safe nonce
        2. 计算 Safe tx hash
        3. EOA 签名
        4. 发送 execTransaction
        """
        from web3 import Web3
        from eth_account import Account

        try:
            proxy_addr = Web3.to_checksum_address(self._proxy)
            target_addr = Web3.to_checksum_address(to)
            eoa = Account.from_key(self._private_key)
            eoa_addr = Web3.to_checksum_address(eoa.address)

            if isinstance(data, str):
                data_bytes = bytes.fromhex(data.replace("0x", ""))
            else:
                data_bytes = data

            # 1. 获取 Safe nonce
            nonce = await asyncio.to_thread(
                self._safe.functions.nonce().call
            )

            # 2. 计算 Safe tx hash
            tx_hash = await asyncio.to_thread(
                self._safe.functions.getTransactionHash(
                    target_addr,       # to
                    0,                 # value
                    data_bytes,        # data
                    0,                 # operation (CALL)
                    0,                 # safeTxGas
                    0,                 # baseGas
                    0,                 # gasPrice
                    Web3.to_checksum_address(ZERO_ADDR),  # gasToken
                    Web3.to_checksum_address(ZERO_ADDR),  # refundReceiver
                    nonce,             # _nonce
                ).call
            )

            # 3. EOA 签名 (Gnosis Safe 使用 raw hash 签名)
            # web3 7.x: signHash 已废弃, 使用 unsafe_sign_hash
            if hasattr(eoa, 'unsafe_sign_hash'):
                signed = eoa.unsafe_sign_hash(tx_hash)
            else:
                signed = eoa.signHash(tx_hash)
            # Pack signature: r (32) + s (32) + v (1)
            sig = (
                signed.r.to_bytes(32, "big")
                + signed.s.to_bytes(32, "big")
                + signed.v.to_bytes(1, "big")
            )

            # 4. 构造并发送 execTransaction
            tx = self._safe.functions.execTransaction(
                target_addr,
                0,
                data_bytes,
                0,                 # operation
                0,                 # safeTxGas
                0,                 # baseGas
                0,                 # gasPrice
                Web3.to_checksum_address(ZERO_ADDR),
                Web3.to_checksum_address(ZERO_ADDR),
                sig,
            ).build_transaction({
                "from": eoa_addr,
                "nonce": await asyncio.to_thread(
                    self._w3.eth.get_transaction_count, eoa_addr
                ),
                "gas": 300_000,
                "maxFeePerGas": await self._get_gas_price(),
                "maxPriorityFeePerGas": self._w3.to_wei(30, "gwei"),
                "chainId": 137,
            })

            # 签名并发送
            signed_tx = eoa.sign_transaction(tx)
            tx_hash = await asyncio.to_thread(
                self._w3.eth.send_raw_transaction, signed_tx.raw_transaction
            )

            logger.info(f"[Redeemer] Safe tx 已发送: {tx_hash.hex()}")
            return tx_hash

        except Exception as e:
            logger.error(f"[Redeemer] Safe execTransaction 失败: {e}")
            return None

    async def _get_gas_price(self) -> int:
        """获取当前 gas price (EIP-1559 max fee)"""
        try:
            gas_price = await asyncio.to_thread(self._w3.eth.gas_price)
            # 加 20% buffer
            return int(gas_price * 1.2)
        except Exception:
            return self._w3.to_wei(50, "gwei")  # fallback

    # ────────────── 批量扫描兑付 ──────────────

    async def scan_and_redeem(
        self,
        condition_ids: list[str],
        neg_risk: bool = False,
    ) -> list[RedeemResult]:
        """
        批量扫描并兑付已 resolve 的市场

        Args:
            condition_ids: 市场 conditionId 列表
            neg_risk: 是否 negRisk 市场

        Returns:
            兑付结果列表
        """
        results = []
        for cid in condition_ids:
            try:
                result = await self.redeem(cid, neg_risk=neg_risk)
                results.append(result)
                if result.success and result.tx_hash:
                    # 等待一下避免 nonce 冲突
                    await asyncio.sleep(2)
            except Exception as e:
                logger.error(f"[Redeemer] 兑付 {cid[:18]}... 异常: {e}")
                results.append(RedeemResult(
                    success=False, condition_id=cid, error=str(e)
                ))
        return results
