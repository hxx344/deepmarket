"""
Polymarket CTF Position Redemption via Gnosis Safe Proxy

从已解决 (resolved) 的 Polymarket 条件代币框架 (CTF) 市场中兑付 (redeem) 赢的头寸。

架构:
  EOA (0xbA48...) --execTransaction--> Gnosis Safe Proxy (0x3172...)
                                        |
                                        v
                               ConditionalTokens.redeemPositions(...)
                                        |
                                        v
                               USDC 到达 Proxy 钱包

使用方法:
  1. 直接调用: python scripts/redeem_ctf.py --condition-id 0x4046... [--dry-run]
  2. 扫描所有可兑付头寸: python scripts/redeem_ctf.py --scan [--dry-run]
  3. 导入使用: from scripts.redeem_ctf import CTFRedeemer; r = CTFRedeemer(); await r.redeem(condition_id)

关键合约:
  - ConditionalTokens (CTF): 0x4D97DCd97eC945f40cF65F87097ACe5EA0476045
    - redeemPositions(collateral, parentCollectionId, conditionId, indexSets)
    - balanceOf(owner, tokenId) - ERC1155
    - payoutNumerators(conditionId, index)
    - payoutDenominator(conditionId)
  
  - Gnosis Safe Proxy: execTransaction(to, value, data, operation, ...)
    - operation=0 for CALL
    - Threshold=1, sole owner=EOA → 单签名即可执行

  - USDC (Polygon): 0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174

注意: BTC 5-min 市场 negRisk=False, 直接与 ConditionalTokens 合约交互。
      negRisk=True 的市场需要通过 NegRiskAdapter (0xd91E...) 交互，逻辑不同。
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from typing import Any

from dotenv import load_dotenv
from eth_account import Account
from loguru import logger
from web3 import Web3
from web3.contract import Contract

# ─────────────────── 合约地址 (Polygon Mainnet) ───────────────────

CONDITIONAL_TOKENS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
USDC_POLYGON = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
NEG_RISK_ADAPTER = "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296"
CTF_EXCHANGE = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"
NEG_RISK_CTF_EXCHANGE = "0xC5d563A36AE78145C45a50134d48A1215220f80a"

# ─────────────────── ABI 定义 ───────────────────

# ConditionalTokens (Gnosis CTF) - 关键函数
CTF_ABI = [
    # redeemPositions: 核心兑付函数
    # 当市场 resolved 后，持有获胜方 outcome token 的用户可调用此函数
    # 将 CTF token burn 并收回 collateral (USDC)
    {
        "inputs": [
            {"name": "collateralToken", "type": "address"},
            {"name": "parentCollectionId", "type": "bytes32"},
            {"name": "conditionId", "type": "bytes32"},
            {"name": "indexSets", "type": "uint256[]"},
        ],
        "name": "redeemPositions",
        "outputs": [],
        "type": "function",
        "stateMutability": "nonpayable",
    },
    # balanceOf: ERC1155 余额查询
    {
        "inputs": [
            {"name": "account", "type": "address"},
            {"name": "id", "type": "uint256"},
        ],
        "name": "balanceOf",
        "outputs": [{"name": "", "type": "uint256"}],
        "type": "function",
        "stateMutability": "view",
    },
    # getOutcomeSlotCount: 获取条件的 outcome 数量
    {
        "inputs": [{"name": "conditionId", "type": "bytes32"}],
        "name": "getOutcomeSlotCount",
        "outputs": [{"name": "", "type": "uint256"}],
        "type": "function",
        "stateMutability": "view",
    },
    # payoutNumerators: 获取每个 outcome 的支付分子
    # 只有 resolved 的市场才有值 (未 resolve = 全部为 0)
    {
        "inputs": [
            {"name": "conditionId", "type": "bytes32"},
            {"name": "index", "type": "uint256"},
        ],
        "name": "payoutNumerators",
        "outputs": [{"name": "", "type": "uint256"}],
        "type": "function",
        "stateMutability": "view",
    },
    # payoutDenominator: 支付分母 (通常=1 对于二元市场)
    {
        "inputs": [{"name": "conditionId", "type": "bytes32"}],
        "name": "payoutDenominator",
        "outputs": [{"name": "", "type": "uint256"}],
        "type": "function",
        "stateMutability": "view",
    },
    # getCollectionId: 计算 collection ID
    {
        "inputs": [
            {"name": "parentCollectionId", "type": "bytes32"},
            {"name": "conditionId", "type": "bytes32"},
            {"name": "indexSet", "type": "uint256"},
        ],
        "name": "getCollectionId",
        "outputs": [{"name": "", "type": "bytes32"}],
        "type": "function",
        "stateMutability": "view",
    },
    # getPositionId: 计算 position ID (= ERC1155 token ID)
    {
        "inputs": [
            {"name": "collateralToken", "type": "address"},
            {"name": "collectionId", "type": "bytes32"},
        ],
        "name": "getPositionId",
        "outputs": [{"name": "", "type": "uint256"}],
        "type": "function",
        "stateMutability": "view",
    },
]

# Gnosis Safe Proxy - execTransaction
# 当 threshold=1 且 EOA 是唯一 owner 时，只需 EOA 的一个签名
GNOSIS_SAFE_ABI = [
    {
        "inputs": [
            {"name": "to", "type": "address"},
            {"name": "value", "type": "uint256"},
            {"name": "data", "type": "bytes"},
            {"name": "operation", "type": "uint8"},       # 0=Call, 1=DelegateCall
            {"name": "safeTxGas", "type": "uint256"},
            {"name": "baseGas", "type": "uint256"},
            {"name": "gasPrice", "type": "uint256"},
            {"name": "gasToken", "type": "address"},
            {"name": "refundReceiver", "type": "address"},
            {"name": "signatures", "type": "bytes"},
        ],
        "name": "execTransaction",
        "outputs": [{"name": "success", "type": "bool"}],
        "type": "function",
        "stateMutability": "payable",
    },
    {
        "inputs": [],
        "name": "nonce",
        "outputs": [{"name": "", "type": "uint256"}],
        "type": "function",
        "stateMutability": "view",
    },
    {
        "inputs": [],
        "name": "getThreshold",
        "outputs": [{"name": "", "type": "uint256"}],
        "type": "function",
        "stateMutability": "view",
    },
    {
        "inputs": [],
        "name": "getOwners",
        "outputs": [{"name": "", "type": "address[]"}],
        "type": "function",
        "stateMutability": "view",
    },
    # getTransactionHash - 用于签名
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
        "type": "function",
        "stateMutability": "view",
    },
]

# ERC20 balanceOf
ERC20_ABI = [
    {
        "inputs": [{"name": "account", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "", "type": "uint256"}],
        "type": "function",
        "stateMutability": "view",
    }
]


class CTFRedeemer:
    """
    Polymarket CTF 头寸兑付器

    对于 negRisk=False 的二元市场 (如 BTC 5-min Up/Down):
    1. 检查 conditionId 是否已 resolved (payoutDenominator > 0)
    2. 计算每个 outcome 的 positionId (ERC1155 tokenId)
    3. 检查 proxy wallet 持有的 CTF token 余额
    4. 构造 redeemPositions calldata
    5. 通过 Gnosis Safe execTransaction 从 proxy 执行
    
    redeemPositions 工作原理:
    - 函数由 CTF token 持有者 (这里是 proxy) 调用
    - 对于 indexSets=[1,2] (二元市场的两个 outcome):
      - indexSet=1 → outcome 0 (第一个结果，通常是 "Up")
      - indexSet=2 → outcome 1 (第二个结果，通常是 "Down")
    - 合约根据 payoutNumerators 计算每个 outcome 的兑付金额
    - 只有获胜方的 token 有兑付价值，输方 token 兑付 0
    - 获胜方的 CTF token 被 burn，等额 USDC 释放给调用者 (proxy)
    """

    def __init__(
        self,
        private_key: str = "",
        proxy_address: str = "",
        rpc_url: str = "",
    ):
        load_dotenv()
        self._private_key = private_key or os.environ.get("PM_PRIVATE_KEY", "")
        self._proxy = Web3.to_checksum_address(
            proxy_address or os.environ.get("PM_PROXY_ADDRESS", "")
        )
        self._rpc_url = rpc_url or os.environ.get("POLYGON_RPC", "https://polygon-rpc.com")

        self._w3 = Web3(Web3.HTTPProvider(self._rpc_url))
        self._account = Account.from_key(self._private_key)
        self._eoa = self._account.address

        # 合约实例
        self._ctf: Contract = self._w3.eth.contract(
            address=Web3.to_checksum_address(CONDITIONAL_TOKENS),
            abi=CTF_ABI,
        )
        self._safe: Contract = self._w3.eth.contract(
            address=self._proxy,
            abi=GNOSIS_SAFE_ABI,
        )
        self._usdc: Contract = self._w3.eth.contract(
            address=Web3.to_checksum_address(USDC_POLYGON),
            abi=ERC20_ABI,
        )

        logger.info(f"CTFRedeemer initialized: EOA={self._eoa}, Proxy={self._proxy}")

    # ─────────────────── 查询方法 ───────────────────

    def check_condition_resolved(self, condition_id: str) -> dict[str, Any]:
        """
        检查 conditionId 是否已 resolved，并返回 payout 信息。

        Returns:
            {
                "resolved": bool,
                "outcome_count": int,
                "payout_denominator": int,
                "payout_numerators": [int, ...],
                "winning_indices": [int, ...],  # payout > 0 的 outcome 索引
            }
        """
        cid_bytes = bytes.fromhex(condition_id.replace("0x", ""))
        assert len(cid_bytes) == 32, f"Invalid conditionId length: {len(cid_bytes)}"

        outcome_count = self._ctf.functions.getOutcomeSlotCount(cid_bytes).call()
        if outcome_count == 0:
            return {"resolved": False, "outcome_count": 0, "error": "Condition not found"}

        denom = self._ctf.functions.payoutDenominator(cid_bytes).call()
        numerators = []
        for i in range(outcome_count):
            num = self._ctf.functions.payoutNumerators(cid_bytes, i).call()
            numerators.append(num)

        resolved = denom > 0
        winning = [i for i, n in enumerate(numerators) if n > 0]

        return {
            "resolved": resolved,
            "outcome_count": outcome_count,
            "payout_denominator": denom,
            "payout_numerators": numerators,
            "winning_indices": winning,
        }

    def get_position_id_onchain(self, condition_id: str, index_set: int) -> int:
        """
        通过链上调用获取 CTF ERC1155 token ID (positionId)。

        对于二元市场:
          - index_set=1 → outcome 0 position (e.g., "Up")
          - index_set=2 → outcome 1 position (e.g., "Down")
        
        计算过程 (合约编译后有特定实现，不能可靠地本地复现):
          collectionId = CTF.getCollectionId(parentCollectionId=0x0, conditionId, indexSet)
          positionId = CTF.getPositionId(collateralToken=USDC, collectionId)
        
        注意: Polygon 上的 CTF 合约使用的 getCollectionId 实现与标准
        keccak256(abi.encodePacked(conditionId, indexSet)) 不完全一致,
        因此使用链上调用确保正确性。有 rate limit 时会自动重试。
        """
        import time as _time

        cid_bytes = bytes.fromhex(condition_id.replace("0x", ""))
        parent = b"\x00" * 32  # bytes32(0) for root

        for attempt in range(3):
            try:
                collection_id = self._ctf.functions.getCollectionId(
                    parent, cid_bytes, index_set
                ).call()
                _time.sleep(0.5)  # rate limit courtesy
                position_id = self._ctf.functions.getPositionId(
                    Web3.to_checksum_address(USDC_POLYGON), collection_id
                ).call()
                return position_id
            except Exception as e:
                if "rate limit" in str(e).lower() and attempt < 2:
                    logger.warning(f"RPC rate limited, retrying in {(attempt+1)*5}s...")
                    _time.sleep((attempt + 1) * 5)
                else:
                    raise

        raise RuntimeError("Failed to get position ID after retries")

    def get_ctf_balances(
        self,
        condition_id: str,
        token_ids: list[int] | None = None,
    ) -> dict[str, Any]:
        """
        查询 proxy 钱包在指定 conditionId 下的 CTF token 余额。

        Args:
            condition_id: 市场的 conditionId
            token_ids: 可选, 直接提供 [up_token_id, down_token_id]
                       如果不提供，会通过链上调用计算

        Returns:
            {
                "condition_id": str,
                "positions": [
                    {"index_set": 1, "position_id": int, "balance": int, "balance_usdc": float},
                    {"index_set": 2, "position_id": int, "balance": int, "balance_usdc": float},
                ]
            }
        """
        positions = []
        for i, idx_set in enumerate([1, 2]):  # 二元市场
            if token_ids and i < len(token_ids):
                pos_id = token_ids[i]
            else:
                pos_id = self.get_position_id_onchain(condition_id, idx_set)

            import time as _time
            for attempt in range(3):
                try:
                    balance = self._ctf.functions.balanceOf(self._proxy, pos_id).call()
                    break
                except Exception as e:
                    if "rate limit" in str(e).lower() and attempt < 2:
                        _time.sleep((attempt + 1) * 5)
                    else:
                        raise

            positions.append({
                "index_set": idx_set,
                "position_id": pos_id,
                "balance": balance,
                "balance_usdc": balance / 1e6,
            })

        return {"condition_id": condition_id, "positions": positions}

    def get_usdc_balance(self) -> float:
        """查询 proxy 钱包的 USDC 余额"""
        bal = self._usdc.functions.balanceOf(self._proxy).call()
        return bal / 1e6

    # ─────────────────── 兑付执行 ───────────────────

    def build_redeem_calldata(self, condition_id: str) -> bytes:
        """
        构造 redeemPositions 的 calldata。

        参数:
          - collateralToken: USDC 地址
          - parentCollectionId: bytes32(0) (根层级，非嵌套条件)
          - conditionId: 市场的 conditionId
          - indexSets: [1, 2] 对应二元市场的两个 outcome
            - indexSet 是位掩码: 1=0b01 (outcome 0), 2=0b10 (outcome 1)
            - 传入 [1,2] 会兑付两个 outcome 的 position
            - 输的那个 balance=0，redeem 什么也不做 (不会 revert)
            - 赢的那个会 burn CTF token 并释放 USDC
        """
        cid_bytes = bytes.fromhex(condition_id.replace("0x", ""))
        parent = b"\x00" * 32

        calldata = self._ctf.encodeABI(
            fn_name="redeemPositions",
            args=[
                Web3.to_checksum_address(USDC_POLYGON),  # collateralToken
                parent,                                     # parentCollectionId
                cid_bytes,                                  # conditionId
                [1, 2],                                     # indexSets (binary)
            ],
        )
        return bytes.fromhex(calldata[2:])  # remove 0x prefix

    def execute_via_safe(
        self,
        to: str,
        data: bytes,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """
        通过 Gnosis Safe execTransaction 执行交易。

        对于 threshold=1 + 单一 owner 的 Safe:
        1. 获取 Safe nonce
        2. 计算 Safe tx hash
        3. EOA 签名该 hash
        4. 构造 packed signature (r + s + v)
        5. 发送 execTransaction 交易

        签名格式 (Gnosis Safe):
          对于 EOA 签名 (非合约签名), signature = abi.encodePacked(r, s, v)
          其中 v 已经 +4 被 Gnosis Safe adjust (实际上不需要 +4, Safe 会自动识别)
          但为安全起见, v 保持原始值 (27 或 28)。
        """
        to = Web3.to_checksum_address(to)
        nonce = self._safe.functions.nonce().call()
        
        logger.info(f"Safe nonce: {nonce}")

        # 参数: to, value=0, data, operation=0 (CALL), safeTxGas=0, baseGas=0,
        #       gasPrice=0, gasToken=0x0, refundReceiver=0x0
        zero_addr = "0x0000000000000000000000000000000000000000"

        # 计算 Safe transaction hash
        tx_hash = self._safe.functions.getTransactionHash(
            to,             # to
            0,              # value
            data,           # data
            0,              # operation (CALL)
            0,              # safeTxGas
            0,              # baseGas
            0,              # gasPrice
            zero_addr,      # gasToken
            zero_addr,      # refundReceiver
            nonce,          # _nonce
        ).call()

        logger.info(f"Safe tx hash: 0x{tx_hash.hex()}")

        # EOA 签名 tx hash
        signed = self._account.signHash(tx_hash)
        # Pack signature: r(32 bytes) + s(32 bytes) + v(1 byte)
        signature = (
            signed.r.to_bytes(32, "big")
            + signed.s.to_bytes(32, "big")
            + signed.v.to_bytes(1, "big")
        )

        logger.info(f"Signature v={signed.v}, len={len(signature)}")

        if dry_run:
            # 模拟调用
            try:
                result = self._safe.functions.execTransaction(
                    to, 0, data, 0, 0, 0, 0,
                    Web3.to_checksum_address(zero_addr),
                    Web3.to_checksum_address(zero_addr),
                    signature,
                ).call({"from": self._eoa})
                return {
                    "dry_run": True,
                    "success": result,
                    "nonce": nonce,
                    "tx_hash_safe": f"0x{tx_hash.hex()}",
                }
            except Exception as e:
                return {"dry_run": True, "success": False, "error": str(e)}

        # 发送真实交易
        tx = self._safe.functions.execTransaction(
            to, 0, data, 0, 0, 0, 0,
            Web3.to_checksum_address(zero_addr),
            Web3.to_checksum_address(zero_addr),
            signature,
        ).build_transaction({
            "from": self._eoa,
            "nonce": self._w3.eth.get_transaction_count(self._eoa),
            "gas": 300_000,  # 预估, 实际通常 ~150k
            "maxFeePerGas": self._w3.eth.gas_price * 2,
            "maxPriorityFeePerGas": self._w3.to_wei(30, "gwei"),
            "chainId": 137,
        })

        signed_tx = self._account.sign_transaction(tx)
        tx_hash_sent = self._w3.eth.send_raw_transaction(signed_tx.raw_transaction)

        logger.info(f"TX sent: 0x{tx_hash_sent.hex()}")
        logger.info("Waiting for confirmation...")

        receipt = self._w3.eth.wait_for_transaction_receipt(tx_hash_sent, timeout=120)

        return {
            "dry_run": False,
            "success": receipt["status"] == 1,
            "tx_hash": f"0x{tx_hash_sent.hex()}",
            "block": receipt["blockNumber"],
            "gas_used": receipt["gasUsed"],
        }

    def redeem(
        self,
        condition_id: str,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """
        主兑付流程:
        1. 验证市场已 resolved
        2. 查询 CTF token 余额
        3. 构造 calldata
        4. 通过 Safe 执行
        """
        logger.info(f"=== Redeeming conditionId: {condition_id} ===")

        # Step 1: Check resolved
        resolved_info = self.check_condition_resolved(condition_id)
        if not resolved_info["resolved"]:
            logger.warning("Market not resolved yet!")
            return {"success": False, "error": "Market not resolved", "info": resolved_info}

        winning = resolved_info["winning_indices"]
        logger.info(
            f"Market resolved: winning outcomes={winning}, "
            f"payout={resolved_info['payout_numerators']}/{resolved_info['payout_denominator']}"
        )

        # Step 2: Check balances
        balances = self.get_ctf_balances(condition_id)
        total_redeemable = 0
        for pos in balances["positions"]:
            outcome_idx = pos["index_set"] - 1  # indexSet 1 → outcome 0, indexSet 2 → outcome 1
            is_winner = outcome_idx in winning
            logger.info(
                f"  Outcome {outcome_idx} (indexSet={pos['index_set']}): "
                f"balance={pos['balance_usdc']:.6f} USDC, "
                f"{'WINNER ✅' if is_winner else 'LOSER ❌'}"
            )
            if is_winner:
                total_redeemable += pos["balance_usdc"]

        if total_redeemable == 0:
            logger.info("No redeemable balance (either already redeemed or no winning positions)")
            return {
                "success": True,
                "redeemed": 0,
                "message": "Nothing to redeem",
                "balances": balances,
            }

        logger.info(f"Total redeemable: {total_redeemable:.6f} USDC")

        # Step 3: USDC balance before
        usdc_before = self.get_usdc_balance()
        logger.info(f"Proxy USDC balance before: {usdc_before:.6f}")

        # Step 4: Build & execute
        calldata = self.build_redeem_calldata(condition_id)
        logger.info(f"Calldata: {len(calldata)} bytes")

        result = self.execute_via_safe(
            to=CONDITIONAL_TOKENS,
            data=calldata,
            dry_run=dry_run,
        )

        if not dry_run and result.get("success"):
            usdc_after = self.get_usdc_balance()
            result["usdc_before"] = usdc_before
            result["usdc_after"] = usdc_after
            result["usdc_gained"] = usdc_after - usdc_before
            logger.info(
                f"✅ Redeemed! USDC: {usdc_before:.6f} → {usdc_after:.6f} "
                f"(+{usdc_after - usdc_before:.6f})"
            )

        result["condition_id"] = condition_id
        result["redeemable"] = total_redeemable
        return result

    def scan_and_redeem(
        self,
        condition_ids: list[str],
        dry_run: bool = False,
    ) -> list[dict[str, Any]]:
        """批量扫描并兑付多个 conditionId"""
        results = []
        for cid in condition_ids:
            try:
                r = self.redeem(cid, dry_run=dry_run)
                results.append(r)
            except Exception as e:
                logger.error(f"Redeem failed for {cid}: {e}")
                results.append({"condition_id": cid, "success": False, "error": str(e)})
        return results


# ─────────────────── 辅助: 查找最近的已交易市场 ───────────────────

def find_recent_resolved_markets(hours_back: int = 2) -> list[dict[str, Any]]:
    """
    从 Gamma API 查找最近 N 小时内过期的 BTC 5-min 市场，
    返回 conditionId、token_ids 等信息。
    """
    import datetime
    from datetime import timezone
    import httpx

    now = datetime.datetime.now(timezone.utc)
    current_ts = int(now.timestamp())
    window_len = 300  # 5 min

    markets = []
    # 扫描最近 hours_back 小时的所有 5-min 窗口
    num_windows = (hours_back * 3600) // window_len
    for i in range(1, num_windows + 1):
        ws = ((current_ts // window_len) - i) * window_len
        slug = f"btc-updown-5m-{ws}"
        try:
            r = httpx.get(
                f"https://gamma-api.polymarket.com/events?slug={slug}",
                timeout=10,
            )
            data = r.json()
            if data:
                event = data[0]
                for m in event.get("markets", []):
                    token_ids_raw = m.get("clobTokenIds", "[]")
                    if isinstance(token_ids_raw, str):
                        import json as _json
                        token_ids_raw = _json.loads(token_ids_raw)
                    markets.append({
                        "condition_id": m.get("conditionId", ""),
                        "question": m.get("question", ""),
                        "token_ids": [int(t) for t in token_ids_raw],
                        "closed": m.get("closed", False),
                        "neg_risk": m.get("negRisk", False),
                        "slug": slug,
                        "window_start": ws,
                    })
        except Exception as e:
            logger.debug(f"Failed to fetch {slug}: {e}")
        import time as _time
        _time.sleep(0.3)  # API courtesy

    return markets


# ─────────────────── CLI ───────────────────

def main():
    parser = argparse.ArgumentParser(description="Polymarket CTF Position Redeemer")
    parser.add_argument("--condition-id", "-c", help="Condition ID to redeem")
    parser.add_argument("--token-ids", help="Comma-separated token IDs (up,down)")
    parser.add_argument("--dry-run", "-d", action="store_true", help="Simulate only, don't send tx")
    parser.add_argument("--scan", "-s", action="store_true", help="Scan recent markets for redeemable positions")
    parser.add_argument("--scan-hours", type=int, default=2, help="Hours to scan back (default: 2)")
    parser.add_argument("--check", action="store_true", help="Only check resolution status and balances")
    args = parser.parse_args()

    redeemer = CTFRedeemer()

    # Parse token IDs if provided (handle scientific notation from shell)
    token_ids = None
    if args.token_ids:
        from decimal import Decimal
        token_ids = [int(Decimal(t.strip())) for t in args.token_ids.split(",")]

    if args.check and args.condition_id:
        # 仅查询
        info = redeemer.check_condition_resolved(args.condition_id)
        print(json.dumps(info, indent=2))
        balances = redeemer.get_ctf_balances(args.condition_id, token_ids=token_ids)
        print(json.dumps(balances, indent=2, default=str))
        return

    if args.condition_id:
        result = redeemer.redeem(args.condition_id, dry_run=args.dry_run)
        print(json.dumps(result, indent=2, default=str))

    elif args.scan:
        logger.info(f"Scanning last {args.scan_hours} hours for redeemable positions...")
        markets = find_recent_resolved_markets(hours_back=args.scan_hours)
        logger.info(f"Found {len(markets)} recent markets")

        redeemable = []
        for m in markets:
            cid = m["condition_id"]
            try:
                info = redeemer.check_condition_resolved(cid)
                if not info["resolved"]:
                    continue
                balances = redeemer.get_ctf_balances(cid, token_ids=m.get("token_ids"))
                has_balance = any(p["balance"] > 0 for p in balances["positions"])
                if has_balance:
                    redeemable.append({
                        **m,
                        "resolution": info,
                        "balances": balances,
                    })
                    logger.info(f"💰 Redeemable: {m['question']}")
                    for p in balances["positions"]:
                        if p["balance"] > 0:
                            logger.info(f"   indexSet={p['index_set']}: {p['balance_usdc']:.6f} USDC")
            except Exception as e:
                logger.debug(f"Error checking {cid[:16]}...: {e}")
            import time as _time
            _time.sleep(1)  # rate limit

        if redeemable:
            print(f"\n{'='*60}")
            print(f"Found {len(redeemable)} redeemable markets:")
            for r in redeemable:
                print(f"  - {r['question']}")
                print(f"    conditionId: {r['condition_id']}")
                for p in r['balances']['positions']:
                    if p['balance'] > 0:
                        print(f"    indexSet={p['index_set']}: {p['balance_usdc']:.6f} USDC")

            if not args.dry_run:
                print("\nUse --dry-run to simulate, or run without it to execute:")
                for r in redeemable:
                    print(f"  python scripts/redeem_ctf.py -c {r['condition_id']}")
        else:
            print("No redeemable positions found")
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
