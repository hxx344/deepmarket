"""
Wallet Manager - 钱包与签名管理

安全管理 Polymarket 交易所需的私钥和签名操作。
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from loguru import logger


class WalletManager:
    """
    钱包管理器

    管理交易签名私钥、查询余额、授权等操作。
    安全要求：
    - 私钥仅从环境变量或加密文件加载
    - 永远不在日志中打印私钥
    - 支持 read-only 模式（仅查询不签名）
    """

    def __init__(self, read_only: bool = True) -> None:
        self.read_only = read_only
        self._address: str = ""
        self._private_key: str = ""
        self._initialized = False

    def initialize_from_env(self) -> bool:
        """从环境变量加载钱包"""
        self._private_key = os.environ.get("PM_PRIVATE_KEY", "")
        self._address = os.environ.get("PM_WALLET_ADDRESS", "")

        if not self._address:
            # 如果有私钥，从私钥推导地址
            if self._private_key:
                try:
                    from eth_account import Account
                    acct = Account.from_key(self._private_key)
                    self._address = acct.address
                except Exception as e:
                    logger.error(f"Failed to derive address from key: {e}")
                    return False

        if self._address:
            self._initialized = True
            masked = self._address[:6] + "..." + self._address[-4:] if len(self._address) > 10 else "***"
            logger.info(f"Wallet initialized: {masked} (read_only={self.read_only})")
            return True

        logger.warning("No wallet configured")
        return False

    def initialize_from_keyfile(self, keyfile: str | Path, password: str = "") -> bool:
        """从加密密钥文件加载"""
        keyfile = Path(keyfile)
        if not keyfile.exists():
            logger.error(f"Keyfile not found: {keyfile}")
            return False

        try:
            from eth_account import Account
            with open(keyfile) as f:
                keystore = json.load(f)
            self._private_key = Account.decrypt(keystore, password).hex()
            acct = Account.from_key(self._private_key)
            self._address = acct.address
            self._initialized = True
            return True
        except Exception as e:
            logger.error(f"Failed to load keyfile: {e}")
            return False

    @property
    def address(self) -> str:
        return self._address

    @property
    def is_initialized(self) -> bool:
        return self._initialized

    def can_sign(self) -> bool:
        return self._initialized and not self.read_only and bool(self._private_key)

    def sign_message(self, message: str) -> str:
        """签名消息"""
        if not self.can_sign():
            raise RuntimeError("Wallet is read-only or not initialized")

        from eth_account import Account
        from eth_account.messages import encode_defunct
        msg = encode_defunct(text=message)
        signed = Account.sign_message(msg, self._private_key)
        return signed.signature.hex()

    def sign_typed_data(self, domain: dict, types: dict, value: dict) -> str:
        """EIP-712 签名 (Polymarket 订单需要)"""
        if not self.can_sign():
            raise RuntimeError("Wallet is read-only or not initialized")

        from eth_account import Account
        from eth_account.messages import encode_structured_data

        full_message = {
            "types": types,
            "primaryType": list(types.keys())[0],
            "domain": domain,
            "message": value,
        }
        encoded = encode_structured_data(full_message)
        signed = Account.sign_message(encoded, self._private_key)
        return signed.signature.hex()

    async def get_usdc_balance(self, address: str = "") -> float:
        """查询 USDC 余额 (Polygon)

        Args:
            address: 要查询的地址, 默认使用 self._address (EOA)
                     传入 proxy 地址可查询 Polymarket 代理钱包余额
        """
        if not self._initialized:
            return 0.0

        query_addr = address or self._address
        if not query_addr:
            return 0.0

        try:
            from web3 import Web3

            # Polygon RPC
            rpc_url = os.environ.get("POLYGON_RPC", "https://polygon-rpc.com")
            w3 = Web3(Web3.HTTPProvider(rpc_url))

            # USDC on Polygon
            usdc_address = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
            # 简化的 ERC20 ABI
            erc20_abi = [
                {
                    "constant": True,
                    "inputs": [{"name": "_owner", "type": "address"}],
                    "name": "balanceOf",
                    "outputs": [{"name": "balance", "type": "uint256"}],
                    "type": "function",
                }
            ]

            contract = w3.eth.contract(
                address=Web3.to_checksum_address(usdc_address),
                abi=erc20_abi,
            )
            balance = contract.functions.balanceOf(
                Web3.to_checksum_address(query_addr)
            ).call()
            return balance / 1e6  # USDC has 6 decimals

        except Exception as e:
            logger.error(f"Failed to get USDC balance: {e}")
            return 0.0
