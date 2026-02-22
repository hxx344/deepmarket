"""深度排查 invalid signature - 检查 fee rate, 手动验签, 对比构建"""
from dotenv import load_dotenv
load_dotenv()
import os, json, requests
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, PartialCreateOrderOptions, OrderType
from eth_account import Account
from eth_account.messages import encode_defunct

key = os.environ.get("PM_PRIVATE_KEY", "")
proxy = os.environ.get("PM_PROXY_ADDRESS", "")

# 获取市场
resp = requests.get("https://gamma-api.polymarket.com/markets?active=true&closed=false&limit=1", timeout=10)
m = resp.json()[0]
tokens = json.loads(m["clobTokenIds"]) if isinstance(m["clobTokenIds"], str) else m["clobTokenIds"]
token_id = tokens[0]
neg_risk = m.get("negRisk", False)

# 创建 client
client = ClobClient(
    host="https://clob.polymarket.com",
    chain_id=137,
    key=key,
    signature_type=1,
    funder=proxy,
)
creds = client.derive_api_key()
client.set_api_creds(creds)

# 1. 检查 fee rate
print("=== Fee Rate ===")
try:
    fee = client.get_fee_rate_bps()
    print(f"Fee rate: {fee}")
except Exception as e:
    print(f"Fee rate error: {e}")

# 2. 用 fee rate 创建订单
print("\n=== 创建带 fee rate 的订单 ===")
try:
    fee_data = client.get_fee_rate_bps()
    fee_rate = int(fee_data.get("fee_rate_bps", 0)) if isinstance(fee_data, dict) else 0
    print(f"Using fee_rate_bps={fee_rate}")
except:
    fee_rate = 0

args = OrderArgs(
    token_id=token_id,
    price=0.10,
    size=10.0,
    side="BUY",
    fee_rate_bps=fee_rate,
)
opts = PartialCreateOrderOptions(tick_size="0.01", neg_risk=neg_risk)
signed = client.create_order(args, opts)

o = signed.order
print(f"feeRateBps in order: {o.values['feeRateBps']}")

# 3. 尝试 post
try:
    result = client.post_order(signed, OrderType.FOK)
    print(f"Result: {result}")
except Exception as e:
    print(f"Post error: {e}")

# 4. 手动验签: 从 signature 恢复 signer
print("\n=== 手动验签 ===")
from py_order_utils.builders.base_builder import BaseBuilder
from py_order_utils.signer import Signer as UtilsSigner

# 重建 builder
exchange_addr = "0xC5d563A36AE78145C45a50134d48A1215220f80a" if neg_risk else "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"
print(f"Exchange (domain): {exchange_addr}")

signer_util = UtilsSigner(key=key)
builder = BaseBuilder.__new__(BaseBuilder)
builder.contract_address = exchange_addr
from poly_eip712_structs import make_domain
builder.domain_separator = make_domain(
    name="Polymarket CTF Exchange",
    version="1",
    chainId="137",
    verifyingContract=exchange_addr,
)

# 计算 struct hash
from eth_utils import keccak
signable = o.signable_bytes(domain=builder.domain_separator)
msg_hash = keccak(signable)
print(f"Message hash: 0x{msg_hash.hex()}")

# 从签名恢复地址
sig_bytes = bytes.fromhex(signed.signature[2:])
print(f"Signature bytes len: {len(sig_bytes)}")
print(f"v={sig_bytes[-1]}, r=0x{sig_bytes[:32].hex()}, s=0x{sig_bytes[32:64].hex()}")

# 用 eth_account 恢复
recovered = Account._recover_hash(msg_hash, signature=sig_bytes)
print(f"Recovered signer: {recovered}")
print(f"Expected signer:  {client.signer.address()}")
print(f"Match: {recovered.lower() == client.signer.address().lower()}")

# 5. 检查 builder 内部使用的 domain 和 exchange
print("\n=== Builder 内部状态 ===")
internal_builder = client.builder
print(f"Builder sig_type: {internal_builder.sig_type}")
print(f"Builder funder: {internal_builder.funder}")
print(f"Signer address: {internal_builder.signer.address()}")
