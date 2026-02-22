"""快速诊断 CLOB 签名问题"""
from dotenv import load_dotenv
load_dotenv()
import os

key = os.environ.get("PM_PRIVATE_KEY", "")
proxy = os.environ.get("PM_PROXY_ADDRESS", "")

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, PartialCreateOrderOptions, OrderType

client = ClobClient(
    host="https://clob.polymarket.com",
    chain_id=137,
    key=key,
    signature_type=1,
    funder=proxy,
)

print(f"EOA (signer): {client.signer.address()}")
print(f"Funder (maker): {client.builder.funder}")
print(f"sig_type: {client.builder.sig_type}")

# 1. 获取 API creds
try:
    creds = client.derive_api_key()
    print(f"derive OK: {creds.api_key[:12]}...")
except Exception as e:
    print(f"derive failed: {e}")
    creds = client.create_api_key()
    print(f"create OK: {creds.api_key[:12]}...")

client.set_api_creds(creds)

# 2. 获取活跃市场 token_id
import requests, json
print("\n获取活跃市场...")
resp = requests.get("https://gamma-api.polymarket.com/markets?active=true&closed=false&limit=1", timeout=10)
m = resp.json()[0]
tokens = json.loads(m["clobTokenIds"]) if isinstance(m["clobTokenIds"], str) else m["clobTokenIds"]
token_id = tokens[0]
neg_risk = m.get("negRisk", False)
print(f"Market: {m['question'][:60]}")
print(f"token_id: {token_id[:30]}...")
print(f"neg_risk: {neg_risk}")

# 3. 签名一个测试订单
order_args = OrderArgs(
    token_id=token_id,
    price=0.10,   # 用很低的价格确保不会成交
    size=1.0,
    side="BUY",
)
opts = PartialCreateOrderOptions(tick_size="0.01", neg_risk=neg_risk)

signed = client.create_order(order_args, opts)
order_obj = signed.order if hasattr(signed, "order") else signed
print(f"\nOrder signed:")
print(f"  type: {type(order_obj)}")
d = signed.dict()
print(f"  dict: {d}")
print(f"  signature: {signed.signature[:30]}...")

# 4. 尝试提交
import json
from py_clob_client.utilities import order_to_json
body = order_to_json(signed, creds.api_key, OrderType.FOK, False)
print(f"\nFull request body:")
print(json.dumps(body, indent=2))

try:
    resp = client.post_order(signed, OrderType.FOK)
    print(f"\nPost response: {resp}")
except Exception as e:
    print(f"\nPost FAILED: {e}")

# 5. 本地验证签名
print("\n=== 本地验证签名 ===")
from eth_account import Account
from eth_utils import keccak
from py_order_utils.utils import prepend_zx
from py_clob_client.config import get_contract_config

contract_config = get_contract_config(137, neg_risk)
print(f"Exchange address: {contract_config.exchange}")

from poly_eip712_structs import make_domain
domain = make_domain(
    name="Polymarket CTF Exchange",
    version="1",
    chainId="137",
    verifyingContract=contract_config.exchange,
)
order_obj = signed.order
struct_hash_bytes = keccak(order_obj.signable_bytes(domain=domain))
struct_hash_hex = prepend_zx(struct_hash_bytes.hex())
print(f"struct_hash: {struct_hash_hex[:20]}...")

sig_bytes = bytes.fromhex(signed.signature[2:])
try:
    recovered_addr = Account._recover_hash(struct_hash_hex, signature=sig_bytes)
    print(f"Recovered signer: {recovered_addr}")
    print(f"Expected signer:  {client.signer.address()}")
    print(f"Match: {recovered_addr.lower() == client.signer.address().lower()}")
except Exception as e:
    print(f"Recovery failed: {e}")
    # try with vrs
    print("Trying Account.recover_message approach...")
    from eth_account.messages import encode_defunct
    try:
        recovered_addr = Account.recoverHash(struct_hash_hex, signature=sig_bytes)
        print(f"Recovered (v2): {recovered_addr}")
    except Exception as e2:
        print(f"Also failed: {e2}")

# 6. 尝试 EOA 模式 (sig_type=0)
print("\n=== 测试 EOA 模式 (sig_type=0, maker=EOA) ===")
client2 = ClobClient(
    host="https://clob.polymarket.com",
    chain_id=137,
    key=key,
    signature_type=0,
    # NO funder - maker = EOA directly
)
try:
    creds2 = client2.derive_api_key()
    print(f"derive OK: {creds2.api_key[:12]}...")
except:
    creds2 = client2.create_api_key()
    print(f"create OK: {creds2.api_key[:12]}...")
client2.set_api_creds(creds2)

signed2 = client2.create_order(order_args, opts)
d2 = signed2.dict()
print(f"maker: {d2.get('maker','?')}")
print(f"signer: {d2.get('signer','?')}")
print(f"sigType: {d2.get('signatureType','?')}")
try:
    resp2 = client2.post_order(signed2, OrderType.FOK)
    print(f"EOA Post response: {resp2}")
except Exception as e2:
    print(f"EOA Post FAILED: {e2}")
