"""测试不同签名模式对比"""
from dotenv import load_dotenv
load_dotenv()
import os, json, requests

key = os.environ.get("PM_PRIVATE_KEY", "")
proxy = os.environ.get("PM_PROXY_ADDRESS", "")

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, PartialCreateOrderOptions, OrderType, BalanceAllowanceParams

# 先找一个活跃市场
resp = requests.get("https://gamma-api.polymarket.com/markets?active=true&closed=false&limit=1", timeout=10)
m = resp.json()[0]
tokens = json.loads(m["clobTokenIds"]) if isinstance(m["clobTokenIds"], str) else m["clobTokenIds"]
token_id = tokens[0]
neg_risk = m.get("negRisk", False)
question = m.get("question", "?")
print(f"Market: {question[:60]}")
print(f"neg_risk={neg_risk}, token={token_id[:20]}...")

## 测试 1: EOA 模式 (sig_type=0)
print("\n=== TEST 1: EOA mode (sig_type=0) ===")
client_eoa = ClobClient(
    host="https://clob.polymarket.com",
    chain_id=137,
    key=key,
    signature_type=0,  # EOA
)
creds_eoa = client_eoa.derive_api_key()
client_eoa.set_api_creds(creds_eoa)

args = OrderArgs(token_id=token_id, price=0.10, size=10.0, side="BUY")
opts = PartialCreateOrderOptions(tick_size="0.01", neg_risk=neg_risk)
signed_eoa = client_eoa.create_order(args, opts)
print(f"  maker={signed_eoa.order.get('maker','?')[:20]}...")
print(f"  signer={signed_eoa.order.get('signer','?')[:20]}...")
print(f"  signatureType={signed_eoa.order.get('signatureType')}")

try:
    resp1 = client_eoa.post_order(signed_eoa, OrderType.FOK)
    print(f"  Result: {resp1}")
except Exception as e:
    print(f"  Error: {e}")

## 测试 2: POLY_PROXY 模式 (sig_type=1)
print("\n=== TEST 2: POLY_PROXY mode (sig_type=1) ===")
client_proxy = ClobClient(
    host="https://clob.polymarket.com",
    chain_id=137,
    key=key,
    signature_type=1,
    funder=proxy,
)
creds_proxy = client_proxy.derive_api_key()
client_proxy.set_api_creds(creds_proxy)

# 先调用 update_balance_allowance
print("  Calling update_balance_allowance...")
try:
    ba_params = BalanceAllowanceParams(asset_type="CONDITIONAL", token_id=token_id, signature_type=1)
    ba_resp = client_proxy.update_balance_allowance(ba_params)
    print(f"  Balance/allowance response: {ba_resp}")
except Exception as e:
    print(f"  Balance/allowance error: {e}")

signed_proxy = client_proxy.create_order(args, opts)
print(f"  maker={signed_proxy.order.get('maker','?')[:20]}...")
print(f"  signer={signed_proxy.order.get('signer','?')[:20]}...")
print(f"  signatureType={signed_proxy.order.get('signatureType')}")

# 打印完整 order body 对比
print(f"\n  Full order body:")
for k, v in signed_proxy.order.items():
    val = str(v)
    if len(val) > 50:
        val = val[:50] + "..."
    print(f"    {k}: {val}")

try:
    resp2 = client_proxy.post_order(signed_proxy, OrderType.FOK)
    print(f"  Result: {resp2}")
except Exception as e:
    print(f"  Error: {e}")

## 测试 3: POLY_PROXY 但用 GTC 而不是 FOK
print("\n=== TEST 3: POLY_PROXY + GTC ===")
try:
    resp3 = client_proxy.post_order(signed_proxy, OrderType.GTC)
    print(f"  Result: {resp3}")
except Exception as e:
    print(f"  Error: {e}")
