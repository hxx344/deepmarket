"""检查 signed order 各字段"""
from dotenv import load_dotenv
load_dotenv()
import os, json, requests
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, PartialCreateOrderOptions, OrderType

key = os.environ.get("PM_PRIVATE_KEY", "")
proxy = os.environ.get("PM_PROXY_ADDRESS", "")

resp = requests.get("https://gamma-api.polymarket.com/markets?active=true&closed=false&limit=1", timeout=10)
m = resp.json()[0]
tokens = json.loads(m["clobTokenIds"]) if isinstance(m["clobTokenIds"], str) else m["clobTokenIds"]
token_id = tokens[0]
neg_risk = m.get("negRisk", False)
print(f"neg_risk={neg_risk}, token={token_id[:20]}...")

args = OrderArgs(token_id=token_id, price=0.10, size=10.0, side="BUY")
opts = PartialCreateOrderOptions(tick_size="0.01", neg_risk=neg_risk)

# POLY_PROXY client
client = ClobClient(
    host="https://clob.polymarket.com",
    chain_id=137,
    key=key,
    signature_type=1,
    funder=proxy,
)
creds = client.derive_api_key()
client.set_api_creds(creds)

signed = client.create_order(args, opts)
o = signed.order
vals = o.values

print("\nOrder fields:")
for k in ["salt", "maker", "signer", "taker", "tokenId", "makerAmount",
           "takerAmount", "expiration", "nonce", "feeRateBps", "side", "signatureType"]:
    v = vals[k]
    print(f"  {k}: {v}")

print(f"\nSignature: {signed.signature}")
print(f"Sig length: {len(signed.signature)} chars")

# 检查 order_to_json 输出
from py_clob_client.utilities import order_to_json
body = order_to_json(signed, creds.api_key, OrderType.FOK, False)
print(f"\nJSON body:\n{json.dumps(body, indent=2)}")

# 尝试 post
print("\n=== Posting order... ===")
try:
    result = client.post_order(signed, OrderType.FOK)
    print(f"Result: {result}")
except Exception as e:
    print(f"Error: {type(e).__name__}: {e}")

# 也尝试 EOA 模式
print("\n=== TEST: EOA mode (sig_type=0) ===")
client_eoa = ClobClient(
    host="https://clob.polymarket.com",
    chain_id=137,
    key=key,
    signature_type=0,
)
creds_eoa = client_eoa.derive_api_key()
client_eoa.set_api_creds(creds_eoa)

signed_eoa = client_eoa.create_order(args, opts)
try:
    result_eoa = client_eoa.post_order(signed_eoa, OrderType.FOK)
    print(f"EOA Result: {result_eoa}")
except Exception as e:
    print(f"EOA Error: {type(e).__name__}: {e}")
