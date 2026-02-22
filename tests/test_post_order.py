"""测试 CLOB 下单全流程"""
from dotenv import load_dotenv
load_dotenv()
import os, json

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

# derive creds
creds = client.derive_api_key()
client.set_api_creds(creds)
print(f"API key OK: {creds.api_key[:12]}...")

# 用当前活跃市场
import requests
resp = requests.get("https://gamma-api.polymarket.com/markets?active=true&closed=false&limit=1", timeout=10)
m = resp.json()[0]
tokens = json.loads(m["clobTokenIds"]) if isinstance(m["clobTokenIds"], str) else m["clobTokenIds"]
token_id = tokens[0]
neg_risk = m.get("negRisk", False)
question = m.get("question", "?")
print(f"Market: {question[:50]}")
print(f"neg_risk={neg_risk}, token={token_id[:20]}...")

# create order
args = OrderArgs(token_id=token_id, price=0.10, size=1.0, side="BUY")
opts = PartialCreateOrderOptions(tick_size="0.01", neg_risk=neg_risk)
signed = client.create_order(args, opts)
print("Order signed OK")

# post order
try:
    resp2 = client.post_order(signed, OrderType.FOK)
    print(f"Post response: {resp2}")
except Exception as e:
    print(f"Post FAILED: {type(e).__name__}: {e}")

    # 手动拆解请求, 直接用 httpx
    from py_clob_client.utilities import order_to_json
    from py_clob_client.headers.headers import create_level_2_headers
    from py_clob_client.clob_types import RequestArgs
    from py_clob_client.endpoints import POST_ORDER

    body = order_to_json(signed, creds.api_key, OrderType.FOK, False)
    serialized = json.dumps(body, separators=(",", ":"), ensure_ascii=False)

    request_args = RequestArgs(
        method="POST",
        request_path=POST_ORDER,
        body=body,
        serialized_body=serialized,
    )
    headers = create_level_2_headers(client.signer, client.creds, request_args)
    print(f"\nHeaders: {json.dumps(dict(headers), indent=2)}")
    print(f"Body ({len(serialized)} bytes): {serialized[:200]}...")
    print(f"Endpoint: https://clob.polymarket.com{POST_ORDER}")

    # 直接用 httpx 发
    import httpx
    try:
        r = httpx.post(
            f"https://clob.polymarket.com{POST_ORDER}",
            headers=headers,
            content=serialized.encode("utf-8"),
            timeout=10,
        )
        print(f"\nManual POST: {r.status_code} {r.text[:300]}")
    except Exception as e2:
        print(f"\nManual POST also failed: {type(e2).__name__}: {e2}")
