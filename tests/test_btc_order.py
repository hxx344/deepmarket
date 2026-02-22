"""用 BTC 5-min 市场的真实 token (negRisk=False) 测试下单"""
import asyncio
from dotenv import load_dotenv
load_dotenv()
import os, json, requests, time

# 1. 获取当前 BTC 5-min 窗口
now = time.time()
window_start = int(now // 300) * 300
slug = f"btc-updown-5m-{window_start}"

r = requests.get(
    "https://gamma-api.polymarket.com/events",
    params={"slug": slug},
    timeout=10,
)
events = r.json()
if not events:
    window_start -= 300
    slug = f"btc-updown-5m-{window_start}"
    r = requests.get(
        "https://gamma-api.polymarket.com/events",
        params={"slug": slug},
        timeout=10,
    )
    events = r.json()

event = events[0]
market = event["markets"][0]

tokens = market.get("clobTokenIds", "[]")
if isinstance(tokens, str):
    tokens = json.loads(tokens)
neg_risk = market.get("negRisk", False)
question = market.get("question", "?")

print(f"Market: {question}")
print(f"negRisk: {neg_risk}")
print(f"Token 0 (Up): {tokens[0][:30]}...")
print(f"Token 1 (Dn): {tokens[1][:30]}..." if len(tokens) > 1 else "No token 1")

# 2. 用 ClobApiClient 下单
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.trading.clob_api import ClobApiClient

async def main():
    key = os.environ.get("PM_PRIVATE_KEY", "")
    client = ClobApiClient(private_key=key)
    
    ok = await client.initialize()
    print(f"\nInit: {'OK' if ok else 'FAILED'}")
    if not ok:
        return

    # FOK BUY token[0] (Up) at low price
    print(f"\n=== FOK BUY Up @ 0.05 (neg_risk={neg_risk}) ===")
    result = await client.place_order(
        token_id=tokens[0],
        price=0.05,
        size=20.0,
        side="BUY",
        order_type="FOK",
        neg_risk=neg_risk,  # ← False for BTC 5-min
    )
    print(f"Result: success={result.success}, error={result.error}")
    if result.raw:
        print(f"Raw: {json.dumps(result.raw, indent=2)}")

    # FOK BUY token[1] (Down) 
    if len(tokens) > 1:
        print(f"\n=== FOK BUY Down @ 0.05 (neg_risk={neg_risk}) ===")
        result2 = await client.place_order(
            token_id=tokens[1],
            price=0.05,
            size=20.0,
            side="BUY",
            order_type="FOK",
            neg_risk=neg_risk,
        )
        print(f"Result: success={result2.success}, error={result2.error}")
        if result2.raw:
            print(f"Raw: {json.dumps(result2.raw, indent=2)}")

asyncio.run(main())
