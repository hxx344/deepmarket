"""端到端测试: 使用修改后的 ClobApiClient (sig_type=2) 下单"""
import asyncio
from dotenv import load_dotenv
load_dotenv()

import sys, os, json
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.trading.clob_api import ClobApiClient
import requests

async def main():
    # 获取市场
    resp = requests.get(
        "https://gamma-api.polymarket.com/markets?active=true&closed=false&limit=1",
        timeout=10,
    )
    m = resp.json()[0]
    tokens = json.loads(m["clobTokenIds"]) if isinstance(m["clobTokenIds"], str) else m["clobTokenIds"]
    token_id = tokens[0]
    neg_risk = m.get("negRisk", False)
    question = m.get("question", "?")
    print(f"Market: {question[:60]}")
    print(f"neg_risk={neg_risk}")

    # 初始化 client (应自动选择 sig_type=2)
    key = os.environ.get("PM_PRIVATE_KEY", "")
    client = ClobApiClient(private_key=key)
    
    print(f"\nSignature type: {client._sig_type} (expect 2=GNOSIS_SAFE)")
    print(f"Funder: {client._funder}")
    
    ok = await client.initialize()
    print(f"Init: {'OK' if ok else 'FAILED'}")
    if not ok:
        return

    # FOK 买入 - 低价确保不会实际成交太多
    print("\n=== FOK BUY 0.05 x 20 tokens ===")
    result = await client.place_order(
        token_id=token_id,
        price=0.05,
        size=20.0,
        side="BUY",
        order_type="FOK",
        neg_risk=neg_risk,
    )
    print(f"Result: success={result.success}, order_id={result.order_id}, error={result.error}")
    if result.raw:
        print(f"Raw: {json.dumps(result.raw, indent=2)}")

    # GTC 挂单测试 - 极低价确保不成交, 然后撤单
    print("\n=== GTC BUY 0.01 x 50 tokens (then cancel) ===")
    result2 = await client.place_order(
        token_id=token_id,
        price=0.01,
        size=100.0,
        side="BUY",
        order_type="GTC",
        neg_risk=neg_risk,
    )
    print(f"Result: success={result2.success}, order_id={result2.order_id}, error={result2.error}")
    if result2.raw:
        print(f"Raw: {json.dumps(result2.raw, indent=2)}")

    if result2.success and result2.order_id:
        # 查询状态
        status = await client.get_order_status(result2.order_id)
        print(f"Status: {status.status}")
        
        # 撤单
        cancelled = await client.cancel_order(result2.order_id)
        print(f"Cancelled: {cancelled}")

asyncio.run(main())
