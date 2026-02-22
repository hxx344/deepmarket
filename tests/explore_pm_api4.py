"""获取当前 BTC 相关活跃市场的完整数据"""
import asyncio
import aiohttp
import json

async def main():
    timeout = aiohttp.ClientTimeout(total=15)
    async with aiohttp.ClientSession(timeout=timeout) as s:

        # 1) 获取 BTC 相关的活跃 Gamma markets 详细信息
        print("=== 当前 BTC 相关活跃市场 ===")
        url = "https://gamma-api.polymarket.com/markets"
        params = {
            "active": "true",
            "closed": "false",
            "limit": "100",
            "order": "volume24hr",
            "ascending": "false",
        }
        async with s.get(url, params=params) as r:
            markets = await r.json()
            btc = [m for m in markets
                   if any(kw in m.get("question","").lower() for kw in ["bitcoin", "btc"])]
            print(f"活跃 BTC 市场: {len(btc)} 个")
            for m in btc:
                print(f"\n--- Market ---")
                q = m.get("question", "")
                print(f"  Question: {q}")
                print(f"  condition_id: {m.get('condition_id', '')}")
                print(f"  market_slug: {m.get('market_slug', '')}")
                print(f"  end_date: {m.get('end_date_iso', 'N/A')}")
                print(f"  volume24hr: {m.get('volume24hr', 0)}")
                tokens = m.get("tokens", [])
                for t in tokens:
                    oc = t.get("outcome", "?")
                    p = t.get("price", "?")
                    tid = t.get("token_id", "")
                    print(f"  Token {oc}: price={p}, id={tid}")

        # 2) 对找到的市场获取 CLOB midpoint
        print("\n\n=== CLOB midpoint 价格 ===")
        for m in btc[:3]:
            tokens = m.get("tokens", [])
            for t in tokens:
                tid = t.get("token_id", "")
                oc = t.get("outcome", "?")
                if tid:
                    try:
                        mid_url = f"https://clob.polymarket.com/midpoint"
                        params = {"token_id": tid}
                        async with s.get(mid_url, params=params) as r:
                            data = await r.json()
                            print(f"  {m.get('question','')[:50]} [{oc}]: midpoint={data.get('mid', 'N/A')}")
                    except Exception as e:
                        print(f"  Error getting midpoint: {e}")

        # 3) 检查每日/每周 BTC 短期市场 event
        print("\n\n=== 搜索 BTC daily 事件 ===")
        url = "https://gamma-api.polymarket.com/events"
        params = {"active": "true", "closed": "false", "limit": "200"}
        async with s.get(url, params=params) as r:
            events = await r.json()
            for ev in events:
                title = ev.get("title", "").lower()
                if any(kw in title for kw in ["bitcoin", "btc"]):
                    n = len(ev.get("markets", []))
                    print(f"\n  Event: {ev.get('title','')}")
                    print(f"  Slug: {ev.get('slug','')}")
                    print(f"  Sub-markets: {n}")
                    for mm in ev.get("markets", [])[:5]:
                        print(f"    Q: {mm.get('question','')}")
                        for t in mm.get("tokens", []):
                            print(f"      {t.get('outcome')}: price={t.get('price')}")

asyncio.run(main())
