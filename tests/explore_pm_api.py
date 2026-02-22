"""探索 Polymarket API 获取 BTC 5-min 市场数据结构"""
import asyncio
import aiohttp
import json

async def main():
    async with aiohttp.ClientSession() as session:
        # 1) Gamma API - 搜索 BTC 市场
        print("=== Gamma API: BTC markets ===")
        url = "https://gamma-api.polymarket.com/markets"
        params = {"tag": "btc", "active": "true", "closed": "false", "limit": "10"}
        try:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                markets = await resp.json()
                print(f"Found {len(markets)} markets")
                for m in markets[:5]:
                    print(f"\n  Q: {m.get('question','')}")
                    print(f"  condition_id: {m.get('condition_id','')[:30]}...")
                    print(f"  market_slug: {m.get('market_slug','')}")
                    print(f"  end_date: {m.get('end_date_iso','')}")
                    tokens = m.get("tokens", [])
                    for t in tokens:
                        tid = t.get("token_id", "")
                        print(f"  token: outcome={t.get('outcome')}, price={t.get('price')}, id={tid[:20]}...")
        except Exception as e:
            print(f"Gamma API error: {e}")

        # 2) CLOB API - 搜索市场
        print("\n\n=== CLOB API: events ===")
        try:
            url2 = "https://clob.polymarket.com/markets"
            params2 = {"next_cursor": "MA=="}
            async with session.get(url2, params=params2, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                data = await resp.json()
                all_markets = data if isinstance(data, list) else data.get("data", [])
                btc_markets = [m for m in all_markets if "bitcoin" in m.get("question", "").lower() or "btc" in m.get("question", "").lower()]
                print(f"Total markets: {len(all_markets)}, BTC-related: {len(btc_markets)}")
                for m in btc_markets[:3]:
                    print(f"\n  Q: {m.get('question','')}")
                    print(f"  condition_id: {m.get('condition_id','')[:30]}...")
                    tokens = m.get("tokens", [])
                    for t in tokens:
                        print(f"  token: outcome={t.get('outcome')}, token_id={t.get('token_id','')[:20]}...")
        except Exception as e:
            print(f"CLOB API error: {e}")

        # 3) 尝试 Gamma events API (event = 一组市场)
        print("\n\n=== Gamma API: events with btc tag ===")
        try:
            url3 = "https://gamma-api.polymarket.com/events"
            params3 = {"tag": "btc", "active": "true", "closed": "false", "limit": "5"}
            async with session.get(url3, params=params3, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                events = await resp.json()
                print(f"Found {len(events)} events")
                for ev in events[:3]:
                    print(f"\n  title: {ev.get('title','')}")
                    print(f"  slug: {ev.get('slug','')}")
                    sub_markets = ev.get("markets", [])
                    print(f"  sub-markets: {len(sub_markets)}")
                    for sm in sub_markets[:2]:
                        print(f"    Q: {sm.get('question','')}")
                        tokens = sm.get("tokens", [])
                        for t in tokens:
                            print(f"    token: outcome={t.get('outcome')}, price={t.get('price')}")
        except Exception as e:
            print(f"Events API error: {e}")

asyncio.run(main())
