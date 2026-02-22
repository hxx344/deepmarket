"""深度搜索 Polymarket BTC 5-min 预测市场"""
import asyncio
import aiohttp
import json

async def main():
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as s:

        # 1) Gamma events - crypto tag
        print("=== Gamma events: crypto ===")
        try:
            url = "https://gamma-api.polymarket.com/events"
            params = {"tag": "crypto", "active": "true", "closed": "false", "limit": "20"}
            async with s.get(url, params=params) as r:
                events = await r.json()
                print(f"Found {len(events)} crypto events")
                for ev in events:
                    title = ev.get("title", "")
                    slug = ev.get("slug", "")
                    n_markets = len(ev.get("markets", []))
                    # 只显示包含 bitcoin/btc 的
                    if any(kw in title.lower() for kw in ["bitcoin", "btc", "minute", "5-min", "price"]):
                        print(f"  * {title} (slug={slug}, {n_markets} markets)")
                        for m in ev.get("markets", [])[:2]:
                            print(f"    Q: {m.get('question','')}")
                            for t in m.get("tokens", []):
                                print(f"    token: {t.get('outcome')} price={t.get('price')} id={t.get('token_id','')[:20]}...")
        except Exception as e:
            print(f"Error: {e}")

        # 2) Gamma markets - text_query
        print("\n=== Gamma markets: Bitcoin 5 minute ===")
        try:
            url = "https://gamma-api.polymarket.com/markets"
            for query in ["Bitcoin above", "BTC above", "Bitcoin 5 minute", "Bitcoin price"]:
                params = {"active": "true", "closed": "false", "limit": "5"}
                # Gamma API doesn't have text search, let's try with slug_contains
                async with s.get(url, params=params) as r:
                    pass  # skip this approach
        except:
            pass

        # 3) CLOB quick-search
        print("\n=== CLOB: sampling (next few pages) ===")
        try:
            cursor = "MA=="
            btc_5min = []
            for page in range(10):
                url = f"https://clob.polymarket.com/markets"
                params = {"next_cursor": cursor}
                async with s.get(url, params=params) as r:
                    data = await r.json()
                    markets = data.get("data", data) if isinstance(data, dict) else data
                    if isinstance(data, dict):
                        cursor = data.get("next_cursor", "")
                    for m in (markets if isinstance(markets, list) else []):
                        q = m.get("question", "").lower()
                        if any(kw in q for kw in ["bitcoin above", "btc above", "bitcoin below", "bitcoin price at"]):
                            btc_5min.append(m)
                    if not cursor or cursor == "LTE=":
                        break
            print(f"Found {len(btc_5min)} BTC price prediction markets")
            for m in btc_5min[:5]:
                print(f"  Q: {m.get('question','')}")
                print(f"  condition_id: {m.get('condition_id','')}")
                print(f"  active: {m.get('active')}, closed: {m.get('closed')}, end: {m.get('end_date_iso','')}")
                for t in m.get("tokens", []):
                    print(f"    {t.get('outcome')}: token_id={t.get('token_id','')[:30]}... price={t.get('price')}")
        except Exception as e:
            print(f"CLOB search error: {e}")

        # 4) Try Gamma with slug search
        print("\n=== Gamma: slug contains bitcoin-above ===")
        try:
            url = "https://gamma-api.polymarket.com/markets"
            params = {"slug_contains": "bitcoin-above", "active": "true", "closed": "false", "limit": "5", "order": "end_date_iso", "ascending": "false"}
            async with s.get(url, params=params) as r:
                markets = await r.json()
                print(f"Found {len(markets)}")
                for m in markets[:5]:
                    print(f"  Q: {m.get('question','')}")
                    print(f"  end: {m.get('end_date_iso','')}")
                    for t in m.get("tokens", []):
                        print(f"    {t.get('outcome')}: price={t.get('price')} id={t.get('token_id','')[:20]}...")
        except Exception as e:
            print(f"Error: {e}")

        # 5) 搜索 "minute" 相关事件
        print("\n=== Gamma events: 搜索5分钟相关 ===")
        try:
            url = "https://gamma-api.polymarket.com/events"
            params = {"active": "true", "closed": "false", "limit": "50"}
            async with s.get(url, params=params) as r:
                events = await r.json()
                for ev in events:
                    title = ev.get("title", "").lower()
                    if any(kw in title for kw in ["minute", "5-min", "5min", "bitcoin price", "btc price"]):
                        print(f"  * {ev.get('title','')} ({len(ev.get('markets',[]))} markets)")
        except Exception as e:
            print(f"Error: {e}")

asyncio.run(main())
