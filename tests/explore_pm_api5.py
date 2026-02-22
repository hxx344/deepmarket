"""获取 BTC 市场完整 token 数据"""
import asyncio
import aiohttp
import json

async def main():
    timeout = aiohttp.ClientTimeout(total=15)
    async with aiohttp.ClientSession(timeout=timeout) as s:

        # 1) 直接从 Gamma API 获取完整字段
        print("=== Gamma markets: 完整字段 ===")
        url = "https://gamma-api.polymarket.com/markets"
        params = {
            "active": "true",
            "closed": "false",
            "limit": "5",
            "order": "volume24hr",
            "ascending": "false",
        }
        async with s.get(url, params=params) as r:
            markets = await r.json()
            btc = [m for m in markets if "bitcoin" in m.get("question","").lower()]
            if btc:
                m = btc[0]
                print(f"Question: {m.get('question','')}")
                print(f"所有字段key: {list(m.keys())}")
                print(f"\n完整JSON (前2000字符):")
                print(json.dumps(m, indent=2, default=str)[:2000])

        # 2) 搜索 events 并获取子市场的完整数据
        print("\n\n=== Gamma events with markets detail ===")
        url = "https://gamma-api.polymarket.com/events"
        params = {"active": "true", "closed": "false", "limit": "200"}
        async with s.get(url, params=params) as r:
            events = await r.json()
            btc_ev = [e for e in events if "bitcoin" in e.get("title", "").lower() and "price" in e.get("title", "").lower()]
            if not btc_ev:
                # fallback: any btc event with > 3 markets
                btc_ev = [e for e in events if "bitcoin" in e.get("title","").lower() and len(e.get("markets", [])) > 3]
            for ev in btc_ev[:2]:
                print(f"\nEvent: {ev.get('title','')}")
                print(f"Event keys: {list(ev.keys())}")
                sub = ev.get("markets", [])
                if sub:
                    print(f"子市场数: {len(sub)}")
                    # 打印第一个子市场的完整数据
                    m0 = sub[0]
                    print(f"子市场0 所有字段: {list(m0.keys())}")
                    print(json.dumps(m0, indent=2, default=str)[:2000])

        # 3) 使用 CLOB /simplified-markets 端点
        print("\n\n=== CLOB: /simplified-markets ===")
        try:
            url = "https://clob.polymarket.com/simplified-markets"
            params = {"next_cursor": "MA=="}
            async with s.get(url, params=params) as r:
                data = await r.json()
                print(f"Response type: {type(data)}")
                if isinstance(data, dict):
                    print(f"Keys: {list(data.keys())}")
                    items = data.get("data", [])
                    btc_items = [m for m in items if "bitcoin" in m.get("question","").lower()]
                    print(f"Total: {len(items)}, BTC: {len(btc_items)}")
                    for m in btc_items[:2]:
                        print(json.dumps(m, indent=2, default=str)[:1000])
        except Exception as e:
            print(f"Error: {e}")

        # 4) 搜索 Gamma market by slug that contains "bitcoin-above"
        print("\n\n=== Gamma: bitcoin-above 市场 ===")
        try:
            # 直接搜索 question_contains
            url = "https://gamma-api.polymarket.com/markets"
            params = {
                "active": "true",
                "closed": "false",
                "limit": "3",
                "order": "volume24hr",
                "ascending": "false",
            }
            async with s.get(url, params=params) as r:
                markets = await r.json()
                btc70k = [m for m in markets if "70,000" in m.get("question","")]
                if btc70k:
                    m = btc70k[0]
                    print(f"Question: {m['question']}")
                    print(f"condition_id: '{m.get('condition_id', '')}'")
                    print(f"clob_token_ids: '{m.get('clob_token_ids', '')}'")
                    print(f"clobTokenIds: '{m.get('clobTokenIds', '')}'")
                    # print all keys and their non-empty values
                    for k, v in m.items():
                        if v and str(v).strip():
                            sv = str(v)[:100]
                            print(f"  {k}: {sv}")
        except Exception as e:
            print(f"Error: {e}")

asyncio.run(main())
