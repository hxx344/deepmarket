"""探索 Polymarket 5-min BTC 市场的 API 数据结构"""
import asyncio
import aiohttp
import json
from datetime import datetime, timezone, timedelta

async def main():
    timeout = aiohttp.ClientTimeout(total=15)
    async with aiohttp.ClientSession(timeout=timeout) as s:

        # 1) Gamma events: 搜索 "5 min" 或 "5-min" 相关事件
        print("=== Gamma events: 搜索 5min BTC ===")
        url = "https://gamma-api.polymarket.com/events"
        params = {"active": "true", "closed": "false", "limit": "200"}
        async with s.get(url, params=params) as r:
            events = await r.json()
            btc_5min_events = []
            for ev in events:
                title = ev.get("title", "")
                tl = title.lower()
                if "5 min" in tl or "5-min" in tl:
                    btc_5min_events.append(ev)
                    n = len(ev.get("markets", []))
                    print(f"\n  Event: {title}")
                    print(f"  Slug: {ev.get('slug', '')}")
                    print(f"  Sub-markets: {n}")
                    # 打印前几个子市场
                    for mm in ev.get("markets", [])[:3]:
                        q = mm.get("question", "")
                        prices = mm.get("outcomePrices", "[]")
                        closed = mm.get("closed", False)
                        active = mm.get("active", False)
                        end = mm.get("endDate", "")
                        print(f"    Q: {q}")
                        print(f"    active={active}, closed={closed}, end={end}")
                        print(f"    prices={prices}")
                    if n > 5:
                        print(f"    ... and {n - 3} more")

        # 2) 如果找到了5min event, 打印完整的第一个子市场
        if btc_5min_events:
            ev = btc_5min_events[0]
            markets = ev.get("markets", [])
            # 找到 active 且 未 closed 的
            active_markets = [m for m in markets if m.get("active") and not m.get("closed")]
            print(f"\n\n=== 5min event 活跃子市场: {len(active_markets)} 个 ===")
            for m in active_markets[:5]:
                q = m.get("question", "")
                cid = m.get("conditionId", "")
                tokens = m.get("clobTokenIds", "[]")
                prices = m.get("outcomePrices", "[]")
                end = m.get("endDate", "")
                outcomes = m.get("outcomes", "[]")
                print(f"\n  Q: {q}")
                print(f"  conditionId: {cid}")
                print(f"  endDate: {end}")
                print(f"  outcomes: {outcomes}")
                print(f"  prices: {prices}")
                print(f"  clobTokenIds: {tokens}")

            # 打印所有子市场的 question 列表
            print(f"\n\n=== 所有子市场 questions ===")
            for i, m in enumerate(markets):
                q = m.get("question", "")
                closed = m.get("closed", False)
                active = m.get("active", False)
                tag = "ACTIVE" if active and not closed else ("CLOSED" if closed else "inactive")
                print(f"  [{i}] [{tag}] {q}")
                if i > 30:
                    print(f"  ... total {len(markets)}")
                    break

        # 3) 也搜索 Gamma markets 直接
        print(f"\n\n=== Gamma markets: 搜索 '5 min' ===")
        url = "https://gamma-api.polymarket.com/markets"
        params = {
            "active": "true",
            "closed": "false",
            "limit": "20",
            "order": "endDate",
            "ascending": "true",
        }
        async with s.get(url, params=params) as r:
            markets = await r.json()
            fivemin = [m for m in markets if "5 min" in m.get("question", "").lower()]
            print(f"Found {len(fivemin)} '5 min' markets")
            for m in fivemin[:5]:
                q = m.get("question", "")
                end = m.get("endDate", "")
                prices = m.get("outcomePrices", "[]")
                print(f"  Q: {q}")
                print(f"  end: {end}, prices: {prices}")

asyncio.run(main())
