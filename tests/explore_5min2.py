"""更广泛搜索 Polymarket 5-min BTC 市场"""
import asyncio
import aiohttp
import json

async def main():
    timeout = aiohttp.ClientTimeout(total=15)
    async with aiohttp.ClientSession(timeout=timeout) as s:

        # 1) 搜索所有 events 带 bitcoin/btc, 看有没有5min
        print("=== 搜索所有 BTC events (前 500) ===")
        all_btc = []
        for offset in range(0, 500, 100):
            url = "https://gamma-api.polymarket.com/events"
            params = {"active": "true", "closed": "false", "limit": "100", "offset": str(offset)}
            async with s.get(url, params=params) as r:
                events = await r.json()
                if not events:
                    break
                for ev in events:
                    title = ev.get("title", "").lower()
                    if "bitcoin" in title or "btc" in title:
                        all_btc.append(ev)

        print(f"Total active BTC events: {len(all_btc)}")
        for ev in all_btc:
            title = ev.get("title", "")
            slug = ev.get("slug", "")
            n = len(ev.get("markets", []))
            print(f"  [{n} mkts] {title} (slug={slug})")

        # 2) 找到 "up or down" event, 深入看子市场
        up_down_events = [e for e in all_btc if "up or down" in e.get("title","").lower()]
        print(f"\n\n=== Up or Down events: {len(up_down_events)} ===")
        for ev in up_down_events:
            title = ev.get("title", "")
            markets = ev.get("markets", [])
            print(f"\nEvent: {title} ({len(markets)} markets)")
            # 打印所有子市场
            for i, m in enumerate(markets):
                q = m.get("question", "")
                closed = m.get("closed", False)
                active = m.get("active", False)
                tag = "LIVE" if active and not closed else "CLOSED" if closed else "?"
                print(f"  [{tag}] {q}")
                if i >= 20:
                    print(f"  ... and {len(markets) - 20} more")
                    break

        # 3) 搜索 "5 min" 或 "minute" 在 markets API
        print(f"\n\n=== Gamma markets: broad search ===")
        for search_term in ["5 min", "minute", "5-min"]:
            url = "https://gamma-api.polymarket.com/markets"
            # 尝试 slug 搜索
            params = {"active": "true", "closed": "false", "limit": "5"}
            async with s.get(url, params=params) as r:
                markets = await r.json()
                found = [m for m in markets if search_term.lower() in m.get("question","").lower() or search_term.lower() in m.get("slug","").lower()]
                if found:
                    print(f"  '{search_term}': found {len(found)}")
                    for m in found[:3]:
                        print(f"    {m.get('question','')}")

        # 4) 用 slug 搜索
        print(f"\n\n=== Gamma markets: slug search ===")
        for slug_part in ["5-min", "5min", "bitcoin-up-or-down-5", "btc-5-min"]:
            url = "https://gamma-api.polymarket.com/markets"
            params = {"slug_contains": slug_part, "limit": "5"}
            async with s.get(url, params=params) as r:
                data = await r.json()
                if data:
                    print(f"  slug_contains='{slug_part}': {len(data)} results")
                    for m in data[:3]:
                        print(f"    {m.get('question','')}")

        # 5) 用 events slug 搜索
        print(f"\n\n=== Gamma events: slug search ===")
        for slug_part in ["5-min", "5min", "bitcoin-5", "btc-5-min", "bitcoin-up-or-down-5"]:
            url = "https://gamma-api.polymarket.com/events"
            params = {"slug": slug_part}
            async with s.get(url, params=params) as r:
                data = await r.json()
                if data:
                    print(f"  slug='{slug_part}': {len(data)} results")
                    for ev in data[:2]:
                        print(f"    {ev.get('title','')}")

        # 6) 试试搜索所有 BTC "up or down" 的 daily event 里是否嵌套 5min
        if up_down_events:
            ev = up_down_events[0]
            markets = ev.get("markets", [])
            five_min = [m for m in markets if "5" in m.get("question","") and "min" in m.get("question","").lower()]
            print(f"\n\n=== '5 min' in up-or-down sub-markets: {len(five_min)} ===")
            for m in five_min[:5]:
                print(f"  Q: {m.get('question','')}")

        # 7) 直接请求一个已知可能的 slug
        print(f"\n\n=== 直接请求特定 event slugs ===")
        for slug in [
            "bitcoin-up-or-down-5-min",
            "bitcoin-5-minute",
            "btc-5-min-february-15",
            "bitcoin-up-or-down-5-min-february-15",
        ]:
            url = f"https://gamma-api.polymarket.com/events?slug={slug}"
            async with s.get(url) as r:
                data = await r.json()
                if data:
                    print(f"  slug={slug}: found!")
                    if isinstance(data, list) and data:
                        print(f"    title: {data[0].get('title','')}")
                        print(f"    markets: {len(data[0].get('markets',[]))}")

        # 8) 搜索 negRisk events (5-min markets 通常在 negRisk event 下)
        print(f"\n\n=== Gamma events: negRisk with BTC ===")
        url = "https://gamma-api.polymarket.com/events"
        params = {"active": "true", "closed": "false", "limit": "200", "tag": "crypto"}
        async with s.get(url, params=params) as r:
            events = await r.json()
            for ev in events:
                title = ev.get("title","")
                neg = ev.get("negRisk", False)
                n = len(ev.get("markets",[]))
                if n > 10 or "5" in title:
                    print(f"  [negRisk={neg}] [{n} mkts] {title}")

asyncio.run(main())
