"""
Deep search for Polymarket 5-minute BTC markets.
Try CLOB API, different Gamma search strategies, and direct URL patterns.
"""
import asyncio, aiohttp, json, datetime

GAMMA = "https://gamma-api.polymarket.com"
CLOB  = "https://clob.polymarket.com"

async def main():
    async with aiohttp.ClientSession() as s:
        # Strategy 1: Search Gamma markets with various text queries
        print("=== Strategy 1: Gamma markets text search ===")
        queries = [
            "Bitcoin Up or Down",
            "Bitcoin 5 min",
            "BTC 5 min",
            "Bitcoin Up or Down - 5 min",
            "bitcoin-5-minute",
        ]
        for q in queries:
            url = f"{GAMMA}/markets?active=true&closed=false&limit=10"
            async with s.get(url, params={"tag": "crypto", "textQuery": q}) as r:
                data = await r.json()
                print(f"  textQuery='{q}': {len(data)} results")
                for m in data[:3]:
                    print(f"    {m.get('question','?')[:80]}")
        
        # Strategy 2: Search Gamma events with text query
        print("\n=== Strategy 2: Gamma events text search ===")
        event_queries = [
            "Bitcoin Up or Down",
            "Bitcoin 5 minute",
            "BTC Up or Down",
            "5 min",
        ]
        for q in event_queries:
            url = f"{GAMMA}/events?active=true&closed=false&limit=20"
            async with s.get(url, params={"textQuery": q}) as r:
                data = await r.json()
                print(f"  textQuery='{q}': {len(data)} events")
                for e in data[:5]:
                    title = e.get('title', '?')
                    slug = e.get('slug', '')
                    n = len(e.get('markets', []))
                    print(f"    [{n} mkts] {title[:80]} (slug={slug})")
                    # If it has many sub-markets, show first few
                    if n > 5:
                        for m in e.get('markets', [])[:3]:
                            print(f"      sub: {m.get('question','?')[:80]}")

        # Strategy 3: Try CLOB /markets with next_cursor pagination to find BTC markets
        print("\n=== Strategy 3: CLOB API /markets search ===")
        url = f"{CLOB}/markets"
        async with s.get(url, params={"limit": "100"}) as r:
            text = await r.text()
            try:
                data = json.loads(text)
                if isinstance(data, list):
                    btc_mkts = [m for m in data if 'bitcoin' in str(m).lower() or 'btc' in str(m).lower()]
                    five_min = [m for m in data if '5 min' in str(m).lower() or '5-min' in str(m).lower()]
                    print(f"  Total: {len(data)}, BTC-related: {len(btc_mkts)}, 5-min: {len(five_min)}")
                    for m in five_min[:5]:
                        q = m.get('question', m.get('description', '?'))
                        print(f"    {str(q)[:100]}")
                elif isinstance(data, dict):
                    items = data.get('data', data.get('markets', data.get('results', [])))
                    next_cursor = data.get('next_cursor', '')
                    print(f"  Got dict with {len(items)} items, next_cursor={next_cursor}")
                    btc_items = [m for m in items if 'bitcoin' in str(m).lower() or 'btc' in str(m).lower()]
                    five_min = [m for m in items if '5 min' in str(m).lower() or '5-min' in str(m).lower()]
                    print(f"  BTC-related: {len(btc_items)}, 5-min: {len(five_min)}")
                    for m in five_min[:5]:
                        print(f"    {str(m)[:200]}")
                    # Show first BTC item structure
                    if btc_items:
                        print(f"  First BTC item keys: {list(btc_items[0].keys()) if isinstance(btc_items[0], dict) else type(btc_items[0])}")
                        print(f"  First BTC: {str(btc_items[0])[:300]}")
            except:
                print(f"  Raw response: {text[:500]}")

        # Strategy 4: Try fetching large number of Gamma markets sorted by start_date
        print("\n=== Strategy 4: Gamma markets - most recently created ===")
        url = f"{GAMMA}/markets?active=true&closed=false&limit=100&order=startDate&ascending=false"
        async with s.get(url) as r:
            data = await r.json()
            five_min = [m for m in data if '5 min' in m.get('question','').lower()]
            btc_up = [m for m in data if 'bitcoin up or down' in m.get('question','').lower()]
            print(f"  Total: {len(data)}, '5 min': {len(five_min)}, 'bitcoin up or down': {len(btc_up)}")
            for m in (five_min + btc_up)[:10]:
                print(f"    {m.get('question','?')[:100]}")
                print(f"      startDate={m.get('startDate','?')} endDate={m.get('endDate','?')}")

        # Strategy 5: Try Gamma with tag=crypto and look for high-volume short-lived markets  
        print("\n=== Strategy 5: Gamma crypto markets with end dates ===")
        url = f"{GAMMA}/markets?active=true&closed=false&limit=200&tag=crypto"
        async with s.get(url) as r:
            data = await r.json()
            print(f"  Total crypto markets: {len(data)}")
            # Find markets ending today or tomorrow
            now = datetime.datetime.utcnow()
            short_lived = []
            for m in data:
                end = m.get('endDate', '')
                start = m.get('startDate', '')
                q = m.get('question', '')
                if end:
                    try:
                        end_dt = datetime.datetime.fromisoformat(end.replace('Z','+00:00'))
                        delta = end_dt.replace(tzinfo=None) - now
                        if delta.total_seconds() < 86400:  # ends within 24 hours
                            short_lived.append((delta, q, start, end, m.get('conditionId','')))
                    except:
                        pass
            short_lived.sort()
            print(f"  Markets ending within 24h: {len(short_lived)}")
            for delta, q, start, end, cid in short_lived[:20]:
                print(f"    [{delta}] {q[:80]}")
                print(f"      start={start} end={end} cid={cid[:20]}...")

        # Strategy 6: Try different Gamma API params
        print("\n=== Strategy 6: Gamma with various params ===")
        # Try tag_id for crypto
        test_urls = [
            f"{GAMMA}/markets?active=true&limit=20&tag_id=crypto&textQuery=Bitcoin+Up",
            f"{GAMMA}/markets?limit=20&textQuery=Bitcoin+Up+or+Down+5+min",
            f"{GAMMA}/markets?limit=20&slug_contains=bitcoin-up-or-down",
            f"{GAMMA}/events?limit=20&slug_contains=bitcoin-up-or-down",
            f"{GAMMA}/events?limit=20&textQuery=bitcoin+up+or+down",
        ]
        for url in test_urls:
            async with s.get(url) as r:
                data = await r.json()
                print(f"  {url.split('?')[1][:60]}: {len(data)} results")
                for item in data[:3]:
                    t = item.get('title', item.get('question', '?'))
                    print(f"    {t[:80]}")

        # Strategy 7: Fetch webpage to find the right slug
        print("\n=== Strategy 7: Try known Polymarket URL patterns ===")
        known_slugs = [
            "bitcoin-up-or-down-5-min",
            "btc-5-min", 
            "bitcoin-5-minute",
            "bitcoin-up-or-down-5-minute",
        ]
        for slug in known_slugs:
            url = f"{GAMMA}/events?slug={slug}"
            async with s.get(url) as r:
                data = await r.json()
                print(f"  slug={slug}: {len(data)} events")
                if data:
                    print(f"    {json.dumps(data[0], indent=2)[:300]}")

        # Strategy 8: Try to search for recently resolved markets
        print("\n=== Strategy 8: Recently closed markets (might find past 5min) ===")
        url = f"{GAMMA}/markets?closed=true&limit=50&order=endDate&ascending=false"
        async with s.get(url, params={"textQuery": "Bitcoin Up or Down"}) as r:
            data = await r.json()
            print(f"  Closed 'Bitcoin Up or Down' markets: {len(data)}")
            for m in data[:10]:
                print(f"    {m.get('question','?')[:100]}")
                print(f"      end={m.get('endDate','')} cid={m.get('conditionId','')[:20]}")

asyncio.run(main())
