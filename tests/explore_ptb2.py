"""
Try multiple approaches to get the 5-min Price to Beat:
1. Polymarket CLOB /market endpoint for detailed market data
2. Polymarket data API
3. Chainlink data feed historical price
4. Buffer-based: use our own RTDS recent prices
"""
import asyncio, aiohttp, json, datetime, time

GAMMA = "https://gamma-api.polymarket.com"
CLOB  = "https://clob.polymarket.com"

async def main():
    async with aiohttp.ClientSession() as s:
        now_utc = datetime.datetime.utcnow()
        now_et = now_utc - datetime.timedelta(hours=5)
        minute = now_et.minute
        ws = (minute // 5) * 5
        window_start_et = now_et.replace(minute=ws, second=0, microsecond=0)
        window_start_utc = window_start_et + datetime.timedelta(hours=5)
        ts = int(window_start_utc.replace(tzinfo=datetime.timezone.utc).timestamp())

        # Use a recently closed window so we can see the settled price
        prev_ts = ts - 300
        slug = f"btc-updown-5m-{prev_ts}"
        print(f"Searching for previous window: {slug} (ts={prev_ts})")

        # Get market from Gamma
        async with s.get(f"{GAMMA}/events", params={"slug": slug}) as r:
            events = await r.json()
        if not events:
            slug = f"btc-updown-5m-{ts}"
            print(f"Trying current: {slug}")
            async with s.get(f"{GAMMA}/events", params={"slug": slug}) as r:
                events = await r.json()

        if not events:
            print("No events found")
            return

        ev = events[0]
        m = ev["markets"][0] if ev.get("markets") else None
        if not m:
            print("No market in event")
            return

        condition_id = m.get("conditionId", "")
        question = m.get("question", "")
        event_start = m.get("eventStartTime", "")
        print(f"Question: {question}")
        print(f"conditionId: {condition_id}")
        print(f"eventStartTime: {event_start}")

        # ── Approach 1: CLOB /market endpoint ──
        print("\n=== Approach 1: CLOB /market ===")
        try:
            async with s.get(f"{CLOB}/markets/{condition_id}") as r:
                if r.status == 200:
                    data = await r.json()
                    print(f"Keys: {sorted(data.keys()) if isinstance(data, dict) else type(data)}")
                    if isinstance(data, dict):
                        for k in sorted(data.keys()):
                            print(f"  {k}: {str(data[k])[:200]}")
                else:
                    text = await r.text()
                    print(f"Status {r.status}: {text[:200]}")
        except Exception as e:
            print(f"Error: {e}")

        # ── Approach 2: CLOB /neg-risk/market ──
        print("\n=== Approach 2: CLOB /neg-risk/market ===")
        try:
            async with s.get(f"{CLOB}/neg-risk/market/{condition_id}") as r:
                if r.status == 200:
                    data = await r.json()
                    print(f"Keys: {sorted(data.keys()) if isinstance(data, dict) else type(data)}")
                    if isinstance(data, dict):
                        for k in sorted(data.keys()):
                            print(f"  {k}: {str(data[k])[:200]}")
                else:
                    text = await r.text()
                    print(f"Status {r.status}: {text[:200]}")
        except Exception as e:
            print(f"Error: {e}")

        # ── Approach 3: Polymarket Data API ──
        print("\n=== Approach 3: Polymarket Data API ===")
        try:
            data_url = f"https://data-api.polymarket.com/markets/{condition_id}"
            async with s.get(data_url) as r:
                if r.status == 200:
                    data = await r.json()
                    print(f"Keys: {sorted(data.keys()) if isinstance(data, dict) else type(data)}")
                    if isinstance(data, dict):
                        for k in sorted(data.keys()):
                            print(f"  {k}: {str(data[k])[:200]}")
                else:
                    print(f"Status {r.status}: {(await r.text())[:200]}")
        except Exception as e:
            print(f"Error: {e}")

        # ── Approach 4: Polymarket Strapi API ──
        print("\n=== Approach 4: Polymarket Strapi ===")
        try:
            strapi_url = f"https://strapi-matic.polymarket.com/markets?conditionId={condition_id}"
            async with s.get(strapi_url) as r:
                if r.status == 200:
                    data = await r.json()
                    if isinstance(data, list) and data:
                        item = data[0]
                        print(f"Keys: {sorted(item.keys())}")
                        for k in sorted(item.keys()):
                            print(f"  {k}: {str(item[k])[:200]}")
                    else:
                        print(f"Response: {str(data)[:500]}")
                else:
                    print(f"Status {r.status}: {(await r.text())[:200]}")
        except Exception as e:
            print(f"Error: {e}")

        # ── Approach 5: Chainlink data.chain.link API ──
        print("\n=== Approach 5: Chainlink Data Feed API ===")
        try:
            # Try the data.chain.link REST API
            cl_url = "https://data.chain.link/api/query"
            # Try getting historical data
            for endpoint in [
                f"https://data.chain.link/api/feed-data/9/btc-usd",
                f"https://data.chain.link/api/streams/btc-usd",
                f"https://data.chain.link/streams/btc-usd",
            ]:
                try:
                    async with s.get(endpoint, timeout=aiohttp.ClientTimeout(total=5)) as r:
                        ct = r.headers.get("content-type", "")
                        if r.status == 200 and "json" in ct:
                            data = await r.json()
                            print(f"  {endpoint}: {str(data)[:500]}")
                        else:
                            text = await r.text()
                            print(f"  {endpoint}: status={r.status}, ct={ct}, len={len(text)}")
                except Exception as e:
                    print(f"  {endpoint}: {e}")
        except Exception as e:
            print(f"Error: {e}")

        # ── Approach 6: Polymarket /prices endpoint ──
        print("\n=== Approach 6: CLOB /prices ===")
        try:
            token_ids = m.get("clobTokenIds", "[]")
            if isinstance(token_ids, str):
                token_ids = json.loads(token_ids)
            if token_ids:
                async with s.get(f"{CLOB}/prices", params={"token_ids": token_ids[0]}) as r:
                    if r.status == 200:
                        data = await r.json()
                        print(f"Response: {str(data)[:500]}")
                    else:
                        print(f"Status {r.status}: {(await r.text())[:200]}")
        except Exception as e:
            print(f"Error: {e}")

        # ── Approach 7: Try event-level resolution data ──
        print("\n=== Approach 7: Gamma event resolution ===")
        try:
            event_id = ev.get("id", "")
            async with s.get(f"{GAMMA}/events/{event_id}") as r:
                if r.status == 200:
                    data = await r.json()
                    # look for price-related fields
                    interesting = {k: v for k, v in data.items()
                                   if any(w in k.lower() for w in ["price", "resolut", "anchor", "start", "beat", "oracle"])}
                    print(f"Interesting fields: {interesting}")
                else:
                    print(f"Status {r.status}")
        except Exception as e:
            print(f"Error: {e}")

asyncio.run(main())
