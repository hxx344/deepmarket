"""
Verify full lifecycle: discover current 5-min window -> get prices -> show rotation timing.
"""
import asyncio, aiohttp, json, datetime, time

GAMMA = "https://gamma-api.polymarket.com"
CLOB  = "https://clob.polymarket.com"

async def main():
    async with aiohttp.ClientSession() as s:
        now_utc = datetime.datetime.utcnow()
        now_et = now_utc - datetime.timedelta(hours=5)
        
        # Floor to 5-min boundary
        minute = now_et.minute
        ws = (minute // 5) * 5
        window_start_et = now_et.replace(minute=ws, second=0, microsecond=0)
        window_end_et = window_start_et + datetime.timedelta(minutes=5)
        window_start_utc = window_start_et + datetime.timedelta(hours=5)
        ts = int(window_start_utc.replace(tzinfo=datetime.timezone.utc).timestamp())
        
        slug = f"btc-updown-5m-{ts}"
        print(f"Current ET: {now_et.strftime('%Y-%m-%d %I:%M:%S %p')}")
        print(f"Window: {window_start_et.strftime('%I:%M%p')}-{window_end_et.strftime('%I:%M%p')} ET")
        print(f"Slug: {slug}")
        
        # Fetch event
        async with s.get(f"{GAMMA}/events", params={"slug": slug}) as r:
            events = await r.json()
        
        if not events:
            print("ERROR: No event found!")
            return
        
        event = events[0]
        markets = event.get('markets', [])
        if not markets:
            print("ERROR: No markets in event!")
            return
        
        # Find the 5-min market (not 15-min)
        target = None
        for m in markets:
            q = m.get('question', '')
            # Check duration is 5 min by parsing time range
            import re
            match = re.search(r'(\d{1,2}:\d{2}(?:AM|PM))-(\d{1,2}:\d{2}(?:AM|PM))', q)
            if match:
                t1 = datetime.datetime.strptime(match.group(1), "%I:%M%p")
                t2 = datetime.datetime.strptime(match.group(2), "%I:%M%p")
                delta = (t2 - t1).total_seconds() / 60
                if delta == 5:
                    target = m
                    break
        
        if not target:
            # If only one market, use it
            target = markets[0]
        
        question = target.get('question', '')
        cid = target.get('conditionId', '')
        tokens_raw = target.get('clobTokenIds', '')
        outcomes = target.get('outcomes', '')
        prices_raw = target.get('outcomePrices', '')
        
        tokens = json.loads(tokens_raw) if isinstance(tokens_raw, str) else tokens_raw
        outcomes_list = json.loads(outcomes) if isinstance(outcomes, str) else outcomes
        prices = json.loads(prices_raw) if isinstance(prices_raw, str) else prices_raw
        
        print(f"\n=== Market Found ===")
        print(f"Question: {question}")
        print(f"conditionId: {cid}")
        print(f"Outcomes: {outcomes_list}")
        print(f"Gamma prices: {prices}")
        print(f"Token IDs: {[t[:20]+'...' for t in tokens]}")
        
        # Get CLOB midpoint prices
        print(f"\n=== CLOB Midpoint Prices ===")
        for i, tid in enumerate(tokens):
            async with s.get(f"{CLOB}/midpoint", params={"token_id": tid}) as r:
                mid = await r.json()
                print(f"  {outcomes_list[i]}: {mid}")
        
        # Get CLOB book
        print(f"\n=== CLOB Order Book ===")
        for i, tid in enumerate(tokens):
            async with s.get(f"{CLOB}/book", params={"token_id": tid}) as r:
                book = await r.json()
                bids = book.get('bids', [])[:3]
                asks = book.get('asks', [])[:3]
                print(f"  {outcomes_list[i]}:")
                print(f"    Top bids: {[(b.get('price'), b.get('size')) for b in bids]}")
                print(f"    Top asks: {[(a.get('price'), a.get('size')) for a in asks]}")
        
        # Calculate time to next rotation
        now_utc2 = datetime.datetime.utcnow()
        next_window_start_utc = window_start_utc + datetime.timedelta(minutes=5)
        seconds_left = (next_window_start_utc - now_utc2).total_seconds()
        print(f"\n=== Timing ===")
        print(f"Seconds until next 5-min window: {seconds_left:.1f}")
        print(f"Next window slug: btc-updown-5m-{ts + 300}")
        
        # Verify next window also exists
        next_slug = f"btc-updown-5m-{ts + 300}"
        async with s.get(f"{GAMMA}/events", params={"slug": next_slug}) as r:
            next_events = await r.json()
            if next_events:
                next_q = next_events[0].get('markets', [{}])[0].get('question', '?')
                print(f"Next window market: {next_q}")
            else:
                print("Next window market: NOT YET AVAILABLE")

asyncio.run(main())
