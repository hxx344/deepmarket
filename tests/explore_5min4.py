"""
Analyze 5-min BTC markets structure and test matching logic.
"""
import asyncio, aiohttp, json, datetime, re

GAMMA = "https://gamma-api.polymarket.com"
CLOB  = "https://clob.polymarket.com"

async def main():
    async with aiohttp.ClientSession() as s:
        # 1. Get all recent "Bitcoin Up or Down" markets
        print("=== All Bitcoin Up or Down markets (recent, active) ===")
        url = f"{GAMMA}/markets"
        params = {
            "active": "true",
            "closed": "false",
            "limit": "100",
            "order": "startDate",
            "ascending": "false",
        }
        async with s.get(url, params=params) as r:
            data = await r.json()
        
        btc_markets = [m for m in data if 'bitcoin up or down' in m.get('question','').lower()]
        print(f"Total recent active markets: {len(data)}, BTC Up/Down: {len(btc_markets)}")
        
        # Separate 5-min and non-5-min
        five_min = []
        other = []
        for m in btc_markets:
            q = m.get('question', '')
            # Parse time range from question
            # Format: "Bitcoin Up or Down - February 16, 7:30AM-7:35AM ET"
            match = re.search(r'(\d{1,2}:\d{2}(?:AM|PM))-(\d{1,2}:\d{2}(?:AM|PM))\s+ET', q)
            if match:
                start_str, end_str = match.group(1), match.group(2)
                # Parse times
                fmt = "%I:%M%p"
                try:
                    t1 = datetime.datetime.strptime(start_str, fmt)
                    t2 = datetime.datetime.strptime(end_str, fmt)
                    delta = (t2 - t1).total_seconds() / 60
                    if delta == 5:
                        five_min.append(m)
                    else:
                        other.append(m)
                except:
                    other.append(m)
            else:
                other.append(m)
        
        print(f"\n5-min markets: {len(five_min)}")
        for m in five_min:
            q = m.get('question', '')
            cid = m.get('conditionId', '')[:20]
            end = m.get('endDate', '')
            start = m.get('startDate', '')
            prices = m.get('outcomePrices', '')
            tokens = m.get('clobTokenIds', '')
            print(f"  {q}")
            print(f"    cid={cid}... end={end} start={start}")
            print(f"    prices={prices} tokens={tokens[:60]}...")
        
        print(f"\nOther duration markets: {len(other)}")
        for m in other:
            print(f"  {m.get('question','')}")
        
        # 2. Full structure of one 5-min market
        if five_min:
            print("\n=== Full structure of first 5-min market ===")
            m = five_min[0]
            for k, v in m.items():
                val = str(v)
                if len(val) > 200:
                    val = val[:200] + "..."
                print(f"  {k}: {val}")
        
        # 3. Now figure out current ET time and what market to match
        import pytz
        try:
            et = pytz.timezone('US/Eastern')
            now_et = datetime.datetime.now(et)
        except ImportError:
            # Calculate manually: UTC-5
            now_utc = datetime.datetime.utcnow()
            now_et = now_utc - datetime.timedelta(hours=5)
        
        print(f"\n=== Current time ===")
        print(f"  UTC: {datetime.datetime.utcnow()}")
        print(f"  ET:  {now_et}")
        
        # Current 5-min window
        minute = now_et.minute
        window_start_min = (minute // 5) * 5
        window_end_min = window_start_min + 5
        
        hour = now_et.hour
        am_pm = "AM" if hour < 12 else "PM"
        h12 = hour % 12
        if h12 == 0:
            h12 = 12
        
        end_hour = hour
        end_am_pm = am_pm
        if window_end_min >= 60:
            window_end_min = 0
            end_hour = hour + 1
            end_am_pm = "AM" if end_hour < 12 else "PM"
            end_h12 = end_hour % 12
            if end_h12 == 0:
                end_h12 = 12
        else:
            end_h12 = h12
            end_am_pm = am_pm
        
        start_time_str = f"{h12}:{window_start_min:02d}{am_pm}"
        end_time_str = f"{end_h12}:{window_end_min:02d}{end_am_pm}"
        
        month_names = ["January", "February", "March", "April", "May", "June",
                       "July", "August", "September", "October", "November", "December"]
        month_name = month_names[now_et.month - 1]
        day = now_et.day
        
        expected_question = f"Bitcoin Up or Down - {month_name} {day}, {start_time_str}-{end_time_str} ET"
        print(f"\n  Expected current window market: {expected_question}")
        
        # 4. Search for this specific market
        print(f"\n=== Searching for current window market ===")
        # Try Gamma markets with text query
        params2 = {
            "active": "true",
            "closed": "false",
            "limit": "50",
            "order": "startDate",
            "ascending": "false",
        }
        async with s.get(f"{GAMMA}/markets", params=params2) as r:
            data2 = await r.json()
        
        btc2 = [m for m in data2 if 'bitcoin up or down' in m.get('question','').lower()]
        print(f"  Recent BTC Up/Down markets: {len(btc2)}")
        
        # Find exact match
        found = None
        for m in btc2:
            if m.get('question', '') == expected_question:
                found = m
                break
        
        if found:
            print(f"  FOUND: {found.get('question')}")
            cid = found.get('conditionId', '')
            tokens_raw = found.get('clobTokenIds', '')
            print(f"  conditionId: {cid}")
            print(f"  clobTokenIds: {tokens_raw}")
            # Get midpoint
            if tokens_raw:
                tokens = json.loads(tokens_raw) if isinstance(tokens_raw, str) else tokens_raw
                for i, tid in enumerate(tokens):
                    async with s.get(f"{CLOB}/midpoint", params={"token_id": tid}) as r:
                        mid = await r.json()
                        print(f"  Token {i} midpoint: {mid}")
        else:
            print(f"  NOT FOUND by exact name")
            # Show closest matches
            print(f"  Available BTC markets:")
            for m in btc2:
                print(f"    {m.get('question','')}")
            
            # Maybe the market hasn't been created yet, or the naming is slightly different
            # Let's also check without exact format matching
            for m in btc2:
                q = m.get('question', '')
                if start_time_str in q or f"{h12}:{window_start_min:02d}" in q:
                    print(f"\n  PARTIAL MATCH: {q}")
                    cid = m.get('conditionId', '')
                    tokens_raw = m.get('clobTokenIds', '')
                    print(f"    conditionId: {cid}")
                    print(f"    clobTokenIds: {tokens_raw}")

        # 5. Also check: are there many more pages of "Bitcoin Up or Down" markets?
        print(f"\n=== Check total count with offset ===")
        for offset in [0, 100, 200, 500]:
            params3 = {
                "active": "true", 
                "closed": "false",
                "limit": "100",
                "offset": str(offset),
                "order": "startDate",
                "ascending": "false",
            }
            async with s.get(f"{GAMMA}/markets", params=params3) as r:
                d = await r.json()
            btc3 = [m for m in d if 'bitcoin up or down' in m.get('question','').lower()]
            if btc3:
                print(f"  offset={offset}: total={len(d)}, btc_up_down={len(btc3)}")
                print(f"    first: {btc3[0].get('question','')}")
                print(f"    last:  {btc3[-1].get('question','')}")
            else:
                print(f"  offset={offset}: total={len(d)}, btc_up_down=0")
                break

asyncio.run(main())
