"""
Find CURRENT 5-min window market and understand creation/lifecycle pattern.
"""
import asyncio, aiohttp, json, datetime, time

GAMMA = "https://gamma-api.polymarket.com"
CLOB  = "https://clob.polymarket.com"

async def main():
    async with aiohttp.ClientSession() as s:
        now_utc = datetime.datetime.utcnow()
        now_et = now_utc - datetime.timedelta(hours=5)
        print(f"UTC: {now_utc}, ET: {now_et}")
        
        # Calculate current 5-min window in ET
        minute = now_et.minute
        ws = (minute // 5) * 5
        we = ws + 5
        
        h = now_et.hour
        d = now_et.day
        mo = now_et.month
        yr = now_et.year
        
        # Window start as UTC datetime
        window_start_et = now_et.replace(minute=ws, second=0, microsecond=0)
        window_start_utc = window_start_et + datetime.timedelta(hours=5)
        window_start_ts = int(window_start_utc.replace(tzinfo=datetime.timezone.utc).timestamp())
        
        print(f"Current window: {window_start_et.strftime('%I:%M%p')}-{(window_start_et + datetime.timedelta(minutes=5)).strftime('%I:%M%p')} ET")
        print(f"Window start UTC: {window_start_utc}")
        print(f"Window start unix: {window_start_ts}")
        
        # Expected slug based on pattern: btc-updown-5m-{unix_ts}
        expected_slug = f"btc-updown-5m-{window_start_ts}"
        print(f"Expected slug: {expected_slug}")
        
        # Strategy 1: Search by slug
        print(f"\n=== Strategy 1: Gamma events by slug ===")
        async with s.get(f"{GAMMA}/events", params={"slug": expected_slug}) as r:
            data = await r.json()
            print(f"  slug={expected_slug}: {len(data)} events")
            for e in data:
                print(f"    {e.get('title', '?')}")
                for m in e.get('markets', []):
                    print(f"      market: {m.get('question','?')}")
        
        # Strategy 2: Search market by slug
        print(f"\n=== Strategy 2: Gamma markets by slug ===")
        async with s.get(f"{GAMMA}/markets", params={"slug": expected_slug}) as r:
            data = await r.json()
            print(f"  {len(data)} results")
            for m in data:
                print(f"    {m.get('question','?')}")
        
        # Strategy 3: Try recently closed markets for today
        print(f"\n=== Strategy 3: Recently closed BTC Up/Down ===")
        params = {"closed": "true", "limit": "20", "order": "endDate", "ascending": "false"}
        async with s.get(f"{GAMMA}/markets", params=params) as r:
            data = await r.json()
            btc = [m for m in data if 'bitcoin up or down' in m.get('question','').lower()]
            print(f"  Recently closed: {len(data)} total, {len(btc)} BTC Up/Down")
            for m in btc[:10]:
                print(f"    {m.get('question','?')}")
                print(f"      end={m.get('endDate','')} closed={m.get('closed','')} active={m.get('active','')}")
        
        # Strategy 4: Try without closed filter
        print(f"\n=== Strategy 4: All BTC Up/Down (no closed filter) ===")
        params = {"limit": "200", "order": "endDate", "ascending": "false"}
        async with s.get(f"{GAMMA}/markets", params=params) as r:
            data = await r.json()
            btc = [m for m in data if 'bitcoin up or down' in m.get('question','').lower()]
            print(f"  Total: {len(data)}, BTC Up/Down: {len(btc)}")
            # Get unique dates
            dates = set()
            for m in btc:
                q = m.get('question', '')
                import re
                dm = re.search(r'(February|January|March) (\d+)', q)
                if dm:
                    dates.add(f"{dm.group(1)} {dm.group(2)}")
            print(f"  Dates found: {sorted(dates)}")
            # Show first and last
            for m in btc[:5]:
                print(f"    {m.get('question','?')}")
                print(f"      end={m.get('endDate','')} closed={m.get('closed','')}")
            print("    ...")
            for m in btc[-3:]:
                print(f"    {m.get('question','?')}")
                print(f"      end={m.get('endDate','')} closed={m.get('closed','')}")
        
        # Strategy 5: Try CLOB market by slug or condition
        print(f"\n=== Strategy 5: CLOB market by condition_id ===")
        # Use a known condition_id from a 5-min market we found
        known_cid = "0x23ba9568dea52f9568e411c902fee0d4cca10bb37bda07c088d04a0f7590cc0c"
        async with s.get(f"{CLOB}/markets/{known_cid}") as r:
            text = await r.text()
            print(f"  CLOB market status: {r.status}")
            if r.status == 200:
                data = json.loads(text)
                print(f"  question: {data.get('question','?')}")
                print(f"  active: {data.get('active','?')}")
                print(f"  closed: {data.get('closed','?')}")
                print(f"  end_date_iso: {data.get('end_date_iso','?')}")
            else:
                print(f"  Response: {text[:200]}")
        
        # Strategy 6: Calculate slugs for a range of windows and test each
        print(f"\n=== Strategy 6: Test slug pattern for past/current/future windows ===")
        for offset_min in [-15, -10, -5, 0, 5, 10]:
            w = window_start_et + datetime.timedelta(minutes=offset_min)
            w_utc = w + datetime.timedelta(hours=5)
            ts = int(w_utc.replace(tzinfo=datetime.timezone.utc).timestamp())
            slug = f"btc-updown-5m-{ts}"
            async with s.get(f"{GAMMA}/events", params={"slug": slug}) as r:
                data = await r.json()
                status = f"FOUND {len(data)} events" if data else "NOT FOUND"
                if data:
                    title = data[0].get('title', '?')
                    status += f" - {title}"
                print(f"  {w.strftime('%H:%M')} ET (ts={ts}): {status}")
        
        # Strategy 7: Try endDate-based filter for current window
        print(f"\n=== Strategy 7: Gamma markets by endDate range ===")
        window_end_utc = window_start_utc + datetime.timedelta(minutes=5)
        params = {
            "active": "true",
            "limit": "20",
            "endDate": window_end_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        async with s.get(f"{GAMMA}/markets", params=params) as r:
            data = await r.json()
            btc = [m for m in data if 'bitcoin up or down' in m.get('question','').lower()]
            print(f"  endDate={params['endDate']}: total={len(data)}, btc={len(btc)}")
            for m in btc:
                print(f"    {m.get('question','?')}")

        # Strategy 8: Try CLOB markets list with cursor pagination to find today's markets
        print(f"\n=== Strategy 8: CLOB paginated search ===")
        found_today = []
        cursor = ""
        for page in range(3):
            p = {"limit": "500"}
            if cursor:
                p["next_cursor"] = cursor
            async with s.get(f"{CLOB}/markets", params=p) as r:
                data = json.loads(await r.text())
                items = data.get('data', data) if isinstance(data, dict) else data
                cursor = data.get('next_cursor', '') if isinstance(data, dict) else ''
                
                for m in (items if isinstance(items, list) else []):
                    q = str(m.get('question', ''))
                    if 'bitcoin up or down' in q.lower() and 'february 15' in q.lower():
                        found_today.append(q)
                
                if not cursor:
                    break
        
        print(f"  Feb 15 BTC markets in CLOB: {len(found_today)}")
        for q in found_today[:10]:
            print(f"    {q}")

asyncio.run(main())
