"""Explore Gamma API fields to find Price to Beat for 5-min markets."""
import asyncio, aiohttp, json, datetime

GAMMA = "https://gamma-api.polymarket.com"

async def main():
    async with aiohttp.ClientSession() as s:
        now_utc = datetime.datetime.utcnow()
        now_et = now_utc - datetime.timedelta(hours=5)
        minute = now_et.minute
        ws = (minute // 5) * 5
        window_start_et = now_et.replace(minute=ws, second=0, microsecond=0)
        window_start_utc = window_start_et + datetime.timedelta(hours=5)
        ts = int(window_start_utc.replace(tzinfo=datetime.timezone.utc).timestamp())
        slug = f"btc-updown-5m-{ts}"
        print(f"ET: {now_et.strftime('%H:%M')}, slug: {slug}")

        # Try current window, then previous
        events = []
        for attempt_ts in [ts, ts - 300, ts - 600]:
            attempt_slug = f"btc-updown-5m-{attempt_ts}"
            async with s.get(f"{GAMMA}/events", params={"slug": attempt_slug}) as r:
                data = await r.json()
            if data:
                slug = attempt_slug
                events = data
                print(f"Found event with slug: {slug}")
                break
            else:
                print(f"No event for slug: {attempt_slug}")

        if not events:
            print("No events found at all")
            return

        ev = events[0]
        print(f"\n=== EVENT ===")
        print(f"title: {ev.get('title','')}")
        print(f"description: {ev.get('description','')[:500]}")
        print(f"All event keys: {sorted(ev.keys())}")
        
        # Print all event-level fields
        for k in sorted(ev.keys()):
            if k == "markets":
                continue
            v = str(ev[k])[:300]
            print(f"  {k}: {v}")

        ms = ev.get("markets", [])
        print(f"\n=== MARKETS ({len(ms)} total) ===")
        for i, m in enumerate(ms):
            print(f"\n--- Market {i} ---")
            print(f"question: {m.get('question','')}")
            print(f"description: {m.get('description','')[:500]}")
            print(f"All keys: {sorted(m.keys())}")
            for k in sorted(m.keys()):
                v = str(m[k])[:300]
                print(f"  {k}: {v}")

asyncio.run(main())
