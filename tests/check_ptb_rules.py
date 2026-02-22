"""Check Polymarket 5-min market rules and description."""
import asyncio, aiohttp, datetime, json

GAMMA = "https://gamma-api.polymarket.com"

async def main():
    async with aiohttp.ClientSession() as s:
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        minute = now_utc.minute
        ws = (minute // 5) * 5
        window_start = now_utc.replace(minute=ws, second=0, microsecond=0)
        ts = int(window_start.timestamp())

        for attempt_ts in [ts, ts - 300, ts - 600]:
            slug = f"btc-updown-5m-{attempt_ts}"
            async with s.get(f"{GAMMA}/events", params={"slug": slug}) as r:
                events = await r.json()
            if events:
                ev = events[0]
                print(f"=== slug: {slug} ===")
                print(f"title: {ev.get('title', '')}")
                print(f"event description:\n{ev.get('description', '')}\n")
                for i, m in enumerate(ev.get("markets", [])):
                    print(f"--- Market {i} ---")
                    print(f"question: {m.get('question', '')}")
                    print(f"market description:\n{m.get('description', '')}\n")
                    # All potentially interesting fields
                    for k in sorted(m.keys()):
                        v = str(m[k])
                        if any(w in k.lower() for w in ["resol", "rule", "start", "end", "time", "close"]):
                            print(f"  {k}: {v[:500]}")
                break
        else:
            print("No events found")

asyncio.run(main())
