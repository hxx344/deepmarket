"""Test CLOB /book API response format"""
import asyncio
import datetime
import json
import aiohttp

async def test():
    async with aiohttp.ClientSession() as s:
        now = datetime.datetime.utcnow()
        m = (now.minute // 5) * 5
        ws = now.replace(minute=m, second=0, microsecond=0)
        ts = int(ws.replace(tzinfo=datetime.timezone.utc).timestamp())
        slug = f"btc-updown-5m-{ts}"
        print(f"Slug: {slug}")

        async with s.get(f"https://gamma-api.polymarket.com/events?slug={slug}") as r:
            events = await r.json()
        if not events:
            print("No events found")
            return
        mkt = events[0]["markets"][0]
        tokens = json.loads(mkt.get("clobTokenIds", "[]"))
        if not tokens:
            print("No tokens found")
            return

        token_id = tokens[0]
        print(f"Token: {token_id[:40]}...")

        async with s.get("https://clob.polymarket.com/book", params={"token_id": token_id}) as r:
            book = await r.json()

        print(f"Keys: {list(book.keys())}")
        bids = book.get("bids", [])
        asks = book.get("asks", [])
        print(f"Bids: {len(bids)}, Asks: {len(asks)}")
        if bids:
            print(f"Bid[0]: {bids[0]}")
            print(f"Bid[0] keys: {list(bids[0].keys())}")
        if asks:
            print(f"Ask[0]: {asks[0]}")
            print(f"Ask[0] keys: {list(asks[0].keys())}")

asyncio.run(test())
