"""验证 PM 自动发现 + 价格获取"""
import asyncio
import sys
sys.path.insert(0, ".")

async def main():
    from src.core.event_bus import Event, EventType, get_event_bus
    from src.market.datasources.polymarket_ds import PolymarketDataSource

    bus = get_event_bus()

    pm_events = []
    async def on_pm_price(event: Event):
        pm_events.append(event)

    bus.subscribe(EventType.PM_PRICE, on_pm_price)

    ds = PolymarketDataSource(event_bus=bus, poll_interval=3.0)

    print("=== 1. connect (auto-discover) ===")
    await ds.connect()

    if ds._active_market:
        mkt = ds._active_market
        print(f"  Question: {mkt.question}")
        print(f"  Strike: ${mkt.strike_price:,.0f}")
        print(f"  Outcomes: {mkt.outcomes}")
        print(f"  Prices: {mkt.prices}")
        print(f"  Token IDs: {[t[:25]+'...' for t in mkt.token_ids]}")
    else:
        print("  !! No market found")
        return

    print(f"\nTotal BTC markets discovered: {len(ds._all_btc_markets)}")
    for m in ds._all_btc_markets[:5]:
        p = m.prices[0] if m.prices else 0
        print(f"  {m.question} (YES={p:.4f})")

    print(f"\n=== 2. start_streaming (10s) ===")
    task = asyncio.create_task(ds.start_streaming())
    await asyncio.sleep(10)
    ds._running = False
    task.cancel()

    print(f"\n=== 3. Results ===")
    print(f"PM_PRICE events received: {len(pm_events)}")
    if pm_events:
        last = pm_events[-1]
        d = last.data
        print(f"  Latest: YES={d.get('yes_price',0):.4f}, NO={d.get('no_price',0):.4f}")
        print(f"  Question: {d.get('question','')}")
        print(f"  Strike: ${d.get('strike_price',0):,.0f}")

    await ds.disconnect()
    print("\nDONE")

asyncio.run(main())
