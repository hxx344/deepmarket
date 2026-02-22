"""Minimal test: just discover current 5-min window and get one price."""
import asyncio, sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import loguru
loguru.logger.remove()

from src.core.event_bus import EventBus, EventType
from src.market.datasources.polymarket_ds import PolymarketDataSource

results = []

async def test():
    bus = EventBus()
    pm = PolymarketDataSource(event_bus=bus, poll_interval=3)
    
    async def on_pm(event):
        results.append(event.data)
    bus.subscribe(EventType.PM_PRICE, on_pm)
    # Don't await bus.start() - it blocks forever. publish() already dispatches directly.
    
    # Connect (discovers current window)
    await pm.connect()
    m = pm._active_market
    if not m:
        print("FAIL: No market")
        return
    
    print(f"PASS: {m.question}")
    print(f"  Outcomes: {m.outcomes}")
    print(f"  Prices: {m.prices}")
    print(f"  window_ts: {m.window_start_ts}-{m.window_end_ts}")
    
    # One price poll
    pm._running = True
    await pm._poll_midpoints()
    print(f"  Events: {len(results)}")
    if results:
        d = results[0]
        print(f"  Up={d['yes_price']:.4f} Down={d['no_price']:.4f} SecsLeft={d['seconds_left']:.0f}")
    
    await pm.disconnect()
    print("DONE")

asyncio.run(test())
