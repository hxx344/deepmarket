"""End-to-end test: 5-min window discovery + price polling + rotation."""
import asyncio
from src.core.event_bus import EventBus, EventType
from src.market.datasources.polymarket_ds import PolymarketDataSource

async def test():
    bus = EventBus()
    pm = PolymarketDataSource(event_bus=bus, poll_interval=3)
    
    received = []
    async def on_pm(event):
        d = event.data
        received.append(d)
        print(f'PM_PRICE #{len(received)}: {d["question"][:60]}')
        print(f'  Up={d["yes_price"]:.4f} Down={d["no_price"]:.4f} SecsLeft={d["seconds_left"]:.0f}')
    
    bus.subscribe(EventType.PM_PRICE, on_pm)
    await bus.start()
    
    await pm.connect()
    m = pm._active_market
    if m:
        print(f'\n✓ Active market: {m.question}')
        print(f'  window_start_ts={m.window_start_ts}  window_end_ts={m.window_end_ts}')
        print(f'  Outcomes: {m.outcomes}')
        print(f'  Token IDs: {[t[:20]+"..." for t in m.token_ids]}')
    else:
        print('✗ No active market found!')
        return
    
    # Run streaming for 15 seconds
    task = asyncio.create_task(pm.start_streaming())
    await asyncio.sleep(15)
    pm._running = False
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    
    await pm.disconnect()
    print(f'\n✓ Total PM_PRICE events: {len(received)}')
    if received:
        last = received[-1]
        print(f'  Last: Up={last["yes_price"]:.4f} Down={last["no_price"]:.4f} SecsLeft={last["seconds_left"]:.0f}')

asyncio.run(test())
