"""Quick test: 5-min window discovery + price polling."""
import asyncio, sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Disable loguru stderr to reduce noise
import loguru
loguru.logger.remove()
loguru.logger.add(sys.stderr, level="WARNING")

from src.core.event_bus import EventBus, EventType
from src.market.datasources.polymarket_ds import PolymarketDataSource

OUT = open("tests/e2e_result.txt", "w", encoding="utf-8")
def log(msg):
    print(msg, flush=True)
    OUT.write(msg + "\n")
    OUT.flush()

async def test():
    bus = EventBus()
    pm = PolymarketDataSource(event_bus=bus, poll_interval=3)
    
    received = []
    async def on_pm(event):
        d = event.data
        received.append(d)
        q = d.get("question", "?")[:60]
        log(f'PM_PRICE #{len(received)}: {q}')
        log(f'  Up={d["yes_price"]:.4f} Down={d["no_price"]:.4f} SecsLeft={d["seconds_left"]:.0f}')
    
    bus.subscribe(EventType.PM_PRICE, on_pm)
    await bus.start()
    
    log("Connecting to Polymarket...")
    await pm.connect()
    m = pm._active_market
    if m:
        log(f'PASS Active market: {m.question}')
        log(f'  window_start_ts={m.window_start_ts}  window_end_ts={m.window_end_ts}')
        log(f'  Outcomes: {m.outcomes}')
        log(f'  Tokens: {[t[:20]+"..." for t in m.token_ids]}')
    else:
        log('FAIL No active market found!')
        OUT.close()
        return
    
    # Run streaming for 15 seconds
    log("Starting streaming (15s)...")
    task = asyncio.create_task(pm.start_streaming())
    await asyncio.sleep(15)
    pm._running = False
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    
    await pm.disconnect()
    log(f'\nRESULT: Total PM_PRICE events: {len(received)}')
    if received:
        last = received[-1]
        log(f'  Last: Up={last["yes_price"]:.4f} Down={last["no_price"]:.4f} SecsLeft={last["seconds_left"]:.0f}')
    log("DONE")
    OUT.close()

asyncio.run(test())
