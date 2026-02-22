"""Quick test: connect to Dashboard WS and print all messages for 20s."""
import asyncio, aiohttp, json, time

async def test():
    async with aiohttp.ClientSession() as session:
        try:
            ws = await session.ws_connect("ws://localhost:8080/ws", timeout=5)
            print("Connected to Dashboard WS")
            start = time.time()
            count = 0
            async for msg in ws:
                elapsed = time.time() - start
                if elapsed > 20:
                    break
                if msg.type == aiohttp.WSMsgType.TEXT:
                    data = json.loads(msg.data)
                    msg_type = data.get("type", "unknown")
                    if msg_type == "snapshot":
                        market = data.get("data", {}).get("market", {})
                        btc = market.get("btc_price")
                        yes = market.get("pm_yes_price")
                        no = market.get("pm_no_price")
                        print(f"[{elapsed:.1f}s] SNAPSHOT: btc={btc}, yes={yes}, no={no}")
                    elif msg_type == "price":
                        price = data.get("data", {}).get("price", 0)
                        source = data.get("data", {}).get("source", "")
                        print(f"[{elapsed:.1f}s] PRICE: {price} (source={source})")
                    elif msg_type == "heartbeat":
                        btc = data.get("data", {}).get("btc_price", 0)
                        clients = data.get("data", {}).get("ws_clients", 0)
                        print(f"[{elapsed:.1f}s] HEARTBEAT: btc={btc}, clients={clients}")
                    elif msg_type == "price_history":
                        prices = data.get("data", {}).get("prices", [])
                        print(f"[{elapsed:.1f}s] PRICE_HISTORY: {len(prices)} ticks")
                    elif msg_type == "pong":
                        pass
                    else:
                        print(f"[{elapsed:.1f}s] {msg_type}: {str(data)[:150]}")
                    count += 1
                elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                    print("WS closed/error")
                    break
            print(f"Total: {count} messages in {time.time()-start:.1f}s")
            await ws.close()
        except Exception as e:
            print(f"Error: {e}")

asyncio.run(test())
