"""查看无 filter 时 RTDS update 消息的完整结构"""
import asyncio, json, time, sys, aiohttp

async def main():
    async with aiohttp.ClientSession() as s:
        async with s.ws_connect("wss://ws-live-data.polymarket.com", heartbeat=5.0) as ws:
            await ws.send_json({
                "action": "subscribe",
                "subscriptions": [{"topic": "crypto_prices_chainlink", "type": "*"}],
            })
            t0 = time.time()
            seen = 0
            while time.time() - t0 < 8:
                try:
                    msg = await asyncio.wait_for(ws.receive(), timeout=3)
                except asyncio.TimeoutError:
                    continue
                if msg.type == aiohttp.WSMsgType.TEXT and msg.data.strip():
                    obj = json.loads(msg.data)
                    if obj.get("type") == "update":
                        seen += 1
                        if seen <= 12:
                            print(json.dumps(obj, indent=2, ensure_ascii=False))
                            print("---")
                        if seen >= 12:
                            break
            await ws.close()

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
asyncio.run(main())
