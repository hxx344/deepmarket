"""诊断 RTDS: 模拟完整 connect() + start_streaming() 流程, 打印每一条 WS 消息"""
import asyncio, aiohttp, json, time

RTDS_WS = "wss://ws-live-data.polymarket.com"
SUB_MSG = {
    "action": "subscribe",
    "subscriptions": [
        {
            "topic": "crypto_prices_chainlink",
            "type": "*",
            "filters": json.dumps({"symbol": "btc/usd"}),
        }
    ],
}

async def test():
    session = aiohttp.ClientSession()

    # ── Phase 1: 模拟 connect() → _test_rtds_connection() ──
    print("=== Phase 1: connect() test ===")
    try:
        async with session.ws_connect(RTDS_WS, heartbeat=5.0) as ws:
            await ws.send_json(SUB_MSG)
            start = time.time()
            async for msg in ws:
                if time.time() - start > 10:
                    break
                if msg.type == aiohttp.WSMsgType.TEXT:
                    raw = msg.data
                    elapsed = time.time() - start
                    if raw.strip():
                        try:
                            obj = json.loads(raw)
                            payload = obj.get("payload", {})
                            data_arr = payload.get("data", [])
                            if data_arr:
                                price = data_arr[0].get("value", "N/A")
                                print(f"  [{elapsed:.1f}s] GOT PRICE: ${price}")
                                break  # 模拟 connect() 拿到价格就退
                            else:
                                print(f"  [{elapsed:.1f}s] Non-price: {raw[:100]}")
                        except:
                            print(f"  [{elapsed:.1f}s] Parse error: {raw[:100]}")
                    else:
                        print(f"  [{elapsed:.1f}s] Empty msg (subscription ack)")
    except Exception as e:
        print(f"  Phase 1 error: {e}")
    
    print(f"\n=== Phase 2: start_streaming() (30s) ===")
    # 短暂等待模拟真实间隔
    await asyncio.sleep(0.5)
    
    # ── Phase 2: 模拟 _stream_rtds_ws() ──
    msg_count = 0
    price_count = 0
    try:
        async with session.ws_connect(RTDS_WS, heartbeat=5.0) as ws:
            await ws.send_json(SUB_MSG)
            print("  Connected + subscribed")
            start = time.time()
            async for msg in ws:
                elapsed = time.time() - start
                if elapsed > 30:
                    break
                if msg.type == aiohttp.WSMsgType.TEXT:
                    msg_count += 1
                    raw = msg.data
                    if raw.strip():
                        try:
                            obj = json.loads(raw)
                            payload = obj.get("payload", {})
                            data_arr = payload.get("data", [])
                            if data_arr:
                                price = data_arr[0].get("value", "N/A")
                                price_count += 1
                                print(f"  [{elapsed:.1f}s] PRICE #{price_count}: ${price}")
                            else:
                                print(f"  [{elapsed:.1f}s] msg #{msg_count}: keys={list(obj.keys())}")
                        except:
                            print(f"  [{elapsed:.1f}s] msg #{msg_count}: parse error")
                    else:
                        print(f"  [{elapsed:.1f}s] msg #{msg_count}: empty")
                elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                    print(f"  [{elapsed:.1f}s] WS ERROR/CLOSED")
                    break
    except Exception as e:
        print(f"  Phase 2 error: {e}")
    
    print(f"\n=== Result: {msg_count} messages, {price_count} prices in 30s ===")
    await session.close()

asyncio.run(test())
