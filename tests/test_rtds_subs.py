"""RTDS 单次快速测试 - 连接后等 15s 看有多少消息"""
import asyncio
import json
import time
import sys
import aiohttp

RTDS_URL = "wss://ws-live-data.polymarket.com"

async def test_one(topic: str, sub_type: str, use_filter: bool):
    sub = {
        "action": "subscribe",
        "subscriptions": [{
            "topic": topic,
            "type": sub_type,
        }],
    }
    if use_filter:
        sub["subscriptions"][0]["filters"] = json.dumps({"symbol": "btc/usd"})
    
    print(f"Topic={topic}, type={sub_type}, filter={use_filter}")
    msgs = 0
    init_prices = 0
    update_prices = 0
    got_init = False

    try:
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(RTDS_URL, heartbeat=5.0) as ws:
                await ws.send_json(sub)
                t0 = time.time()
                while time.time() - t0 < 15:
                    try:
                        msg = await asyncio.wait_for(ws.receive(), timeout=3.0)
                    except asyncio.TimeoutError:
                        # 3秒没消息，继续等
                        continue
                    
                    if msg.type != aiohttp.WSMsgType.TEXT:
                        if msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                            print(f"  WS closed/error at {time.time()-t0:.1f}s")
                            break
                        continue
                    
                    raw = msg.data
                    if not raw or not raw.strip():
                        msgs += 1
                        continue
                    
                    try:
                        obj = json.loads(raw)
                    except:
                        msgs += 1
                        continue
                    
                    msgs += 1
                    elapsed = time.time() - t0
                    mtype = obj.get("type", "?")
                    topic_r = obj.get("topic", "?")
                    payload = obj.get("payload", {})
                    data_arr = payload.get("data", []) if isinstance(payload, dict) else []
                    n = len(data_arr) if isinstance(data_arr, list) else 0
                    value = payload.get("value") if isinstance(payload, dict) else None
                    
                    if n > 0:
                        last_val = data_arr[-1].get("value", "?")
                        if not got_init:
                            init_prices = n
                            got_init = True
                            print(f"  [{elapsed:5.1f}s] INIT: type={mtype} topic={topic_r} data[{n}] last=${last_val}")
                        else:
                            update_prices += n
                            print(f"  [{elapsed:5.1f}s] UPDATE: type={mtype} topic={topic_r} data[{n}] last=${last_val}")
                    elif value is not None:
                        update_prices += 1
                        print(f"  [{elapsed:5.1f}s] UPDATE: type={mtype} topic={topic_r} value=${value}")
                    else:
                        print(f"  [{elapsed:5.1f}s] OTHER: type={mtype} topic={topic_r} keys={list(obj.keys())}")
                
                await ws.close()
    except Exception as e:
        print(f"  ERROR: {e}")
    
    print(f"  => msgs={msgs}, init_prices={init_prices}, stream_updates={update_prices}")
    print()
    return update_prices


async def main():
    print("=== RTDS 订阅方式快速对比 (每种15s) ===\n")
    
    tests = [
        ("crypto_prices_chainlink", "*", True),
        ("crypto_prices", "*", True),
        ("crypto_prices_chainlink", "update", True),
        ("crypto_prices", "update", True),
        ("crypto_prices_chainlink", "*", False),
    ]
    
    for topic, stype, filt in tests:
        await test_one(topic, stype, filt)
    
    print("DONE")

if __name__ == "__main__":
    # Windows 兼容
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
