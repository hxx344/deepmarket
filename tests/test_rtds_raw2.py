"""RTDS 诊断 v2: 打印每条消息的 type/topic, 看后续实时推送是否到达"""
import asyncio
import json
import time
import aiohttp

RTDS_URL = "wss://ws-live-data.polymarket.com"
DURATION = 30

async def main():
    print(f"=== RTDS 消息流诊断 ({DURATION}s) ===\n")
    msg_count = 0
    price_total = 0

    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(
            RTDS_URL,
            heartbeat=5.0,
            timeout=aiohttp.ClientTimeout(total=15),
        ) as ws:
            await ws.send_json({
                "action": "subscribe",
                "subscriptions": [
                    {
                        "topic": "crypto_prices_chainlink",
                        "type": "*",
                        "filters": json.dumps({"symbol": "btc/usd"}),
                    }
                ],
            })
            print("已发送订阅: topic=crypto_prices_chainlink, type=*, symbol=btc/usd\n")

            start = time.time()
            async for msg in ws:
                elapsed = time.time() - start
                if elapsed > DURATION:
                    break

                msg_count += 1
                if msg.type == aiohttp.WSMsgType.TEXT:
                    raw = msg.data
                    if not raw or not raw.strip():
                        print(f"[{elapsed:5.1f}s] msg#{msg_count}: <空>")
                        continue

                    try:
                        obj = json.loads(raw)
                    except json.JSONDecodeError:
                        print(f"[{elapsed:5.1f}s] msg#{msg_count}: <非JSON> {raw[:80]}")
                        continue

                    topic = obj.get("topic", "?")
                    mtype = obj.get("type", "?")
                    ts = obj.get("timestamp", "?")
                    payload = obj.get("payload", {})
                    symbol = payload.get("symbol", "?") if isinstance(payload, dict) else "?"
                    
                    data_arr = payload.get("data", []) if isinstance(payload, dict) else []
                    n_prices = len(data_arr) if isinstance(data_arr, list) else 0
                    
                    # 如果有 value 直接在 payload
                    value = payload.get("value") if isinstance(payload, dict) else None
                    
                    if n_prices > 0:
                        first_p = data_arr[0].get("value", "?")
                        last_p = data_arr[-1].get("value", "?")
                        first_ts = data_arr[0].get("timestamp", "?")
                        last_ts = data_arr[-1].get("timestamp", "?")
                        price_total += n_prices
                        print(f"[{elapsed:5.1f}s] msg#{msg_count}: topic={topic} type={mtype} symbol={symbol}")
                        print(f"         data[{n_prices}]: first=${first_p} ({first_ts}) → last=${last_p} ({last_ts})")
                    elif value is not None:
                        price_total += 1
                        print(f"[{elapsed:5.1f}s] msg#{msg_count}: topic={topic} type={mtype} symbol={symbol} value={value}")
                    else:
                        # 打印完整消息看看是啥
                        print(f"[{elapsed:5.1f}s] msg#{msg_count}: topic={topic} type={mtype}")
                        print(f"         FULL: {json.dumps(obj, ensure_ascii=False)[:300]}")
                    
                elif msg.type == aiohttp.WSMsgType.BINARY:
                    print(f"[{elapsed:5.1f}s] msg#{msg_count}: BINARY {len(msg.data)} bytes")
                elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                    print(f"[{elapsed:5.1f}s] msg#{msg_count}: {msg.type}")
                    break

    print(f"\n=== 结果: {msg_count} 条消息, {price_total} 个价格点, {DURATION}s ===")

if __name__ == "__main__":
    asyncio.run(main())
