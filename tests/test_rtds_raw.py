"""直接连接 RTDS WS，打印所有原始消息，诊断推送频率和格式"""
import asyncio
import json
import time
import aiohttp

RTDS_URL = "wss://ws-live-data.polymarket.com"
DURATION = 60  # 监听秒数

async def main():
    print(f"=== RTDS 原始消息诊断 ({DURATION}s) ===")
    print(f"连接: {RTDS_URL}")
    
    msg_count = 0
    price_count = 0
    last_price = 0

    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(
            RTDS_URL,
            heartbeat=5.0,
            timeout=aiohttp.ClientTimeout(total=15),
        ) as ws:
            # 订阅
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
            print("已订阅 crypto_prices_chainlink btc/usd\n")

            start = time.time()
            async for msg in ws:
                elapsed = time.time() - start
                if elapsed > DURATION:
                    break

                msg_count += 1
                
                if msg.type == aiohttp.WSMsgType.TEXT:
                    raw = msg.data
                    if not raw or not raw.strip():
                        print(f"[{elapsed:6.1f}s] #{msg_count} <空消息>")
                        continue
                    
                    # 打印原始 JSON（截断到300字符）
                    print(f"[{elapsed:6.1f}s] #{msg_count} RAW: {raw[:300]}")
                    
                    # 尝试解析
                    try:
                        obj = json.loads(raw)
                        # 列出所有顶级 key
                        print(f"         keys: {list(obj.keys())}")
                        
                        # 深入检查各种可能格式
                        payload = obj.get("payload", {})
                        if payload:
                            print(f"         payload keys: {list(payload.keys()) if isinstance(payload, dict) else type(payload)}")
                            
                            data_arr = payload.get("data")
                            if isinstance(data_arr, list) and len(data_arr) > 0:
                                print(f"         payload.data[0]: {data_arr[0]}")
                                print(f"         payload.data length: {len(data_arr)}")
                                for item in data_arr:
                                    p = float(item.get("value", 0))
                                    if p > 0:
                                        price_count += 1
                                        diff = p - last_price if last_price else 0
                                        print(f"         >>> 价格: ${p:,.6f}  差额: ${diff:+,.2f}")
                                        last_price = p
                            
                            value = payload.get("value")
                            if value is not None:
                                print(f"         payload.value: {value}")
                                price_count += 1
                                p = float(value)
                                diff = p - last_price if last_price else 0
                                print(f"         >>> 价格: ${p:,.6f}  差额: ${diff:+,.2f}")
                                last_price = p

                        # 检查是否有 topic 字段
                        topic = obj.get("topic")
                        if topic:
                            print(f"         topic: {topic}")
                        
                        # 检查是否有 type/action 字段
                        for k in ("type", "action", "event", "channel"):
                            v = obj.get(k)
                            if v:
                                print(f"         {k}: {v}")
                                
                    except json.JSONDecodeError:
                        print(f"         <非JSON>")
                    
                    print()
                    
                elif msg.type == aiohttp.WSMsgType.BINARY:
                    print(f"[{elapsed:6.1f}s] #{msg_count} BINARY: {len(msg.data)} bytes")
                elif msg.type == aiohttp.WSMsgType.PING:
                    print(f"[{elapsed:6.1f}s] #{msg_count} PING")
                elif msg.type == aiohttp.WSMsgType.PONG:
                    print(f"[{elapsed:6.1f}s] #{msg_count} PONG")
                elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                    print(f"[{elapsed:6.1f}s] #{msg_count} {msg.type}")
                    break

    print(f"\n=== 统计 ===")
    print(f"总消息数: {msg_count}")
    print(f"解析到价格数: {price_count}")
    print(f"时长: {DURATION}s")
    if price_count > 0:
        print(f"价格推送频率: ~{DURATION/price_count:.1f}s/条")
    print(f"最后价格: ${last_price:,.6f}")

if __name__ == "__main__":
    asyncio.run(main())
