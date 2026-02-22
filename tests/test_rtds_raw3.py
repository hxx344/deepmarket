"""RTDS 诊断 v3: 测试多种订阅参数，找到正确的流式推送方式"""
import asyncio
import json
import time
import aiohttp

RTDS_URL = "wss://ws-live-data.polymarket.com"
DURATION = 20  # 每种测试20秒

SUBSCRIBE_VARIANTS = [
    {
        "name": "原始: crypto_prices_chainlink + type=*",
        "msg": {
            "action": "subscribe",
            "subscriptions": [
                {
                    "topic": "crypto_prices_chainlink",
                    "type": "*",
                    "filters": json.dumps({"symbol": "btc/usd"}),
                }
            ],
        },
    },
    {
        "name": "变体1: crypto_prices + type=*",
        "msg": {
            "action": "subscribe",
            "subscriptions": [
                {
                    "topic": "crypto_prices",
                    "type": "*",
                    "filters": json.dumps({"symbol": "btc/usd"}),
                }
            ],
        },
    },
    {
        "name": "变体2: crypto_prices_chainlink + type=update",
        "msg": {
            "action": "subscribe",
            "subscriptions": [
                {
                    "topic": "crypto_prices_chainlink",
                    "type": "update",
                    "filters": json.dumps({"symbol": "btc/usd"}),
                }
            ],
        },
    },
    {
        "name": "变体3: crypto_prices + type=update",
        "msg": {
            "action": "subscribe",
            "subscriptions": [
                {
                    "topic": "crypto_prices",
                    "type": "update",
                    "filters": json.dumps({"symbol": "btc/usd"}),
                }
            ],
        },
    },
    {
        "name": "变体4: crypto_prices_chainlink 不带 filters",
        "msg": {
            "action": "subscribe",
            "subscriptions": [
                {
                    "topic": "crypto_prices_chainlink",
                    "type": "*",
                }
            ],
        },
    },
]


async def test_variant(variant: dict) -> dict:
    name = variant["name"]
    sub_msg = variant["msg"]
    result = {"name": name, "msgs": 0, "prices": 0, "updates_after_init": 0}

    print(f"\n--- {name} ---")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(
                RTDS_URL,
                heartbeat=5.0,
                timeout=aiohttp.ClientTimeout(total=DURATION + 10),
            ) as ws:
                await ws.send_json(sub_msg)
                print(f"  已发送: {json.dumps(sub_msg, ensure_ascii=False)[:200]}")

                init_done = False
                start = time.time()
                
                async for msg in ws:
                    elapsed = time.time() - start
                    if elapsed > DURATION:
                        break

                    if msg.type == aiohttp.WSMsgType.TEXT:
                        raw = msg.data
                        if not raw or not raw.strip():
                            result["msgs"] += 1
                            continue

                        try:
                            obj = json.loads(raw)
                        except json.JSONDecodeError:
                            result["msgs"] += 1
                            continue

                        result["msgs"] += 1
                        mtype = obj.get("type", "?")
                        topic = obj.get("topic", "?")
                        payload = obj.get("payload", {})
                        
                        data_arr = payload.get("data", []) if isinstance(payload, dict) else []
                        n = len(data_arr) if isinstance(data_arr, list) else 0
                        
                        value = payload.get("value") if isinstance(payload, dict) else None

                        if n > 0:
                            result["prices"] += n
                            last_p = data_arr[-1].get("value", "?")
                            print(f"  [{elapsed:5.1f}s] type={mtype} topic={topic} data[{n}] last=${last_p}")
                            if init_done:
                                result["updates_after_init"] += n
                            init_done = True
                        elif value is not None:
                            result["prices"] += 1
                            print(f"  [{elapsed:5.1f}s] type={mtype} topic={topic} value=${value}")
                            if init_done:
                                result["updates_after_init"] += 1
                        else:
                            print(f"  [{elapsed:5.1f}s] type={mtype} topic={topic} (no price data)")
                            print(f"         {json.dumps(obj, ensure_ascii=False)[:200]}")

                    elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                        print(f"  WS {msg.type}")
                        break
                
                # 如果循环正常结束（超时），主动关
                await ws.close()

    except Exception as e:
        print(f"  错误: {e}")
        result["error"] = str(e)

    print(f"  结果: {result['msgs']}条消息, {result['prices']}个价格, 初始后更新: {result['updates_after_init']}")
    return result


async def main():
    print(f"=== RTDS 订阅方式对比测试 (每种{DURATION}s) ===")
    results = []
    for v in SUBSCRIBE_VARIANTS:
        r = await test_variant(v)
        results.append(r)

    print("\n\n=== 汇总 ===")
    print(f"{'方式':<50} {'消息':>5} {'价格':>6} {'初始后更新':>10}")
    print("-" * 75)
    for r in results:
        print(f"{r['name']:<50} {r['msgs']:>5} {r['prices']:>6} {r['updates_after_init']:>10}")

if __name__ == "__main__":
    asyncio.run(main())
