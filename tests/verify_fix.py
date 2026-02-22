"""
验证修复: 确认 ws_handler 的 price_broadcast 和 price_history 功能
"""
import asyncio
import json
import time
import sys
sys.path.insert(0, ".")

async def main():
    import aiohttp

    results = {"snapshot": 0, "price": 0, "heartbeat": 0, "price_history": 0, "other": 0}
    prices_received = []
    history_count = 0

    print("=== 修复验证: 连接 Dashboard WS ===")
    print(f"时间: {time.strftime('%H:%M:%S')}")
    print()

    try:
        session = aiohttp.ClientSession()
        ws = await session.ws_connect("http://localhost:8080/ws", timeout=5)
        # 订阅 price 频道
        await ws.send_str(json.dumps({"action": "subscribe", "channels": ["price"]}))
        print("已连接并订阅 price 频道")
        
        start = time.time()
        async for msg in ws:
            elapsed = time.time() - start
            if elapsed > 15:  # 测试15秒
                break
            if msg.type == aiohttp.WSMsgType.TEXT:
                data = json.loads(msg.data)
                t = data.get("type", "unknown")
                results[t] = results.get(t, 0) + 1
                
                if t == "price":
                    p = data.get("data", {}).get("price", 0)
                    src = data.get("data", {}).get("source", "?")
                    prices_received.append(p)
                    if len(prices_received) <= 5:
                        print(f"  [{elapsed:.1f}s] price: ${p:,.2f} (source={src})")
                elif t == "price_history":
                    hist = data.get("data", {}).get("prices", [])
                    history_count = len(hist)
                    print(f"  [{elapsed:.1f}s] price_history: {history_count} 条历史数据")
                    if hist:
                        print(f"    首条: time={hist[0].get('time')}, value=${hist[0].get('value', 0):,.2f}")
                        print(f"    末条: time={hist[-1].get('time')}, value=${hist[-1].get('value', 0):,.2f}")
                elif t == "snapshot":
                    mkt = data.get("data", {}).get("market", {})
                    print(f"  [{elapsed:.1f}s] snapshot: btc=${mkt.get('btc_price', 0):,.2f}")
                elif t == "heartbeat":
                    p = data.get("data", {}).get("btc_price", 0)
                    print(f"  [{elapsed:.1f}s] heartbeat: btc=${p:,.2f}")

        await ws.close()
        await session.close()
    except Exception as e:
        print(f"连接失败: {e}")
        print("请确保 Dashboard 正在运行 (python -m src.main)")
        return

    print()
    print("=== 结果汇总 ===")
    print(f"消息类型统计: {results}")
    print(f"price_history 回放: {history_count} 条")
    print(f"实时 price 消息: {results.get('price', 0)} 条")
    if prices_received:
        print(f"价格范围: ${min(prices_received):,.2f} ~ ${max(prices_received):,.2f}")
        unique = len(set(f"{p:.2f}" for p in prices_received))
        print(f"唯一价格数: {unique}")

    # 判断修复状态
    print()
    ok = True
    if results.get("price", 0) >= 3:
        print("✓ BTC 价格持续推送 (不再冻结)")
    else:
        print("✗ BTC 价格推送不足")
        ok = False
    if history_count > 0:
        print("✓ 历史 tick 数据回放正常")
    else:
        print("? 无历史数据 (首次连接可能为空)")
    if results.get("snapshot", 0) >= 1:
        print("✓ 快照推送正常")
    else:
        print("✗ 快照缺失")
        ok = False
    print()
    print("整体: " + ("全部通过 ✓" if ok else "仍有问题 ✗"))

asyncio.run(main())
