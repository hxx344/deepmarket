"""诊断脚本: 检测价格数据流管道"""
import asyncio, json, time, aiohttp
from src.core.context import Context, RunMode, TradingMode
from src.core.event_bus import Event, EventType, get_event_bus
from src.dashboard.server import DashboardServer
from src.market.datasources.chainlink_ds import ChainlinkDataSource, ChainlinkConfig

event_counts = {"chainlink_price": 0, "market_tick": 0}

async def test():
    ctx = Context(run_mode=RunMode.PAPER, trading_mode=TradingMode.PAPER)
    ctx.account.balance = 100.0
    ctx.account.initial_balance = 100.0
    ctx.account.total_equity = 100.0
    bus = get_event_bus()

    # 1. 价格同步 + 计数
    async def _sync(event):
        event_counts["chainlink_price"] += 1
        price = event.data.get("price", 0)
        if price > 0:
            ctx.market.btc_price = price

    async def _tick(event):
        event_counts["market_tick"] += 1

    bus.subscribe(EventType.CHAINLINK_PRICE, _sync, priority=100)
    bus.subscribe(EventType.MARKET_TICK, _tick, priority=100)

    # 2. 启动 Dashboard
    server = DashboardServer(context=ctx, event_bus=bus, port=8081)
    await server.start()
    print("Dashboard started at :8081")

    # 3. 启动 Chainlink
    ds = ChainlinkDataSource(event_bus=bus, config=ChainlinkConfig(mode="rtds"))
    await ds.connect()
    if ds._last_price > 0:
        ctx.market.btc_price = ds._last_price
        print(f"Initial price: ${ds._last_price:,.2f}")
    stream = asyncio.create_task(ds.start_streaming())

    await asyncio.sleep(2)  # 等 RTDS WS 连接

    # 4. 模拟浏览器 WS 客户端
    ws_msgs = []
    async def ws_client():
        session = aiohttp.ClientSession()
        try:
            async with session.ws_connect("http://localhost:8081/ws") as ws:
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = json.loads(msg.data)
                        ws_msgs.append(data)
                        t = data.get("type", "?")
                        d = data.get("data", {})
                        if len(ws_msgs) <= 5:
                            print(f"  WS [{len(ws_msgs)}] type={t} price={d.get('price','N/A')} keys={list(d.keys())[:5]}")
                    if len(ws_msgs) >= 30:
                        break
        except asyncio.CancelledError:
            pass
        finally:
            await session.close()

    client_task = asyncio.create_task(ws_client())

    # 5. 检查 8 秒
    for i in range(8):
        await asyncio.sleep(1)
        print(
            f"[{i+1}s] chainlink={event_counts['chainlink_price']} "
            f"tick={event_counts['market_tick']} "
            f"ctx.btc=${ctx.market.btc_price:,.2f} "
            f"ws_conns={server.ws_manager.client_count} "
            f"ws_msgs={len(ws_msgs)}"
        )

    # 清理
    client_task.cancel()
    try:
        await client_task
    except asyncio.CancelledError:
        pass
    ds._running = False
    stream.cancel()
    await server.stop()

    # 统计
    types = {}
    for m in ws_msgs:
        t = m.get("type", "?")
        types[t] = types.get(t, 0) + 1
    print(f"\n=== SUMMARY ===")
    print(f"EventBus: chainlink={event_counts['chainlink_price']}, tick={event_counts['market_tick']}")
    print(f"WS msgs total: {len(ws_msgs)}")
    print(f"WS by type: {types}")
    if ws_msgs:
        for m in ws_msgs[:3]:
            print(f"  Sample: {json.dumps(m, default=str)[:200]}")

asyncio.run(test())
