"""
Check RTDS timestamp alignment: are prices at :00 or :01?
Record RTDS payload timestamps vs. local clock to understand timing.
"""
import asyncio, json, time, datetime, aiohttp

RTDS_URL = "wss://ws-live-data.polymarket.com"

async def main():
    print("Connecting to RTDS to check timestamp alignment...")
    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(
            RTDS_URL, heartbeat=10.0,
            timeout=aiohttp.ClientTimeout(total=120),
        ) as ws:
            await ws.send_json({
                "action": "subscribe",
                "subscriptions": [{
                    "topic": "crypto_prices_chainlink",
                    "type": "*",
                }],
            })
            
            count = 0
            start = time.time()
            print(f"\n{'recv_time':>14} | {'payload_ts':>14} | {'delta':>8} | {'price':>14} | payload_ts%300")
            print("-" * 80)
            
            async for msg in ws:
                if time.time() - start > 45:
                    break
                if msg.type != aiohttp.WSMsgType.TEXT:
                    continue
                raw = msg.data.strip()
                if not raw:
                    continue
                try:
                    obj = json.loads(raw)
                except:
                    continue
                
                payload = obj.get("payload", {})
                msg_type = obj.get("type", "")
                sym = payload.get("symbol", "").lower()
                
                if sym != "btc/usd":
                    continue
                
                if msg_type == "update":
                    recv_ts = time.time()
                    price = float(payload.get("value", 0))
                    raw_ts = payload.get("timestamp", 0)
                    
                    # RTDS timestamp is in milliseconds
                    if raw_ts > 1e12:
                        payload_ts = raw_ts / 1000.0
                    else:
                        payload_ts = float(raw_ts)
                    
                    delta = recv_ts - payload_ts
                    
                    recv_dt = datetime.datetime.fromtimestamp(recv_ts, tz=datetime.timezone.utc)
                    payload_dt = datetime.datetime.fromtimestamp(payload_ts, tz=datetime.timezone.utc)
                    
                    mod300 = int(payload_ts) % 300
                    
                    print(f"{recv_dt.strftime('%H:%M:%S.%f')[:14]} | "
                          f"{payload_dt.strftime('%H:%M:%S.%f')[:14]} | "
                          f"{delta:>7.3f}s | "
                          f"${price:>12,.2f} | "
                          f"{mod300:>3}s into window")
                    
                    count += 1
                
                elif msg_type == "subscribe":
                    data_arr = payload.get("data", [])
                    print(f"[INIT] Got {len(data_arr)} historical ticks")
                    # Show last 5 historical ticks
                    for item in data_arr[-5:]:
                        p = float(item.get("value", 0))
                        ts_raw = item.get("timestamp", 0)
                        if ts_raw > 1e12:
                            ts_sec = ts_raw / 1000.0
                        else:
                            ts_sec = float(ts_raw)
                        dt = datetime.datetime.fromtimestamp(ts_sec, tz=datetime.timezone.utc)
                        mod300 = int(ts_sec) % 300
                        print(f"  HIST: {dt.strftime('%H:%M:%S.%f')[:14]} | ${p:>12,.2f} | {mod300:>3}s into window")
            
            print(f"\nTotal BTC ticks received: {count}")

asyncio.run(main())
