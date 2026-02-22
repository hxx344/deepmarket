"""搜索当前活跃未关闭的 BTC 价格预测市场"""
import asyncio
import aiohttp
import json
from datetime import datetime, timezone

async def main():
    timeout = aiohttp.ClientTimeout(total=15)
    async with aiohttp.ClientSession(timeout=timeout) as s:

        # 1) Gamma events - 大量加载, 搜索bitcoin/crypto price相关
        print("=== 搜索所有活跃的 events (前100个) ===")
        try:
            url = "https://gamma-api.polymarket.com/events"
            params = {"active": "true", "closed": "false", "limit": "100"}
            async with s.get(url, params=params) as r:
                events = await r.json()
                print(f"Total active events: {len(events)}")
                for ev in events:
                    title = ev.get("title", "")
                    tl = title.lower()
                    if any(kw in tl for kw in ["bitcoin", "btc", "crypto price", "minute"]):
                        n = len(ev.get("markets", []))
                        print(f"  [{n} mkts] {title}")
                        if n > 0:
                            m0 = ev["markets"][0]
                            print(f"    sample Q: {m0.get('question','')}")
                            print(f"    end: {m0.get('end_date_iso','')}")
        except Exception as e:
            print(f"Error: {e}")

        # 2) CLOB API - 搜索活跃未关闭的 BTC 市场（第一页有1000个市场很杂）
        # 尝试更多页面找到最近的
        print("\n=== CLOB: 搜索活跃未关闭的 BTC price 市场 ===")
        try:
            found = []
            cursor = "MA=="
            now = datetime.now(timezone.utc)
            for page in range(20):  # 最多20页
                url = "https://clob.polymarket.com/markets"
                params = {"next_cursor": cursor}
                async with s.get(url, params=params) as r:
                    data = await r.json()
                    markets = data.get("data", []) if isinstance(data, dict) else data
                    cursor = data.get("next_cursor", "") if isinstance(data, dict) else ""

                    for m in markets:
                        q = m.get("question", "").lower()
                        closed = m.get("closed", True)
                        active = m.get("active", False)
                        end_str = m.get("end_date_iso", "")

                        if not active or closed:
                            continue
                        if not any(kw in q for kw in ["btc above", "bitcoin above", "btc below", "bitcoin below"]):
                            continue

                        # 检查 end_date 是否在未来
                        if end_str:
                            try:
                                end_dt = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
                                if end_dt < now:
                                    continue
                            except:
                                pass

                        found.append(m)

                    if not cursor or cursor == "LTE=":
                        break

            print(f"Found {len(found)} active unclosed future BTC price markets")
            for m in found[:10]:
                print(f"\n  Q: {m.get('question','')}")
                print(f"  condition_id: {m.get('condition_id','')}")
                print(f"  end: {m.get('end_date_iso','')}")
                for t in m.get("tokens", []):
                    oc = t.get("outcome", "?")
                    p = t.get("price", "?")
                    tid = t.get("token_id", "")[:25]
                    print(f"    {oc}: price={p} id={tid}...")
        except Exception as e:
            print(f"CLOB error: {e}")

        # 3) 尝试直接的 Gamma markets with end_date_min
        print("\n=== Gamma markets: future BTC ===")
        try:
            url = "https://gamma-api.polymarket.com/markets"
            now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            params = {
                "active": "true",
                "closed": "false",
                "limit": "20",
                "end_date_min": now_iso,
                "order": "volume24hr",
                "ascending": "false",
            }
            async with s.get(url, params=params) as r:
                markets = await r.json()
                btc = [m for m in markets if any(kw in m.get("question","").lower() for kw in ["bitcoin", "btc"])]
                print(f"Total future markets: {len(markets)}, BTC: {len(btc)}")
                for m in btc[:5]:
                    print(f"  Q: {m.get('question','')}")
                    print(f"  end: {m.get('end_date_iso','')}")
                    for t in m.get("tokens", []):
                        print(f"    {t.get('outcome')}: price={t.get('price')}")
        except Exception as e:
            print(f"Error: {e}")

        # 4) 尝试搜索 Gamma 里 "5 minute" 相关的
        print("\n=== Gamma events: 全量搜索 'minute' 或 'price prediction' ===")
        try:
            for offset in range(0, 300, 100):
                url = "https://gamma-api.polymarket.com/events"
                params = {"active": "true", "closed": "false", "limit": "100", "offset": str(offset)}
                async with s.get(url, params=params) as r:
                    events = await r.json()
                    if not events:
                        break
                    for ev in events:
                        title = ev.get("title", "")
                        tl = title.lower()
                        n = len(ev.get("markets", []))
                        if any(kw in tl for kw in ["minute", "price prediction", "btc above", "bitcoin above", "5-min", "5min"]):
                            print(f"  [{n} mkts] {title}")
        except Exception as e:
            print(f"Error: {e}")

asyncio.run(main())
