"""获取今天 BTC 上下市场的 token ID 和实际价格"""
import asyncio
import aiohttp
import json

async def main():
    timeout = aiohttp.ClientTimeout(total=15)
    async with aiohttp.ClientSession(timeout=timeout) as s:

        # 获取 BTC 价格预测市场
        url = "https://gamma-api.polymarket.com/markets"
        params = {
            "active": "true",
            "closed": "false",
            "limit": "100",
            "order": "volume24hr",
            "ascending": "false",
        }
        async with s.get(url, params=params) as r:
            markets = await r.json()

        # 筛选 BTC 价格相关
        btc_price_markets = []
        for m in markets:
            q = m.get("question", "").lower()
            if ("bitcoin" in q or "btc" in q) and any(kw in q for kw in ["above", "below", "up or down", "price"]):
                btc_price_markets.append(m)

        print(f"找到 {len(btc_price_markets)} 个 BTC 价格市场\n")

        for m in btc_price_markets:
            q = m.get("question", "")
            cid = m.get("conditionId", "")
            outcomes = m.get("outcomes", "[]")
            prices = m.get("outcomePrices", "[]")
            token_ids = m.get("clobTokenIds", "[]")
            vol24 = m.get("volume24hr", 0)
            end = m.get("endDateIso", "N/A")

            # 解析 JSON 字符串字段
            try:
                outcomes_list = json.loads(outcomes) if isinstance(outcomes, str) else outcomes
                prices_list = json.loads(prices) if isinstance(prices, str) else prices
                token_list = json.loads(token_ids) if isinstance(token_ids, str) else token_ids
            except:
                outcomes_list = prices_list = token_list = []

            print(f"Q: {q}")
            print(f"  conditionId: {cid[:40]}...")
            print(f"  endDate: {end}")
            print(f"  vol24h: ${vol24:,.0f}")
            for i, oc in enumerate(outcomes_list):
                p = prices_list[i] if i < len(prices_list) else "?"
                tid = token_list[i] if i < len(token_list) else "?"
                print(f"  {oc}: price={p}, tokenId={tid}")

            # 获取 CLOB midpoint
            for i, tid in enumerate(token_list):
                if tid:
                    try:
                        mid_url = "https://clob.polymarket.com/midpoint"
                        async with s.get(mid_url, params={"token_id": tid}) as r2:
                            data = await r2.json()
                            oc = outcomes_list[i] if i < len(outcomes_list) else "?"
                            print(f"  {oc} midpoint: {data.get('mid', 'N/A')}")
                    except Exception as e:
                        print(f"  midpoint error: {e}")
            print()

asyncio.run(main())
