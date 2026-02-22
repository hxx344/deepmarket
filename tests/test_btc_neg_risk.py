"""查找当前活跃的 BTC 5-min 市场的 negRisk"""
import requests, json

# 搜索 BTC 5-min markets
keywords = ["Bitcoin 5-Minute", "BTC 5 min", "Bitcoin price at"]
for kw in keywords:
    r = requests.get(
        "https://gamma-api.polymarket.com/markets",
        params={"active": "true", "closed": "false", "limit": "10", "query": kw},
        timeout=10,
    )
    markets = r.json()
    if markets:
        print(f"\nSearch '{kw}': {len(markets)} results")
        for m in markets[:5]:
            q = m.get("question", "?")
            neg = m.get("negRisk", None)
            tokens = m.get("clobTokenIds", "[]")
            if isinstance(tokens, str):
                tokens = json.loads(tokens)
            print(f"  Q: {q[:80]}")
            print(f"    negRisk={neg}, tokens={[t[:15]+'...' for t in tokens]}")

# 搜索更多关键词
print("\n\n=== 搜索 'Bitcoin Up' ===")
r2 = requests.get(
    "https://gamma-api.polymarket.com/markets",
    params={"active": "true", "closed": "false", "limit": "10", "query": "Bitcoin Up"},
    timeout=10,
)
for m in r2.json()[:5]:
    q = m.get("question", "?")
    neg = m.get("negRisk", None)
    tokens = m.get("clobTokenIds", "[]")
    if isinstance(tokens, str):
        tokens = json.loads(tokens)
    print(f"  Q: {q[:80]}")
    print(f"    negRisk={neg}, tokens={[t[:15]+'...' for t in tokens]}")
