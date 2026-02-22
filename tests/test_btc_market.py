"""查询 BTC 5-min 市场的 neg_risk 和 token IDs"""
import requests, json

# 搜索 BTC 5-min 市场
resp = requests.get(
    "https://gamma-api.polymarket.com/markets?tag=btc-5-minute&active=true&closed=false&limit=5",
    timeout=10,
)
markets = resp.json()
print(f"Found {len(markets)} BTC 5-min markets\n")

for m in markets[:3]:
    q = m.get("question", "?")
    neg = m.get("negRisk", None)
    tokens = m.get("clobTokenIds", "[]")
    if isinstance(tokens, str):
        tokens = json.loads(tokens)
    cid = m.get("conditionId", "?")
    
    print(f"Question: {q}")
    print(f"  negRisk: {neg}")
    print(f"  conditionId: {cid[:30]}...")
    print(f"  tokens: {[t[:15]+'...' for t in tokens]}")
    
    # 检查 token 是否以 330764 开头
    for t in tokens:
        if t.startswith("330764"):
            print(f"  *** FOUND TOKEN 330764... in this market!")
    print()

# 也搜索包含 330764 的市场
print("=== 搜索 token 330764214394... ===")
# 用 CLOB API
resp2 = requests.get("https://clob.polymarket.com/markets?next_cursor=LQ==", timeout=10)
# This returns paginated results, not ideal. Let me try gamma API differently
resp3 = requests.get(
    "https://gamma-api.polymarket.com/markets?active=true&closed=false&limit=50&tag=btc-5-minute",
    timeout=10,
)
all_markets = resp3.json()
for m in all_markets:
    tokens = m.get("clobTokenIds", "[]")
    if isinstance(tokens, str):
        tokens = json.loads(tokens)
    for t in tokens:
        if t.startswith("330764"):
            print(f"\nFound market with token 330764:")
            print(f"  Question: {m.get('question','?')}")
            print(f"  negRisk: {m.get('negRisk')}")
            print(f"  conditionId: {m.get('conditionId', '?')[:30]}...")
            print(f"  tokens: {tokens}")
            break
