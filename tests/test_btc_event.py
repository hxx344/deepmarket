"""直接查 BTC 5-min 当前窗口的 negRisk"""
import requests, json, time, math

# 计算当前 5-min 窗口 start timestamp
now = time.time()
window_start = int(now // 300) * 300
slug = f"btc-updown-5m-{window_start}"
print(f"Slug: {slug}")

# 查 events API (跟数据源一样)
r = requests.get(
    "https://gamma-api.polymarket.com/events",
    params={"slug": slug},
    timeout=10,
)
events = r.json()
if not events:
    # 试上一个窗口
    window_start -= 300
    slug = f"btc-updown-5m-{window_start}"
    print(f"No event, trying previous: {slug}")
    r = requests.get(
        "https://gamma-api.polymarket.com/events",
        params={"slug": slug},
        timeout=10,
    )
    events = r.json()

if events:
    event = events[0]
    print(f"Event: {event.get('title', '?')}")
    print(f"Event negRisk: {event.get('negRisk', 'N/A')}")
    
    markets = event.get("markets", [])
    print(f"\nMarkets ({len(markets)}):")
    for m in markets:
        q = m.get("question", "?")
        neg = m.get("negRisk", None)
        tokens = m.get("clobTokenIds", "[]")
        if isinstance(tokens, str):
            tokens = json.loads(tokens)
        cid = m.get("conditionId", "?")
        print(f"\n  Q: {q}")
        print(f"  negRisk: {neg}")
        print(f"  conditionId: {cid}")
        print(f"  tokens: {tokens}")
        
        # 看看 token 是否以 330764 开头
        for t in tokens:
            if t.startswith("3307"):
                print(f"  *** Token starts with 3307!")
else:
    print("No events found")
