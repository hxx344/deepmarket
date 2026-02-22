"""确定每个 BTC 5min 窗口的赢家, 然后计算精确 P&L"""
import json
from collections import defaultdict
from datetime import datetime, timezone

with open("docs/pm_account_0x1d_raw_trades.json") as f:
    trades = json.load(f)

btc5m = [t for t in trades if "btc-updown-5m" in t.get("eventSlug", "")]

# 1. 看结算交易 (price=0)
settle = [t for t in btc5m if t["price"] == 0]
print(f"=== 结算交易 (price=0) ===")
print(f"数量: {len(settle)}")
for t in settle[:5]:
    print(f"  outcome='{t['outcome']}' side={t['side']} size={t['size']} "
          f"usdc={t.get('usdcSize','?')} conditionId={t['conditionId'][:30]}...")

# 2. 看每个条件ID对应的Up/Down
cid_map = {}
for t in btc5m:
    if t["outcome"] in ("Up", "Down") and t["price"] > 0:
        cid_map[t["conditionId"]] = {
            "outcome": t["outcome"],
            "eventSlug": t["eventSlug"]
        }

print(f"\n=== conditionId 映射 ({len(cid_map)}) ===")
for cid, info in list(cid_map.items())[:5]:
    print(f"  {cid[:30]}... → {info['outcome']} ({info['eventSlug'][-10:]})")

# 3. 尝试通过 Polymarket API 获取结算结果
import urllib.request, urllib.error
print(f"\n=== 通过 API 查询市场结果 ===")

events = defaultdict(list)
for t in btc5m:
    events[t["eventSlug"]].append(t)

# 获取 condition_ids per event
for slug in sorted(events.keys())[:3]:
    etrades = events[slug]
    cids = set(t["conditionId"] for t in etrades if t["conditionId"])
    print(f"\n{slug}:")
    print(f"  conditionIds: {[c[:20]+'...' for c in cids]}")
    
    # 用 gamma API 查
    for cid in cids:
        url = f"https://gamma-api.polymarket.com/markets?conditionId={cid}"
        try:
            req = urllib.request.Request(url)
            req.add_header("User-Agent", "Mozilla/5.0")
            resp = urllib.request.urlopen(req, timeout=10)
            data = json.loads(resp.read())
            if data:
                m = data[0] if isinstance(data, list) else data
                resolved = m.get("resolved", "?")
                winner = m.get("winner", "?")
                outcome_prices = m.get("outcomePrices", "?")
                question = m.get("question", "?")[:60]
                print(f"  → resolved={resolved}, winner={winner}, "
                      f"outcomePrices={outcome_prices}, Q={question}")
        except Exception as e:
            print(f"  → API error: {e}")

# 4. 用最后一笔交易价格推断: 价格>0.90 的一方是赢家
print(f"\n=== 通过末期价格推断赢家 ===")
for slug in sorted(events.keys()):
    etrades = [t for t in events[slug] if t["price"] > 0 and t["outcome"] in ("Up", "Down")]
    if not etrades:
        continue
    etrades.sort(key=lambda t: t["timestamp"])
    
    # 最后5笔
    last = etrades[-5:]
    up_last = [t["price"] for t in last if t["outcome"] == "Up"]
    dn_last = [t["price"] for t in last if t["outcome"] == "Down"]
    
    # 最高Up价格 vs 最高Dn价格
    all_up = [t["price"] for t in etrades if t["outcome"] == "Up"]
    all_dn = [t["price"] for t in etrades if t["outcome"] == "Down"]
    
    max_up = max(all_up) if all_up else 0
    max_dn = max(all_dn) if all_dn else 0
    
    # 窗口最后30秒的价格
    window_start = int(slug.split("-")[-1])
    late_trades = [t for t in etrades if t["timestamp"] - window_start >= 240]
    late_up = [t["price"] for t in late_trades if t["outcome"] == "Up"]
    late_dn = [t["price"] for t in late_trades if t["outcome"] == "Down"]
    
    late_up_avg = sum(late_up)/len(late_up) if late_up else 0
    late_dn_avg = sum(late_dn)/len(late_dn) if late_dn else 0
    
    winner = "Up" if late_up_avg > late_dn_avg else "Down" if late_dn_avg > late_up_avg else "?"
    
    dt = datetime.fromtimestamp(window_start, tz=timezone.utc)
    print(f"  {dt.strftime('%H:%M')} ({slug[-10:]}): "
          f"late_up_avg={late_up_avg:.3f} late_dn_avg={late_dn_avg:.3f} "
          f"max_up={max_up:.2f} max_dn={max_dn:.2f} → winner={winner}")
