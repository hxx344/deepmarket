"""
精确分析交易者的盈亏机制和策略核心
重点: 每个窗口的双边买入比例、净成本、预期盈亏
"""
import requests
import json
from datetime import datetime, timezone
from collections import defaultdict
import statistics

ADDR = "0x1d0034134e"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
}

# 拉取交易
all_trades = []
offset = 0
while True:
    url = f"https://data-api.polymarket.com/trades?address={ADDR}&limit=100&offset={offset}"
    r = requests.get(url, headers=HEADERS, timeout=15)
    if r.status_code != 200 or not r.json():
        break
    batch = r.json()
    all_trades.extend(batch)
    if len(batch) < 100:
        break
    offset += 100
print(f"Total trades: {len(all_trades)}")

# 过滤 BTC 5min
btc_5m = [t for t in all_trades if "5m" in (t.get("slug") or "").lower() and "btc" in (t.get("slug") or "").lower()]
print(f"BTC 5m trades: {len(btc_5m)}")

# ════════════════════════════════════════════════════════════
# 按窗口 (slug) 分组 — 详细分析每个窗口的双边策略
# ════════════════════════════════════════════════════════════
by_slug = defaultdict(list)
for t in btc_5m:
    by_slug[t["slug"]].append(t)

print(f"\n{'='*80}")
print(f"BTC 5min — {len(by_slug)} 个窗口的双边策略分析")
print(f"{'='*80}")

for slug in sorted(by_slug.keys()):
    trades = sorted(by_slug[slug], key=lambda x: x["timestamp"])
    title = trades[0].get("title", "")
    
    # 按 outcome 分组，计算买入/卖出
    up_buys = [t for t in trades if t["side"] == "BUY" and t.get("outcome") == "Up"]
    up_sells = [t for t in trades if t["side"] == "SELL" and t.get("outcome") == "Up"]
    dn_buys = [t for t in trades if t["side"] == "BUY" and t.get("outcome") == "Down"]
    dn_sells = [t for t in trades if t["side"] == "SELL" and t.get("outcome") == "Down"]
    
    # 计算净持仓 shares (size / price)
    up_buy_cost = sum(t["size"] for t in up_buys)
    up_buy_shares = sum(t["size"] / t["price"] for t in up_buys) if up_buys else 0
    up_sell_cost = sum(t["size"] for t in up_sells)
    up_sell_shares = sum(t["size"] / t["price"] for t in up_sells) if up_sells else 0
    
    dn_buy_cost = sum(t["size"] for t in dn_buys)
    dn_buy_shares = sum(t["size"] / t["price"] for t in dn_buys) if dn_buys else 0
    dn_sell_cost = sum(t["size"] for t in dn_sells)
    dn_sell_shares = sum(t["size"] / t["price"] for t in dn_sells) if dn_sells else 0
    
    # 净持仓
    net_up_shares = up_buy_shares - up_sell_shares
    net_dn_shares = dn_buy_shares - dn_sell_shares
    
    # 净成本 = 买入花费 - 卖出收入
    net_up_cost = up_buy_cost - up_sell_cost
    net_dn_cost = dn_buy_cost - dn_sell_cost
    total_cost = net_up_cost + net_dn_cost
    
    # 加权平均买入价格
    avg_up_buy_price = up_buy_cost / up_buy_shares if up_buy_shares > 0 else 0
    avg_dn_buy_price = dn_buy_cost / dn_buy_shares if dn_buy_shares > 0 else 0
    
    # 预期盈亏: Up赢 vs Down赢
    # 如果 Up 赢: 获得 net_up_shares * $1.00, Down=0
    pnl_if_up_wins = net_up_shares * 1.0 - total_cost
    # 如果 Down 赢: 获得 net_dn_shares * $1.00, Up=0
    pnl_if_dn_wins = net_dn_shares * 1.0 - total_cost
    
    # 价格时序: 最早和最晚的价格
    up_prices_ts = [(t["timestamp"], t["price"]) for t in up_buys]
    
    print(f"\n{'─'*80}")
    print(f"窗口: {title}")
    print(f"Slug: {slug}")
    print(f"交易笔数: {len(trades)}")
    print(f"")
    print(f"  ┌─ UP 方向 ────────────────────────────────────────")
    print(f"  │ BUY:  {len(up_buys):4d} 笔  花费 ${up_buy_cost:10.2f}  份额 {up_buy_shares:10.1f}  均价 ¢{avg_up_buy_price*100:.1f}")
    print(f"  │ SELL: {len(up_sells):4d} 笔  收入 ${up_sell_cost:10.2f}  份额 {up_sell_shares:10.1f}")
    print(f"  │ 净持仓: {net_up_shares:10.1f} 份  净成本 ${net_up_cost:10.2f}")
    print(f"  │")
    print(f"  ├─ DOWN 方向 ──────────────────────────────────────")
    print(f"  │ BUY:  {len(dn_buys):4d} 笔  花费 ${dn_buy_cost:10.2f}  份额 {dn_buy_shares:10.1f}  均价 ¢{avg_dn_buy_price*100:.1f}")
    print(f"  │ SELL: {len(dn_sells):4d} 笔  收入 ${dn_sell_cost:10.2f}  份额 {dn_sell_shares:10.1f}")
    print(f"  │ 净持仓: {net_dn_shares:10.1f} 份  净成本 ${net_dn_cost:10.2f}")
    print(f"  │")
    print(f"  ├─ 汇总 ──────────────────────────────────────────")
    print(f"  │ 总投入:  ${total_cost:10.2f}")
    print(f"  │ Up+Down 净份额每份均价: ¢{total_cost/(net_up_shares+net_dn_shares)*100:.1f}" if (net_up_shares+net_dn_shares) > 0 else "  │")
    print(f"  │ 双边价格之和 (Up均价+Down均价): ¢{(avg_up_buy_price+avg_dn_buy_price)*100:.1f}")
    print(f"  │")
    print(f"  ├─ 预期盈亏 ──────────────────────────────────────")
    print(f"  │ 如果 UP 获胜:   收回 ${net_up_shares:.2f}  →  PnL = ${pnl_if_up_wins:+.2f}")
    print(f"  │ 如果 DOWN 获胜: 收回 ${net_dn_shares:.2f}  →  PnL = ${pnl_if_dn_wins:+.2f}")
    print(f"  │ 持仓偏向: {'UP' if net_up_shares > net_dn_shares else 'DOWN'} (比率 {max(net_up_shares,net_dn_shares)/max(min(net_up_shares,net_dn_shares),1):.1f}x)")
    print(f"  └─────────────────────────────────────────────────")
    
    # 价格波动时间线 (取前5笔和后5笔)
    if len(trades) > 10:
        early = trades[:5]
        late = trades[-5:]
        print(f"\n  前5笔: ", " | ".join(f"{t['side']}_{t.get('outcome','?')}@{t['price']:.2f}x${t['size']:.1f}" for t in early))
        print(f"  后5笔: ", " | ".join(f"{t['side']}_{t.get('outcome','?')}@{t['price']:.2f}x${t['size']:.1f}" for t in late))

# ════════════════════════════════════════════════════════════
# 价格阶段分析 — 看早期 vs 中期 vs 晚期的买入价
# ════════════════════════════════════════════════════════════
print(f"\n\n{'='*80}")
print("价格阶段分析 (按交易时间三等分)")
print(f"{'='*80}")

for slug in sorted(by_slug.keys()):
    trades = sorted(by_slug[slug], key=lambda x: x["timestamp"])
    if len(trades) < 20:
        continue
    
    n = len(trades)
    thirds = [trades[:n//3], trades[n//3:2*n//3], trades[2*n//3:]]
    labels = ["早期(前1/3)", "中期(中1/3)", "晚期(后1/3)"]
    
    print(f"\n{trades[0].get('title','?')}")
    for label, chunk in zip(labels, thirds):
        up_b = [t for t in chunk if t["side"] == "BUY" and t.get("outcome") == "Up"]
        dn_b = [t for t in chunk if t["side"] == "BUY" and t.get("outcome") == "Down"]
        up_prices = [t["price"] for t in up_b] if up_b else [0]
        dn_prices = [t["price"] for t in dn_b] if dn_b else [0]
        up_vol = sum(t["size"] for t in up_b)
        dn_vol = sum(t["size"] for t in dn_b)
        
        print(f"  {label}:")
        print(f"    Up  BUY: {len(up_b):3d}笔 avg¢{statistics.mean(up_prices)*100:.1f} vol${up_vol:.0f}")
        print(f"    Dn  BUY: {len(dn_b):3d}笔 avg¢{statistics.mean(dn_prices)*100:.1f} vol${dn_vol:.0f}")
        print(f"    Up+Dn 均价之和: ¢{(statistics.mean(up_prices)+statistics.mean(dn_prices))*100:.1f}")

# ════════════════════════════════════════════════════════════
# 非 5min 交易统计 (看看他还做什么)
# ════════════════════════════════════════════════════════════
print(f"\n\n{'='*80}")
print("其他市场 (非 5min) 交易分类")
print(f"{'='*80}")

other_cats = defaultdict(lambda: {"count": 0, "vol": 0.0})
for t in all_trades:
    slug = t.get("slug", "")
    title = t.get("title", "")
    if "5m" in slug.lower():
        continue
    # 提取事件类型
    if "bitcoin" in title.lower() or "btc" in slug.lower():
        cat = "BTC other"
    elif "ethereum" in title.lower() or "eth" in slug.lower():
        cat = "ETH other"
    else:
        cat = "Non-crypto"
    other_cats[cat]["count"] += 1
    other_cats[cat]["vol"] += t["size"]

for cat in sorted(other_cats.keys()):
    d = other_cats[cat]
    print(f"  {cat}: {d['count']} 笔  ${d['vol']:.2f}")

# 列出所有不同的 slug 前缀
print(f"\n所有 slug 类型:")
slug_types = defaultdict(int)
for t in all_trades:
    slug = t.get("slug", "")
    # 提取 slug 前缀 (去掉时间戳)
    parts = slug.rsplit("-", 1)
    prefix = parts[0] if len(parts) > 1 else slug
    slug_types[prefix] += 1

for prefix, count in sorted(slug_types.items(), key=lambda x: -x[1]):
    print(f"  {count:4d}x | {prefix}")

print("\nDone.")
