"""深度分析 PM 账号 0x1d0034134e 每个market的套利模式"""
import json
from collections import defaultdict

with open("docs/pm_account_0x1d_raw_trades.json") as f:
    trades = json.load(f)

print(f"Total unique trades: {len(trades)}")
print(f"All BUY: {all(t['side']=='BUY' for t in trades)}")

# Group by event
events = defaultdict(list)
for t in trades:
    events[t["eventSlug"]].append(t)

event_list = sorted(events.items(), key=lambda x: min(t["timestamp"] for t in x[1]))
print(f"Total events: {len(event_list)}")
print()

results = []
for slug, etrades in event_list:
    by_outcome = defaultdict(list)
    for t in etrades:
        by_outcome[t["outcome"]].append(t)
    
    up = by_outcome.get("Up", [])
    dn = by_outcome.get("Down", [])
    if not up or not dn:
        continue
    
    up_shares = sum(t["size"] for t in up)
    dn_shares = sum(t["size"] for t in dn)
    up_cost = sum(t["usdcSize"] for t in up)
    dn_cost = sum(t["usdcSize"] for t in dn)
    total_cost = up_cost + dn_cost
    
    avg_up_p = up_cost / up_shares if up_shares > 0 else 0
    avg_dn_p = dn_cost / dn_shares if dn_shares > 0 else 0
    price_sum = avg_up_p + avg_dn_p
    
    share_ratio = up_shares / dn_shares if dn_shares > 0 else 0
    
    pnl_up = up_shares - total_cost
    pnl_dn = dn_shares - total_cost
    
    mtype = "btc5m" if "5m" in slug else ("btc15m" if "btc" in slug else slug.split("-")[0] + "15m")
    
    results.append({
        "slug": slug, "type": mtype,
        "up_shares": up_shares, "dn_shares": dn_shares,
        "share_ratio": share_ratio,
        "up_cost": up_cost, "dn_cost": dn_cost,
        "total_cost": total_cost,
        "avg_up_p": avg_up_p, "avg_dn_p": avg_dn_p,
        "price_sum": price_sum, "edge": 1 - price_sum,
        "pnl_up": pnl_up, "pnl_dn": pnl_dn,
    })

# Print table
header = f"{'Slug':<40} {'Type':<8} {'UpSh':>8} {'DnSh':>8} {'ShRatio':>7} {'Cost':>8} {'Psum':>6} {'Edge':>7} {'PnL_Up':>9} {'PnL_Dn':>9}"
print(header)
print("-" * len(header))
for r in results:
    print(
        f"{r['slug']:<40} {r['type']:<8} "
        f"{r['up_shares']:>8.1f} {r['dn_shares']:>8.1f} {r['share_ratio']:>7.3f} "
        f"{r['total_cost']:>7.1f} {r['price_sum']:>6.4f} {r['edge']:>+7.4f} "
        f"{r['pnl_up']:>+9.1f} {r['pnl_dn']:>+9.1f}"
    )

# Aggregate
print()
for label, filt in [("BTC 5min", "btc5m"), ("BTC 15min", "btc15m"), ("ETH 15min", "eth15m"), ("SOL 15min", "sol15m"), ("XRP 15min", "xrp15m")]:
    group = [r for r in results if r["type"] == filt]
    if not group:
        continue
    edges = [r["edge"] for r in group]
    ratios = [r["share_ratio"] for r in group]
    total = sum(r["total_cost"] for r in group)
    min_pnls = [min(r["pnl_up"], r["pnl_dn"]) for r in group]
    max_pnls = [max(r["pnl_up"], r["pnl_dn"]) for r in group]
    print(f"{label}: {len(group)} markets, total cost=${total:.0f}")
    print(f"  Avg edge: {sum(edges)/len(edges)*100:+.2f}%")
    print(f"  Avg share ratio (Up/Dn): {sum(ratios)/len(ratios):.3f}")
    print(f"  Sum guaranteed min PnL: ${sum(min_pnls):+.2f}")
    print(f"  Sum best-case PnL: ${sum(max_pnls):+.2f}")
    print()
