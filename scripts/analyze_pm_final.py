"""最终分析 - 修复分类 + 时序分析"""
import json
from collections import defaultdict
from datetime import datetime, timezone

with open("docs/pm_account_0x1d_raw_trades.json") as f:
    trades = json.load(f)

print(f"Total unique trades: {len(trades)}")
print(f"All BUY: {sum(1 for t in trades if t['side']=='BUY')}")
print(f"SELL: {sum(1 for t in trades if t['side']=='SELL')}")

# Fix type classification
def get_type(slug):
    parts = slug.split("-")
    coin = parts[0]  # btc, eth, sol, xrp
    if "5m" in slug and "15m" not in slug:
        return f"{coin}_5m"
    elif "15m" in slug:
        return f"{coin}_15m"
    return "other"

# Group by event
events = defaultdict(list)
for t in trades:
    events[t["eventSlug"]].append(t)

results = []
for slug, etrades in sorted(events.items(), key=lambda x: min(t["timestamp"] for t in x[1])):
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
    
    # Check simultaneous entry
    up_ts = sorted(t["timestamp"] for t in up)
    dn_ts = sorted(t["timestamp"] for t in dn)
    first_up = up_ts[0] if up_ts else 0
    first_dn = dn_ts[0] if dn_ts else 0
    entry_gap = abs(first_up - first_dn)
    
    # Check if SELL trades exist
    sells = [t for t in etrades if t["side"] == "SELL"]
    
    results.append({
        "slug": slug, "type": get_type(slug),
        "trades": len(etrades), "sells": len(sells),
        "up_shares": up_shares, "dn_shares": dn_shares,
        "share_ratio": share_ratio,
        "up_cost": up_cost, "dn_cost": dn_cost,
        "total_cost": total_cost,
        "avg_up_p": avg_up_p, "avg_dn_p": avg_dn_p,
        "price_sum": price_sum, "edge": 1 - price_sum,
        "pnl_up": pnl_up, "pnl_dn": pnl_dn,
        "entry_gap": entry_gap,
        "up_count": len(up), "dn_count": len(dn),
    })

# Print table
print()
print(f"{'Type':<8} {'Slug':<40} {'#':>3} {'UpSh':>8} {'DnSh':>8} {'ShR':>5} {'$Tot':>8} {'Psum':>7} {'Edge%':>6} {'PnLUp':>9} {'PnLDn':>9} {'Gap':>4}")
print("-" * 130)
for r in results:
    print(
        f"{r['type']:<8} {r['slug']:<40} {r['trades']:>3} "
        f"{r['up_shares']:>8.0f} {r['dn_shares']:>8.0f} {r['share_ratio']:>5.2f} "
        f"{r['total_cost']:>7.0f} {r['price_sum']:>7.4f} {r['edge']*100:>+6.2f} "
        f"{r['pnl_up']:>+9.0f} {r['pnl_dn']:>+9.0f} {r['entry_gap']:>4}s"
    )

# Aggregate by type
print("\n\n===== AGGREGATE BY MARKET TYPE =====")
types = defaultdict(list)
for r in results:
    types[r["type"]].append(r)

for mtype, group in sorted(types.items()):
    n = len(group)
    edges = [r["edge"] for r in group]
    ratios = [r["share_ratio"] for r in group]
    total = sum(r["total_cost"] for r in group)
    min_pnls = [min(r["pnl_up"], r["pnl_dn"]) for r in group]
    max_pnls = [max(r["pnl_up"], r["pnl_dn"]) for r in group]
    gaps = [r["entry_gap"] for r in group]
    
    # How many have positive edge?
    pos_edge = sum(1 for e in edges if e > 0)
    
    print(f"\n{mtype}: {n} markets, total deployed=${total:.0f}")
    print(f"  Avg edge: {sum(edges)/n*100:+.2f}%  (positive: {pos_edge}/{n})")
    print(f"  Avg share ratio (Up/Dn): {sum(ratios)/n:.3f}")
    print(f"  Entry time gap (Up vs Dn): avg={sum(gaps)/n:.0f}s, max={max(gaps)}s")
    print(f"  Guaranteed min PnL sum: ${sum(min_pnls):+.0f}")
    print(f"  Best-case PnL sum: ${sum(max_pnls):+.0f}")

# KEY QUESTION: Are they buying equal shares?
print("\n\n===== SHARE RATIO ANALYSIS (Equal shares?) =====")
all_ratios = [r["share_ratio"] for r in results]
close_to_1 = sum(1 for r in all_ratios if 0.9 <= r <= 1.1)
print(f"Share ratio 0.9-1.1 (equal shares): {close_to_1}/{len(all_ratios)}")
print(f"Share ratio <0.9 or >1.1 (biased): {len(all_ratios)-close_to_1}/{len(all_ratios)}")

# Strategy conclusion
print("\n\n===== STRATEGY CONCLUSION =====")
all_buy = sum(1 for t in trades if t["side"] == "BUY")
all_sell = sum(1 for t in trades if t["side"] == "SELL")
print(f"BUY trades: {all_buy}, SELL trades: {all_sell}")
print(f"100% dual-side entry: {sum(1 for r in results if r['up_count']>0 and r['dn_count']>0)}/{len(results)}")

avg_edge = sum(r["edge"] for r in results) / len(results)
print(f"Average price_sum: {1-avg_edge:.4f}")
print(f"Average edge: {avg_edge*100:+.2f}%")

if avg_edge < 0:
    print("=> NOT profitable arbitrage (avg_up + avg_dn > 1.0)")
    print("=> Possible explanations:")
    print("   1. Taker orders pay spread, walking up the book")
    print("   2. Strategy relies on SELECTIVE market timing")
    print("   3. Some markets have edge, others don't")
    print("   4. May be a market-making / liquidity strategy")
