"""完整的盈利机制分析 — 用末期价格+最高价推断赢家"""
import json
from collections import defaultdict
from datetime import datetime, timezone

with open("docs/pm_account_0x1d_raw_trades.json") as f:
    trades = json.load(f)

btc5m = [t for t in trades if "btc-updown-5m" in t.get("eventSlug", "")]

events = defaultdict(list)
for t in btc5m:
    events[t["eventSlug"]].append(t)

# 推断赢家 (基于末期价格和最高价)
# 末期价格>0.7的一方是赢家, 或者最高价接近1.0的一方
def infer_winner(slug, etrades):
    trades_priced = [t for t in etrades if t["price"] > 0 and t["outcome"] in ("Up", "Down")]
    if not trades_priced:
        return "?"
    
    window_start = int(slug.split("-")[-1])
    
    # 方法1: 最后60s的均价
    late = [t for t in trades_priced if t["timestamp"] - window_start >= 240]
    if late:
        late_up = [t["price"] for t in late if t["outcome"] == "Up"]
        late_dn = [t["price"] for t in late if t["outcome"] == "Down"]
        late_up_avg = sum(late_up)/len(late_up) if late_up else 0
        late_dn_avg = sum(late_dn)/len(late_dn) if late_dn else 0
        if late_up_avg > 0.6 and late_dn_avg < 0.4:
            return "Up"
        if late_dn_avg > 0.6 and late_up_avg < 0.4:
            return "Down"
        if late_up_avg > late_dn_avg:
            return "Up"
        if late_dn_avg > late_up_avg:
            return "Down"
    
    # 方法2: 最高价
    all_up = [t["price"] for t in trades_priced if t["outcome"] == "Up"]
    all_dn = [t["price"] for t in trades_priced if t["outcome"] == "Down"]
    max_up = max(all_up) if all_up else 0
    max_dn = max(all_dn) if all_dn else 0
    if max_up > 0.9 and max_dn < 0.7:
        return "Up"
    if max_dn > 0.9 and max_up < 0.7:
        return "Down"
    if max_up > max_dn:
        return "Up"
    return "Down"

# ================================================================
# 逐窗口 P&L
# ================================================================
print("=" * 130)
print("BTC 5min 逐窗口盈亏分析")
print("=" * 130)
print(f"{'时间':>8} │ {'Up$':>6} {'Up份':>7} {'Up均价':>6} │ "
      f"{'Dn$':>6} {'Dn份':>7} {'Dn均价':>6} │ "
      f"{'总投':>6} {'对冲对':>6} {'差额份':>6} │ "
      f"{'赢':>3} {'收入':>7} {'利润':>8} {'ROI':>7} │ {'CombP':>6}")
print("─" * 130)

results = []
for slug in sorted(events.keys()):
    etrades = events[slug]
    window_start = int(slug.split("-")[-1])
    dt = datetime.fromtimestamp(window_start, tz=timezone.utc)
    
    buy_trades = [t for t in etrades if t["price"] > 0 and t["outcome"] in ("Up", "Down")]
    if not buy_trades:
        continue
    
    up_buys = [t for t in buy_trades if t["outcome"] == "Up"]
    dn_buys = [t for t in buy_trades if t["outcome"] == "Down"]
    
    up_usdc = sum(float(t.get("usdcSize", 0)) for t in up_buys)
    dn_usdc = sum(float(t.get("usdcSize", 0)) for t in dn_buys)
    up_shares = sum(float(t["size"]) for t in up_buys)
    dn_shares = sum(float(t["size"]) for t in dn_buys)
    
    if up_shares == 0 or dn_shares == 0:
        continue
    
    up_avg_p = up_usdc / up_shares
    dn_avg_p = dn_usdc / dn_shares
    total_cost = up_usdc + dn_usdc
    combined_p = up_avg_p + dn_avg_p
    
    winner = infer_winner(slug, etrades)
    
    # 收入: 赢家侧 shares × $1.0
    if winner == "Up":
        revenue = up_shares
    elif winner == "Down":
        revenue = dn_shares
    else:
        revenue = 0
    
    profit = revenue - total_cost
    roi = profit / total_cost * 100
    
    paired = min(up_shares, dn_shares)
    excess = abs(up_shares - dn_shares)
    
    results.append({
        "dt": dt, "slug": slug, "winner": winner,
        "up_usdc": up_usdc, "dn_usdc": dn_usdc,
        "up_shares": up_shares, "dn_shares": dn_shares,
        "up_avg_p": up_avg_p, "dn_avg_p": dn_avg_p,
        "total_cost": total_cost, "combined_p": combined_p,
        "paired": paired, "excess": excess,
        "revenue": revenue, "profit": profit, "roi": roi,
    })
    
    excess_side = "Up" if up_shares > dn_shares else "Dn"
    print(f"{dt.strftime('%H:%M'):>8} │ "
          f"${up_usdc:>5.0f} {up_shares:>6.0f} {up_avg_p:>6.3f} │ "
          f"${dn_usdc:>5.0f} {dn_shares:>6.0f} {dn_avg_p:>6.3f} │ "
          f"${total_cost:>5.0f} {paired:>5.0f} {excess:>4.0f}{excess_side} │ "
          f"{winner:>3} ${revenue:>6.0f} ${profit:>+7.0f} {roi:>+6.1f}% │ {combined_p:.4f}")

# ================================================================
# 汇总
# ================================================================
print(f"\n{'='*80}")
print(f"汇总统计")
print(f"{'='*80}")

total_cost = sum(r["total_cost"] for r in results)
total_revenue = sum(r["revenue"] for r in results)
total_profit = total_revenue - total_cost
overall_roi = total_profit / total_cost * 100

wins = [r for r in results if r["profit"] > 0]
losses = [r for r in results if r["profit"] <= 0]

print(f"\n  总投入:   ${total_cost:>10,.0f}")
print(f"  总收入:   ${total_revenue:>10,.0f}")
print(f"  总利润:   ${total_profit:>+10,.0f}")
print(f"  总 ROI:   {overall_roi:>+9.2f}%")
print(f"  盈利窗口: {len(wins)}/{len(results)} ({len(wins)/len(results)*100:.0f}%)")

if wins:
    max_win = max(r["profit"] for r in wins)
    avg_win = sum(r["profit"] for r in wins) / len(wins)
    print(f"  最大单笔盈利: ${max_win:+,.0f}")
    print(f"  平均单笔盈利: ${avg_win:+,.0f}")
if losses:
    max_loss = min(r["profit"] for r in losses)
    avg_loss = sum(r["profit"] for r in losses) / len(losses)
    print(f"  最大单笔亏损: ${max_loss:+,.0f}")
    print(f"  平均单笔亏损: ${avg_loss:+,.0f}")

# ================================================================
# 盈利来源分解
# ================================================================
print(f"\n{'='*80}")
print(f"盈利来源分解")
print(f"{'='*80}")

total_hedged_profit = 0
total_excess_profit = 0
total_hedged_invest = 0
total_excess_invest = 0

print(f"\n{'时间':>8} │ {'对冲对':>6} {'每对成本':>8} {'对冲利润':>8} │ "
      f"{'多余份':>6} {'多余方':>4} {'方向赢?':>6} {'方向利润':>8} │ {'合计':>8}")
print("─" * 95)

for r in results:
    paired = r["paired"]
    excess = r["excess"]
    
    # 对冲部分: paired 对, 每对成本 = combined_p, 收入固定 $1/对
    hedge_profit = paired * (1.0 - r["combined_p"])
    hedge_invest = paired * r["combined_p"]
    total_hedged_profit += hedge_profit
    total_hedged_invest += hedge_invest
    
    # 方向性部分: 多余的 shares
    if r["up_shares"] > r["dn_shares"]:
        excess_side = "Up"
        excess_price = r["up_avg_p"]
        excess_won = r["winner"] == "Up"
    else:
        excess_side = "Dn"
        excess_price = r["dn_avg_p"]
        excess_won = r["winner"] == "Down"
    
    excess_cost = excess * excess_price
    excess_rev = excess * 1.0 if excess_won else 0
    excess_profit = excess_rev - excess_cost
    total_excess_profit += excess_profit
    total_excess_invest += excess_cost
    
    total = hedge_profit + excess_profit
    
    print(f"{r['dt'].strftime('%H:%M'):>8} │ "
          f"{paired:>5.0f} {r['combined_p']:>8.4f} ${hedge_profit:>+7.0f} │ "
          f"{excess:>5.0f}  {excess_side:>3}  {'✓' if excess_won else '✗':>5} ${excess_profit:>+7.0f} │ ${total:>+7.0f}")

print(f"\n  对冲套利总利润:     ${total_hedged_profit:>+10,.0f}  (投入 ${total_hedged_invest:>,.0f}, ROI {total_hedged_profit/total_hedged_invest*100 if total_hedged_invest else 0:+.2f}%)")
print(f"  方向性excess利润:   ${total_excess_profit:>+10,.0f}  (投入 ${total_excess_invest:>,.0f})")
print(f"  合计:               ${total_hedged_profit+total_excess_profit:>+10,.0f}")

# ================================================================
# 核心机制解释
# ================================================================
print(f"\n{'='*80}")
print(f"核心盈利机制")
print(f"{'='*80}")

avg_combined = sum(r["combined_p"] for r in results) / len(results)
avg_paired_ratio = sum(r["paired"]/(r["up_shares"]+r["dn_shares"]) for r in results) / len(results)
avg_excess_pct = sum(r["excess"]/max(r["up_shares"],r["dn_shares"]) for r in results) / len(results)

print(f"""
  平均 Combined Price: {avg_combined:.4f} (< 1.0 才有套利空间)
  平均对冲覆盖率: {avg_paired_ratio*100:.1f}% (份数匹配度)
  平均 excess 占比: {avg_excess_pct*100:.1f}%

  盈利逻辑:
  ─────────
  1. 双边买入近似等量 shares (Up ~= Down 份数)
  2. 如果 Up均价 + Down均价 < 1.0, 则对冲部分必赚
     例: Up=0.45 + Dn=0.50 = 0.95 → 每对赚 $0.05 (5.3% ROI)
  3. 由于不完全等份, 会有少量方向性 excess
     这部分是赌方向的, 赢了额外赚, 输了额外亏
  4. 只要 combined price < 1.0 的"确定性利润"
     大于 excess 部分的"方向性亏损", 就能持续盈利

  为什么 Combined Price 能 < 1.0?
  ───────────────────────────────
  - 市场流动性不完美, bid-ask spread 给taker留有空间
  - 他在窗口早期入场 (9-15s), 此时市场信息不充分, 
    Up 和 Down 的 ask 价格之和可能 < 1.0
  - TWAP 分批买入, 获得了从低到高的平均价格
  - BTC 5min 这种高频市场, 做市商的价差更大
""")

# ================================================================
# Combined Price 随时间变化
# ================================================================
print(f"{'='*80}")
print(f"Combined Price 随窗口内时间变化 (是否越早越有利)")
print(f"{'='*80}")

for r in results[:5]:  # 看几个有代表性的
    slug = r["slug"]
    etrades = [t for t in events[slug] if t["price"] > 0 and t["outcome"] in ("Up","Down")]
    window_start = int(slug.split("-")[-1])
    
    # 按30s分桶, 计算每桶的 combined price
    by_bucket = defaultdict(lambda: {"up_usdc": 0, "up_shares": 0, "dn_usdc": 0, "dn_shares": 0})
    for t in etrades:
        offset = t["timestamp"] - window_start
        bucket = (offset // 30) * 30
        if bucket > 300:
            continue
        b = by_bucket[bucket]
        if t["outcome"] == "Up":
            b["up_usdc"] += float(t.get("usdcSize", 0))
            b["up_shares"] += float(t["size"])
        else:
            b["dn_usdc"] += float(t.get("usdcSize", 0))
            b["dn_shares"] += float(t["size"])
    
    print(f"\n  {r['dt'].strftime('%H:%M')} (winner={r['winner']}):")
    for bucket in sorted(by_bucket.keys()):
        b = by_bucket[bucket]
        if b["up_shares"] > 0 and b["dn_shares"] > 0:
            up_p = b["up_usdc"] / b["up_shares"]
            dn_p = b["dn_usdc"] / b["dn_shares"]
            cp = up_p + dn_p
            edge = 1.0 - cp
            bar = "+" * int(max(0, edge) * 200) + "-" * int(max(0, -edge) * 200)
            print(f"    {bucket:>3}s-{bucket+30:>3}s: up={up_p:.3f} dn={dn_p:.3f} "
                  f"sum={cp:.4f} edge={edge:+.4f} {bar}")
