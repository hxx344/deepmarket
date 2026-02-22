"""深度分析 PM 账号 0x1d 的盈利机制 — BTC 5min"""
import json
from collections import defaultdict
from datetime import datetime, timezone

with open("docs/pm_account_0x1d_raw_trades.json") as f:
    trades = json.load(f)

btc5m = [t for t in trades if "btc-updown-5m" in t.get("eventSlug", "")]
print(f"BTC 5min trades: {len(btc5m)}")

events = defaultdict(list)
for t in btc5m:
    events[t["eventSlug"]].append(t)

print(f"Windows: {len(events)}")

# ================================================================
# 逐窗口精确 P&L 计算
# ================================================================
print(f"\n{'='*120}")
print(f"逐窗口 P&L 分析")
print(f"{'='*120}")

print(f"{'时间':>18} │ {'Up$':>7} {'Up份':>8} {'Up均价':>7} │ "
      f"{'Dn$':>7} {'Dn份':>8} {'Dn均价':>7} │ "
      f"{'总投':>7} │ {'赢家':>4} {'收入':>8} │ {'利润':>8} {'ROI':>7} │ 解释")
print("─" * 120)

total_invested = 0
total_revenue = 0
window_results = []

for slug in sorted(events.keys()):
    etrades = events[slug]
    window_start = int(slug.split("-")[-1])
    dt = datetime.fromtimestamp(window_start, tz=timezone.utc)
    
    # 分离买入交易和结算交易
    buy_trades = [t for t in etrades if t["price"] > 0 and t["side"] == "BUY"]
    # price=0 或 非买入 = 结算/赎回
    settle_trades = [t for t in etrades if t["price"] == 0 or t["side"] != "BUY"]
    
    up_buys = [t for t in buy_trades if t["outcome"] == "Up"]
    dn_buys = [t for t in buy_trades if t["outcome"] == "Down"]
    
    up_usdc = sum(float(t.get("usdcSize", 0)) for t in up_buys)
    dn_usdc = sum(float(t.get("usdcSize", 0)) for t in dn_buys)
    up_shares = sum(float(t["size"]) for t in up_buys)
    dn_shares = sum(float(t["size"]) for t in dn_buys)
    
    up_avg_p = up_usdc / up_shares if up_shares > 0 else 0
    dn_avg_p = dn_usdc / dn_shares if dn_shares > 0 else 0
    
    total_cost = up_usdc + dn_usdc
    
    # 判断赢家：
    # 方法1: 看结算交易中哪个 outcome 有 size > 0 且 price=0 (赎回)
    # 方法2: 如果 up_avg_p + dn_avg_p 组合, 稳赢方=shares多的那边? 不对...
    # 方法3: 赎回交易的 outcome 就是赢家
    winner = "?"
    redeem_shares = 0
    for t in settle_trades:
        if t.get("outcome") in ("Up", "Down"):
            winner = t["outcome"]
            redeem_shares += float(t["size"])
    
    # 如果没有结算交易(可能还没结算), 用价格推测
    # 高概率方(均价高的)更可能赢, 但不确定
    if winner == "?" and up_avg_p > 0 and dn_avg_p > 0:
        # 结算后还没赎回也正常, 跳过
        pass
    
    # 计算收入: 赢家侧的 shares × $1.0
    if winner == "Up":
        revenue = up_shares  # 1 share = $1
    elif winner == "Down":
        revenue = dn_shares
    else:
        revenue = 0  # 未知
    
    profit = revenue - total_cost
    roi = (profit / total_cost * 100) if total_cost > 0 else 0
    
    if total_cost > 0:
        total_invested += total_cost
        total_revenue += revenue
    
    # 关键指标: 每份 share 平均成本
    combined_avg = (up_avg_p + dn_avg_p) if (up_avg_p > 0 and dn_avg_p > 0) else 0
    
    # 解释盈利/亏损原因
    explanation = ""
    if winner != "?" and up_shares > 0 and dn_shares > 0:
        shares_diff_pct = abs(up_shares - dn_shares) / max(up_shares, dn_shares) * 100
        if winner == "Up":
            explanation = f"up份数{'>' if up_shares > dn_shares else '<'}dn ({up_shares:.0f} vs {dn_shares:.0f}, 差{shares_diff_pct:.0f}%)"
        else:
            explanation = f"dn份数{'>' if dn_shares > up_shares else '<'}up ({dn_shares:.0f} vs {up_shares:.0f}, 差{shares_diff_pct:.0f}%)"
    
    w = {
        "slug": slug, "dt": dt, "winner": winner,
        "up_usdc": up_usdc, "dn_usdc": dn_usdc,
        "up_shares": up_shares, "dn_shares": dn_shares,
        "up_avg_p": up_avg_p, "dn_avg_p": dn_avg_p,
        "total_cost": total_cost, "revenue": revenue, "profit": profit, "roi": roi,
        "combined_avg": combined_avg,
    }
    window_results.append(w)
    
    if total_cost > 0:
        print(f"{dt.strftime('%m-%d %H:%M'):>18} │ "
              f"${up_usdc:>6.0f} {up_shares:>7.0f} {up_avg_p:>6.3f}  │ "
              f"${dn_usdc:>6.0f} {dn_shares:>7.0f} {dn_avg_p:>6.3f}  │ "
              f"${total_cost:>6.0f} │ "
              f"{winner:>4} ${revenue:>7.0f} │ "
              f"${profit:>+7.0f} {roi:>+6.1f}% │ {explanation}")

# ================================================================
# 汇总
# ================================================================
valid = [w for w in window_results if w["total_cost"] > 0 and w["winner"] != "?"]
print(f"\n{'='*80}")
print(f"盈利机制总结 (有效窗口: {len(valid)})")
print(f"{'='*80}")

total_profit = total_revenue - total_invested
overall_roi = total_profit / total_invested * 100 if total_invested > 0 else 0
wins = [w for w in valid if w["profit"] > 0]
losses = [w for w in valid if w["profit"] <= 0]

print(f"\n总投入: ${total_invested:,.0f}")
print(f"总收入: ${total_revenue:,.0f}")
print(f"总利润: ${total_profit:+,.0f}")
print(f"总 ROI: {overall_roi:+.2f}%")
print(f"胜率:   {len(wins)}/{len(valid)} ({len(wins)/len(valid)*100:.0f}%)")

# 分析盈利核心: 等份 shares 的成本优势
print(f"\n{'='*80}")
print(f"核心机制: Equal Shares 成本分析")
print(f"{'='*80}")

for w in valid:
    # 如果买入完全等量 shares, Combined cost per share pair 就是 up_avg_p + dn_avg_p
    # 只要 < 1.0, 就稳赚
    shares_min = min(w["up_shares"], w["dn_shares"])
    shares_max = max(w["up_shares"], w["dn_shares"])
    excess = shares_max - shares_min
    
    # 被对冲掉的部分: min_shares 对, 每对收益 = $1 - (up_avg_p + dn_avg_p)
    hedged_cost = shares_min * w["combined_avg"]
    hedged_rev = shares_min * 1.0  # 无论谁赢, 对冲部分都收 $1/对
    hedged_profit = hedged_rev - hedged_cost
    
    # 多出来未对冲的部分: 纯单边赌注
    if w["up_shares"] > w["dn_shares"]:
        excess_side = "Up"
        excess_cost = excess * w["up_avg_p"]
        excess_rev = excess * 1.0 if w["winner"] == "Up" else 0
    else:
        excess_side = "Dn"
        excess_cost = excess * w["dn_avg_p"]
        excess_rev = excess * 1.0 if w["winner"] == "Down" else 0
    excess_profit = excess_rev - excess_cost
    
    dt_str = w["dt"].strftime("%H:%M")
    print(f"  {dt_str}: combined_price={w['combined_avg']:.4f}, "
          f"hedged_pairs={shares_min:.0f} profit=${hedged_profit:+.0f}, "  
          f"excess={excess:.0f}({excess_side}) profit=${excess_profit:+.0f}, "
          f"total=${w['profit']:+.0f}")

# ================================================================
# 盈利来源分解
# ================================================================
print(f"\n{'='*80}")
print(f"盈利来源分解")
print(f"{'='*80}")

total_hedged_profit = 0
total_excess_profit = 0
total_hedged_cost = 0

for w in valid:
    shares_min = min(w["up_shares"], w["dn_shares"])
    shares_max = max(w["up_shares"], w["dn_shares"])
    excess = shares_max - shares_min
    
    hedged_profit = shares_min * (1.0 - w["combined_avg"])
    total_hedged_profit += hedged_profit
    total_hedged_cost += shares_min * w["combined_avg"]
    
    if w["up_shares"] > w["dn_shares"]:
        excess_cost = excess * w["up_avg_p"]
        excess_rev = excess * 1.0 if w["winner"] == "Up" else 0
    else:
        excess_cost = excess * w["dn_avg_p"]
        excess_rev = excess * 1.0 if w["winner"] == "Down" else 0
    total_excess_profit += (excess_rev - excess_cost)

print(f"\n1. 对冲套利利润 (equal shares × (1 - combined_price)):")
print(f"   ${total_hedged_profit:+,.0f}")
print(f"   (对冲部分投入: ${total_hedged_cost:,.0f}, ROI: {total_hedged_profit/total_hedged_cost*100:+.1f}%)")

print(f"\n2. 方向性 excess shares 利润:")
print(f"   ${total_excess_profit:+,.0f}")

print(f"\n3. 总利润 = ${total_hedged_profit + total_excess_profit:+,.0f}")

# ================================================================
# 关键数字: combined price (套利空间)
# ================================================================
print(f"\n{'='*80}")
print(f"Combined Price 分布 (Up_avg + Dn_avg)")
print(f"{'='*80}")
print(f"理论值=1.0, <1.0=有套利空间, >1.0=付出溢价")
print()

combined_prices = [w["combined_avg"] for w in valid if w["combined_avg"] > 0]
combined_prices.sort()
for w in sorted(valid, key=lambda x: x["combined_avg"]):
    cp = w["combined_avg"]
    edge = 1.0 - cp
    bar = "+" * int(edge * 500) if edge > 0 else "-" * int(-edge * 500)
    print(f"  {w['dt'].strftime('%H:%M')}: {cp:.4f}  edge={edge:+.4f}  {bar}")

avg_cp = sum(combined_prices) / len(combined_prices)
print(f"\n平均 combined price: {avg_cp:.4f}")
print(f"平均 edge per share pair: {1-avg_cp:+.4f} (即每对shares赚 ${1-avg_cp:.4f})")

# ================================================================
# 但等等 — USDC加权的结果不一样!
# ================================================================
print(f"\n{'='*80}")
print(f"重要: 简单均价 vs USDC加权均价 的区别")
print(f"{'='*80}")
print("""
简单均价 = sum(price) / count  → 每笔交易权重相同
USDC加权 = sum(price * usdc) / sum(usdc) → 大额交易权重高

他的策略是 TWAP 分批下单, 随着窗口时间推移, 市场价格在变化.
如果某一方越来越贵(概率升高), 后面的订单价格更高, 但 shares 更少.

关键在于: 他买的是 SHARES, 不是 USDC.
- 花 $50 买 price=0.50 的 Up → 获得 100 shares
- 花 $50 买 price=0.50 的 Dn → 获得 100 shares
- 总投入 $100, 获得 100 对, 无论谁赢都收 $100
- 如果 up_price + dn_price < 1.0 (比如 0.48 + 0.48 = 0.96)
  → 花 $96 获得 100 对, 收 $100, 利润 $4

实际情况: 他不是同时下单, 而是交替分批下. 
先买的那些单价格可能更有利, 后买的可能市场已变.
所以要看 SHARES 统计, 不是 USDC 统计.
""")

# 按 shares 计算每对的实际成本
print(f"{'='*80}")
print(f"按 SHARES 配对计算实际套利空间")
print(f"{'='*80}")

for w in valid:
    paired = min(w["up_shares"], w["dn_shares"])
    # 配对部分的成本
    # 假设按比例分配: paired/total_shares 比例的 USDC
    if w["up_shares"] > 0:
        up_cost_per_share = w["up_usdc"] / w["up_shares"]
    else:
        up_cost_per_share = 0
    if w["dn_shares"] > 0:
        dn_cost_per_share = w["dn_usdc"] / w["dn_shares"]
    else:
        dn_cost_per_share = 0
    
    cost_per_pair = up_cost_per_share + dn_cost_per_share  # = up_avg_p + dn_avg_p
    edge_per_pair = 1.0 - cost_per_pair
    total_hedged = paired * edge_per_pair
    
    excess = abs(w["up_shares"] - w["dn_shares"])
    excess_side = "Up" if w["up_shares"] > w["dn_shares"] else "Dn"
    
    print(f"  {w['dt'].strftime('%H:%M')}: paired={paired:>6.0f}对 × ${edge_per_pair:+.4f}/对 = ${total_hedged:+.0f}  "
          f"+ excess {excess:.0f} {excess_side}")
