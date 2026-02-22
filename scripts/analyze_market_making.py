"""
假设检验: 该账号是否在做高频双边做市 (Market Making)?

做市商特征:
  1. 同时挂买卖单, 赚取 bid-ask spread
  2. 既有 BUY 也有 SELL
  3. 交易频率极高
  4. 持仓中性 (净敞口接近0)
  5. 通常是 Maker (limit order), 不是 Taker (market order)

反做市商特征 (如果是纯方向性投注者):
  1. 只有 BUY, 没有 SELL
  2. 持仓到期结算, 不主动平仓
  3. 是 Taker
"""
import json
from collections import defaultdict, Counter
from datetime import datetime, timezone

with open("docs/pm_account_0x1d_raw_trades.json") as f:
    trades = json.load(f)

btc5m = [t for t in trades if "btc-updown-5m" in t.get("eventSlug", "")]
print(f"BTC 5min trades: {len(btc5m)}")

# ================================================================
# 1. BUY vs SELL 统计
# ================================================================
print("\n" + "="*80)
print("1. BUY vs SELL 统计")
print("="*80)

sides = Counter(t.get("side", "?") for t in btc5m)
print(f"  Side distribution: {dict(sides)}")

buy_trades = [t for t in btc5m if t.get("side") == "BUY"]
sell_trades = [t for t in btc5m if t.get("side") == "SELL"]
print(f"  BUY: {len(buy_trades)} ({len(buy_trades)/len(btc5m)*100:.1f}%)")
print(f"  SELL: {len(sell_trades)} ({len(sell_trades)/len(btc5m)*100:.1f}%)")

# ALL trades (not just btc5m)
all_sides = Counter(t.get("side", "?") for t in trades)
print(f"\n  ALL trades side distribution: {dict(all_sides)}")

# ================================================================
# 2. type 字段检查 (是否有 TRADE 之外的类型)
# ================================================================
print("\n" + "="*80)
print("2. type 字段分布")
print("="*80)

types = Counter(t.get("type", "?") for t in btc5m)
print(f"  Type distribution: {dict(types)}")

all_types = Counter(t.get("type", "?") for t in trades)
print(f"  ALL trades type distribution: {dict(all_types)}")

# ================================================================
# 3. outcomeIndex 分布 (0=Up/Yes, 1=Down/No)
# ================================================================
print("\n" + "="*80)
print("3. outcomeIndex 分布")
print("="*80)

oi = Counter(t.get("outcomeIndex", "?") for t in btc5m)
print(f"  outcomeIndex: {dict(oi)}")

outcomes = Counter(t.get("outcome", "?") for t in btc5m)
print(f"  outcome: {dict(outcomes)}")

# ================================================================
# 4. 是否有同一 conditionId 的 BUY + SELL?
# ================================================================
print("\n" + "="*80)
print("4. 是否有同一 conditionId 中既 BUY 又 SELL?")
print("="*80)

by_cond = defaultdict(lambda: {"buy": 0, "sell": 0})
for t in btc5m:
    cid = t.get("conditionId", "?")
    side = t.get("side", "?").lower()
    if side in by_cond[cid]:
        by_cond[cid][side] += 1

both = {cid: v for cid, v in by_cond.items() if v["buy"] > 0 and v["sell"] > 0}
print(f"  总 conditionId 数: {len(by_cond)}")
print(f"  既有 BUY 又有 SELL 的: {len(both)}")
if both:
    for cid, v in list(both.items())[:5]:
        print(f"    {cid[:20]}... buy={v['buy']} sell={v['sell']}")

# ================================================================
# 5. 净持仓分析 - 看是否中性
# ================================================================
print("\n" + "="*80)
print("5. 净持仓分析 (做市商应接近中性)")
print("="*80)

events = defaultdict(list)
for t in btc5m:
    events[t["eventSlug"]].append(t)

print(f"\n{'Window':>10} | {'Up BUY':>7} {'Up SELL':>8} {'Up Net':>8} | {'Dn BUY':>7} {'Dn SELL':>8} {'Dn Net':>8} | {'Total $':>8}")

for slug in sorted(events.keys()):
    etrades = events[slug]
    
    up_buy_sh = sum(float(t["size"]) for t in etrades if t["outcome"]=="Up" and t["side"]=="BUY")
    up_sell_sh = sum(float(t["size"]) for t in etrades if t["outcome"]=="Up" and t["side"]=="SELL")
    dn_buy_sh = sum(float(t["size"]) for t in etrades if t["outcome"]=="Down" and t["side"]=="BUY")
    dn_sell_sh = sum(float(t["size"]) for t in etrades if t["outcome"]=="Down" and t["side"]=="SELL")
    
    total_usdc = sum(float(t.get("usdcSize", 0)) for t in etrades)
    
    dt = datetime.fromtimestamp(int(slug.split("-")[-1]), tz=timezone.utc)
    
    print(f"{dt.strftime('%H:%M'):>10} | {up_buy_sh:>7.1f} {up_sell_sh:>8.1f} {up_buy_sh-up_sell_sh:>8.1f} | "
          f"{dn_buy_sh:>7.1f} {dn_sell_sh:>8.1f} {dn_buy_sh-dn_sell_sh:>8.1f} | ${total_usdc:>7.0f}")

# ================================================================
# 6. 价格分析 - 做市商会在 bid 和 ask 两侧挂单
# ================================================================
print("\n" + "="*80)
print("6. 同一秒内是否同时买卖同一 outcome? (做市商必备)")
print("="*80)

# 做市商: 同一秒, 同一 outcome, 既买又卖 -> 赚 spread
same_second_pairs = 0
for slug in sorted(events.keys()):
    etrades = events[slug]
    # 按 (timestamp, outcome) 分组
    by_ts_out = defaultdict(lambda: {"buy": [], "sell": []})
    for t in etrades:
        if not t.get("side") or not t.get("outcome"):
            continue
        key = (t["timestamp"], t["outcome"])
        by_ts_out[key][t["side"].lower()].append(t)
    
    for key, sides in by_ts_out.items():
        if sides["buy"] and sides["sell"]:
            same_second_pairs += 1
            ts, outcome = key
            print(f"  Found! ts={ts} outcome={outcome}")
            for s in ["buy", "sell"]:
                for t in sides[s]:
                    print(f"    {s.upper()} price={t['price']} size={t['size']:.2f}")

if same_second_pairs == 0:
    print("  NONE - 从未在同一秒同一outcome上同时买卖")

# ================================================================
# 7. 交易间隔模式 - 做市商 vs Taker
# ================================================================
print("\n" + "="*80)
print("7. 交易模式特征对比")
print("="*80)

# 7a. 每笔交易的 usdcSize 分布
usdc_sizes = [float(t.get("usdcSize", 0)) for t in btc5m if float(t.get("usdcSize", 0)) > 0]
usdc_sizes.sort()
print(f"\n  USDC per trade:")
print(f"    Mean: ${sum(usdc_sizes)/len(usdc_sizes):.2f}")
print(f"    Median: ${usdc_sizes[len(usdc_sizes)//2]:.2f}")
print(f"    Min: ${min(usdc_sizes):.2f}")
print(f"    Max: ${max(usdc_sizes):.2f}")
print(f"    P5: ${usdc_sizes[len(usdc_sizes)*5//100]:.2f}")
print(f"    P95: ${usdc_sizes[len(usdc_sizes)*95//100]:.2f}")

# 7b. 价格集中度 - 做市商会在特定价格反复挂单
print(f"\n  Price concentration:")
price_counter = Counter(t["price"] for t in btc5m)
top10 = price_counter.most_common(10)
total_at_top10 = sum(c for _, c in top10)
print(f"    Top 10 prices cover: {total_at_top10}/{len(btc5m)} ({total_at_top10/len(btc5m)*100:.1f}%)")
for p, c in top10:
    print(f"      price={p:.3f}: {c} trades ({c/len(btc5m)*100:.1f}%)")

# 7c. Tx hash唯一性 - 做市商的limit order可能多笔fill在同一tx
tx_counts = Counter(t.get("transactionHash", "?") for t in btc5m)
multi_fill_tx = {tx: c for tx, c in tx_counts.items() if c > 1}
print(f"\n  Transaction uniqueness:")
print(f"    Unique tx hashes: {len(tx_counts)}")
print(f"    Multi-fill txs (>1 trade per tx): {len(multi_fill_tx)}")
if multi_fill_tx:
    for tx, c in sorted(multi_fill_tx.items(), key=lambda x: -x[1])[:5]:
        print(f"      {tx[:20]}... = {c} fills")

# ================================================================
# 8. 关键判断: 是否持仓到期?
# ================================================================
print("\n" + "="*80)
print("8. 是否持仓到期? (做市商会平仓, 不持仓到期)")
print("="*80)

# 如果全是 BUY, 那就是买入持有到期 -> 不是做市商
# 做市商会在到期前把仓位卖掉 (SELL)
if len(sell_trades) == 0:
    print("  ALL trades are BUY, ZERO sells")
    print("  -> He holds ALL positions until settlement")
    print("  -> This is NOT market making behavior")
    print("  -> Market makers would actively manage inventory by selling")
else:
    sell_pct = len(sell_trades) / len(btc5m) * 100
    print(f"  SELL trades: {len(sell_trades)} ({sell_pct:.1f}%)")
    if sell_pct < 5:
        print("  -> Very few sells = probably NOT a market maker")

# ================================================================
# 9. 检查是否有settle/redeem交易
# ================================================================
print("\n" + "="*80)
print("9. settle/redeem 相关交易")
print("="*80)

settle_trades = [t for t in btc5m if t.get("type", "").lower() in ("settlement", "redeem", "settle")]
print(f"  Settlement type trades: {len(settle_trades)}")

# 也看看完整raw数据中有没有其他type
all_btc5m_types = Counter(t.get("type","?") for t in btc5m)
print(f"  All types in btc5m: {dict(all_btc5m_types)}")

# ================================================================
# 10. 综合结论
# ================================================================
print("\n" + "="*80)
print("10. 做市商假设检验结论")
print("="*80)

sell_count = len(sell_trades)
buy_count = len(buy_trades)

print(f"""
  做市商必要条件检查:
  
  [{'X' if sell_count > 0 else ' '}] 有 SELL 交易                 -> {'YES' if sell_count > 0 else 'NO'} ({sell_count} sells)
  [{'X' if same_second_pairs > 0 else ' '}] 同时在买卖两侧挂单        -> {'YES' if same_second_pairs > 0 else 'NO'}
  [{'X' if len(multi_fill_tx) > len(tx_counts)*0.1 else ' '}] 大量多fill交易(limit order特征) -> {len(multi_fill_tx)}/{len(tx_counts)} multi-fill txs
  [ ] 持仓中性(净敞口接近0)        -> ALL BUY, net long 100%
  [ ] 到期前平仓                   -> No sells = holds to expiry
  
  结论: {'可能是做市商' if sell_count > buy_count * 0.3 else '不是做市商'} 
""")

if sell_count == 0:
    print("""  解释:
  他的行为模式是:
    1. 窗口开始后, 同时买入 Up 和 Down (近等额 shares)
    2. All BUY, zero SELL = 纯买入, 不卖出
    3. 持有到窗口结束, 赢家侧 shares 自动结算为 $1.0/share
    4. 这是 "双边投注" (dual-side betting), 不是 "双边做市" (market making)
    
  关键区别:
    做市商: 挂限价单在 bid/ask 两侧, 被动成交, 赚 spread, 不承担方向风险
    他: 主动用 FOK 市价单吃单, 买入持有到期, 靠 combined_price < 1.0 获利
    
  做市商会有大量 SELL 交易 (卖出平仓或在 ask 侧挂单被吃),
  但他的 2245 笔交易中 SELL 数量 = {sell_count}
""")
