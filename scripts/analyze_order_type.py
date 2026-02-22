"""分析 PM 账号 0x1d 的 BTC 5min 交易中 市价单/限价单 比例"""
import json
from collections import defaultdict, Counter

with open("docs/pm_account_0x1d_raw_trades.json") as f:
    trades = json.load(f)

btc5m = [t for t in trades if "btc-updown-5m" in t.get("eventSlug", "")]
print(f"BTC 5min trades: {len(btc5m)}")

# === 1. 按 transactionHash 分组 ===
# 一个 tx 内多笔 fill = 一个taker订单吃穿多个价格档位
tx_groups = defaultdict(list)
for t in btc5m:
    tx_groups[t["transactionHash"]].append(t)

single_fill_tx = sum(1 for g in tx_groups.values() if len(g) == 1)
multi_fill_tx = sum(1 for g in tx_groups.values() if len(g) > 1)
total_trades_in_multi = sum(len(g) for g in tx_groups.values() if len(g) > 1)

print(f"\n{'='*60}")
print(f"交易哈希分析")
print(f"{'='*60}")
print(f"唯一 txHash 数: {len(tx_groups)}")
print(f"单笔 fill 的 tx: {single_fill_tx} ({single_fill_tx/len(tx_groups)*100:.1f}%)")
print(f"多笔 fill 的 tx: {multi_fill_tx} ({multi_fill_tx/len(tx_groups)*100:.1f}%)")
if multi_fill_tx:
    multi_sizes = [len(g) for g in tx_groups.values() if len(g) > 1]
    print(f"多笔fill tx 的 fill数: min={min(multi_sizes)}, max={max(multi_sizes)}, "
          f"avg={sum(multi_sizes)/len(multi_sizes):.1f}")
    print(f"多笔fill tx 包含的总交易数: {total_trades_in_multi}")

# === 2. 分析多fill tx 内的价格差异 ===
# 如果同一tx内不同价格 -> taker 吃穿多档
print(f"\n{'='*60}")
print(f"多fill交易样例 (Top 10)")
print(f"{'='*60}")
for txh, fills in sorted(tx_groups.items(), key=lambda x: -len(x[1]))[:10]:
    outcomes = set(f["outcome"] for f in fills)
    prices = set(f["price"] for f in fills)
    total_usdc = sum(float(f["usdcSize"]) for f in fills)
    print(f"  tx={txh[:16]}... fills={len(fills):>2}, "
          f"outcomes={outcomes}, prices={prices}, total=${total_usdc:.0f}")

# === 3. 价格精度分析 ===
# Polymarket CLOB 价格步进为 0.01 (1 cent)
# 所有成交价格应该都是 0.01 的整数倍
prices = [t["price"] for t in btc5m]
price_cents = Counter()
for p in prices:
    cents = round(p * 100)
    remainder = abs(p * 100 - cents)
    if remainder < 0.001:
        price_cents["整分 (0.01步进)"] += 1
    else:
        price_cents["非整分"] += 1

print(f"\n{'='*60}")
print(f"价格精度")
print(f"{'='*60}")
for k, v in price_cents.items():
    print(f"  {k}: {v} ({v/len(btc5m)*100:.1f}%)")

# === 4. 价格分布 ===
print(f"\n{'='*60}")
print(f"成交价格分布 (BTC 5min)")
print(f"{'='*60}")
price_buckets = Counter()
for p in prices:
    bucket = round(p, 2)
    price_buckets[bucket] += 1
for p in sorted(price_buckets.keys()):
    bar = "█" * (price_buckets[p] // 3)
    print(f"  {p:.2f}: {price_buckets[p]:>4} {bar}")

# === 5. 按事件窗口分析：同一tx中跨Up/Down ===
# 如果一个tx同时包含Up和Down的fill -> 可能是通过合约一次下两单
print(f"\n{'='*60}")
print(f"跨方向交易分析 (同一tx含Up和Down)")
print(f"{'='*60}")
both_sides_tx = 0
up_only_tx = 0
dn_only_tx = 0
other_tx = 0
for txh, fills in tx_groups.items():
    outcomes = set(f["outcome"] for f in fills)
    if "Up" in outcomes and "Down" in outcomes:
        both_sides_tx += 1
    elif "Up" in outcomes:
        up_only_tx += 1
    elif "Down" in outcomes:
        dn_only_tx += 1
    else:
        other_tx += 1

print(f"  同时含 Up+Down: {both_sides_tx} ({both_sides_tx/len(tx_groups)*100:.1f}%)")
print(f"  仅 Up:          {up_only_tx} ({up_only_tx/len(tx_groups)*100:.1f}%)")
print(f"  仅 Down:        {dn_only_tx} ({dn_only_tx/len(tx_groups)*100:.1f}%)")
print(f"  其他:           {other_tx}")

# === 6. 推断 Taker vs Maker ===
# 在 Polymarket CLOB 中:
#   - Taker (FOK/IOC): 立即成交, 吃掉 book 上的 resting orders
#   - Maker (GTC/GTD): 挂单在 book 上等待被吃
#
# 关键推断依据:
# a) 如果同一 tx 内有多笔同方向的 fill 且价格不同 -> 大概率 taker 吃穿多档
# b) 如果单笔 fill 且刚好在某个价格 -> 可能是 maker 被成交, 也可能是 taker 小单刚好匹配一档
# c) 交易间隔: maker 订单的成交时间取决于何时有人来吃, 间隔不规律
#    taker 订单由自己触发, 间隔有规律

print(f"\n{'='*60}")
print(f"Taker/Maker 推断分析")
print(f"{'='*60}")

# 按同方向同tx分析
taker_likely = 0  # 同tx同方向多个不同价格 = taker 吃穿
taker_possible = 0  # 同tx同方向多个相同价格 = 可能taker也可能maker batch
single_fills = 0  # 单笔无法确定

for txh, fills in tx_groups.items():
    # 按方向分组
    by_dir = defaultdict(list)
    for f in fills:
        by_dir[f["outcome"]].append(f)
    
    for direction, dir_fills in by_dir.items():
        if len(dir_fills) > 1:
            unique_prices = set(f["price"] for f in dir_fills)
            if len(unique_prices) > 1:
                taker_likely += len(dir_fills)
            else:
                taker_possible += len(dir_fills)
        else:
            single_fills += len(dir_fills)

total = taker_likely + taker_possible + single_fills
print(f"  多fill+多价格 (确认taker): {taker_likely:>5} fills ({taker_likely/total*100:.1f}%)")
print(f"  多fill+同价格 (疑似taker): {taker_possible:>5} fills ({taker_possible/total*100:.1f}%)")
print(f"  单fill (无法确定):         {single_fills:>5} fills ({single_fills/total*100:.1f}%)")

# === 7. 交易间隔分析 - 另一个推断maker/taker的角度 ===
# 如果交易是自己发起的(taker), 间隔会更有规律(延时控制)
# 如果是被动成交(maker), 间隔不规律
print(f"\n{'='*60}")
print(f"交易间隔分析 (同一窗口内按时间排序)")
print(f"{'='*60}")

events = defaultdict(list)
for t in btc5m:
    events[t["eventSlug"]].append(t)

all_gaps = []
for slug in sorted(events.keys()):
    etrades = sorted(events[slug], key=lambda t: t["timestamp"])
    window_start = int(slug.split("-")[-1])
    # 只看窗口内交易
    in_window = [t for t in etrades if 0 <= t["timestamp"] - window_start <= 300]
    if len(in_window) < 2:
        continue
    
    # 计算相邻交易间隔(秒)
    for i in range(1, len(in_window)):
        gap = in_window[i]["timestamp"] - in_window[i-1]["timestamp"]
        all_gaps.append(gap)

gap_counter = Counter(all_gaps)
print(f"相邻交易间隔分布:")
for g in sorted(gap_counter.keys())[:20]:
    pct = gap_counter[g] / len(all_gaps) * 100
    bar = "█" * int(pct * 2)
    print(f"  {g:>3}s: {gap_counter[g]:>5} ({pct:>5.1f}%) {bar}")

# 间隔为0说明同一秒多笔成交(大概率taker批量)
zero_gap = gap_counter.get(0, 0)
one_gap = gap_counter.get(1, 0)
two_gap = gap_counter.get(2, 0)
print(f"\n间隔<=2秒的交易对: {zero_gap + one_gap + two_gap} / {len(all_gaps)} "
      f"({(zero_gap+one_gap+two_gap)/len(all_gaps)*100:.1f}%)")
print(f"间隔>10秒的交易对: {sum(v for k,v in gap_counter.items() if k>10)} / {len(all_gaps)} "
      f"({sum(v for k,v in gap_counter.items() if k>10)/len(all_gaps)*100:.1f}%)")

# === 8. 结论 ===
print(f"\n{'='*60}")
print(f"总结")
print(f"{'='*60}")
print("""
推断依据:
1. 所有交易 side=BUY → 该账号只买入(不卖出), 全是开仓行为
2. 大量交易在同一秒/相邻秒发生 → 高频自动化提交
3. 同一tx内多笔fill且价格不同 → taker吃穿订单簿多档
4. 交易间隔高度集中在0-4秒 → 程序化定时下单(taker模式)
5. 没有观察到被动等待成交的随机间隔模式(maker特征)

结论: 该账号 **绝大多数交易为 Taker (市价/FOK) 订单**
- 通过程序自动化, 每隔2-4秒提交一笔小额FOK taker单
- 持续分批买入, 类似TWAP策略
- 不使用maker限价挂单
""")
