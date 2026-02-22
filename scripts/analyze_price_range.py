"""分析 PM 账号 0x1d 在 BTC 5min 中 Up/Down 的成交价格范围分布"""
import json
from collections import defaultdict, Counter

with open("docs/pm_account_0x1d_raw_trades.json") as f:
    trades = json.load(f)

btc5m = [t for t in trades if "btc-updown-5m" in t.get("eventSlug", "")]
print(f"BTC 5min trades: {len(btc5m)}")

# 过滤掉 price=0 的结算交易
btc5m = [t for t in btc5m if t["price"] > 0]
print(f"排除 price=0 后: {len(btc5m)}")

up_trades = [t for t in btc5m if t["outcome"] == "Up"]
dn_trades = [t for t in btc5m if t["outcome"] == "Down"]
print(f"Up trades: {len(up_trades)}, Down trades: {len(dn_trades)}")

# === 1. 整体价格统计 ===
print(f"\n{'='*70}")
print(f"整体价格统计")
print(f"{'='*70}")

for label, subset in [("Up", up_trades), ("Down", dn_trades)]:
    prices = [t["price"] for t in subset]
    prices.sort()
    avg = sum(prices) / len(prices)
    p10 = prices[int(len(prices) * 0.1)]
    p25 = prices[int(len(prices) * 0.25)]
    p50 = prices[int(len(prices) * 0.5)]
    p75 = prices[int(len(prices) * 0.75)]
    p90 = prices[int(len(prices) * 0.9)]
    print(f"\n  {label} (n={len(subset)}):")
    print(f"    Min={min(prices):.2f}  P10={p10:.2f}  P25={p25:.2f}  "
          f"Median={p50:.2f}  P75={p75:.2f}  P90={p90:.2f}  Max={max(prices):.2f}")
    print(f"    Mean={avg:.3f}")

# === 2. Up/Down 价格分布直方图 (0.05步进) ===
print(f"\n{'='*70}")
print(f"Up vs Down 价格分布 (0.05步进)")
print(f"{'='*70}")

up_prices = [t["price"] for t in up_trades]
dn_prices = [t["price"] for t in dn_trades]

up_buckets = Counter()
dn_buckets = Counter()
for p in up_prices:
    up_buckets[round(int(p / 0.05) * 0.05, 2)] += 1
for p in dn_prices:
    dn_buckets[round(int(p / 0.05) * 0.05, 2)] += 1

all_bucket_keys = sorted(set(list(up_buckets.keys()) + list(dn_buckets.keys())))
max_cnt = max(max(up_buckets.values(), default=1), max(dn_buckets.values(), default=1))
scale = 30 / max_cnt

print(f"\n{'价格区间':>12} {'Up笔数':>6} {'Up':>32} | {'Down':>32} {'Dn笔数':>6}")
print("-" * 95)
for b in all_bucket_keys:
    uc = up_buckets.get(b, 0)
    dc = dn_buckets.get(b, 0)
    up_bar = "█" * int(uc * scale)
    dn_bar = "█" * int(dc * scale)
    print(f"  {b:.2f}-{b+0.05:.2f}  {uc:>5}  {up_bar:>32} | {dn_bar:<32} {dc:>5}")

# === 3. 每个窗口内的 Up/Down 价格范围 ===
print(f"\n{'='*70}")
print(f"每窗口 Up/Down 价格范围")
print(f"{'='*70}")

events = defaultdict(list)
for t in btc5m:
    events[t["eventSlug"]].append(t)

print(f"\n{'窗口':>28} │ {'Up_min':>6} {'Up_avg':>6} {'Up_max':>6} │ "
      f"{'Dn_min':>6} {'Dn_avg':>6} {'Dn_max':>6} │ {'Sum':>6} │ {'边际差':>8}")
print("─" * 105)

sum_diffs = []
for slug in sorted(events.keys()):
    etrades = events[slug]
    window_start = int(slug.split("-")[-1])
    # 只看窗口内
    in_window = [t for t in etrades if 0 <= t["timestamp"] - window_start <= 300 and t["price"] > 0]
    if not in_window:
        continue
    
    up = [t for t in in_window if t["outcome"] == "Up"]
    dn = [t for t in in_window if t["outcome"] == "Down"]
    
    if not up or not dn:
        continue
    
    up_p = [t["price"] for t in up]
    dn_p = [t["price"] for t in dn]
    
    up_avg = sum(up_p) / len(up_p)
    dn_avg = sum(dn_p) / len(dn_p)
    price_sum = up_avg + dn_avg
    edge = 1.0 - price_sum  # 正=有套利空间, 负=付出溢价
    sum_diffs.append(price_sum)
    
    from datetime import datetime, timezone
    dt = datetime.fromtimestamp(window_start, tz=timezone.utc)
    
    print(f"  {dt.strftime('%Y-%m-%d %H:%M')} ({slug[-10:]}) │ "
          f"{min(up_p):.2f}  {up_avg:.3f}  {max(up_p):.2f}  │ "
          f"{min(dn_p):.2f}  {dn_avg:.3f}  {max(dn_p):.2f}  │ "
          f"{price_sum:.3f} │ {edge:+.4f}")

if sum_diffs:
    avg_sum = sum(sum_diffs) / len(sum_diffs)
    print(f"\n  平均 Up_avg + Dn_avg = {avg_sum:.4f}  (边际: {1-avg_sum:+.4f})")

# === 4. Up vs Down 配对分析 (同一窗口中的关系) ===
print(f"\n{'='*70}")
print(f"Up+Down 价格之和分析 (理论值=1.0)")
print(f"{'='*70}")

# 按USDC加权平均价格
for slug in sorted(events.keys()):
    etrades = events[slug]
    window_start = int(slug.split("-")[-1])
    in_window = [t for t in etrades if 0 <= t["timestamp"] - window_start <= 300 and t["price"] > 0]
    if not in_window:
        continue
    
    up = [t for t in in_window if t["outcome"] == "Up"]
    dn = [t for t in in_window if t["outcome"] == "Down"]
    
    if not up or not dn:
        continue
    
    # USDC 加权平均价格
    up_usdc_total = sum(float(t["usdcSize"]) for t in up)
    dn_usdc_total = sum(float(t["usdcSize"]) for t in dn)
    up_wavg = sum(t["price"] * float(t["usdcSize"]) for t in up) / up_usdc_total if up_usdc_total > 0 else 0
    dn_wavg = sum(t["price"] * float(t["usdcSize"]) for t in dn) / dn_usdc_total if dn_usdc_total > 0 else 0
    
    from datetime import datetime, timezone
    dt = datetime.fromtimestamp(window_start, tz=timezone.utc)
    
    print(f"  {dt.strftime('%H:%M')}  Up_wavg={up_wavg:.4f} (${up_usdc_total:>6.0f})  "
          f"Dn_wavg={dn_wavg:.4f} (${dn_usdc_total:>6.0f})  "
          f"Sum={up_wavg+dn_wavg:.4f}  Edge={1-up_wavg-dn_wavg:+.4f}")

# === 5. 价格分段统计 ===
print(f"\n{'='*70}")
print(f"价格分段: 低赔率(近确定) vs 高赔率(长射)")  
print(f"{'='*70}")

for label, subset in [("Up", up_trades), ("Down", dn_trades)]:
    total = len(subset)
    total_usdc = sum(float(t.get("usdcSize", 0)) for t in subset)
    
    ranges = [
        ("极低  <0.10", 0, 0.10),
        ("低   0.10-0.30", 0.10, 0.30),
        ("中低 0.30-0.45", 0.30, 0.45),
        ("中   0.45-0.55", 0.45, 0.55),
        ("中高 0.55-0.70", 0.55, 0.70),
        ("高   0.70-0.90", 0.70, 0.90),
        ("极高  >0.90", 0.90, 1.01),
    ]
    
    print(f"\n  {label}:")
    print(f"  {'区间':>16} {'笔数':>5} {'占比':>6} {'USDC':>8} {'USDC占比':>8} {'均价':>6}")
    for name, lo, hi in ranges:
        sub = [t for t in subset if lo <= t["price"] < hi]
        cnt = len(sub)
        usdc = sum(float(t.get("usdcSize", 0)) for t in sub)
        avg_p = sum(t["price"] for t in sub) / cnt if cnt > 0 else 0
        pct = cnt / total * 100 if total > 0 else 0
        usdc_pct = usdc / total_usdc * 100 if total_usdc > 0 else 0
        bar = "█" * int(pct)
        print(f"  {name:>16} {cnt:>5} {pct:>5.1f}% ${usdc:>7.0f} {usdc_pct:>6.1f}%  {avg_p:.3f} {bar}")
