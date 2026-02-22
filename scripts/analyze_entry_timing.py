"""分析 PM 账号 0x1d0034134e 在 BTC 5min 事件中的入场时机分布"""
import json
from collections import defaultdict
from datetime import datetime, timezone

with open("docs/pm_account_0x1d_raw_trades.json") as f:
    trades = json.load(f)

# 只看 BTC 5min
btc5m = [t for t in trades if "btc-updown-5m" in t.get("eventSlug", "")]
print(f"BTC 5min trades: {len(btc5m)}")

# 从 slug 提取窗口开始时间: btc-updown-5m-{timestamp}
events = defaultdict(list)
for t in btc5m:
    slug = t["eventSlug"]
    events[slug].append(t)

print(f"BTC 5min windows: {len(events)}")
print()

# 分析每个窗口中交易的时间偏移
print("=" * 80)
print("每个窗口的入场时机分析 (相对窗口开始时间)")
print("=" * 80)

all_offsets = []      # 所有交易的偏移
first_offsets = []    # 每窗口首笔交易的偏移
up_first_offsets = []
dn_first_offsets = []

for slug in sorted(events.keys()):
    etrades = events[slug]
    # 窗口开始时间从 slug 提取
    window_start = int(slug.split("-")[-1])
    window_end = window_start + 300  # 5 minutes

    # 按时间排序
    etrades.sort(key=lambda t: t["timestamp"])
    
    by_outcome = defaultdict(list)
    for t in etrades:
        by_outcome[t["outcome"]].append(t)
    
    # 所有交易的偏移
    offsets = [t["timestamp"] - window_start for t in etrades]
    all_offsets.extend(offsets)
    
    # 首笔交易
    first_ts = etrades[0]["timestamp"]
    first_offset = first_ts - window_start
    first_offsets.append(first_offset)
    
    # 最后一笔交易
    last_ts = etrades[-1]["timestamp"]
    last_offset = last_ts - window_start
    
    # Up 首笔
    up_trades = sorted(by_outcome.get("Up", []), key=lambda t: t["timestamp"])
    dn_trades = sorted(by_outcome.get("Down", []), key=lambda t: t["timestamp"])
    
    up_first = up_trades[0]["timestamp"] - window_start if up_trades else -1
    dn_first = dn_trades[0]["timestamp"] - window_start if dn_trades else -1
    up_last = up_trades[-1]["timestamp"] - window_start if up_trades else -1
    dn_last = dn_trades[-1]["timestamp"] - window_start if dn_trades else -1
    
    if up_trades:
        up_first_offsets.append(up_first)
    if dn_trades:
        dn_first_offsets.append(dn_first)
    
    # 交易持续时间
    duration = last_ts - first_ts
    
    secs_left_at_first = 300 - first_offset  # 窗口剩余秒数
    
    print(f"{slug}:")
    print(f"  trades={len(etrades)}, first={first_offset}s, last={last_offset}s, "
          f"duration={duration}s, secs_left={secs_left_at_first}s")
    print(f"  Up: first={up_first}s, last={up_last}s, count={len(up_trades)}")
    print(f"  Dn: first={dn_first}s, last={dn_last}s, count={len(dn_trades)}")
    print()

# 统计汇总
print("=" * 80)
print("入场时机分布统计")
print("=" * 80)

# 首笔交易偏移分布
first_offsets.sort()
print(f"\n首笔交易偏移 (距窗口开始):")
print(f"  Min: {min(first_offsets)}s")
print(f"  Max: {max(first_offsets)}s")
print(f"  Mean: {sum(first_offsets)/len(first_offsets):.0f}s")
print(f"  Median: {first_offsets[len(first_offsets)//2]}s")

# 转换为"窗口剩余秒数"
secs_left_at_entry = [300 - o for o in first_offsets]
print(f"\n首笔交易时窗口剩余:")
print(f"  Min: {min(secs_left_at_entry)}s")
print(f"  Max: {max(secs_left_at_entry)}s")
print(f"  Mean: {sum(secs_left_at_entry)/len(secs_left_at_entry):.0f}s")
print(f"  Median: {secs_left_at_entry[len(secs_left_at_entry)//2]}s")

# Up vs Down 首笔偏移
print(f"\nUp侧首笔偏移: min={min(up_first_offsets)}s, max={max(up_first_offsets)}s, mean={sum(up_first_offsets)/len(up_first_offsets):.0f}s")
print(f"Dn侧首笔偏移: min={min(dn_first_offsets)}s, max={max(dn_first_offsets)}s, mean={sum(dn_first_offsets)/len(dn_first_offsets):.0f}s")

# 所有交易的偏移分布 (按30s桶)
print(f"\n所有交易时间分布 (30s桶):")
buckets = defaultdict(int)
for o in all_offsets:
    bucket = (o // 30) * 30
    buckets[bucket] += 1

for b in sorted(buckets.keys()):
    bar = "#" * (buckets[b] // 5)
    pct = buckets[b] / len(all_offsets) * 100
    left = 300 - b
    print(f"  {b:>3}s-{b+30:>3}s (剩{left:>3}s-{left-30:>3}s): {buckets[b]:>4} ({pct:>5.1f}%) {bar}")

# 首笔交易偏移分布 (30s桶)
print(f"\n首笔入场时间分布 (30s桶):")
fbuckets = defaultdict(int)
for o in first_offsets:
    bucket = (o // 30) * 30
    fbuckets[bucket] += 1

for b in sorted(fbuckets.keys()):
    bar = "█" * fbuckets[b]
    left = 300 - b
    print(f"  {b:>3}s-{b+30:>3}s (剩{left:>3}s-{left-30:>3}s): {fbuckets[b]:>2} {bar}")
