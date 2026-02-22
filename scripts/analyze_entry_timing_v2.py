"""分析 PM 账号 0x1d 在 BTC 5min 事件中的入场时机分布 (仅窗口内交易)"""
import json
from collections import defaultdict
from datetime import datetime, timezone

with open("docs/pm_account_0x1d_raw_trades.json") as f:
    trades = json.load(f)

btc5m = [t for t in trades if "btc-updown-5m" in t.get("eventSlug", "")]
print(f"BTC 5min trades total: {len(btc5m)}")

events = defaultdict(list)
for t in btc5m:
    events[t["eventSlug"]].append(t)

print(f"BTC 5min windows: {len(events)}")

# 只保留窗口内交易 (offset 0~300s)
all_offsets = []
first_offsets = []
up_first_offsets = []
dn_first_offsets = []
up_dn_gaps = []     # Up首笔 - Dn首笔 的时间差
entry_spans = []    # 从首笔到最后一笔的持续时长(窗口内)
window_data = []

for slug in sorted(events.keys()):
    etrades = events[slug]
    window_start = int(slug.split("-")[-1])
    
    # 只保留窗口内交易
    in_window = [t for t in etrades if 0 <= t["timestamp"] - window_start <= 300]
    if not in_window:
        continue
    
    in_window.sort(key=lambda t: t["timestamp"])
    
    by_outcome = defaultdict(list)
    for t in in_window:
        by_outcome[t["outcome"]].append(t)
    
    offsets = [t["timestamp"] - window_start for t in in_window]
    all_offsets.extend(offsets)
    
    first_offset = offsets[0]
    last_offset = offsets[-1]
    first_offsets.append(first_offset)
    entry_spans.append(last_offset - first_offset)
    
    up_trades = sorted(by_outcome.get("Up", []), key=lambda t: t["timestamp"])
    dn_trades = sorted(by_outcome.get("Down", []), key=lambda t: t["timestamp"])
    
    up_first = up_trades[0]["timestamp"] - window_start if up_trades else None
    dn_first = dn_trades[0]["timestamp"] - window_start if dn_trades else None
    up_last = up_trades[-1]["timestamp"] - window_start if up_trades else None
    dn_last = dn_trades[-1]["timestamp"] - window_start if dn_trades else None
    
    if up_first is not None:
        up_first_offsets.append(up_first)
    if dn_first is not None:
        dn_first_offsets.append(dn_first)
    if up_first is not None and dn_first is not None:
        up_dn_gaps.append(abs(up_first - dn_first))
    
    # 总买入金额 (USDC)
    total_usdc = sum(float(t["usdcSize"]) for t in in_window)
    up_usdc = sum(float(t["usdcSize"]) for t in up_trades)
    dn_usdc = sum(float(t["usdcSize"]) for t in dn_trades)
    
    # $window时间 (UTC)
    dt = datetime.fromtimestamp(window_start, tz=timezone.utc)
    
    who_first = "Up" if (up_first or 999) <= (dn_first or 999) else "Dn"
    
    window_data.append({
        "slug": slug, "dt": dt,
        "trades": len(in_window), "first": first_offset, "last": last_offset,
        "up_cnt": len(up_trades), "dn_cnt": len(dn_trades),
        "up_first": up_first, "dn_first": dn_first,
        "up_last": up_last, "dn_last": dn_last,
        "total_usdc": total_usdc, "up_usdc": up_usdc, "dn_usdc": dn_usdc,
        "who_first": who_first,
    })

print(f"有效窗口 (含窗口内交易): {len(window_data)}")
print()

# 详细
print("=" * 100)
print(f"{'时间(UTC)':>20} {'笔数':>4} {'首笔':>4} {'末笔':>4} {'Up首':>5} {'Dn首':>5} {'先手':>4} "
      f"{'Up笔':>4} {'Dn笔':>4} {'总$':>8} {'Up$':>8} {'Dn$':>8}")
print("=" * 100)
for w in window_data:
    print(f"{w['dt'].strftime('%Y-%m-%d %H:%M'):>20} "
          f"{w['trades']:>4} {w['first']:>4}s {w['last']:>4}s "
          f"{w['up_first'] or '-':>5}s {w['dn_first'] or '-':>5}s "
          f"{w['who_first']:>4} "
          f"{w['up_cnt']:>4} {w['dn_cnt']:>4} "
          f"${w['total_usdc']:>7.0f} ${w['up_usdc']:>7.0f} ${w['dn_usdc']:>7.0f}")

# 汇总统计
print()
print("=" * 80)
print("入场时机统计汇总")
print("=" * 80)

print(f"\n【首笔交易偏移 (距窗口开始)】")
first_offsets_valid = [f for f in first_offsets if f < 600]
print(f"  Min: {min(first_offsets_valid)}s")
print(f"  Max: {max(first_offsets_valid)}s")
print(f"  Mean: {sum(first_offsets_valid)/len(first_offsets_valid):.1f}s")
first_offsets_valid.sort()
print(f"  Median: {first_offsets_valid[len(first_offsets_valid)//2]}s")

print(f"\n【Up侧首笔偏移】")
print(f"  Min: {min(up_first_offsets)}s, Max: {max(up_first_offsets)}s, Mean: {sum(up_first_offsets)/len(up_first_offsets):.1f}s")
print(f"\n【Dn侧首笔偏移】")
print(f"  Min: {min(dn_first_offsets)}s, Max: {max(dn_first_offsets)}s, Mean: {sum(dn_first_offsets)/len(dn_first_offsets):.1f}s")

print(f"\n【Up-Dn入场时间间隔 (绝对值)】")
print(f"  Min: {min(up_dn_gaps)}s, Max: {max(up_dn_gaps)}s, Mean: {sum(up_dn_gaps)/len(up_dn_gaps):.1f}s")

print(f"\n【入场持续时长 (首笔到末笔)】")
print(f"  Min: {min(entry_spans)}s, Max: {max(entry_spans)}s, Mean: {sum(entry_spans)/len(entry_spans):.0f}s")

# 窗口内交易时间分布 (10s桶) - 更细粒度
print(f"\n【交易时间分布 (10s 桶, 共{len(all_offsets)}笔)】")
buckets_10 = defaultdict(int)
for o in all_offsets:
    b = (o // 10) * 10
    buckets_10[b] += 1

max_cnt = max(buckets_10.values())
scale = 60 / max_cnt

for b in sorted(buckets_10.keys()):
    if b > 300:
        break
    cnt = buckets_10[b]
    bar = "█" * int(cnt * scale)
    pct = cnt / len(all_offsets) * 100
    print(f"  {b:>3}s-{b+10:>3}s: {cnt:>4} ({pct:>5.1f}%) {bar}")

# 分阶段统计
phase1 = [o for o in all_offsets if o <= 60]
phase2 = [o for o in all_offsets if 60 < o <= 180]
phase3 = [o for o in all_offsets if 180 < o <= 300]
total_in = len(phase1) + len(phase2) + len(phase3)

print(f"\n【分阶段占比】")
print(f"  0-60s   (窗口前1/5): {len(phase1):>4}笔 ({len(phase1)/total_in*100:>5.1f}%)")
print(f"  60-180s (窗口中间):  {len(phase2):>4}笔 ({len(phase2)/total_in*100:>5.1f}%)")
print(f"  180-300s(窗口后2/5): {len(phase3):>4}笔 ({len(phase3)/total_in*100:>5.1f}%)")

# 先手统计 (Up先还是Dn先)
up_first_cnt = sum(1 for w in window_data if w["who_first"] == "Up")
dn_first_cnt = sum(1 for w in window_data if w["who_first"] == "Dn")
print(f"\n【先手方向】")
print(f"  Up先入: {up_first_cnt}次")
print(f"  Dn先入: {dn_first_cnt}次")
print(f"  (注: 样本量={len(window_data)}, 不足以判断偏好)")

# 入场密度分析 - 前60s vs 其他
print(f"\n【入场密度 (按窗口阶段, 每秒笔数)】")
for phase_name, lo, hi in [("0-30s", 0, 30), ("30-60s", 30, 60), 
                            ("60-120s", 60, 120), ("120-180s", 120, 180),
                            ("180-240s", 180, 240), ("240-300s", 240, 300)]:
    cnt = len([o for o in all_offsets if lo <= o < hi])
    density = cnt / (hi - lo) / len(window_data)  # 笔/秒/窗口
    print(f"  {phase_name}: {cnt:>4}笔, 密度={density:.2f}笔/秒/窗口")
