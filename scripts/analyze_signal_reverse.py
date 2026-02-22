"""逆向推断 0x1d 的入场信号 — 通过交易时序和方向倾斜模式"""
import json
from collections import defaultdict
from datetime import datetime, timezone

with open("docs/pm_account_0x1d_raw_trades.json") as f:
    trades = json.load(f)

btc5m = [t for t in trades if "btc-updown-5m" in t.get("eventSlug", "")]

events = defaultdict(list)
for t in btc5m:
    events[t["eventSlug"]].append(t)

# ================================================================
# 分析1: 方向倾斜是"从一开始就有"还是"逐渐发展"
# 如果一开始就倾斜 → 入场前就有信号
# 如果逐渐倾斜 → 窗口内观察信号(实时适应)
# ================================================================
print("=" * 100)
print("分析1: 方向倾斜随时间的演变 (是入场前决定 还是 窗口内适应?)")
print("=" * 100)

for slug in sorted(events.keys()):
    etrades = [t for t in events[slug] if t["price"] > 0 and t["outcome"] in ("Up", "Down")]
    if len(etrades) < 10:
        continue
    
    window_start = int(slug.split("-")[-1])
    dt = datetime.fromtimestamp(window_start, tz=timezone.utc)
    etrades.sort(key=lambda t: t["timestamp"])
    
    # 按30s桶看累计 shares 差异
    print(f"\n{dt.strftime('%H:%M')} ({slug[-10:]}):")
    
    cum_up_shares = 0
    cum_dn_shares = 0
    cum_up_usdc = 0
    cum_dn_usdc = 0
    
    buckets = defaultdict(lambda: {"up_s": 0, "dn_s": 0, "up_u": 0, "dn_u": 0, "up_n": 0, "dn_n": 0})
    for t in etrades:
        offset = t["timestamp"] - window_start
        if offset > 300:
            continue
        b = (offset // 30) * 30
        if t["outcome"] == "Up":
            buckets[b]["up_s"] += float(t["size"])
            buckets[b]["up_u"] += float(t.get("usdcSize", 0))
            buckets[b]["up_n"] += 1
        else:
            buckets[b]["dn_s"] += float(t["size"])
            buckets[b]["dn_u"] += float(t.get("usdcSize", 0))
            buckets[b]["dn_n"] += 1
    
    for b in sorted(buckets.keys()):
        bk = buckets[b]
        cum_up_shares += bk["up_s"]
        cum_dn_shares += bk["dn_s"]
        cum_up_usdc += bk["up_u"]
        cum_dn_usdc += bk["dn_u"]
        
        total_cum = cum_up_shares + cum_dn_shares
        if total_cum > 0:
            up_pct = cum_up_shares / total_cum * 100
            dn_pct = cum_dn_shares / total_cum * 100
            bias = "Up" if cum_up_shares > cum_dn_shares else "Dn"
            bias_pct = abs(cum_up_shares - cum_dn_shares) / total_cum * 100
            
            # 本桶的倾斜
            bucket_total = bk["up_s"] + bk["dn_s"]
            if bucket_total > 0:
                bucket_up_pct = bk["up_s"] / bucket_total * 100
                bucket_bias = "Up" if bk["up_s"] > bk["dn_s"] else "Dn"
            else:
                bucket_up_pct = 0
                bucket_bias = "-"
            
            bar_up = "█" * int(up_pct / 2)
            bar_dn = "█" * int(dn_pct / 2)
            print(f"  {b:>3}s: 本桶Up/Dn={bk['up_n']:>2}/{bk['dn_n']:>2} "
                  f"本桶bias={bucket_bias}{bucket_up_pct:>5.1f}%Up │ "
                  f"累计: Up{cum_up_shares:>7.0f} Dn{cum_dn_shares:>7.0f} "
                  f"bias={bias}{bias_pct:>4.1f}% │ {bar_up}|{bar_dn}")

# ================================================================
# 分析2: 他的方向倾斜 vs 窗口开始前的BTC动量
# 如果他的 excess 方向跟入场前BTC趋势一致 → 用动量信号
# 如果反向 → 用均值回归信号
# ================================================================
print(f"\n{'='*100}")
print(f"分析2: 哪边先入场? 哪边笔数/金额更多?")
print(f"{'='*100}")

for slug in sorted(events.keys()):
    etrades = [t for t in events[slug] if t["price"] > 0 and t["outcome"] in ("Up", "Down")]
    if len(etrades) < 10:
        continue
    
    window_start = int(slug.split("-")[-1])
    dt = datetime.fromtimestamp(window_start, tz=timezone.utc)
    etrades.sort(key=lambda t: t["timestamp"])
    
    # 前30s的交易
    early = [t for t in etrades if t["timestamp"] - window_start <= 30]
    early_up = [t for t in early if t["outcome"] == "Up"]
    early_dn = [t for t in early if t["outcome"] == "Down"]
    
    early_up_s = sum(float(t["size"]) for t in early_up)
    early_dn_s = sum(float(t["size"]) for t in early_dn)
    early_up_u = sum(float(t.get("usdcSize", 0)) for t in early_up)
    early_dn_u = sum(float(t.get("usdcSize", 0)) for t in early_dn)
    
    # 全窗口
    all_up = [t for t in etrades if t["outcome"] == "Up" and t["timestamp"] - window_start <= 300]
    all_dn = [t for t in etrades if t["outcome"] == "Down" and t["timestamp"] - window_start <= 300]
    
    total_up_s = sum(float(t["size"]) for t in all_up)
    total_dn_s = sum(float(t["size"]) for t in all_dn)
    
    final_bias = "Up" if total_up_s > total_dn_s else "Down"
    excess = abs(total_up_s - total_dn_s)
    excess_pct = excess / max(total_up_s, total_dn_s) * 100
    
    # 谁先入场
    first_up = min((t["timestamp"] for t in all_up), default=999999)
    first_dn = min((t["timestamp"] for t in all_dn), default=999999)
    who_first = "Up" if first_up <= first_dn else "Dn"
    
    print(f"  {dt.strftime('%H:%M')}: "
          f"先手={who_first}({min(first_up,first_dn)-window_start}s) "
          f"前30s: Up={len(early_up)}/{early_up_s:.0f}份/${early_up_u:.0f} "
          f"Dn={len(early_dn)}/{early_dn_s:.0f}份/${early_dn_u:.0f} "
          f"│ 最终bias={final_bias}(excess={excess:.0f}, {excess_pct:.1f}%)")

# ================================================================
# 分析3: 同一个window内, Up和Down的下单是交替的还是分批的?
# 交替 → 可能是两条独立的机器人线程
# 先买完一侧再买另一侧 → 可能根据信号调整第二侧的量
# ================================================================
print(f"\n{'='*100}")
print(f"分析3: Up/Down 下单顺序模式 (交替 vs 批量)")
print(f"{'='*100}")

for slug in sorted(events.keys()):
    etrades = [t for t in events[slug] if t["price"] > 0 and t["outcome"] in ("Up", "Down")]
    if len(etrades) < 20:
        continue
    
    window_start = int(slug.split("-")[-1])
    dt = datetime.fromtimestamp(window_start, tz=timezone.utc)
    etrades.sort(key=lambda t: t["timestamp"])
    in_window = [t for t in etrades if 0 <= t["timestamp"] - window_start <= 300]
    
    # 看前20笔的方向序列
    seq = [t["outcome"][0] for t in in_window[:30]]  # U or D
    
    # 计算方向切换次数
    switches = sum(1 for i in range(1, len(in_window)) if in_window[i]["outcome"] != in_window[i-1]["outcome"])
    max_switches = len(in_window) - 1
    switch_rate = switches / max_switches * 100 if max_switches > 0 else 0
    
    # 最长连续同方向
    max_run = 1
    cur_run = 1
    for i in range(1, len(in_window)):
        if in_window[i]["outcome"] == in_window[i-1]["outcome"]:
            cur_run += 1
            max_run = max(max_run, cur_run)
        else:
            cur_run = 1
    
    print(f"  {dt.strftime('%H:%M')}: 前30笔={''.join(seq)} "
          f"│ 切换率={switch_rate:.0f}% 最长连续={max_run}")

# ================================================================
# 分析4: Market price 在入场时的分布
# 市场共识价格(Up ask) 在他入场时是什么?
# 如果他在 Up~0.50 时入场, 说明市场不确定, 他用其他信号
# 如果他在 Up~0.30 或 Up~0.70 时入场, 说明他跟随市场
# ================================================================
print(f"\n{'='*100}")
print(f"分析4: 入场时的市场价格 (能否推断信号来源)")
print(f"{'='*100}")

for slug in sorted(events.keys()):
    etrades = [t for t in events[slug] if t["price"] > 0 and t["outcome"] in ("Up", "Down")]
    if len(etrades) < 10:
        continue
    
    window_start = int(slug.split("-")[-1])
    dt = datetime.fromtimestamp(window_start, tz=timezone.utc)
    
    # 前30s的价格(代表入场时市场状态)
    early = [t for t in etrades if t["timestamp"] - window_start <= 30]
    early_up_prices = [t["price"] for t in early if t["outcome"] == "Up"]
    early_dn_prices = [t["price"] for t in early if t["outcome"] == "Down"]
    
    entry_up = sum(early_up_prices)/len(early_up_prices) if early_up_prices else 0
    entry_dn = sum(early_dn_prices)/len(early_dn_prices) if early_dn_prices else 0
    
    # 全窗口方向倾斜
    all_up = [t for t in etrades if t["outcome"] == "Up" and t["timestamp"] - window_start <= 300]
    all_dn = [t for t in etrades if t["outcome"] == "Down" and t["timestamp"] - window_start <= 300]
    total_up_s = sum(float(t["size"]) for t in all_up)
    total_dn_s = sum(float(t["size"]) for t in all_dn)
    final_bias = "Up" if total_up_s > total_dn_s else "Down"
    
    # 入场价格暗示的市场状态
    if entry_up > 0.55:
        market_sentiment = "偏Up"
    elif entry_dn > 0.55:
        market_sentiment = "偏Dn"
    else:
        market_sentiment = "中性"
    
    # 他的倾斜是跟随还是逆市场
    if entry_up > entry_dn:
        market_fav = "Up"
    else:
        market_fav = "Down"
    
    follows_market = "跟随" if final_bias == market_fav else "逆向"
    
    print(f"  {dt.strftime('%H:%M')}: 入场Up={entry_up:.3f} Dn={entry_dn:.3f} "
          f"市场={market_sentiment}({market_fav}) "
          f"他的bias={final_bias} → {follows_market}")

# ================================================================
# 分析5: 他的倾斜方向是否在窗口中段改变
# 如果改变 → 实时信号驱动
# 如果不变 → 入场前就决定了方向
# ================================================================
print(f"\n{'='*100}")
print(f"分析5: 方向倾斜是否在窗口中途翻转")
print(f"{'='*100}")

for slug in sorted(events.keys()):
    etrades = [t for t in events[slug] if t["price"] > 0 and t["outcome"] in ("Up", "Down")]
    if len(etrades) < 20:
        continue
    
    window_start = int(slug.split("-")[-1])
    dt = datetime.fromtimestamp(window_start, tz=timezone.utc)
    in_window = sorted([t for t in etrades if 0 <= t["timestamp"] - window_start <= 300],
                       key=lambda t: t["timestamp"])
    
    # 每60s看一次累计方向
    phases = [(0, 60), (0, 120), (0, 180), (0, 240), (0, 300)]
    phase_biases = []
    
    for start, end in phases:
        phase_trades = [t for t in in_window if start <= t["timestamp"] - window_start < end]
        up_s = sum(float(t["size"]) for t in phase_trades if t["outcome"] == "Up")
        dn_s = sum(float(t["size"]) for t in phase_trades if t["outcome"] == "Down")
        if up_s + dn_s > 0:
            bias = "Up" if up_s > dn_s else "Dn"
            pct = abs(up_s - dn_s) / (up_s + dn_s) * 100
        else:
            bias = "?"
            pct = 0
        phase_biases.append(f"{bias}{pct:.0f}%")
    
    # 检查是否一直同方向
    dirs = [b[0:2] for b in phase_biases if b[0] != "?"]
    consistent = len(set(dirs)) <= 1
    
    print(f"  {dt.strftime('%H:%M')}: "
          f"0-60s={phase_biases[0]:>6}  "
          f"0-120s={phase_biases[1]:>6}  "
          f"0-180s={phase_biases[2]:>6}  "
          f"0-240s={phase_biases[3]:>6}  "
          f"0-300s={phase_biases[4]:>6}  "
          f"{'一致' if consistent else '翻转!'}")

# ================================================================
# 分析6: 增量倾斜分析 - 每段新增份数的倾斜
# ================================================================
print(f"\n{'='*100}")
print(f"分析6: 每60s新增份数的方向倾斜 (是否动态调整)")
print(f"{'='*100}")

for slug in sorted(events.keys()):
    etrades = [t for t in events[slug] if t["price"] > 0 and t["outcome"] in ("Up", "Down")]
    if len(etrades) < 20:
        continue
    
    window_start = int(slug.split("-")[-1])
    dt = datetime.fromtimestamp(window_start, tz=timezone.utc)
    in_window = [t for t in etrades if 0 <= t["timestamp"] - window_start <= 300]
    
    phases = [(0, 60), (60, 120), (120, 180), (180, 240), (240, 300)]
    parts = []
    
    for start, end in phases:
        phase_trades = [t for t in in_window if start <= t["timestamp"] - window_start < end]
        up_s = sum(float(t["size"]) for t in phase_trades if t["outcome"] == "Up")
        dn_s = sum(float(t["size"]) for t in phase_trades if t["outcome"] == "Down")
        total = up_s + dn_s
        if total > 0:
            up_pct = up_s / total * 100
            parts.append(f"Up{up_pct:.0f}%")
        else:
            parts.append("  -  ")
    
    print(f"  {dt.strftime('%H:%M')}: " + " │ ".join(
        f"{s}-{e}s: {p}" for (s, e), p in zip(phases, parts)))
