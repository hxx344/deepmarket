"""
深度分析 Polymarket 交易者 0x1d0034134e 在 BTC/ETH 5min 事件上的下注策略
"""
import requests
import json
from datetime import datetime, timezone
from collections import defaultdict

ADDR = "0x1d0034134e"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
}

# ────────────────────────────────────────────
# 1) 拉取全部可用交易 (分页)
# ────────────────────────────────────────────
all_trades = []
offset = 0
BATCH = 100

print("Fetching trades...")
while True:
    url = f"https://data-api.polymarket.com/trades?address={ADDR}&limit={BATCH}&offset={offset}"
    r = requests.get(url, headers=HEADERS, timeout=15)
    if r.status_code != 200:
        print(f"  offset={offset} => HTTP {r.status_code}, stopping")
        break
    batch = r.json()
    if not batch:
        break
    all_trades.extend(batch)
    print(f"  offset={offset}: got {len(batch)} trades (total={len(all_trades)})")
    if len(batch) < BATCH:
        break
    offset += BATCH

print(f"\nTotal trades fetched: {len(all_trades)}")

# ────────────────────────────────────────────
# 2) 过滤 BTC & ETH 5-min 事件
# ────────────────────────────────────────────
btc_5m = []
eth_5m = []
other = []

for t in all_trades:
    title = (t.get("title") or "").lower()
    slug = (t.get("slug") or "").lower()
    if "5m" in slug or "5min" in slug or "5-min" in title or "2:10pm-2:15pm" in title or "5m" in title:
        if "btc" in title or "bitcoin" in title or "btc" in slug:
            btc_5m.append(t)
        elif "eth" in title or "ethereum" in title or "eth" in slug:
            eth_5m.append(t)
        else:
            other.append(t)

print(f"\nBTC 5-min trades: {len(btc_5m)}")
print(f"ETH 5-min trades: {len(eth_5m)}")
print(f"Other 5-min trades: {len(other)}")

# ────────────────────────────────────────────
# 3) 分析每笔交易的详细模式
# ────────────────────────────────────────────
def analyze_trades(trades, label):
    if not trades:
        print(f"\n{'='*60}\n{label}: 无数据\n{'='*60}")
        return

    print(f"\n{'='*60}")
    print(f"{label}: 共 {len(trades)} 笔交易")
    print(f"{'='*60}")

    # 基础统计
    buy_count = sum(1 for t in trades if t["side"] == "BUY")
    sell_count = sum(1 for t in trades if t["side"] == "SELL")
    print(f"  BUY: {buy_count}  |  SELL: {sell_count}")

    # 按 outcome 统计
    outcome_buy = defaultdict(int)
    outcome_sell = defaultdict(int)
    for t in trades:
        outcome = t.get("outcome", "?")
        if t["side"] == "BUY":
            outcome_buy[outcome] += 1
        else:
            outcome_sell[outcome] += 1
    print(f"  BUY by outcome:  {dict(outcome_buy)}")
    print(f"  SELL by outcome: {dict(outcome_sell)}")

    # 价格分布
    buy_prices = [t["price"] for t in trades if t["side"] == "BUY"]
    sell_prices = [t["price"] for t in trades if t["side"] == "SELL"]
    if buy_prices:
        print(f"  BUY price:  min={min(buy_prices):.3f}  avg={sum(buy_prices)/len(buy_prices):.3f}  max={max(buy_prices):.3f}")
    if sell_prices:
        print(f"  SELL price: min={min(sell_prices):.3f}  avg={sum(sell_prices)/len(sell_prices):.3f}  max={max(sell_prices):.3f}")

    # size 分布
    sizes = [t["size"] for t in trades]
    print(f"  Size: min=${min(sizes):.2f}  avg=${sum(sizes)/len(sizes):.2f}  max=${max(sizes):.2f}  total=${sum(sizes):.2f}")

    # 时间分布 - 按 slug 分组（每个 slug = 一个 5min 窗口）
    by_slug = defaultdict(list)
    for t in trades:
        by_slug[t.get("slug", "?")].append(t)

    print(f"\n  === 按窗口 (slug) 分组: {len(by_slug)} 个不同窗口 ===")

    # 分析每个窗口的策略模式
    patterns = defaultdict(int)
    
    # 详细列出最近 20 个窗口
    sorted_slugs = sorted(by_slug.items(), key=lambda x: x[1][0]["timestamp"], reverse=True)
    
    print(f"\n  --- 最近 30 个窗口的详细交易 ---")
    for slug, slug_trades in sorted_slugs[:30]:
        slug_trades.sort(key=lambda x: x["timestamp"])
        ts0 = datetime.fromtimestamp(slug_trades[0]["timestamp"], tz=timezone.utc)
        title = slug_trades[0].get("title", "?")
        
        # 构建操作序列
        ops = []
        total_buy = 0
        total_sell = 0
        net_cost = 0
        for st in slug_trades:
            side = st["side"]
            outcome = st.get("outcome", "?")
            price = st["price"]
            size = st["size"]
            ops.append(f"{side}_{outcome}@{price:.2f}x${size:.1f}")
            if side == "BUY":
                total_buy += size
                net_cost += size * price
            else:
                total_sell += size
                net_cost -= size * price
        
        op_str = " → ".join(ops)
        print(f"\n  [{ts0.strftime('%m-%d %H:%M')}] {title}")
        print(f"    操作: {op_str}")
        print(f"    买入总额: ${total_buy:.2f}  卖出总额: ${total_sell:.2f}  净成本: ${net_cost:.2f}")

    # 统计常见模式
    print(f"\n  --- 操作模式统计 ---")
    for slug, slug_trades in sorted_slugs:
        slug_trades.sort(key=lambda x: x["timestamp"])
        pattern_key = " → ".join(f"{t['side']}_{t.get('outcome','?')}" for t in slug_trades)
        patterns[pattern_key] += 1

    for pattern, count in sorted(patterns.items(), key=lambda x: -x[1]):
        print(f"    {count:3d}x | {pattern}")

    # 分析策略特征
    print(f"\n  --- 策略特征分析 ---")
    
    # 是否有配对交易 (同一窗口买卖)
    paired_windows = 0
    buy_only_windows = 0
    sell_only_windows = 0
    for slug, slug_trades in by_slug.items():
        sides = set(t["side"] for t in slug_trades)
        if "BUY" in sides and "SELL" in sides:
            paired_windows += 1
        elif "BUY" in sides:
            buy_only_windows += 1
        else:
            sell_only_windows += 1
    
    print(f"    配对交易窗口 (BUY+SELL): {paired_windows}")
    print(f"    纯买入窗口: {buy_only_windows}")
    print(f"    纯卖出窗口: {sell_only_windows}")
    
    # 按 outcome 的倾向
    up_buy = sum(t["size"] for t in trades if t["side"] == "BUY" and t.get("outcome") in ("Up", "Yes", "UP"))
    down_buy = sum(t["size"] for t in trades if t["side"] == "BUY" and t.get("outcome") in ("Down", "No", "DOWN"))
    up_sell = sum(t["size"] for t in trades if t["side"] == "SELL" and t.get("outcome") in ("Up", "Yes", "UP"))
    down_sell = sum(t["size"] for t in trades if t["side"] == "SELL" and t.get("outcome") in ("Down", "No", "DOWN"))
    
    print(f"    BUY Up/Yes 总额: ${up_buy:.2f}")
    print(f"    BUY Down/No 总额: ${down_buy:.2f}")
    print(f"    SELL Up/Yes 总额: ${up_sell:.2f}")
    print(f"    SELL Down/No 总额: ${down_sell:.2f}")
    
    # 时间间隔分析 - 距窗口结束多远入场
    # slug 格式通常: btc-updown-5m-{unix}
    entry_timing = []
    for t in trades:
        slug = t.get("slug", "")
        parts = slug.split("-")
        try:
            window_ts = int(parts[-1])  # 窗口开始时间
            window_end = window_ts + 300  # 5分钟
            trade_ts = t["timestamp"]
            secs_before_end = window_end - trade_ts
            secs_after_start = trade_ts - window_ts
            entry_timing.append({
                "secs_before_end": secs_before_end,
                "secs_after_start": secs_after_start,
                "side": t["side"],
            })
        except (ValueError, IndexError):
            pass
    
    if entry_timing:
        buy_timings = [e["secs_before_end"] for e in entry_timing if e["side"] == "BUY"]
        sell_timings = [e["secs_before_end"] for e in entry_timing if e["side"] == "SELL"]
        
        print(f"\n    入场时机 (距窗口结束秒数):")
        if buy_timings:
            print(f"      BUY:  min={min(buy_timings)}s  avg={sum(buy_timings)/len(buy_timings):.0f}s  max={max(buy_timings)}s")
        if sell_timings:
            print(f"      SELL: min={min(sell_timings)}s  avg={sum(sell_timings)/len(sell_timings):.0f}s  max={max(sell_timings)}s")
        
        # 分桶: 0-60s, 60-120s, 120-180s, 180-240s, 240-300s
        print(f"\n    入场时间分布 (距窗口结束):")
        for lo, hi, label in [(0,60,"0-60s(临近)"), (60,120,"60-120s"), (120,180,"120-180s"), (180,240,"180-240s"), (240,300,"240-300s(早期)")]:
            count = sum(1 for e in entry_timing if lo <= e["secs_before_end"] < hi)
            total = len(entry_timing)
            pct = count / total * 100 if total else 0
            bar = "█" * int(pct / 2)
            print(f"      {label:16s}: {count:3d} ({pct:5.1f}%) {bar}")

analyze_trades(btc_5m, "BTC 5-min")
analyze_trades(eth_5m, "ETH 5-min")

# ────────────────────────────────────────────
# 4) 综合洞察
# ────────────────────────────────────────────
print(f"\n{'='*60}")
print("4. 综合洞察")
print(f"{'='*60}")

# 打印所有唯一的 outcome 值
all_outcomes = set(t.get("outcome") for t in all_trades)
print(f"  所有 outcome 值: {all_outcomes}")

# 按日期统计交易量
by_date = defaultdict(lambda: {"count": 0, "volume": 0.0})
for t in btc_5m + eth_5m:
    dt = datetime.fromtimestamp(t["timestamp"], tz=timezone.utc)
    day = dt.strftime("%Y-%m-%d")
    by_date[day]["count"] += 1
    by_date[day]["volume"] += t["size"]

print(f"\n  日交易量:")
for day in sorted(by_date.keys()):
    d = by_date[day]
    print(f"    {day}: {d['count']:3d} 笔  ${d['volume']:.2f}")

print("\nDone.")
