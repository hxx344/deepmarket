"""
假设检验: Lead-Lag 套利
  - Binance BTC 现货/期货价格领先于 Polymarket Up/Down 定价
  - 如果 BTC 在最近几秒内涨了, PM 上的 Up 价格可能还没涨 -> 买 Up 便宜
  - 如果 BTC 在最近几秒内跌了, PM 上的 Down 价格可能还没涨 -> 买 Down 便宜

分析维度:
  1. 他的下单方向是否跟随最近 N 秒的 BTC 动量?
  2. BTC 动量大时, 他是否更偏向买动量方向?
  3. 他买的价格 vs 交易后 PM 价格的变化 (有没有"买在低点"的模式)
  4. 按窗口内每秒 BTC 变化 vs 他的下单序列做因果检验
  5. BTC 1s级别价格变化 vs 他的下单时机
"""
import json
import time
import requests
from collections import defaultdict, Counter
from datetime import datetime, timezone

# ================================================================
# 加载数据
# ================================================================
with open("docs/pm_account_0x1d_raw_trades.json") as f:
    trades = json.load(f)

btc5m = [t for t in trades if "btc-updown-5m" in t.get("eventSlug", "")]
btc5m = [t for t in btc5m if t.get("price", 0) > 0 and t.get("outcome") in ("Up", "Down")]
print(f"BTC 5min valid trades: {len(btc5m)}")

events = defaultdict(list)
for t in btc5m:
    events[t["eventSlug"]].append(t)
print(f"Windows: {len(events)}")

# ================================================================
# 获取 Binance 高精度数据 (1s K线 如果可用, 否则用 1m)
# ================================================================
all_starts = [int(slug.split("-")[-1]) for slug in events]
global_start = min(all_starts) - 120
global_end = max(all_starts) + 420

print(f"\nFetching Binance 1s klines...")
print(f"  Range: {datetime.fromtimestamp(global_start, tz=timezone.utc)} to {datetime.fromtimestamp(global_end, tz=timezone.utc)}")

def fetch_binance_1s(start_ts, end_ts):
    """Binance 1s K线 (2023年起可用)"""
    url = "https://api.binance.com/api/v3/klines"
    all_k = []
    cur = start_ts * 1000
    end_ms = end_ts * 1000
    while cur < end_ms:
        params = {
            "symbol": "BTCUSDT", "interval": "1s",
            "startTime": int(cur), "endTime": int(end_ms), "limit": 1000
        }
        try:
            r = requests.get(url, params=params, timeout=15)
            r.raise_for_status()
            data = r.json()
            if not data:
                break
            all_k.extend(data)
            cur = data[-1][0] + 1000
            time.sleep(0.15)
        except Exception as e:
            print(f"  Error: {e}")
            break
    return all_k

klines_1s = fetch_binance_1s(global_start, global_end)
print(f"  Got {len(klines_1s)} 1s klines")

if len(klines_1s) < 100:
    print("  Falling back to 1m klines...")
    def fetch_binance_1m(start_ts, end_ts):
        url = "https://api.binance.com/api/v3/klines"
        all_k = []
        cur = start_ts * 1000
        end_ms = end_ts * 1000
        while cur < end_ms:
            params = {"symbol": "BTCUSDT", "interval": "1m",
                      "startTime": int(cur), "endTime": int(end_ms), "limit": 1000}
            r = requests.get(url, params=params, timeout=15)
            data = r.json()
            if not data: break
            all_k.extend(data)
            cur = data[-1][0] + 60000
            time.sleep(0.2)
        return all_k
    klines_1m = fetch_binance_1m(global_start, global_end)
    print(f"  Got {len(klines_1m)} 1m klines")
    # 用 1m 数据填充 price_at 查找
    use_1s = False
    price_by_sec = {}
    for k in klines_1m:
        ts_s = k[0] // 1000
        o, c = float(k[1]), float(k[4])
        for s in range(60):
            # 线性插值
            frac = s / 60.0
            price_by_sec[ts_s + s] = o + (c - o) * frac
else:
    use_1s = True
    price_by_sec = {}
    for k in klines_1s:
        ts_s = k[0] // 1000
        # 1s K线: 用 close 作为该秒的价格
        price_by_sec[ts_s] = float(k[4])

print(f"  Price map: {len(price_by_sec)} seconds covered")

def btc_at(ts):
    if ts in price_by_sec:
        return price_by_sec[ts]
    # 找最近的
    for d in range(1, 5):
        if ts - d in price_by_sec: return price_by_sec[ts - d]
        if ts + d in price_by_sec: return price_by_sec[ts + d]
    return None

# ================================================================
# 分析 1: 下单前 N 秒的 BTC 动量 vs 下单方向
# ================================================================
print("\n" + "="*100)
print("分析1: 下单前 N 秒的 BTC 动量 vs 下单方向")
print("="*100)

lookbacks = [1, 2, 3, 5, 10, 15, 30, 60]

for lb in lookbacks:
    follow = 0   # BTC涨→买Up 或 BTC跌→买Down
    reverse = 0  # 反向
    neutral = 0  # BTC没动
    total_follow_usdc = 0
    total_reverse_usdc = 0
    
    for t in btc5m:
        ts = t["timestamp"]
        p_now = btc_at(ts)
        p_prev = btc_at(ts - lb)
        if p_now is None or p_prev is None:
            continue
        
        move = p_now - p_prev
        usdc = float(t.get("usdcSize", 0))
        
        if abs(move) < 0.5:  # < $0.5 = no movement
            neutral += 1
            continue
        
        if (move > 0 and t["outcome"] == "Up") or (move < 0 and t["outcome"] == "Down"):
            follow += 1
            total_follow_usdc += usdc
        else:
            reverse += 1
            total_reverse_usdc += usdc
    
    total = follow + reverse
    if total > 0:
        follow_pct = follow / total * 100
        print(f"  Lookback {lb:>2}s: follow={follow}({follow_pct:.1f}%) reverse={reverse}({100-follow_pct:.1f}%) "
              f"neutral={neutral} | follow$={total_follow_usdc:,.0f} reverse$={total_reverse_usdc:,.0f} "
              f"| USD bias={total_follow_usdc/(total_follow_usdc+total_reverse_usdc)*100:.1f}%")

# ================================================================
# 分析 2: 按 BTC 动量大小分桶
# ================================================================
print("\n" + "="*100)
print("分析2: BTC 5秒动量大小 vs 下单方向 (动量越大越follow?)")
print("="*100)

lb = 5  # 5秒lookback
buckets_def = [(-9999, -20), (-20, -10), (-10, -3), (-3, 3), (3, 10), (10, 20), (20, 9999)]
bucket_labels = ["<-$20", "-$20~-10", "-$10~-3", "-$3~+$3", "+$3~+$10", "+$10~+$20", ">+$20"]

for (lo, hi), label in zip(buckets_def, bucket_labels):
    bucket_trades = []
    for t in btc5m:
        ts = t["timestamp"]
        p_now = btc_at(ts)
        p_prev = btc_at(ts - lb)
        if p_now is None or p_prev is None:
            continue
        move = p_now - p_prev
        if lo <= move < hi:
            bucket_trades.append((t, move))
    
    if not bucket_trades:
        continue
    
    up_count = sum(1 for t, _ in bucket_trades if t["outcome"] == "Up")
    dn_count = sum(1 for t, _ in bucket_trades if t["outcome"] == "Down")
    up_pct = up_count / len(bucket_trades) * 100
    
    # 如果在lead-lag: BTC涨时应该多买Up, 跌时多买Down
    print(f"  BTC 5s move {label:>12}: n={len(bucket_trades):>4} | Up buy={up_pct:>5.1f}% Dn buy={100-up_pct:>5.1f}%")

# ================================================================
# 分析 3: 时间序列 - 他是否在 BTC 大动之后紧跟下单?
# ================================================================
print("\n" + "="*100)
print("分析3: BTC 大动(>$10/5s)后, 他的下单延迟和方向")
print("="*100)

big_moves = []  # (timestamp, move_usd, direction)
for ts in range(global_start, global_end):
    p_now = btc_at(ts)
    p_prev = btc_at(ts - 5)
    if p_now and p_prev and abs(p_now - p_prev) > 10:
        big_moves.append((ts, p_now - p_prev, "up" if p_now > p_prev else "down"))

print(f"  BTC big moves (>$10/5s): {len(big_moves)}")

# 去重: 同一波动只保留首次
filtered_moves = []
last_ts = 0
for ts, move, d in big_moves:
    if ts - last_ts > 5:
        filtered_moves.append((ts, move, d))
        last_ts = ts
print(f"  After dedup (>5s gap): {len(filtered_moves)}")

# 对每个大波动, 看之后10秒内 他 的交易
for ts, move, d in filtered_moves[:30]:  # 只看前30个
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    # 找该时刻附近的窗口
    nearby_trades = [t for t in btc5m if 0 <= t["timestamp"] - ts <= 10]
    
    if nearby_trades:
        outcomes = Counter(t["outcome"] for t in nearby_trades)
        up_n = outcomes.get("Up", 0)
        dn_n = outcomes.get("Down", 0)
        total_usdc = sum(float(t.get("usdcSize", 0)) for t in nearby_trades)
        follow_n = up_n if d == "up" else dn_n
        n = up_n + dn_n
        follow_pct = follow_n / n * 100 if n > 0 else 0
        
        print(f"  {dt.strftime('%H:%M:%S')} BTC {move:>+7.1f} ({d:>4}) -> "
              f"{len(nearby_trades)} trades in 10s: Up={up_n} Dn={dn_n} "
              f"follow={follow_pct:.0f}% ${total_usdc:.0f}")
    # else: no trade after this move

# ================================================================
# 分析 4: 更精细 - 逐笔交易的 BTC 1s/2s 动量
# ================================================================
print("\n" + "="*100)
print("分析4: 逐笔交易前1s/2s/3s BTC变化 vs 买入方向 (核心lead-lag检测)")
print("="*100)

# For each trade, check if BTC moved in the same direction in the last 1-3 seconds
for lb in [1, 2, 3]:
    pos_follow = 0  # BTC涨->买Up
    neg_follow = 0  # BTC跌->买Down
    pos_reverse = 0 # BTC涨->买Down
    neg_reverse = 0 # BTC跌->买Up
    no_move = 0
    
    # accumulate premium for follow vs reverse
    follow_premiums = []
    reverse_premiums = []
    
    for t in btc5m:
        ts = t["timestamp"]
        p_now = btc_at(ts)
        p_prev = btc_at(ts - lb)
        if not p_now or not p_prev:
            continue
        
        move = p_now - p_prev
        if abs(move) < 0.5:
            no_move += 1
            continue
        
        if move > 0:
            if t["outcome"] == "Up":
                pos_follow += 1
            else:
                pos_reverse += 1
        else:
            if t["outcome"] == "Down":
                neg_follow += 1
            else:
                neg_reverse += 1
    
    total_dir = pos_follow + neg_follow + pos_reverse + neg_reverse
    total_follow = pos_follow + neg_follow
    total_reverse = pos_reverse + neg_reverse
    
    if total_dir > 0:
        print(f"\n  Lookback {lb}s:")
        print(f"    BTC涨 -> 买Up: {pos_follow:>4}  买Down: {pos_reverse:>4}")
        print(f"    BTC跌 -> 买Dn: {neg_follow:>4}  买Up:   {neg_reverse:>4}")
        print(f"    Follow rate: {total_follow/total_dir*100:.1f}% ({total_follow}/{total_dir})")
        print(f"    No movement (<$0.5): {no_move}")

# ================================================================
# 分析 5: 他买了之后 PM 价格是否马上涨? (informed trader 的标志)
# ================================================================
print("\n" + "="*100)
print("分析5: 买入后同 outcome 的 PM 价格变化 (informed trader 检测)")
print("="*100)

# 对每笔交易, 看之后 5/10/30 秒内同 outcome 的下一笔成交价
for slug in sorted(events.keys())[:5]:  # 取前5个窗口示例
    etrades = sorted(events[slug], key=lambda t: t["timestamp"])
    window_start = int(slug.split("-")[-1])
    dt = datetime.fromtimestamp(window_start, tz=timezone.utc)
    
    # 按 outcome 分组, 按时间排序
    up_trades = [(t["timestamp"], t["price"], float(t.get("usdcSize", 0))) 
                 for t in etrades if t["outcome"] == "Up"]
    dn_trades = [(t["timestamp"], t["price"], float(t.get("usdcSize", 0))) 
                 for t in etrades if t["outcome"] == "Down"]
    
    print(f"\n  Window {dt.strftime('%H:%M')} (Up={len(up_trades)}, Dn={len(dn_trades)}):")
    
    # 看 Up 序列的价格趋势
    if len(up_trades) > 5:
        # 分3段看均价
        third = len(up_trades) // 3
        early_up = sum(p for _, p, _ in up_trades[:third]) / third
        mid_up = sum(p for _, p, _ in up_trades[third:2*third]) / third
        late_up = sum(p for _, p, _ in up_trades[2*third:]) / (len(up_trades) - 2*third)
        print(f"    Up price trend: early={early_up:.3f} -> mid={mid_up:.3f} -> late={late_up:.3f}")
    
    if len(dn_trades) > 5:
        third = len(dn_trades) // 3
        early_dn = sum(p for _, p, _ in dn_trades[:third]) / third
        mid_dn = sum(p for _, p, _ in dn_trades[third:2*third]) / third
        late_dn = sum(p for _, p, _ in dn_trades[2*third:]) / (len(dn_trades) - 2*third)
        print(f"    Dn price trend: early={early_dn:.3f} -> mid={mid_dn:.3f} -> late={late_dn:.3f}")

# ================================================================
# 分析 6: 全局 BTC 动量 vs Up/Down 购买比例 (按每 10s 桶)
# ================================================================
print("\n" + "="*100)
print("分析6: 每10s桶内: BTC变化 vs Up购买比例")
print("="*100)

bucket_data = []  # (btc_change_10s, up_buy_ratio, n_trades)

for slug in sorted(events.keys()):
    etrades = sorted(events[slug], key=lambda t: t["timestamp"])
    window_start = int(slug.split("-")[-1])
    
    # 按10s桶
    for bucket_start in range(window_start, window_start + 300, 10):
        bucket_trades = [t for t in etrades if bucket_start <= t["timestamp"] < bucket_start + 10]
        if len(bucket_trades) < 2:
            continue
        
        p_start = btc_at(bucket_start)
        p_end = btc_at(bucket_start + 10)
        if not p_start or not p_end:
            continue
        
        btc_change = p_end - p_start
        up_count = sum(1 for t in bucket_trades if t["outcome"] == "Up")
        up_ratio = up_count / len(bucket_trades)
        
        bucket_data.append({
            "btc_change": btc_change,
            "up_ratio": up_ratio,
            "n": len(bucket_trades),
        })

if bucket_data:
    # 按 BTC 变化分桶
    btc_bins = [(-9999, -5), (-5, -1), (-1, 1), (1, 5), (5, 9999)]
    btc_labels = ["BTC<-$5", "-$5~-1", "-$1~+1", "+$1~+$5", "BTC>+$5"]
    
    print(f"\n  {'BTC 10s change':>15} | {'Buckets':>7} | {'Avg Up%':>7} | {'StdDev':>7} | {'n trades':>8}")
    
    for (lo, hi), label in zip(btc_bins, btc_labels):
        matching = [b for b in bucket_data if lo <= b["btc_change"] < hi]
        if not matching:
            continue
        avg_up = sum(b["up_ratio"] for b in matching) / len(matching)
        n_total = sum(b["n"] for b in matching)
        # std dev
        if len(matching) > 1:
            mean = avg_up
            var = sum((b["up_ratio"] - mean)**2 for b in matching) / len(matching)
            std = var**0.5
        else:
            std = 0
        
        print(f"  {label:>15} | {len(matching):>7} | {avg_up*100:>6.1f}% | {std*100:>6.1f}% | {n_total:>8}")

# ================================================================
# 分析 7: 同一秒内的交易 - 分析是否区分BTC上涨秒和下跌秒
# ================================================================
print("\n" + "="*100)
print("分析7: 逐秒分析 - BTC涨的秒份 vs BTC跌的秒份, 他的行为差异")
print("="*100)

up_seconds = {"up_trades": 0, "dn_trades": 0, "usdc_up": 0, "usdc_dn": 0, "count": 0}
dn_seconds = {"up_trades": 0, "dn_trades": 0, "usdc_up": 0, "usdc_dn": 0, "count": 0}

for t in btc5m:
    ts = t["timestamp"]
    p_now = btc_at(ts)
    p_prev = btc_at(ts - 1)
    if not p_now or not p_prev:
        continue
    
    usdc = float(t.get("usdcSize", 0))
    
    if p_now > p_prev + 0.5:  # BTC涨了
        target = up_seconds
    elif p_now < p_prev - 0.5:  # BTC跌了
        target = dn_seconds
    else:
        continue
    
    target["count"] += 1
    if t["outcome"] == "Up":
        target["up_trades"] += 1
        target["usdc_up"] += usdc
    else:
        target["dn_trades"] += 1
        target["usdc_dn"] += usdc

print(f"\n  BTC涨的秒份 ({up_seconds['count']} trades):")
if up_seconds["count"] > 0:
    total = up_seconds["up_trades"] + up_seconds["dn_trades"]
    print(f"    Up buy: {up_seconds['up_trades']} ({up_seconds['up_trades']/total*100:.1f}%)  "
          f"Dn buy: {up_seconds['dn_trades']} ({up_seconds['dn_trades']/total*100:.1f}%)")
    total_usdc = up_seconds["usdc_up"] + up_seconds["usdc_dn"]
    print(f"    USDC: Up=${up_seconds['usdc_up']:,.0f} ({up_seconds['usdc_up']/total_usdc*100:.1f}%)  "
          f"Dn=${up_seconds['usdc_dn']:,.0f} ({up_seconds['usdc_dn']/total_usdc*100:.1f}%)")

print(f"\n  BTC跌的秒份 ({dn_seconds['count']} trades):")
if dn_seconds["count"] > 0:
    total = dn_seconds["up_trades"] + dn_seconds["dn_trades"]
    print(f"    Up buy: {dn_seconds['up_trades']} ({dn_seconds['up_trades']/total*100:.1f}%)  "
          f"Dn buy: {dn_seconds['dn_trades']} ({dn_seconds['dn_trades']/total*100:.1f}%)")
    total_usdc = dn_seconds["usdc_up"] + dn_seconds["usdc_dn"]
    print(f"    USDC: Up=${dn_seconds['usdc_up']:,.0f} ({dn_seconds['usdc_up']/total_usdc*100:.1f}%)  "
          f"Dn=${dn_seconds['usdc_dn']:,.0f} ({dn_seconds['usdc_dn']/total_usdc*100:.1f}%)")

# ================================================================
# 分析 8: "纯 lead-lag 指标" - 他买 Up 的平均 BTC 1s 动量 vs 买 Down 的
# ================================================================
print("\n" + "="*100)
print("分析8: 买Up时 vs 买Down时 的 BTC 瞬时动量分布")
print("="*100)

up_buy_momentums = {1: [], 2: [], 3: [], 5: []}
dn_buy_momentums = {1: [], 2: [], 3: [], 5: []}

for t in btc5m:
    ts = t["timestamp"]
    for lb in [1, 2, 3, 5]:
        p_now = btc_at(ts)
        p_prev = btc_at(ts - lb)
        if not p_now or not p_prev:
            continue
        m = p_now - p_prev
        if t["outcome"] == "Up":
            up_buy_momentums[lb].append(m)
        else:
            dn_buy_momentums[lb].append(m)

for lb in [1, 2, 3, 5]:
    up_m = up_buy_momentums[lb]
    dn_m = dn_buy_momentums[lb]
    if up_m and dn_m:
        up_avg = sum(up_m) / len(up_m)
        dn_avg = sum(dn_m) / len(dn_m)
        diff = up_avg - dn_avg
        
        # 如果是 lead-lag: 买 Up 时 BTC 应该刚涨 (up_avg > 0)
        #                  买 Down 时 BTC 应该刚跌 (dn_avg < 0)
        #                  diff 应该显著 > 0
        print(f"  {lb}s lookback: Up买时BTC动量=${up_avg:+.2f}  Down买时BTC动量=${dn_avg:+.2f}  "
              f"差值=${diff:+.2f}  {'<-- SIGNAL!' if abs(diff) > 1 else ''}")

# ================================================================
# 结论
# ================================================================
print("\n" + "="*100)
print("Lead-Lag 套利假设检验结论")
print("="*100)

# 综合判断
all_follow_rates = []
for lb in [1, 2, 3, 5]:
    follow = 0
    total = 0
    for t in btc5m:
        ts = t["timestamp"]
        p_now = btc_at(ts)
        p_prev = btc_at(ts - lb)
        if not p_now or not p_prev:
            continue
        move = p_now - p_prev
        if abs(move) < 0.5:
            continue
        total += 1
        if (move > 0 and t["outcome"] == "Up") or (move < 0 and t["outcome"] == "Down"):
            follow += 1
    if total > 0:
        all_follow_rates.append(follow / total * 100)

avg_follow = sum(all_follow_rates) / len(all_follow_rates) if all_follow_rates else 50

print(f"""
  Average follow rate across lookbacks: {avg_follow:.1f}%

  Lead-Lag 套利判定标准:
    follow rate >> 55%  -> Strong lead-lag signal usage
    follow rate 52-55%  -> Weak/possible lead-lag
    follow rate ~50%    -> No lead-lag signal
    follow rate < 48%   -> Contrarian (unlikely for lead-lag)

  如果 follow rate ~50%:
    -> 他不根据 BTC 瞬时动量选择 Up/Down 方向
    -> 他不是在做 lead-lag 套利
    -> 双线程独立下单模型 (一个线程只买Up, 一个只买Down) 更加吻合
  
  如果 follow rate >> 55%:
    -> BTC价格变动确实领先于PM定价
    -> 他在利用这个时间差
    -> 买入方向与BTC动量一致 = informed trader
""")
