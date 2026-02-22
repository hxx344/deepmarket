"""
分析假设: 该账号是否利用 BTC 实时价格偏离基准价与市场定价之间的溢价获利

核心逻辑:
  - BTC 5min 市场: "窗口结束时 BTC 价格是否高于窗口开始时?"
  - 基准价 = 窗口开始时的 BTC 价格
  - 在窗口进行中, BTC 已经偏离基准价 → 理论概率偏移
  - 如果市场定价反应不够快 → 存在定价溢价 → 可套利

分析步骤:
  1. 从 Binance 获取 1 分钟 K 线数据 (覆盖所有交易窗口)
  2. 对每笔交易, 计算当时 BTC 对基准价的偏移量
  3. 用简化模型估算理论公允概率
  4. 对比交易价格与理论价格, 找出定价溢价模式
"""
import json
import time
import math
import requests
from collections import defaultdict
from datetime import datetime, timezone

# ================================================================
# 1. 加载交易数据
# ================================================================
with open("docs/pm_account_0x1d_raw_trades.json") as f:
    trades = json.load(f)

btc5m = [t for t in trades if "btc-updown-5m" in t.get("eventSlug", "")]
print(f"BTC 5min 交易总数: {len(btc5m)}")

events = defaultdict(list)
for t in btc5m:
    events[t["eventSlug"]].append(t)

print(f"窗口数: {len(events)}")

# ================================================================
# 2. 获取 BTC 历史价格 (Binance 1m K线)
# ================================================================
def fetch_binance_klines(start_ts, end_ts):
    """从 Binance 获取 1 分钟 K 线数据"""
    url = "https://api.binance.com/api/v3/klines"
    all_klines = []
    current_start = start_ts * 1000  # ms
    end_ms = end_ts * 1000
    
    while current_start < end_ms:
        params = {
            "symbol": "BTCUSDT",
            "interval": "1m",
            "startTime": int(current_start),
            "endTime": int(end_ms),
            "limit": 1000
        }
        try:
            resp = requests.get(url, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            if not data:
                break
            all_klines.extend(data)
            # 下一批从最后一根K线之后
            current_start = data[-1][0] + 60000
            time.sleep(0.2)  # rate limit
        except Exception as e:
            print(f"  Binance API 错误: {e}")
            break
    
    return all_klines

# 找出所有窗口的时间范围
all_starts = []
for slug in events:
    ws = int(slug.split("-")[-1])
    all_starts.append(ws)

global_start = min(all_starts) - 300  # 提前5分钟
global_end = max(all_starts) + 600    # 延后10分钟

print(f"\n获取 Binance BTC/USDT 1m K线...")
print(f"  时间范围: {datetime.fromtimestamp(global_start, tz=timezone.utc)} 到 {datetime.fromtimestamp(global_end, tz=timezone.utc)}")

klines = fetch_binance_klines(global_start, global_end)
print(f"  获取到 {len(klines)} 根 1m K线")

if not klines:
    print("\n❌ 无法获取 Binance 数据, 尝试备用方案...")
    # 备用: 用交易价格本身推断市场共识概率
    # Up_price ≈ P(BTC上涨), 如果 BTC > base, Up_price 应该 > 0.5
    # 我们可以看他是否在 Up_price 相对偏低时买 Up
    pass

# 构建价格查找表: timestamp(秒) -> close_price
price_map = {}
for k in klines:
    # k: [open_time, open, high, low, close, volume, close_time, ...]
    open_time_s = k[0] // 1000
    close_time_s = k[6] // 1000
    open_p = float(k[1])
    high_p = float(k[2])
    low_p = float(k[3])
    close_p = float(k[4])
    # 每秒插值: 简化为线性插值
    # 但更实际的是: 用该分钟的 open 作为分钟开始的价格
    price_map[open_time_s] = {
        "open": open_p, "high": high_p, "low": low_p, "close": close_p
    }

def get_btc_price_at(ts):
    """获取某时刻的 BTC 近似价格"""
    # 找该ts所在分钟的K线
    minute_start = (ts // 60) * 60
    if minute_start in price_map:
        # 如果在分钟开头, 用open; 否则用close (粗略)
        offset = ts - minute_start
        k = price_map[minute_start]
        if offset < 30:
            return k["open"]
        else:
            return k["close"]
    # 找最近的K线
    best = None
    best_diff = 999999
    for kts in price_map:
        diff = abs(kts - minute_start)
        if diff < best_diff:
            best_diff = diff
            best = kts
    if best and best_diff < 120:
        return price_map[best]["close"]
    return None

# ================================================================
# 3. 估算 BTC 5min 波动率 (用于理论概率计算)
# ================================================================
# 从K线数据中计算5分钟收益率的标准差
five_min_returns = []
kline_closes = [(k[0] // 1000, float(k[4])) for k in klines]
for i in range(5, len(kline_closes)):
    t0, p0 = kline_closes[i-5]
    t1, p1 = kline_closes[i]
    if p0 > 0:
        ret = (p1 - p0) / p0
        five_min_returns.append(ret)

if five_min_returns:
    vol_5m = (sum(r**2 for r in five_min_returns) / len(five_min_returns))**0.5
    print(f"\n5分钟波动率 (σ_5m): {vol_5m*100:.4f}%")
    mean_abs = sum(abs(r) for r in five_min_returns) / len(five_min_returns)
    print(f"5分钟平均绝对偏移: {mean_abs*100:.4f}%")
else:
    vol_5m = 0.001  # 默认 0.1%
    print(f"\n无法计算波动率, 使用默认: {vol_5m*100:.4f}%")

# ================================================================
# 4. 理论公允概率模型
# ================================================================
def theoretical_up_prob(btc_now, btc_base, remaining_seconds, vol_5m):
    """
    根据 BTC 偏移量计算理论 Up 概率
    
    模型: BTC 剩余时间的走势 ~ GBM
    P(BTC_end > BTC_base) = Φ( ln(BTC_now/BTC_base) / (σ * sqrt(T_remaining/300)) )
    
    其中 T_remaining = 剩余秒数, 300 = 5分钟
    """
    if btc_base <= 0 or btc_now <= 0 or remaining_seconds <= 0:
        return 0.5
    
    # 对数偏移
    log_offset = math.log(btc_now / btc_base)
    
    # 剩余时间比例调整波动率
    time_ratio = remaining_seconds / 300.0
    if time_ratio <= 0:
        return 1.0 if log_offset > 0 else 0.0
    
    vol_remaining = vol_5m * math.sqrt(time_ratio)
    
    if vol_remaining < 1e-10:
        return 1.0 if log_offset > 0 else 0.0
    
    # z-score
    z = log_offset / vol_remaining
    
    # 标准正态分布 CDF (用 erf 近似)
    prob = 0.5 * (1 + math.erf(z / math.sqrt(2)))
    
    return prob

# ================================================================
# 5. 逐窗口分析: BTC偏移 vs 市场定价 vs 交易行为
# ================================================================
print("\n" + "=" * 150)
print("BTC偏移量 vs 市场定价溢价分析")
print("=" * 150)

all_trade_analysis = []
window_summaries = []

for slug in sorted(events.keys()):
    etrades = [t for t in events[slug] if t["price"] > 0 and t["outcome"] in ("Up", "Down")]
    if not etrades:
        continue
    
    window_start = int(slug.split("-")[-1])
    window_end = window_start + 300
    dt = datetime.fromtimestamp(window_start, tz=timezone.utc)
    
    # 获取基准价 (窗口开始时的BTC价格)
    btc_base = get_btc_price_at(window_start)
    if btc_base is None:
        print(f"\n{dt.strftime('%H:%M')} — 无法获取基准价, 跳过")
        continue
    
    # 获取窗口结束价 (确定实际结果)
    btc_end = get_btc_price_at(window_end)
    actual_winner = None
    if btc_end:
        if btc_end > btc_base:
            actual_winner = "Up"
        elif btc_end < btc_base:
            actual_winner = "Down"
        else:
            actual_winner = "Tie"
    
    print(f"\n{'─'*150}")
    btc_end_str = f"${btc_end:,.2f}" if btc_end else "N/A"
    btc_move_str = f"${(btc_end-btc_base):+.2f}" if btc_end else "N/A"
    print(f"窗口: {dt.strftime('%Y-%m-%d %H:%M')} │ BTC基准: ${btc_base:,.2f} │ BTC结束: {btc_end_str} │ 偏移: {btc_move_str} │ 结果: {actual_winner}")
    print(f"{'秒':>4} {'方向':>4} {'付价':>6} │ {'BTC现价':>10} {'偏移$':>8} {'偏移%':>8} │ {'剩余s':>4} {'理论Up':>7} {'理论Dn':>7} │ {'溢价':>8} {'方向正确':>8}")
    
    window_trades = sorted(etrades, key=lambda t: t["timestamp"])
    
    # 只取窗口内的交易
    window_trades = [t for t in window_trades if window_start <= t["timestamp"] <= window_end]
    
    premiums = []  # (premium_pct, usdc_size, direction_correct)
    
    for t in window_trades:
        ts = t["timestamp"]
        offset_s = ts - window_start
        remaining = window_end - ts
        outcome = t["outcome"]
        paid_price = t["price"]
        usdc = float(t.get("usdcSize", 0))
        
        btc_now = get_btc_price_at(ts)
        if btc_now is None:
            continue
        
        btc_offset_usd = btc_now - btc_base
        btc_offset_pct = (btc_now - btc_base) / btc_base * 100
        
        # 理论概率
        theo_up = theoretical_up_prob(btc_now, btc_base, remaining, vol_5m)
        theo_dn = 1 - theo_up
        
        # 该交易的理论公允价格
        if outcome == "Up":
            theo_price = theo_up
        else:
            theo_price = theo_dn
        
        # 溢价 = 理论价 - 付出价 (正数=买便宜了, 有利)
        premium = theo_price - paid_price
        premium_pct = premium * 100
        
        # 方向是否与偏移一致 (BTC涨了→买Up, BTC跌了→买Down)
        offset_aligned = False
        if btc_offset_usd > 0 and outcome == "Up":
            offset_aligned = True
        elif btc_offset_usd < 0 and outcome == "Down":
            offset_aligned = True
        elif abs(btc_offset_usd) < 1:
            offset_aligned = None  # 中性
        
        premiums.append({
            "offset_s": offset_s,
            "outcome": outcome,
            "paid": paid_price,
            "btc_now": btc_now,
            "btc_offset_usd": btc_offset_usd,
            "btc_offset_pct": btc_offset_pct,
            "remaining": remaining,
            "theo_up": theo_up,
            "theo_price": theo_price,
            "premium": premium,
            "premium_pct": premium_pct,
            "usdc": usdc,
            "offset_aligned": offset_aligned
        })
        
        all_trade_analysis.append(premiums[-1])
    
    # 打印采样 (每个窗口前5, 中间5, 后5)
    if premiums:
        sample = premiums[:5] + premiums[len(premiums)//2-2:len(premiums)//2+3] + premiums[-5:]
        for p in sample:
            align_str = "Y" if p["offset_aligned"] else ("-" if p["offset_aligned"] is None else "N")
            print(f"{p['offset_s']:>4} {p['outcome']:>4} {p['paid']:>6.3f} │ "
                  f"${p['btc_now']:>10,.2f} {p['btc_offset_usd']:>+8.2f} {p['btc_offset_pct']:>+7.3f}% │ "
                  f"{p['remaining']:>4} {p['theo_up']:>7.4f} {1-p['theo_up']:>7.4f} │ "
                  f"{p['premium_pct']:>+7.2f}% {align_str:>8}")
        
        # 窗口汇总
        up_trades = [p for p in premiums if p["outcome"] == "Up"]
        dn_trades = [p for p in premiums if p["outcome"] == "Down"]
        
        avg_premium_up = sum(p["premium_pct"] for p in up_trades)/len(up_trades) if up_trades else 0
        avg_premium_dn = sum(p["premium_pct"] for p in dn_trades)/len(dn_trades) if dn_trades else 0
        
        usdc_up = sum(p["usdc"] for p in up_trades)
        usdc_dn = sum(p["usdc"] for p in dn_trades)
        
        # USDC加权平均溢价
        wt_premium_up = sum(p["premium_pct"]*p["usdc"] for p in up_trades)/usdc_up if usdc_up > 0 else 0
        wt_premium_dn = sum(p["premium_pct"]*p["usdc"] for p in dn_trades)/usdc_dn if usdc_dn > 0 else 0
        
        # 偏移方向一致率
        aligned = [p for p in premiums if p["offset_aligned"] is True]
        misaligned = [p for p in premiums if p["offset_aligned"] is False]
        align_rate = len(aligned) / (len(aligned)+len(misaligned)) * 100 if (len(aligned)+len(misaligned)) > 0 else 50
        
        # 按偏移量分组: 偏移>0时买Up的比例, 偏移<0时买Down的比例
        btc_up_trades = [p for p in premiums if p["btc_offset_usd"] > 5]  # BTC涨了>$5
        btc_dn_trades = [p for p in premiums if p["btc_offset_usd"] < -5]  # BTC跌了>$5
        
        buy_up_when_btc_up = len([p for p in btc_up_trades if p["outcome"] == "Up"])
        buy_dn_when_btc_dn = len([p for p in btc_dn_trades if p["outcome"] == "Down"])
        
        print(f"\n  汇总: Up笔数={len(up_trades)}, Dn笔数={len(dn_trades)} │ "
              f"Up均溢价={avg_premium_up:+.2f}%, Dn均溢价={avg_premium_dn:+.2f}% │ "
              f"USDC加权溢价: Up={wt_premium_up:+.2f}%, Dn={wt_premium_dn:+.2f}%")
        up_when_up_str = f"{buy_up_when_btc_up/len(btc_up_trades)*100:.0f}%" if btc_up_trades else "N/A"
        dn_when_dn_str = f"{buy_dn_when_btc_dn/len(btc_dn_trades)*100:.0f}%" if btc_dn_trades else "N/A"
        print(f"  偏移方向一致率: {align_rate:.1f}% │ "
              f"BTC涨>$5时: {len(btc_up_trades)}笔, 其中买Up={buy_up_when_btc_up} ({up_when_up_str}) │ "
              f"BTC跌>$5时: {len(btc_dn_trades)}笔, 其中买Dn={buy_dn_when_btc_dn} ({dn_when_dn_str})")
        
        window_summaries.append({
            "dt": dt.strftime('%H:%M'),
            "btc_base": btc_base,
            "btc_end": btc_end,
            "actual_winner": actual_winner,
            "n_trades": len(premiums),
            "avg_premium_up": avg_premium_up,
            "avg_premium_dn": avg_premium_dn,
            "wt_premium_up": wt_premium_up,
            "wt_premium_dn": wt_premium_dn,
            "align_rate": align_rate,
            "usdc_up": usdc_up,
            "usdc_dn": usdc_dn,
        })

# ================================================================
# 6. 全局统计汇总
# ================================================================
print("\n\n" + "=" * 150)
print("全局统计汇总")
print("=" * 150)

if all_trade_analysis:
    # 6a. 溢价分布
    all_prems = [p["premium_pct"] for p in all_trade_analysis]
    print(f"\n--- 溢价分布 (理论价 - 付出价, 正数=买便宜了) ---")
    print(f"  总交易数: {len(all_prems)}")
    print(f"  均值: {sum(all_prems)/len(all_prems):+.3f}%")
    sorted_prems = sorted(all_prems)
    print(f"  中位数: {sorted_prems[len(sorted_prems)//2]:+.3f}%")
    print(f"  P25: {sorted_prems[len(sorted_prems)//4]:+.3f}%")
    print(f"  P75: {sorted_prems[3*len(sorted_prems)//4]:+.3f}%")
    pos_prems = [p for p in all_prems if p > 0]
    neg_prems = [p for p in all_prems if p < 0]
    print(f"  正溢价 (买便宜了): {len(pos_prems)} ({len(pos_prems)/len(all_prems)*100:.1f}%)")
    print(f"  负溢价 (买贵了):   {len(neg_prems)} ({len(neg_prems)/len(all_prems)*100:.1f}%)")
    
    # 6b. 按方向
    up_prems = [p for p in all_trade_analysis if p["outcome"] == "Up"]
    dn_prems = [p for p in all_trade_analysis if p["outcome"] == "Down"]
    print(f"\n--- 按方向分溢价 ---")
    if up_prems:
        avg_up = sum(p["premium_pct"] for p in up_prems)/len(up_prems)
        wt_up = sum(p["premium_pct"]*p["usdc"] for p in up_prems)/sum(p["usdc"] for p in up_prems) if sum(p["usdc"] for p in up_prems) > 0 else 0
        print(f"  Up: n={len(up_prems)}, 均溢价={avg_up:+.3f}%, USDC加权={wt_up:+.3f}%")
    if dn_prems:
        avg_dn = sum(p["premium_pct"] for p in dn_prems)/len(dn_prems)
        wt_dn = sum(p["premium_pct"]*p["usdc"] for p in dn_prems)/sum(p["usdc"] for p in dn_prems) if sum(p["usdc"] for p in dn_prems) > 0 else 0
        print(f"  Dn: n={len(dn_prems)}, 均溢价={avg_dn:+.3f}%, USDC加权={wt_dn:+.3f}%")
    
    # 6c. 按BTC偏移量分桶
    print(f"\n--- 按BTC偏移量分桶 ---")
    buckets = [(-9999, -50), (-50, -20), (-20, -5), (-5, 5), (5, 20), (20, 50), (50, 9999)]
    bucket_labels = ["<-$50", "-$50~-$20", "-$20~-$5", "-$5~+$5", "+$5~+$20", "+$20~+$50", ">+$50"]
    
    for (lo, hi), label in zip(buckets, bucket_labels):
        bucket = [p for p in all_trade_analysis if lo <= p["btc_offset_usd"] < hi]
        if not bucket:
            continue
        up_count = len([p for p in bucket if p["outcome"] == "Up"])
        dn_count = len([p for p in bucket if p["outcome"] == "Down"])
        avg_prem = sum(p["premium_pct"] for p in bucket) / len(bucket)
        up_pct = up_count / len(bucket) * 100
        avg_theo_up = sum(p["theo_up"] for p in bucket) / len(bucket)
        avg_paid_up = sum(p["paid"] for p in bucket if p["outcome"] == "Up") / up_count if up_count > 0 else 0
        avg_paid_dn = sum(p["paid"] for p in bucket if p["outcome"] == "Down") / dn_count if dn_count > 0 else 0
        print(f"  {label:>12}: n={len(bucket):>4} │ Up买={up_pct:>5.1f}% │ 均溢价={avg_prem:>+6.2f}% │ "
              f"理论Up={avg_theo_up:.3f}, 实付Up={avg_paid_up:.3f} Dn={avg_paid_dn:.3f}")
    
    # 6d. 偏移方向一致率
    aligned = [p for p in all_trade_analysis if p["offset_aligned"] is True]
    misaligned = [p for p in all_trade_analysis if p["offset_aligned"] is False]
    neutral = [p for p in all_trade_analysis if p["offset_aligned"] is None]
    total_dir = len(aligned) + len(misaligned)
    print(f"\n--- BTC偏移方向 vs 下单方向一致性 ---")
    print(f"  一致 (BTC涨→买Up, BTC跌→买Down): {len(aligned)} ({len(aligned)/total_dir*100:.1f}%)" if total_dir > 0 else "  无数据")
    print(f"  不一致: {len(misaligned)} ({len(misaligned)/total_dir*100:.1f}%)" if total_dir > 0 else "")
    print(f"  中性 (BTC偏移<$1): {len(neutral)}")
    
    # 6e. 正溢价时的下单特征
    high_prem = [p for p in all_trade_analysis if p["premium_pct"] > 5]  # 溢价>5%
    low_prem = [p for p in all_trade_analysis if p["premium_pct"] < -5]  # 溢价<-5%
    print(f"\n--- 极端溢价分析 ---")
    if high_prem:
        usdc_high = sum(p["usdc"] for p in high_prem)
        print(f"  高正溢价 (>+5%, 买便宜了): {len(high_prem)} 笔, 总${usdc_high:,.0f}")
        print(f"    均偏移: ${sum(p['btc_offset_usd'] for p in high_prem)/len(high_prem):+.1f}")
        print(f"    Up占比: {len([p for p in high_prem if p['outcome']=='Up'])/len(high_prem)*100:.1f}%")
    if low_prem:
        usdc_low = sum(p["usdc"] for p in low_prem)
        print(f"  高负溢价 (<-5%, 买贵了): {len(low_prem)} 笔, 总${usdc_low:,.0f}")
        print(f"    均偏移: ${sum(p['btc_offset_usd'] for p in low_prem)/len(low_prem):+.1f}")
        print(f"    Up占比: {len([p for p in low_prem if p['outcome']=='Up'])/len(low_prem)*100:.1f}%")

    # 6f. 时间维度: 溢价随窗口内时间的变化
    print(f"\n--- 溢价随窗口内时间变化 ---")
    time_buckets = [(0, 30), (30, 60), (60, 120), (120, 180), (180, 240), (240, 300)]
    time_labels = ["0-30s", "30-60s", "60-120s", "120-180s", "180-240s", "240-300s"]
    for (lo, hi), label in zip(time_buckets, time_labels):
        bucket = [p for p in all_trade_analysis if lo <= p["offset_s"] < hi]
        if not bucket:
            continue
        avg_prem = sum(p["premium_pct"] for p in bucket) / len(bucket)
        avg_abs_offset = sum(abs(p["btc_offset_usd"]) for p in bucket) / len(bucket)
        pos_rate = len([p for p in bucket if p["premium_pct"] > 0]) / len(bucket) * 100
        print(f"  {label:>10}: n={len(bucket):>4} │ 均溢价={avg_prem:>+6.2f}% │ 正溢价率={pos_rate:>5.1f}% │ BTC均偏移=${avg_abs_offset:>6.1f}")

    # 6g. 关键问题: 他是否在溢价高时下注更多?
    print(f"\n--- 溢价 vs 下单金额相关性 ---")
    # 分溢价区间看平均USDC
    prem_ranges = [(-100, -10), (-10, -5), (-5, 0), (0, 5), (5, 10), (10, 100)]
    prem_labels = ["<-10%", "-10~-5%", "-5~0%", "0~+5%", "+5~+10%", ">+10%"]
    for (lo, hi), label in zip(prem_ranges, prem_labels):
        bucket = [p for p in all_trade_analysis if lo <= p["premium_pct"] < hi]
        if not bucket:
            continue
        avg_usdc = sum(p["usdc"] for p in bucket) / len(bucket)
        total_usdc = sum(p["usdc"] for p in bucket)
        print(f"  溢价 {label:>8}: n={len(bucket):>4} │ 均单=${ avg_usdc:>5.1f} │ 总$={total_usdc:>8,.0f}")

# ================================================================
# 7. 窗口级汇总表
# ================================================================
print(f"\n\n{'='*150}")
print("窗口级溢价汇总")
print(f"{'='*150}")
print(f"{'时间':>8} {'BTC基准':>10} {'BTC终':>10} {'偏移$':>8} {'胜方':>4} │ "
      f"{'Up均溢':>7} {'Dn均溢':>7} {'Up加权溢':>8} {'Dn加权溢':>8} │ "
      f"{'偏移一致%':>9} {'更重侧':>6}")

for ws in window_summaries:
    btc_move = ws["btc_end"] - ws["btc_base"] if ws["btc_end"] else 0
    heavier = "Up" if ws["usdc_up"] > ws["usdc_dn"] else "Down"
    correct_heavier = "Y" if heavier == ws["actual_winner"] else "N"
    
    print(f"{ws['dt']:>8} ${ws['btc_base']:>9,.0f} ${ws['btc_end']:>9,.0f} {btc_move:>+8.1f} {ws['actual_winner']:>4} │ "
          f"{ws['avg_premium_up']:>+6.2f}% {ws['avg_premium_dn']:>+6.2f}% {ws['wt_premium_up']:>+7.2f}% {ws['wt_premium_dn']:>+7.2f}% │ "
          f"{ws['align_rate']:>8.1f}% {heavier:>4}{correct_heavier}")

# ================================================================
# 8. 最终结论
# ================================================================
print(f"\n\n{'='*150}")
print("结论分析")
print(f"{'='*150}")

if all_trade_analysis:
    overall_avg_prem = sum(p["premium_pct"] for p in all_trade_analysis) / len(all_trade_analysis)
    overall_pos_rate = len([p for p in all_trade_analysis if p["premium_pct"] > 0]) / len(all_trade_analysis) * 100
    
    aligned_count = len([p for p in all_trade_analysis if p["offset_aligned"] is True])
    dir_total = len([p for p in all_trade_analysis if p["offset_aligned"] is not None])
    align_pct = aligned_count / dir_total * 100 if dir_total > 0 else 50
    
    print(f"""
  整体均溢价: {overall_avg_prem:+.3f}% (正=买便宜了)
  正溢价率: {overall_pos_rate:.1f}%
  偏移方向一致率: {align_pct:.1f}%

  如果均溢价显著为正 → 他确实在系统性地买到便宜货 (市场定价跟不上BTC偏移)
  如果偏移一致率 >> 50% → 他在BTC涨时偏向买Up, 跌时偏向买Down (利用价格信息)
  如果正溢价率 >> 50% → 他的大多数交易都有正期望

  关键: combined_price < 1.0 的边际来源是否可以用BTC偏移溢价解释?
""")
