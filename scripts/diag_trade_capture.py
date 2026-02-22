"""
诊断交易快照捕获率
===================
直接调用 Activity API, 对比 DB 中的 trade_snaps,
找出哪些交易被丢弃以及为什么。

用法:
  python scripts/diag_trade_capture.py              # 对比 API vs DB
  python scripts/diag_trade_capture.py --api-only    # 仅查看 API 返回内容
"""

import json
import sys
import sqlite3
import time
from pathlib import Path
from datetime import datetime, timezone
from collections import Counter, defaultdict

# Windows GBK 编码兼容
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

try:
    import requests
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "requests"])
    import requests

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "data" / "0x1d_data.db"
ADDR = "0x1d0034134e339a309700ff2d34e99fa2d48b0313"
ACTIVITY_API = "https://data-api.polymarket.com/activity"
SLUG_PREFIX = "btc-updown-5m-"

def fetch_all_activity(limit=200):
    """获取 Activity API 最近的交易"""
    url = f"{ACTIVITY_API}?user={ADDR}&limit={limit}"
    print(f"请求: {url}")
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    trades = r.json()
    print(f"API 返回: {len(trades)} 条\n")
    return trades

def analyze_api_response(trades):
    """分析 API 返回的字段和 slug 分布"""
    print("=" * 72)
    print("  Activity API 响应分析")
    print("=" * 72)

    if not trades:
        print("  (无数据)")
        return

    # 显示第一条的完整字段
    sample = trades[0]
    print(f"\n样本 (第1条) 的所有字段:")
    for k, v in sample.items():
        val_str = str(v)
        if len(val_str) > 80:
            val_str = val_str[:80] + "..."
        print(f"  {k:<25} = {val_str}")

    # eventSlug vs slug 字段分析
    print(f"\n字段存在性:")
    has_event_slug = sum(1 for t in trades if t.get("eventSlug"))
    has_slug = sum(1 for t in trades if t.get("slug"))
    has_tx_hash = sum(1 for t in trades if t.get("transactionHash"))
    print(f"  eventSlug 非空: {has_event_slug}/{len(trades)} ({has_event_slug/len(trades)*100:.1f}%)")
    print(f"  slug 非空:      {has_slug}/{len(trades)} ({has_slug/len(trades)*100:.1f}%)")
    print(f"  transactionHash 非空: {has_tx_hash}/{len(trades)} ({has_tx_hash/len(trades)*100:.1f}%)")

    # slug 分布
    slug_counter = Counter()
    for t in trades:
        s = t.get("eventSlug", "") or t.get("slug", "")
        if SLUG_PREFIX in s:
            slug_counter["BTC 5-min"] += 1
        elif s:
            # 截取前40字符作为分类
            slug_counter[s[:40]] += 1
        else:
            slug_counter["(空)"] += 1

    print(f"\n按 eventSlug 分类 (前 15):")
    for slug, cnt in slug_counter.most_common(15):
        print(f"  {cnt:>4}x  {slug}")

    btc_count = slug_counter.get("BTC 5-min", 0)
    other_count = len(trades) - btc_count
    print(f"\n总计: BTC 5-min = {btc_count}, 其他市场 = {other_count}")

    # BTC 5-min 交易的详细时间分布
    btc_trades = [t for t in trades if SLUG_PREFIX in (t.get("eventSlug", "") or t.get("slug", ""))]
    if btc_trades:
        print(f"\nBTC 5-min 交易时间分布:")
        windows = defaultdict(list)
        for t in btc_trades:
            s = t.get("eventSlug", "") or t.get("slug", "")
            ts = int(t.get("timestamp", 0))
            windows[s].append({
                "ts": ts,
                "side": t.get("outcome", ""),
                "shares": float(t.get("size", 0)),
                "usdc": float(t.get("usdcSize", 0)),
                "tx": t.get("transactionHash", "")[:16],
            })

        for slug in sorted(windows.keys()):
            trades_in = windows[slug]
            ts_list = [t["ts"] for t in trades_in]
            t_min = datetime.fromtimestamp(min(ts_list), tz=timezone.utc).strftime("%H:%M:%S")
            t_max = datetime.fromtimestamp(max(ts_list), tz=timezone.utc).strftime("%H:%M:%S")
            up_cnt = sum(1 for t in trades_in if "up" in t["side"].lower())
            dn_cnt = sum(1 for t in trades_in if "down" in t["side"].lower())
            total_usdc = sum(t["usdc"] for t in trades_in)
            # 检查 transactionHash 唯一性
            unique_tx = len(set(t["tx"] for t in trades_in))
            print(f"  {slug[-15:]}: {len(trades_in):>3} 笔 (UP={up_cnt}, DN={dn_cnt}) "
                  f"${total_usdc:.0f} | {t_min}-{t_max} | {unique_tx} 唯一tx")

    return btc_trades


def compare_with_db(btc_trades):
    """对比 API 交易和 DB 中的 trade_snaps"""
    print("\n" + "=" * 72)
    print("  DB 对比分析")
    print("=" * 72)

    if not DB_PATH.exists():
        print(f"  DB 不存在: {DB_PATH}")
        return

    conn = sqlite3.connect(str(DB_PATH))
    db_snaps = conn.execute(
        "SELECT tx_hash, ts, slug, side, shares, price, usdc FROM trade_snaps ORDER BY ts"
    ).fetchall()
    db_settlements = conn.execute(
        "SELECT slug, won, pnl, trades FROM settlements ORDER BY settled_at"
    ).fetchall()
    conn.close()

    print(f"\n  DB trade_snaps: {len(db_snaps)} 条")
    print(f"  DB settlements: {len(db_settlements)} 条")

    # DB 中的 tx_hash 集合
    db_tx_set = set(r[0] for r in db_snaps)

    # DB 中的 slug 分布
    db_slug_counter = Counter(r[2] for r in db_snaps)
    print(f"\n  DB 中各窗口交易数:")
    for slug, cnt in sorted(db_slug_counter.items()):
        print(f"    {slug[-15:]}: {cnt} 笔")

    # 对比: API 中有但 DB 中没有的交易
    if btc_trades:
        api_tx_set = set()
        missing = []
        for t in btc_trades:
            tx_key = t.get("transactionHash", "") + "_" + str(t.get("outcomeIndex", 0))
            api_tx_set.add(tx_key)
            if tx_key not in db_tx_set:
                missing.append(t)

        print(f"\n  API BTC交易数: {len(btc_trades)}")
        print(f"  DB中已有: {len(btc_trades) - len(missing)}")
        print(f"  DB中缺失: {len(missing)}")

        if missing:
            print(f"\n  缺失交易详情 (前 20):")
            for t in missing[:20]:
                s = t.get("eventSlug", "") or t.get("slug", "")
                ts = int(t.get("timestamp", 0))
                time_str = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%H:%M:%S")
                side = t.get("outcome", "")
                shares = float(t.get("size", 0))
                usdc = float(t.get("usdcSize", 0))
                tx = t.get("transactionHash", "")[:20]
                print(f"    {time_str} {side:>4} {shares:>8.1f}sh ${usdc:>6.1f} slug={s[-20:]} tx={tx}...")

    # 结算分析
    if db_settlements:
        print(f"\n  结算记录:")
        total_pnl = 0
        for slug, won, pnl, trades in db_settlements:
            total_pnl += pnl
            print(f"    {slug[-15:]}: {won}赢 PnL=${pnl:+.2f} ({trades}笔)")
        print(f"    总 PnL: ${total_pnl:+.2f}")


def check_monitor_state():
    """检查 monitor 可能的 slug 匹配问题"""
    print("\n" + "=" * 72)
    print("  Slug 匹配检查")
    print("=" * 72)

    now = int(time.time())
    ws = now - (now % 300)
    expected_slug = f"{SLUG_PREFIX}{ws}"
    print(f"\n  当前时间: {datetime.fromtimestamp(now, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print(f"  预期窗口 slug: {expected_slug}")

    # 从 API 获取一条最新 BTC 交易
    url = f"{ACTIVITY_API}?user={ADDR}&limit=50"
    r = requests.get(url, timeout=10)
    trades = r.json()

    btc_slugs = set()
    for t in trades:
        s = t.get("eventSlug", "") or t.get("slug", "")
        if SLUG_PREFIX in s:
            btc_slugs.add(s)

    if btc_slugs:
        print(f"\n  API 中实际 BTC slug (最近):")
        for s in sorted(btc_slugs):
            match = "✓ 匹配" if s == expected_slug else "✗ 不匹配"
            print(f"    {s}  {match}")
    else:
        print(f"\n  ⚠ API 最近 50 条中无 BTC 5-min 交易!")

    # 检查 slug 构造逻辑
    for s in btc_slugs:
        # 尝试解析 slug 中的时间戳
        try:
            ts_str = s.replace(SLUG_PREFIX, "")
            ts = int(ts_str)
            t_str = datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%H:%M:%S')
            aligned = ts % 300 == 0
            print(f"    → 时间戳 {ts} = {t_str} UTC, 对齐300s: {'✓' if aligned else '✗'}")
        except:
            print(f"    → 无法解析 slug 时间戳: {s}")


if __name__ == "__main__":
    api_only = "--api-only" in sys.argv

    trades = fetch_all_activity()
    btc_trades = analyze_api_response(trades)

    if not api_only:
        compare_with_db(btc_trades or [])

    check_monitor_state()

    # 总结
    print("\n" + "=" * 72)
    print("  常见丢交易原因:")
    print("=" * 72)
    print("""
  1. eventSlug 为空 → slug 匹配失败, 交易被静默丢弃
  2. 窗口切换延迟 → 新窗口前 1-2s 的交易被丢弃
  3. Activity API limit=200 不够 → 高频交易期间部分交易被覆盖
  4. 0x1d 也交易其他市场 → API 返回混合结果
  5. seen_txs 清理 (>5000) → 短暂重复处理但 DB 去重
  6. 特征中 btc_price=0 → 快照未保存
""")
