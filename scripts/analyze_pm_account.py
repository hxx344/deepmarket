"""分析 Polymarket 账号 0x1d0034134e 的交易策略"""
import httpx
import json
from collections import defaultdict
from datetime import datetime, timezone

ADDR = "0x1d0034134e339a309700ff2d34e99fa2d48b0313"

def fetch_all_trades():
    client = httpx.Client(timeout=15, follow_redirects=True)
    all_trades = []
    cursor = None
    for page in range(20):
        url = f"https://data-api.polymarket.com/activity?user={ADDR}&limit=100"
        if cursor:
            url += f"&cursor={cursor}"
        r = client.get(url)
        if r.status_code != 200:
            break
        data = r.json()
        if not data:
            break
        all_trades.extend(data)
        cursor = data[-1]["timestamp"]
        print(f"Page {page+1}: {len(data)} trades, cursor={cursor}")
        if len(data) < 100:
            break
    return all_trades

def analyze(trades):
    print(f"\n{'='*60}")
    print(f"Account: {ADDR}")
    print(f"Total trades: {len(trades)}")
    
    # Group by market
    markets = defaultdict(list)
    for t in trades:
        markets[t.get("eventSlug", "unknown")].append(t)
    
    print(f"Unique markets: {len(markets)}")
    
    # Overall stats
    total_usdc = sum(t["usdcSize"] for t in trades)
    buy_trades = [t for t in trades if t["side"] == "BUY"]
    sell_trades = [t for t in trades if t["side"] == "SELL"]
    print(f"Total USDC volume: ${total_usdc:.2f}")
    print(f"BUY trades: {len(buy_trades)}, SELL trades: {len(sell_trades)}")
    
    # Price distribution
    prices = [t["price"] for t in trades]
    print(f"Price range: {min(prices):.2f} - {max(prices):.2f}")
    print(f"Avg price: {sum(prices)/len(prices):.4f}")
    
    print(f"\n{'='*60}")
    print("PER-MARKET ANALYSIS (top 10 by trades)")
    print(f"{'='*60}")
    
    dual_side_markets = 0
    same_time_pairs = 0
    total_market_count = 0
    
    all_market_results = []
    
    for slug, mtrades in sorted(markets.items(), key=lambda x: -len(x[1])):
        total_market_count += 1
        by_outcome = defaultdict(list)
        for t in mtrades:
            by_outcome[t["outcome"]].append(t)
        
        has_up = len(by_outcome.get("Up", [])) > 0
        has_dn = len(by_outcome.get("Down", [])) > 0
        is_dual = has_up and has_dn
        if is_dual:
            dual_side_markets += 1
        
        # Check simultaneous entry (within 10s)
        up_entries = sorted(by_outcome.get("Up", []), key=lambda x: x["timestamp"])
        dn_entries = sorted(by_outcome.get("Down", []), key=lambda x: x["timestamp"])
        
        paired = 0
        if up_entries and dn_entries:
            for ut in up_entries:
                for dt in dn_entries:
                    if abs(ut["timestamp"] - dt["timestamp"]) <= 10:
                        paired += 1
                        break
            if paired > 0:
                same_time_pairs += 1
        
        up_usdc = sum(t["usdcSize"] for t in by_outcome.get("Up", []))
        dn_usdc = sum(t["usdcSize"] for t in by_outcome.get("Down", []))
        total_market_usdc = up_usdc + dn_usdc
        
        up_prices = [t["price"] for t in by_outcome.get("Up", [])]
        dn_prices = [t["price"] for t in by_outcome.get("Down", [])]
        
        market_info = {
            "slug": slug,
            "trades": len(mtrades),
            "dual_side": is_dual,
            "up_usdc": up_usdc,
            "dn_usdc": dn_usdc,
            "total_usdc": total_market_usdc,
            "up_prices": up_prices,
            "dn_prices": dn_prices,
            "up_count": len(up_entries),
            "dn_count": len(dn_entries),
            "paired": paired,
            "up_ratio": up_usdc / total_market_usdc if total_market_usdc > 0 else 0,
        }
        all_market_results.append(market_info)
    
    # Print top markets
    for m in all_market_results[:10]:
        title = m["slug"]
        print(f"\n--- {title} ({m['trades']} trades) ---")
        print(f"  Dual-side: {'YES' if m['dual_side'] else 'NO'}")
        print(f"  Up: {m['up_count']} trades, total USDC=${m['up_usdc']:.2f}, prices={m['up_prices'][:5]}")
        print(f"  Down: {m['dn_count']} trades, total USDC=${m['dn_usdc']:.2f}, prices={m['dn_prices'][:5]}")
        print(f"  Up/Total ratio: {m['up_ratio']:.1%}")
        if m["paired"]:
            print(f"  Simultaneous pairs (within 10s): {m['paired']}")
    
    # Summary statistics
    print(f"\n{'='*60}")
    print("STRATEGY SUMMARY")
    print(f"{'='*60}")
    print(f"Total markets traded: {total_market_count}")
    print(f"Dual-side markets (both Up+Down): {dual_side_markets}/{total_market_count} ({dual_side_markets/total_market_count*100:.0f}%)")
    print(f"Simultaneous entry markets (within 10s): {same_time_pairs}/{dual_side_markets}")
    
    # USDC size distribution
    usdc_sizes = [t["usdcSize"] for t in trades]
    usdc_sizes.sort()
    print(f"\nOrder size distribution:")
    print(f"  Min: ${min(usdc_sizes):.2f}")
    print(f"  25th pct: ${usdc_sizes[len(usdc_sizes)//4]:.2f}")
    print(f"  Median: ${usdc_sizes[len(usdc_sizes)//2]:.2f}")
    print(f"  75th pct: ${usdc_sizes[3*len(usdc_sizes)//4]:.2f}")
    print(f"  Max: ${max(usdc_sizes):.2f}")
    print(f"  Mean: ${sum(usdc_sizes)/len(usdc_sizes):.2f}")
    
    # Bias analysis for dual-side markets
    ratios = [m["up_ratio"] for m in all_market_results if m["dual_side"]]
    if ratios:
        print(f"\nUp/Total bias in dual-side markets:")
        print(f"  Min: {min(ratios):.1%}")
        print(f"  Max: {max(ratios):.1%}")
        print(f"  Mean: {sum(ratios)/len(ratios):.1%}")
        # Distribution
        buckets = {"<30%": 0, "30-40%": 0, "40-50%": 0, "50-60%": 0, "60-70%": 0, ">70%": 0}
        for r in ratios:
            if r < 0.30: buckets["<30%"] += 1
            elif r < 0.40: buckets["30-40%"] += 1
            elif r < 0.50: buckets["40-50%"] += 1
            elif r < 0.60: buckets["50-60%"] += 1
            elif r < 0.70: buckets["60-70%"] += 1
            else: buckets[">70%"] += 1
        print(f"  Distribution: {dict(buckets)}")
    
    # Time pattern analysis
    print(f"\nTrade timing analysis:")
    for m in all_market_results[:5]:
        slug = m["slug"]
        mtrades_sorted = sorted(
            [t for t in trades if t.get("eventSlug") == slug],
            key=lambda x: x["timestamp"]
        )
        if len(mtrades_sorted) >= 2:
            ts_list = [t["timestamp"] for t in mtrades_sorted]
            gaps = [ts_list[i+1] - ts_list[i] for i in range(len(ts_list)-1)]
            outcomes = [t["outcome"] for t in mtrades_sorted]
            sizes = [f"${t['usdcSize']:.0f}" for t in mtrades_sorted]
            print(f"\n  {slug}:")
            print(f"    Sequence: {list(zip(outcomes, sizes))[:8]}")
            print(f"    Time gaps(s): {gaps[:8]}")
    
    return all_market_results

if __name__ == "__main__":
    trades = fetch_all_trades()
    # Save raw data
    with open("docs/pm_account_0x1d_raw_trades.json", "w") as f:
        json.dump(trades, f, indent=2)
    print(f"Saved {len(trades)} trades to docs/pm_account_0x1d_raw_trades.json")
    
    results = analyze(trades)
