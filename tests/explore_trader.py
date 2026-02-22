"""
探索 Polymarket 交易者 0x1d0034134e 的活动数据
"""
import requests
import json
import sys

PROFILE_SLUG = "0x1d0034134e"  # from URL @0x1d0034134e

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
}

# --- 1. 尝试拿 profile 信息 (获取完整地址) ---
print("=" * 60)
print("1. Profile lookup")
print("=" * 60)

# 尝试多种 API 路径
urls_to_try = [
    f"https://polymarket.com/api/profile/{PROFILE_SLUG}",
    f"https://gamma-api.polymarket.com/profiles/{PROFILE_SLUG}",
    f"https://data-api.polymarket.com/profiles/{PROFILE_SLUG}",
    f"https://polymarket.com/api/users/{PROFILE_SLUG}",
]

profile_data = None
for url in urls_to_try:
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        print(f"  {url} => {r.status_code}")
        if r.status_code == 200:
            data = r.json()
            print(f"  Response keys: {list(data.keys()) if isinstance(data, dict) else type(data)}")
            print(f"  Data (first 500 chars): {json.dumps(data, ensure_ascii=False)[:500]}")
            profile_data = data
            break
    except Exception as e:
        print(f"  {url} => ERROR: {e}")

# --- 2. 尝试 activity / trades 端点 ---
print("\n" + "=" * 60)
print("2. Activity / trades lookup")
print("=" * 60)

activity_urls = [
    f"https://data-api.polymarket.com/activity/{PROFILE_SLUG}?limit=50",
    f"https://data-api.polymarket.com/activity?address={PROFILE_SLUG}&limit=50",
    f"https://data-api.polymarket.com/trades?address={PROFILE_SLUG}&limit=50",
    f"https://clob.polymarket.com/trades?maker_address={PROFILE_SLUG}&limit=50",
    f"https://clob.polymarket.com/trades?taker={PROFILE_SLUG}&limit=50",
]

for url in activity_urls:
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        print(f"  {url} => {r.status_code}")
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, list):
                print(f"  Got {len(data)} items")
                if data:
                    print(f"  First item keys: {list(data[0].keys())}")
                    print(f"  First item: {json.dumps(data[0], ensure_ascii=False)[:500]}")
            elif isinstance(data, dict):
                print(f"  Keys: {list(data.keys())}")
                print(f"  Data (500): {json.dumps(data, ensure_ascii=False)[:500]}")
    except Exception as e:
        print(f"  {url} => ERROR: {e}")

# --- 3. 尝试 positions 端点 ---
print("\n" + "=" * 60)
print("3. Positions lookup")
print("=" * 60)

pos_urls = [
    f"https://data-api.polymarket.com/positions?user={PROFILE_SLUG}",
    f"https://gamma-api.polymarket.com/positions?user={PROFILE_SLUG}",
]

for url in pos_urls:
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        print(f"  {url} => {r.status_code}")
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, list):
                print(f"  Got {len(data)} items")
                if data:
                    print(f"  First: {json.dumps(data[0], ensure_ascii=False)[:500]}")
            else:
                print(f"  Data (500): {json.dumps(data, ensure_ascii=False)[:500]}")
    except Exception as e:
        print(f"  {url} => ERROR: {e}")

print("\nDone.")
