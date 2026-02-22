"""直接在 CLOB 查找 330764 token 的市场"""
import requests, json

# 搜索 330764 token
token_prefix = "330764214394"

# 方法1: 通过 gamma-api 搜索 btc 5 minute
r = requests.get(
    "https://gamma-api.polymarket.com/markets",
    params={"active": "true", "closed": "false", "limit": "100"},
    timeout=10,
)
markets = r.json()
print(f"Total markets from gamma: {len(markets)}")

for m in markets:
    tokens = m.get("clobTokenIds", "[]")
    if isinstance(tokens, str):
        tokens = json.loads(tokens)
    for t in tokens:
        if t.startswith(token_prefix):
            print(f"\n*** FOUND market with token {token_prefix}... ***")
            print(f"  Question: {m.get('question', '?')}")
            print(f"  negRisk: {m.get('negRisk')}")
            print(f"  conditionId: {m.get('conditionId', '?')}")
            print(f"  tokens: {tokens}")
            break

# 方法2: 从数据源找
print("\n=== 检查数据源配置 ===")
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 看一下 pm_datasource 怎么获取市场信息的
from pathlib import Path
# 查 config
settings_f = Path("config/settings.yaml")
if settings_f.exists():
    import yaml
    s = yaml.safe_load(settings_f.read_text(encoding="utf-8"))
    pm_cfg = s.get("datasources", {}).get("polymarket", {})
    print(f"PM config: {json.dumps(pm_cfg, indent=2, default=str)}")

# 方法3: 直接查 CLOB token
print(f"\n=== 直接查 CLOB token {token_prefix}... ===")
try:
    r2 = requests.get(
        f"https://clob.polymarket.com/token/{token_prefix}42342506119830704016847874165636068867998486062843486063302580581511",
        timeout=10,
    )
    print(f"CLOB token response: {r2.status_code} {r2.text[:200]}")
except:
    pass

# 方法4: 搜索日志中完整的 token ID
print("\n=== 从日志获取完整 token ===")
log_file = Path("logs/pm_bot_2026-02-18.log")
if log_file.exists():
    for line in log_file.read_text(encoding="utf-8").splitlines():
        if "token_ids" in line.lower() or "330764" in line:
            print(f"  {line[:200]}")
