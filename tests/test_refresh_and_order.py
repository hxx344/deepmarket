"""刷新服务器缓存并重试下单"""
from dotenv import load_dotenv
load_dotenv()
import os, json, requests, time
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import (
    OrderArgs, PartialCreateOrderOptions, OrderType,
    BalanceAllowanceParams
)

key = os.environ.get("PM_PRIVATE_KEY", "")
proxy = os.environ.get("PM_PROXY_ADDRESS", "")

# 获取市场
resp = requests.get("https://gamma-api.polymarket.com/markets?active=true&closed=false&limit=1", timeout=10)
m = resp.json()[0]
tokens = json.loads(m["clobTokenIds"]) if isinstance(m["clobTokenIds"], str) else m["clobTokenIds"]
token_id = tokens[0]
neg_risk = m.get("negRisk", False)
question = m.get("question", "?")
print(f"Market: {question[:60]}")

# 创建 client
client = ClobClient(
    host="https://clob.polymarket.com", chain_id=137, key=key,
    signature_type=1, funder=proxy,
)
creds = client.derive_api_key()
client.set_api_creds(creds)

# 1. 先调用 update_balance_allowance 刷新缓存
print("\n=== 刷新 COLLATERAL 余额缓存 ===")
try:
    r1 = client.update_balance_allowance(BalanceAllowanceParams(
        asset_type="COLLATERAL", signature_type=1
    ))
    print(f"Update COLLATERAL: {r1}")
except Exception as e:
    print(f"Error: {e}")

print("\n=== 刷新 CONDITIONAL 余额缓存 ===")
try:
    r2 = client.update_balance_allowance(BalanceAllowanceParams(
        asset_type="CONDITIONAL", token_id=token_id, signature_type=1
    ))
    print(f"Update CONDITIONAL: {r2}")
except Exception as e:
    print(f"Error: {e}")

# 等待缓存刷新
time.sleep(2)

# 2. 重新查询余额
print("\n=== 刷新后余额 ===")
try:
    ba = client.get_balance_allowance(BalanceAllowanceParams(
        asset_type="COLLATERAL", signature_type=1
    ))
    print(f"COLLATERAL: {ba}")
except Exception as e:
    print(f"Error: {e}")

# 3. 重试下单
print("\n=== 重试 POLY_PROXY 下单 ===")
args = OrderArgs(token_id=token_id, price=0.10, size=10.0, side="BUY")
opts = PartialCreateOrderOptions(tick_size="0.01", neg_risk=neg_risk)
signed = client.create_order(args, opts)

try:
    result = client.post_order(signed, OrderType.FOK)
    print(f"SUCCESS! Result: {result}")
except Exception as e:
    print(f"Error: {type(e).__name__}: {e}")

# 4. 也试试 GTC 单
print("\n=== 试 GTC 单 ===")
args_gtc = OrderArgs(token_id=token_id, price=0.02, size=50.0, side="BUY")
signed_gtc = client.create_order(args_gtc, opts)
try:
    result_gtc = client.post_order(signed_gtc, OrderType.GTC)
    print(f"GTC Result: {result_gtc}")
    # 如果成功，立即取消
    if isinstance(result_gtc, dict) and result_gtc.get("orderID"):
        oid = result_gtc["orderID"]
        print(f"Cancelling order {oid}...")
        cancel = client.cancel(oid)
        print(f"Cancel: {cancel}")
except Exception as e:
    print(f"GTC Error: {type(e).__name__}: {e}")
