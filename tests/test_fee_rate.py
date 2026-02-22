"""检查 fee rate 并用正确的 fee rate 下单"""
from dotenv import load_dotenv
load_dotenv()
import os, json, requests
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, PartialCreateOrderOptions, OrderType
import inspect

key = os.environ.get("PM_PRIVATE_KEY", "")
proxy = os.environ.get("PM_PROXY_ADDRESS", "")

# 获取市场
resp = requests.get("https://gamma-api.polymarket.com/markets?active=true&closed=false&limit=1", timeout=10)
m = resp.json()[0]
tokens = json.loads(m["clobTokenIds"]) if isinstance(m["clobTokenIds"], str) else m["clobTokenIds"]
token_id = tokens[0]
neg_risk = m.get("negRisk", False)

client = ClobClient(
    host="https://clob.polymarket.com",
    chain_id=137,
    key=key,
    signature_type=1,
    funder=proxy,
)
creds = client.derive_api_key()
client.set_api_creds(creds)

# 1. 查看 create_and_post_order 源码 - 是否自动获取 fee rate
print("=== create_and_post_order source ===")
print(inspect.getsource(client.create_and_post_order))

# 2. 获取正确 fee rate
print("\n=== Fee rate for token ===")
try:
    fee_data = client.get_fee_rate_bps(token_id=token_id)
    print(f"Fee data: {fee_data}")
except Exception as e:
    print(f"Fee error: {e}")
    # 尝试不同的调用方式
    try:
        fee_data = client.get_fee_rate_bps()
        print(f"Fee data (no token): {fee_data}")
    except Exception as e2:
        print(f"Fee error (no token): {e2}")

# 3. 用 create_and_post_order 试试
print("\n=== 使用 create_and_post_order ===")
try:
    args = OrderArgs(token_id=token_id, price=0.10, size=10.0, side="BUY")
    opts = PartialCreateOrderOptions(tick_size="0.01", neg_risk=neg_risk)
    result = client.create_and_post_order(args, opts, OrderType.FOK)
    print(f"Result: {result}")
except Exception as e:
    print(f"Error: {type(e).__name__}: {e}")

# 4. 显式用 fee rate 试
print("\n=== 显式设置 fee rate ===")
try:
    fee_data = client.get_fee_rate_bps(token_id=token_id)
    fee_rate = int(fee_data) if isinstance(fee_data, (int, str)) else 0
    print(f"Using fee_rate_bps={fee_rate}")
    
    args2 = OrderArgs(
        token_id=token_id, price=0.10, size=10.0, side="BUY",
        fee_rate_bps=fee_rate,
    )
    signed = client.create_order(args2, opts)
    print(f"Order feeRateBps: {signed.order.values['feeRateBps']}")
    result2 = client.post_order(signed, OrderType.FOK)
    print(f"Result: {result2}")
except Exception as e:
    print(f"Error: {type(e).__name__}: {e}")
