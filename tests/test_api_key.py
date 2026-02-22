"""检查 API key 派生 - proxy vs EOA 模式"""
from dotenv import load_dotenv
load_dotenv()
import os, json
from py_clob_client.client import ClobClient
import inspect

key = os.environ.get("PM_PRIVATE_KEY", "")
proxy = os.environ.get("PM_PROXY_ADDRESS", "")

# 查看 derive_api_key 源码
print("=== derive_api_key source ===")
print(inspect.getsource(ClobClient.derive_api_key))

# EOA 模式
print("\n=== EOA mode derive ===")
client_eoa = ClobClient(
    host="https://clob.polymarket.com", chain_id=137, key=key, signature_type=0
)
creds_eoa = client_eoa.derive_api_key()
print(f"  API key: {creds_eoa.api_key}")
print(f"  Secret: {creds_eoa.api_secret[:20]}...")
print(f"  Passphrase: {creds_eoa.api_passphrase[:20]}...")

# PROXY 模式
print("\n=== PROXY mode derive ===")
client_proxy = ClobClient(
    host="https://clob.polymarket.com", chain_id=137, key=key,
    signature_type=1, funder=proxy
)
creds_proxy = client_proxy.derive_api_key()
print(f"  API key: {creds_proxy.api_key}")
print(f"  Secret: {creds_proxy.api_secret[:20]}...")
print(f"  Passphrase: {creds_proxy.api_passphrase[:20]}...")

print(f"\nSame API key? {creds_eoa.api_key == creds_proxy.api_key}")

# 查看 derive endpoint 请求详情
print("\n=== Derive API key internal details ===")
# 看看 L1 headers 的构造
from py_clob_client.headers.headers import create_level_1_headers
from py_clob_client.clob_types import RequestArgs
from py_clob_client.endpoints import DERIVE_API_KEY

# EOA mode headers
request_args = RequestArgs(method="GET", request_path=DERIVE_API_KEY)
h_eoa = create_level_1_headers(client_eoa.signer, request_args)
print(f"EOA L1 headers: {dict(h_eoa)}")

h_proxy = create_level_1_headers(client_proxy.signer, request_args)
print(f"PROXY L1 headers: {dict(h_proxy)}")

# 检查 L1 headers 生成逻辑
print("\n=== create_level_1_headers source ===")
print(inspect.getsource(create_level_1_headers))

# 检查 /auth/api-keys 获取所有 API keys
print("\n=== Get API keys ===")
client_proxy.set_api_creds(creds_proxy)
try:
    keys = client_proxy.get_api_keys()
    print(f"API keys: {json.dumps(keys, indent=2)}")
except Exception as e:
    print(f"Error: {e}")
