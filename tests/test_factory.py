"""检查 Polymarket 代理工厂 + 尝试 sig_type=2"""
from dotenv import load_dotenv
load_dotenv()
import os, json, requests, httpx
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, PartialCreateOrderOptions, OrderType

key = os.environ.get("PM_PRIVATE_KEY", "")
proxy = os.environ.get("PM_PROXY_ADDRESS", "")
eoa = '0xbA486fB4eA138242804E0723bcAd9B69227553Fd'

rpc = 'https://polygon-rpc.com'

def eth_call(to, data):
    r = httpx.post(rpc, json={'jsonrpc': '2.0', 'id': 1, 'method': 'eth_call',
                               'params': [{'to': to, 'data': data}, 'latest']}, timeout=10)
    return r.json().get('result', '0x')

# 1. 查询 Polymarket ProxyFactory 的 getProxy(eoa)
# Factory: 0xaB45c5A4B0c941a2F231C04C3f49182e1A254052
# getProxy(address) selector: 0xec8ee7ae (keccak256("getProxy(address)")[:4])
factory = '0xaB45c5A4B0c941a2F231C04C3f49182e1A254052'
# Actually let me compute the selector
from eth_utils import keccak as keccak_eth
selector = keccak_eth(b'getProxy(address)')[:4].hex()
print(f"getProxy selector: 0x{selector}")
addr_padded = eoa[2:].lower().zfill(64)
r1 = eth_call(factory, f'0x{selector}{addr_padded}')
if r1 and r1 != '0x':
    factory_proxy = '0x' + r1[-40:]
    print(f"Factory proxy for EOA: {factory_proxy}")
    print(f"Our proxy:             {proxy}")
    print(f"Match: {factory_proxy.lower() == proxy.lower()}")
else:
    print(f"Factory returned: {r1}")

# 2. 尝试 sig_type=2 (POLY_GNOSIS_SAFE)
print("\n=== TEST: sig_type=2 (POLY_GNOSIS_SAFE) ===")
resp = requests.get("https://gamma-api.polymarket.com/markets?active=true&closed=false&limit=1", timeout=10)
m = resp.json()[0]
tokens = json.loads(m["clobTokenIds"]) if isinstance(m["clobTokenIds"], str) else m["clobTokenIds"]
token_id = tokens[0]
neg_risk = m.get("negRisk", False)

client2 = ClobClient(
    host="https://clob.polymarket.com", chain_id=137, key=key,
    signature_type=2, funder=proxy,
)
creds2 = client2.derive_api_key()
client2.set_api_creds(creds2)

args = OrderArgs(token_id=token_id, price=0.10, size=10.0, side="BUY")
opts = PartialCreateOrderOptions(tick_size="0.01", neg_risk=neg_risk)
signed2 = client2.create_order(args, opts)
print(f"Order signatureType: {signed2.order.values['signatureType']}")

try:
    result2 = client2.post_order(signed2, OrderType.FOK)
    print(f"Result: {result2}")
except Exception as e:
    print(f"Error: {e}")

# 3. 也查一下 are there other factory addresses
print("\n=== 查看合约代码前缀 (proxy) ===")
r_code = httpx.post(rpc, json={'jsonrpc': '2.0', 'id': 1, 'method': 'eth_getCode',
                               'params': [proxy, 'latest']}, timeout=10)
code = r_code.json().get('result', '0x')
print(f"Proxy bytecode: {code}")

# 4. Get the creation tx of the proxy
print("\n=== PolygonScan API 查询部署者 ===")
try:
    r3 = requests.get(
        f"https://api.polygonscan.com/api?module=contract&action=getcontractcreation&contractaddresses={proxy}",
        timeout=10
    )
    print(f"Creator info: {json.dumps(r3.json(), indent=2)}")
except Exception as e:
    print(f"PolygonScan error: {e}")
