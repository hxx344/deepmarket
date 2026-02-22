"""检查 proxy 钱包状态"""
import requests
from eth_utils import keccak

proxy = '0x31720B94C295DA9eC253435a05c7A918D54059C7'
eoa = '0xbA486fB4eA138242804E0723bcAd9B69227553Fd'
neg_risk_exchange = '0xC5d563A36AE78145C45a50134d48A1215220f80a'
normal_exchange = '0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E'
ctf = '0x4D97DCd97eC945f40cF65F87097ACe5EA0476045'
usdc = '0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174'
rpc = 'https://polygon-rpc.com'

def eth_call(to, data):
    payload = {'jsonrpc': '2.0', 'method': 'eth_call', 'params': [{'to': to, 'data': data}, 'latest'], 'id': 1}
    r = requests.post(rpc, json=payload, timeout=10)
    return r.json().get('result', '0x')

def pad(addr):
    return addr[2:].lower().zfill(64)

def decode_bool(result):
    if result == '0x' or result is None:
        return "ERROR/revert"
    return bool(int(result, 16))

def decode_int(result):
    if result == '0x' or result is None:
        return 0
    return int(result, 16)


# 1. Proxy 合约基本信息
print("=== Proxy 合约 ===")
sel = keccak(b'isOperator(address)')[:4].hex()
r = eth_call(proxy, '0x' + sel + pad(eoa))
print(f"Proxy.isOperator(EOA): {decode_bool(r)}")

sel = keccak(b'getOperators()')[:4].hex()
r = eth_call(proxy, '0x' + sel)
print(f"Proxy.getOperators(): {r}")

sel = keccak(b'owner()')[:4].hex()
r = eth_call(proxy, '0x' + sel)
if r and r != '0x':
    owner = '0x' + r[-40:]
    print(f"Proxy.owner(): {owner}")
else:
    print(f"Proxy.owner(): {r}")

# 2. Exchange 授权
print("\n=== Exchange 授权 ===")
# isOperator(address, address) on exchange
sel = keccak(b'isOperator(address,address)')[:4].hex()
r = eth_call(neg_risk_exchange, '0x' + sel + pad(proxy) + pad(eoa))
print(f"NegRiskExchange.isOperator(proxy, EOA): {decode_bool(r)}")

r = eth_call(normal_exchange, '0x' + sel + pad(proxy) + pad(eoa))
print(f"NormalExchange.isOperator(proxy, EOA): {decode_bool(r)}")

# operators mapping
sel = keccak(b'operators(address,address)')[:4].hex()
r = eth_call(neg_risk_exchange, '0x' + sel + pad(proxy) + pad(eoa))
print(f"NegRiskExchange.operators[proxy][EOA]: {decode_bool(r)}")

r = eth_call(normal_exchange, '0x' + sel + pad(proxy) + pad(eoa))
print(f"NormalExchange.operators[proxy][EOA]: {decode_bool(r)}")

# 3. CTF token approvals
print("\n=== CTF Token approvals ===")
sel = keccak(b'isApprovedForAll(address,address)')[:4].hex()

r = eth_call(ctf, '0x' + sel + pad(proxy) + pad(neg_risk_exchange))
print(f"CTF.isApprovedForAll(proxy, NegRiskExchange): {decode_bool(r)}")

r = eth_call(ctf, '0x' + sel + pad(proxy) + pad(normal_exchange))
print(f"CTF.isApprovedForAll(proxy, NormalExchange): {decode_bool(r)}")

# 4. USDC allowance 
print("\n=== USDC approvals ===")
sel = keccak(b'allowance(address,address)')[:4].hex()

r = eth_call(usdc, '0x' + sel + pad(proxy) + pad(neg_risk_exchange))
print(f"USDC.allowance(proxy, NegRiskExchange): {decode_int(r) / 1e6:.2f}")

r = eth_call(usdc, '0x' + sel + pad(proxy) + pad(normal_exchange))
print(f"USDC.allowance(proxy, NormalExchange): {decode_int(r) / 1e6:.2f}")

# 5. Balances
print("\n=== Balances ===")
sel = keccak(b'balanceOf(address)')[:4].hex()
r = eth_call(usdc, '0x' + sel + pad(proxy))
print(f"Proxy USDC: {decode_int(r) / 1e6:.2f}")
r = eth_call(usdc, '0x' + sel + pad(eoa))
print(f"EOA USDC: {decode_int(r) / 1e6:.2f}")

# 6. Tx counts
print("\n=== Tx counts ===")
for name, addr in [("Proxy", proxy), ("EOA", eoa)]:
    pl = {'jsonrpc': '2.0', 'method': 'eth_getTransactionCount', 'params': [addr, 'latest'], 'id': 1}
    r = requests.post(rpc, json=pl, timeout=10)
    nc = int(r.json().get('result', '0x0'), 16)
    print(f"{name} tx count: {nc}")
