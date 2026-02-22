"""检查 proxy wallet 类型和所有权"""
import httpx, json

proxy = '0x31720B94C295DA9eC253435a05c7A918D54059C7'
eoa = '0xbA486fB4eA138242804E0723bcAd9B69227553Fd'
rpc = 'https://polygon-rpc.com'

def eth_call(to, data):
    r = httpx.post(rpc, json={'jsonrpc': '2.0', 'id': 1, 'method': 'eth_call',
                               'params': [{'to': to, 'data': data}, 'latest']}, timeout=10)
    return r.json().get('result', '0x')

# 1. Check if proxy is a contract (getCode)
r = httpx.post(rpc, json={'jsonrpc': '2.0', 'id': 1, 'method': 'eth_getCode',
                           'params': [proxy, 'latest']}, timeout=10)
code = r.json().get('result', '0x')
print(f"Proxy code length: {len(code)} chars")
print(f"Is contract: {code != '0x' and len(code) > 2}")

# 2. Gnosis Safe getOwners() - 0xa0e67e2b
try:
    r1 = eth_call(proxy, '0xa0e67e2b')
    print(f"\ngetOwners() raw: {r1[:200]}...")
    # Decode ABI: dynamic array
    if r1 and r1 != '0x':
        data = bytes.fromhex(r1[2:])
        offset = int.from_bytes(data[0:32], 'big')
        length = int.from_bytes(data[offset:offset+32], 'big')
        print(f"Number of owners: {length}")
        for i in range(length):
            addr_bytes = data[offset+32+i*32:offset+64+i*32]
            addr = '0x' + addr_bytes[-20:].hex()
            print(f"  Owner {i}: {addr}")
            # Check case-insensitive match with EOA
            if addr.lower() == eoa.lower():
                print(f"    ^^^ MATCHES our EOA!")
except Exception as e:
    print(f"getOwners error: {e}")

# 3. Gnosis Safe getThreshold() - 0xe75235b8
try:
    r2 = eth_call(proxy, '0xe75235b8')
    if r2 and r2 != '0x':
        threshold = int(r2, 16)
        print(f"\nThreshold: {threshold}")
except Exception as e:
    print(f"getThreshold error: {e}")

# 4. Check nonce - 0xaffed0e0
try:
    r3 = eth_call(proxy, '0xaffed0e0')
    if r3 and r3 != '0x':
        nonce = int(r3, 16)
        print(f"Safe nonce: {nonce}")
except Exception as e:
    print(f"getNonce error: {e}")

# 5. Check if this proxy was deployed by Polymarket's factory
# PolymarketProxyFactory on Polygon: 0xaB45c5A4B0c941a2F231C04C3f49182e1A254052
factory = '0xaB45c5A4B0c941a2F231C04C3f49182e1A254052'
# Check getProxy(eoa)
sig_get_proxy = '0xef1a7268'  # getProxy has not standard sig, let's try isValidSignerForAddress
# Actually let me check if the polymarket api knows about this proxy
print("\n=== Polymarket API proxy check ===")
try:
    # Check the CLOB server's profile endpoint
    from dotenv import load_dotenv
    load_dotenv()
    import os
    from py_clob_client.client import ClobClient
    
    key = os.environ.get("PM_PRIVATE_KEY", "")
    client = ClobClient(
        host="https://clob.polymarket.com", chain_id=137, key=key,
        signature_type=1, funder=proxy,
    )
    creds = client.derive_api_key()
    client.set_api_creds(creds)
    
    # Get balance/allowance for proxy
    from py_clob_client.clob_types import BalanceAllowanceParams
    ba = client.get_balance_allowance(BalanceAllowanceParams(
        asset_type="COLLATERAL",
        signature_type=1,
    ))
    print(f"Balance/allowance (COLLATERAL): {ba}")
    
    ba2 = client.get_balance_allowance(BalanceAllowanceParams(
        asset_type="COLLATERAL",
        signature_type=0,
    ))
    print(f"Balance/allowance (COLLATERAL, EOA): {ba2}")
except Exception as e:
    print(f"API error: {e}")
