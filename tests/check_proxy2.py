"""检查代理钱包链上授权状态"""
import httpx, json

proxy = '0x31720B94C295DA9eC253435a05c7A918D54059C7'
CTF = '0x4D97DCd97eC945f40cF65F87097ACe5EA0476045'
NegRiskExchange = '0xC5d563A36AE78145C45a50134d48A1215220f80a'
NormalExchange = '0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E'
NegRiskAdapter = '0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296'
USDC = '0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174'
rpc = 'https://polygon-rpc.com'

def eth_call(to, data):
    r = httpx.post(rpc, json={'jsonrpc': '2.0', 'id': 1, 'method': 'eth_call',
                               'params': [{'to': to, 'data': data}, 'latest']}, timeout=10)
    return r.json().get('result', '0x')

sig_approved = 'e985e9c5'
sig_allowance = 'dd62ed3e'
sig_balance = '70a08231'
addr_proxy = proxy[2:].lower().zfill(64)
addr_neg = NegRiskExchange[2:].lower().zfill(64)
addr_norm = NormalExchange[2:].lower().zfill(64)
addr_adapter = NegRiskAdapter[2:].lower().zfill(64)

# isApprovedForAll checks
r1 = eth_call(CTF, '0x' + sig_approved + addr_proxy + addr_neg)
print(f'isApprovedForAll(proxy, NegRiskExchange) = {int(r1, 16) if r1 and r1 != "0x" else 0}')

r2 = eth_call(CTF, '0x' + sig_approved + addr_proxy + addr_norm)
print(f'isApprovedForAll(proxy, NormalExchange)   = {int(r2, 16) if r2 and r2 != "0x" else 0}')

r3 = eth_call(CTF, '0x' + sig_approved + addr_proxy + addr_adapter)
print(f'isApprovedForAll(proxy, NegRiskAdapter)   = {int(r3, 16) if r3 and r3 != "0x" else 0}')

# USDC allowances
r4 = eth_call(USDC, '0x' + sig_allowance + addr_proxy + addr_neg)
print(f'USDC.allowance(proxy, NegRiskExchange) = {int(r4, 16) if r4 and r4 != "0x" else 0}')

r5 = eth_call(USDC, '0x' + sig_allowance + addr_proxy + addr_norm)
print(f'USDC.allowance(proxy, NormalExchange)   = {int(r5, 16) if r5 and r5 != "0x" else 0}')

# USDC balance
r6 = eth_call(USDC, '0x' + sig_balance + addr_proxy)
balance = int(r6, 16) / 1e6 if r6 and r6 != '0x' else 0
print(f'USDC.balanceOf(proxy) = {balance} USDC')
