import urllib.request, json
r = urllib.request.urlopen('http://localhost:8082/api/v1/orderbook')
d = json.loads(r.read())
print(f"Bids: {len(d['bids'])}, Asks: {len(d['asks'])}, Spread: {d['spread']}")
for b in d['bids'][:3]:
    print(f"  Bid: {b}")
for a in d['asks'][:3]:
    print(f"  Ask: {a}")
