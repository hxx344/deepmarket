"""验证新的等shares分配逻辑"""
s_bias_sens = 0.25
s_bias_max = 0.15
base_size = 5.0
max_mult = 2.0
max_cost = 10.0

def calc_shares_bias(score):
    raw = s_bias_sens * score
    offset = max(-s_bias_max, min(s_bias_max, raw))
    return 0.5 + offset

def simulate(up_ask, dn_ask, score):
    combined = up_ask + dn_ask
    conf = min(abs(score), 1.0)
    mult = 1.0 + (max_mult - 1.0) * conf
    budget = min(base_size * mult, max_cost)
    up_r = calc_shares_bias(score)
    dn_r = 1.0 - up_r
    base_sh = budget / combined
    up_sh = base_sh * up_r
    dn_sh = base_sh * dn_r
    up_cost = up_sh * up_ask
    dn_cost = dn_sh * dn_ask
    total = up_cost + dn_cost
    pnl_up = up_sh - total
    pnl_dn = dn_sh - total
    edge = 1.0 - combined
    return {
        "score": score, "mult": mult, "budget": budget,
        "up_r": up_r, "dn_r": dn_r,
        "up_sh": up_sh, "dn_sh": dn_sh,
        "up_cost": up_cost, "dn_cost": dn_cost,
        "total": total, "pnl_up": pnl_up, "pnl_dn": pnl_dn,
        "edge": edge,
    }

header = f"{'score':>6} {'mult':>5} {'budg':>6} {'up_r':>5} {'up_sh':>7} {'dn_sh':>7} {'up$':>6} {'dn$':>6} {'tot$':>6} {'PnL_Up':>7} {'PnL_Dn':>7} {'edge':>6}"
sep = "-" * 95

# Scenario 1: positive edge (sum = 0.98)
print("=== Positive Edge: up=0.45, dn=0.53, sum=0.98, edge=+2% ===")
print(header)
print(sep)
for sc in [0.15, 0.20, 0.30, 0.40, 0.50, 0.60, 0.80, 1.00]:
    r = simulate(0.45, 0.53, sc)
    print(f"{r['score']:>+6.2f} {r['mult']:>5.2f} {r['budget']:>6.2f} {r['up_r']:>5.1%} {r['up_sh']:>7.2f} {r['dn_sh']:>7.2f} {r['up_cost']:>6.2f} {r['dn_cost']:>6.2f} {r['total']:>6.2f} {r['pnl_up']:>+7.2f} {r['pnl_dn']:>+7.2f} {r['edge']:>+6.2%}")

# Scenario 2: no edge (sum = 1.00)
print("\n=== Zero Edge: up=0.45, dn=0.55, sum=1.00, edge=0% ===")
print(header)
print(sep)
for sc in [0.15, 0.30, 0.60, 1.00]:
    r = simulate(0.45, 0.55, sc)
    print(f"{r['score']:>+6.2f} {r['mult']:>5.2f} {r['budget']:>6.2f} {r['up_r']:>5.1%} {r['up_sh']:>7.2f} {r['dn_sh']:>7.2f} {r['up_cost']:>6.2f} {r['dn_cost']:>6.2f} {r['total']:>6.2f} {r['pnl_up']:>+7.2f} {r['pnl_dn']:>+7.2f} {r['edge']:>+6.2%}")

# Scenario 3: negative edge (sum = 1.02)
print("\n=== Negative Edge: up=0.48, dn=0.54, sum=1.02, edge=-2% ===")
print(header)
print(sep)
for sc in [0.15, 0.30, 0.60, 1.00]:
    r = simulate(0.48, 0.54, sc)
    print(f"{r['score']:>+6.2f} {r['mult']:>5.2f} {r['budget']:>6.2f} {r['up_r']:>5.1%} {r['up_sh']:>7.2f} {r['dn_sh']:>7.2f} {r['up_cost']:>6.2f} {r['dn_cost']:>6.2f} {r['total']:>6.2f} {r['pnl_up']:>+7.2f} {r['pnl_dn']:>+7.2f} {r['edge']:>+6.2%}")

# Scenario 4: negative score (bearish)
print("\n=== Bearish: up=0.45, dn=0.53, score<0 ===")
print(header)
print(sep)
for sc in [-0.15, -0.30, -0.60, -1.00]:
    r = simulate(0.45, 0.53, sc)
    print(f"{r['score']:>+6.2f} {r['mult']:>5.2f} {r['budget']:>6.2f} {r['up_r']:>5.1%} {r['up_sh']:>7.2f} {r['dn_sh']:>7.2f} {r['up_cost']:>6.2f} {r['dn_cost']:>6.2f} {r['total']:>6.2f} {r['pnl_up']:>+7.2f} {r['pnl_dn']:>+7.2f} {r['edge']:>+6.2%}")

print("\n=== 新旧对比总结 ===")
print("旧策略 (score=0.5, up=0.45, dn=0.53): bias=80/20")
old_total = 10.0
old_up_cost = old_total * 0.80
old_dn_cost = old_total * 0.20
old_up_sh = old_up_cost / 0.45
old_dn_sh = old_dn_cost / 0.53
old_pnl_up = old_up_sh - old_total
old_pnl_dn = old_dn_sh - old_total
print(f"  up_sh={old_up_sh:.2f}, dn_sh={old_dn_sh:.2f}, ratio={old_up_sh/old_dn_sh:.2f}")
print(f"  PnL(Up)={old_pnl_up:+.2f}, PnL(Dn)={old_pnl_dn:+.2f}")
print(f"  风险: Up赢赚${old_pnl_up:.2f}, Dn赢亏${old_pnl_dn:.2f}")

print()
r = simulate(0.45, 0.53, 0.50)
print(f"新策略 (score=0.5, up=0.45, dn=0.53): shares={r['up_r']:.1%}/{r['dn_r']:.1%}")
print(f"  up_sh={r['up_sh']:.2f}, dn_sh={r['dn_sh']:.2f}, ratio={r['up_sh']/r['dn_sh']:.2f}")
print(f"  PnL(Up)={r['pnl_up']:+.2f}, PnL(Dn)={r['pnl_dn']:+.2f}")
print(f"  风险: 两侧PnL差距大幅缩小, 更接近无风险套利")
