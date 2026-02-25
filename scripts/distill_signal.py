"""
0x1d 实时信号蒸馏 (Real-time Signal Distillation)
==================================================
从 monitor_0x1d.py 收集的 trade_snaps 数据中学习 0x1d 的入场时机和方向决策,
生成一个能在 BTC 任意行情下实时输出 UP / DOWN / HOLD 信号的模型。

核心思路:
  - 每笔 0x1d 的交易 = 一个「在当前市场状态下的入场决策」样本
  - 特征 = 纯市场状态 (BTC 动量/波动率/趋势 + CL 价差 + PM 盘口)
  - 标签 = 交易方向 (UP=1, DOWN=0)
  - 模型输出概率 → 高置信度产生信号, 低置信度 = HOLD

与旧版区别:
  - 旧版: 预测整个 5min 窗口 UP/DOWN 方向 (76个样本, 无法学会)
  - 新版: 学习每一笔交易时刻的市场特征 → 方向 (数千样本, 实时可用)
  - 产出: 一个实时信号生成器, 输入当前市场数据 → 输出交易信号

用法:
  python scripts/distill_signal.py                # 完整训练 + 评估
  python scripts/distill_signal.py --report       # 只看数据分析
  python scripts/distill_signal.py --threshold 0.65  # 自定义信号阈值
  python scripts/distill_signal.py --rich-only    # 仅用有ref_ts的高质量数据
  python scripts/distill_signal.py --pure-market  # 仅用纯市场特征 (排除持仓/行为)
  python scripts/distill_signal.py --compare      # 对比全特征 vs 纯市场特征
  python scripts/distill_signal.py --outcome      # ⭐ 用窗口结算结果作为训练标签
                                                   #   (预测哪边赢, 而非模仿0x1d)
  python scripts/distill_signal.py --outcome --pure-market  # 推荐: 纯市场+结算标签

评估指标:
  模型评估以 **窗口PnL模拟** 为核心: 按信号下注 → 窗口结算 → 计算真实盈亏
  而非简单的方向准确率。

产出:
  data/distill_models/signal_model.pkl     — LightGBM 模型
  data/distill_models/signal_config.json   — 特征列表 + 信号阈值 + 训练模式
"""

import sqlite3
import json
import sys
import pickle
import warnings
from pathlib import Path
from datetime import datetime, timezone
from collections import defaultdict

# Windows GBK 编码兼容
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

import numpy as np
import pandas as pd
from sklearn.model_selection import GroupKFold
from sklearn.metrics import (
    accuracy_score, roc_auc_score, f1_score,
    classification_report, confusion_matrix,
)
import lightgbm as lgb

warnings.filterwarnings("ignore")

# ── 路径 ──
ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "data" / "0x1d_data.db"
MODEL_DIR = ROOT / "data" / "distill_models"
MODEL_DIR.mkdir(parents=True, exist_ok=True)


# ══════════════════════════════════════════════════════════════
# 特征定义: 仅使用纯市场状态特征
# 不包含持仓、窗口累计等推理时不存在的字段
# ══════════════════════════════════════════════════════════════

# Binance BTC 动量 (多时间尺度)
BN_MOMENTUM = [
    "bn_mom_1s", "bn_mom_3s", "bn_mom_5s", "bn_mom_10s", "bn_mom_15s",
    "bn_mom_30s", "bn_mom_60s", "bn_mom_120s",
]
# Binance BTC 波动率
BN_VOLATILITY = ["bn_vol_10s", "bn_vol_30s", "bn_vol_60s"]
# Binance 高级指标 (趋势/z-score/百分位)
BN_ADVANCED = [
    "bn_mom_accel_5s", "bn_mom_accel_10s",
    "bn_trend_30s", "bn_trend_60s",
    "bn_mom_z_5s", "bn_pctl_60s",
]
# Chainlink RTDS 动量
CL_MOMENTUM = [
    "mom_1s", "mom_3s", "mom_5s", "mom_10s", "mom_15s",
    "mom_30s", "mom_60s", "mom_120s",
]
# Chainlink 波动率
CL_VOLATILITY = ["btc_vol_10s", "btc_vol_30s", "btc_vol_60s"]
# Chainlink 高级指标
CL_ADVANCED = [
    "mom_accel_5s", "mom_accel_10s",
    "cl_trend_30s", "cl_trend_60s", "cl_trend_120s",
    "cl_mom_z_5s", "cl_mom_z_10s",
    "cl_pctl_60s", "cl_pctl_300s",
    "cl_dir_changes_30s", "cl_dir_changes_60s",
]
# 跨数据源特征 (BN vs CL 差异)
CROSS_SOURCE = [
    "cl_bn_spread", "cl_bn_mom_diff_5s", "cl_bn_mom_diff_10s",
    "cl_bn_trend_agree",
    "btc_delta_pct", "btc_delta_ptb",
    "bn_delta_pct", "bn_delta_ptb",
    "btc_above_ptb",
]
# PM 盘口特征 (价格机会指标)
PM_ORDERBOOK = [
    "up_bid", "up_ask", "dn_bid", "dn_ask",
    "up_ba_spread", "dn_ba_spread",
    "pm_spread", "pm_edge",
]
# 窗口上下文 (推理时可获得)
WINDOW_CONTEXT = ["elapsed", "elapsed_pct"]
# 时间特征 (交易时段对行为模式有显著影响)
TIME_FEATURES = [
    "hour_utc",        # UTC 小时 (0-23)
    "minute_utc",      # UTC 分钟 (0-59)
    "day_of_week",     # 星期几 (0=Mon, 6=Sun)
    "us_session",      # 美股时段: 0=closed, 1=pre, 2=regular, 3=after
    "asia_session",    # 亚盘 (UTC 0-8)
    "euro_session",    # 欧盘 (UTC 7-16)
]
# 持仓/行为特征 (推理时 bot 自身可追踪)
POSITION_BEHAVIOR = [
    "net_shares",          # UP-DN 原始 shares 差值 (绝对量, 不归一化)
    "cum_trades",          # 当前窗口累计交易笔数
    "cum_up_shares",       # UP 累计 shares
    "cum_dn_shares",       # DOWN 累计 shares
    "cum_up_cost",         # UP 累计成本 USDC
    "cum_dn_cost",         # DOWN 累计成本 USDC
    "avg_up_price",        # UP 平均成交价
    "avg_dn_price",        # DOWN 平均成交价
    "pos_imbalance",       # 仓位偏差 (UP-DN)/max(UP,DN)
    "same_side_streak",    # 连续同向笔数 (正=UP, 负=DN)
    "trade_velocity",      # 交易速度 (笔/窗口进度)
    "time_since_last",     # 距上笔交易秒数
    "burst_seq",           # 突发序号 (1s内连续交易)
    "is_burst",            # 是否突发交易
    "up_price_vs_fair",    # UP 价格 vs 公平价偏差
    "dn_price_vs_fair",    # DN 价格 vs 公平价偏差
]
# Burst 聚合特征 (去重时由多笔交易聚合生成, 反映单次决策的扫单力度)
BURST_AGGREGATE = [
    "burst_n_fills",       # 本次决策的成交笔数 (1=单档, >1=扫多档)
    "burst_total_shares",  # 本次决策总 shares
    "burst_total_usdc",    # 本次决策总 USDC
    "burst_avg_price",     # 本次决策加权均价 (usdc/shares)
]

# 全部信号特征 (含持仓/行为特征)
SIGNAL_FEATURES = (
    BN_MOMENTUM + BN_VOLATILITY + BN_ADVANCED +
    CL_MOMENTUM + CL_VOLATILITY + CL_ADVANCED +
    CROSS_SOURCE + PM_ORDERBOOK + WINDOW_CONTEXT + TIME_FEATURES +
    POSITION_BEHAVIOR + BURST_AGGREGATE
)

# 纯市场特征 (排除持仓/行为/burst — 避免自相关泄漏)
# 这些特征仅依赖外部市场数据, 不依赖 0x1d 自身的交易行为
PURE_MARKET_FEATURES = (
    BN_MOMENTUM + BN_VOLATILITY + BN_ADVANCED +
    CL_MOMENTUM + CL_VOLATILITY + CL_ADVANCED +
    CROSS_SOURCE + PM_ORDERBOOK + WINDOW_CONTEXT + TIME_FEATURES
)

# 排除的特征 (纯元数据/标签 — 推理时无意义)
EXCLUDED = [
    "side", "shares", "price",     # 标签和成交细节
    "ref_ts", "feature_latency", "ts_source",  # 调试元数据
]


# ══════════════════════════════════════════════════════════════
# 1. 数据加载
# ══════════════════════════════════════════════════════════════

def load_settlement_labels() -> dict:
    """加载结算数据, 返回 {slug: {won, pnl}} 映射, 用于 outcome 标签和 PnL 模拟"""
    conn = sqlite3.connect(str(DB_PATH))
    rows = conn.execute(
        "SELECT slug, pnl, won, up_cost, dn_cost, up_shares, dn_shares "
        "FROM settlements ORDER BY settled_at"
    ).fetchall()
    conn.close()
    settle = {}
    for slug, pnl, won, u_cost, d_cost, u_sh, d_sh in rows:
        settle[slug] = {
            "pnl": pnl, "won": won,
            "up_cost": u_cost or 0, "dn_cost": d_cost or 0,
            "up_shares": u_sh or 0, "dn_shares": d_sh or 0,
        }
    print(f"加载 {len(settle)} 条结算记录")
    return settle


def load_trades(rich_only: bool = False) -> pd.DataFrame:
    """加载 trade_snaps, 解析 features JSON"""
    conn = sqlite3.connect(str(DB_PATH))
    rows = conn.execute(
        "SELECT ts, slug, side, shares, price, usdc, features "
        "FROM trade_snaps ORDER BY ts"
    ).fetchall()
    conn.close()

    records = []
    for ts, slug, side, shares, price, usdc, feat_json in rows:
        feat = json.loads(feat_json) if feat_json else {}
        rec = {
            "ts": ts, "slug": slug, "side": side,
            "shares": shares, "price": price, "usdc": usdc,
            "has_ref_ts": "ref_ts" in feat,
        }
        for f in SIGNAL_FEATURES:
            rec[f] = feat.get(f, np.nan)
        records.append(rec)

    df = pd.DataFrame(records)
    if rich_only:
        df = df[df.has_ref_ts].reset_index(drop=True)

    n_up = (df.side == "UP").sum()
    n_dn = (df.side == "DOWN").sum()
    n_ref = df.has_ref_ts.sum()
    n_win = df.slug.nunique()
    print(f"加载 {len(df)} 笔交易 ({n_win} 个窗口)")
    print(f"  UP: {n_up}  |  DOWN: {n_dn}  |  有ref_ts: {n_ref}")
    return df


# ══════════════════════════════════════════════════════════════
# 2. 突发聚类 (Burst Deduplication)
# ══════════════════════════════════════════════════════════════

def deduplicate_bursts(df: pd.DataFrame, gap_sec: int = 3) -> pd.DataFrame:
    """
    将连续的同方向交易聚合为一个「决策点」:
    - 同窗口 + 同方向 + 时间间隔 < gap_sec → 视为同一次入场决策
    - 市场特征: 取首笔 (决策瞬间的市场快照)
    - 持仓特征: 取末笔 (扫单完成后的持仓状态)
    - 新增 burst 聚合特征: 笔数/总shares/总usdc/均价 (反映扫单力度)

    核心逻辑: 0x1d 一次下单可能扫多个档口 → CLOB 记录多笔 fill,
    但这是一个信号, 不是多个信号。模型需要感知「单档 vs 多档扫单」。
    """
    df = df.sort_values(["slug", "side", "ts"]).reset_index(drop=True)

    # 标记 burst 边界
    new_burst = (
        (df["slug"] != df["slug"].shift()) |
        (df["side"] != df["side"].shift()) |
        ((df["ts"] - df["ts"].shift()) > gap_sec)
    )
    df["burst_id"] = new_burst.cumsum()

    # ── 基础聚合 ──
    agg = df.groupby("burst_id").agg(
        ts=("ts", "first"),
        slug=("slug", "first"),
        side=("side", "first"),
        shares=("shares", "sum"),
        usdc=("usdc", "sum"),
        n_trades=("ts", "count"),
        has_ref_ts=("has_ref_ts", "first"),
    ).reset_index(drop=True)

    # ── 市场特征: 取首笔交易 (决策瞬间的市场快照) ──
    # 这些特征在决策那一刻就已确定, 不受后续 fill 影响
    MARKET_FEATURES = (
        BN_MOMENTUM + BN_VOLATILITY + BN_ADVANCED +
        CL_MOMENTUM + CL_VOLATILITY + CL_ADVANCED +
        CROSS_SOURCE + PM_ORDERBOOK + WINDOW_CONTEXT + TIME_FEATURES
    )
    first_idx = df.groupby("burst_id").head(1).index
    market_df = df.loc[first_idx, [f for f in MARKET_FEATURES if f in df.columns]].reset_index(drop=True)

    # ── 持仓特征: 取末笔交易 (扫单完成后的持仓状态) ──
    # 推理时 bot 也是等 burst 结束后才知道最终持仓
    last_idx = df.groupby("burst_id").tail(1).index
    pos_cols = [f for f in POSITION_BEHAVIOR if f in df.columns]
    pos_df = df.loc[last_idx, pos_cols].reset_index(drop=True)

    # ── Burst 聚合特征 (新增: 反映扫单强度) ──
    burst_agg = df.groupby("burst_id").agg(
        burst_n_fills=("ts", "count"),                      # 扫了几个档
        burst_total_shares=("shares", "sum"),                # 总 shares
        burst_total_usdc=("usdc", "sum"),                    # 总 USDC
    ).reset_index(drop=True)
    # 加权均价 = usdc / shares
    burst_agg["burst_avg_price"] = (
        burst_agg["burst_total_usdc"] / burst_agg["burst_total_shares"].replace(0, np.nan)
    ).round(4)

    result = pd.concat([agg, market_df, pos_df, burst_agg], axis=1)

    ratio = len(df) / len(result) if len(result) > 0 else 0
    multi_fill = (result.burst_n_fills > 1).sum()
    print(f"\n突发聚类: {len(df)} 笔交易 → {len(result)} 个决策点 (压缩 {ratio:.1f}x)")
    print(f"  单档成交: {len(result) - multi_fill} | 多档扫单: {multi_fill} ({multi_fill/len(result)*100:.1f}%)")
    print(f"  平均 burst 大小: {result.burst_n_fills.mean():.1f} 笔, ${result.burst_total_usdc.mean():.1f}")
    return result


# ══════════════════════════════════════════════════════════════
# 3. 特征分析
# ══════════════════════════════════════════════════════════════

def analyze_features(df: pd.DataFrame) -> list:
    """分析每个特征对 UP/DOWN 的区分能力"""
    print("\n" + "=" * 72)
    print("  特征分析: UP vs DOWN 区分度 (Cohen's d)")
    print("=" * 72)

    up = df[df.side == "UP"]
    dn = df[df.side == "DOWN"]

    results = []
    for f in SIGNAL_FEATURES:
        u_vals = up[f].dropna()
        d_vals = dn[f].dropna()
        if len(u_vals) < 10 or len(d_vals) < 10:
            continue
        mu, md = u_vals.mean(), d_vals.mean()
        pooled = np.sqrt((u_vals.std() ** 2 + d_vals.std() ** 2) / 2)
        d = abs(mu - md) / pooled if pooled > 0 else 0
        direction = "↑UP" if mu > md else "↓DN"
        results.append({
            "feature": f, "mean_up": mu, "mean_dn": md,
            "cohens_d": d, "direction": direction,
            "coverage": df[f].notna().mean() * 100,
        })

    results.sort(key=lambda x: x["cohens_d"], reverse=True)

    print(f"\n{'特征':<32} {'UP均值':>10} {'DN均值':>10} {'Cohen d':>8} {'方向':>5} {'覆盖':>6}")
    print("-" * 75)
    for r in results[:30]:
        bar = "█" * int(r["cohens_d"] / max(x["cohens_d"] for x in results) * 15)
        print(
            f"  {r['feature']:<30} {r['mean_up']:10.4f} {r['mean_dn']:10.4f} "
            f"{r['cohens_d']:8.4f} {r['direction']:>5} {r['coverage']:5.1f}% {bar}"
        )
    return results


# ══════════════════════════════════════════════════════════════
# 4. 模型训练
# ══════════════════════════════════════════════════════════════

def train_signal_model(df: pd.DataFrame, threshold: float = 0.60,
                       feature_set: list | None = None,
                       label: str = "全特征",
                       outcome_labels: dict | None = None):
    """
    训练实时入场信号模型:
      - 输入: 市场状态特征 (可选排除持仓特征)
      - 输出: P(UP), 用阈值分为 UP / DOWN / HOLD
      - 验证: GroupKFold (按窗口分组, 无时间泄漏)
      - outcome_labels: 若提供, 以窗口结算赢方作为训练标签 (而非模仿 0x1d)
    """
    features = feature_set or SIGNAL_FEATURES
    training_mode = "outcome" if outcome_labels else "mimicry"
    print("\n" + "=" * 72)
    print(f"  训练: 实时入场信号模型 [{label}]")
    print(f"  特征: {len(features)} | 训练标签: {'窗口结算结果' if outcome_labels else '模仿0x1d方向'}")
    print("=" * 72)

    X = df[features].copy()
    groups = df["slug"]                         # 按窗口分组

    if outcome_labels:
        # ── 结算标签: 1=UP赢, 0=DOWN赢 ──
        df = df.copy()
        df["_outcome"] = df["slug"].map(
            lambda s: 1 if outcome_labels.get(s, {}).get("won") == "UP"
            else (0 if outcome_labels.get(s, {}).get("won") == "DOWN" else np.nan)
        )
        valid_mask = df["_outcome"].notna()
        if valid_mask.sum() < len(df):
            n_drop = len(df) - valid_mask.sum()
            print(f"  排除 {n_drop} 笔无结算数据的交易 ({n_drop/len(df)*100:.1f}%)")
            X = X[valid_mask].reset_index(drop=True)
            groups = groups[valid_mask].reset_index(drop=True)
            df = df[valid_mask].reset_index(drop=True)
        y = df["_outcome"].astype(int)
        n_up_win = (y == 1).sum()
        n_dn_win = (y == 0).sum()
        print(f"  标签分布: UP赢={n_up_win} ({n_up_win/len(y)*100:.1f}%) | DOWN赢={n_dn_win} ({n_dn_win/len(y)*100:.1f}%)")
    else:
        y = (df["side"] == "UP").astype(int)       # 1=UP, 0=DOWN

    # 特征覆盖率
    coverage = X.notna().mean().sort_values()
    low = (coverage < 0.3).sum()
    print(f"\n特征数: {len(SIGNAL_FEATURES)}, 覆盖率 <30% 的特征: {low}")
    if low > 0:
        print(f"  低覆盖特征: {coverage[coverage < 0.3].index.tolist()}")

    # ── LightGBM 参数 ──
    params = dict(
        objective="binary",
        metric="binary_logloss",
        n_estimators=500,
        max_depth=6,
        num_leaves=31,
        learning_rate=0.03,
        subsample=0.8,
        colsample_bytree=0.7,
        min_child_samples=30,
        reg_alpha=0.3,
        reg_lambda=0.3,
        random_state=42,
        verbose=-1,
        n_jobs=-1,
    )

    # ── GroupKFold CV ──
    n_windows = groups.nunique()
    n_splits = min(5, n_windows)
    gkf = GroupKFold(n_splits=n_splits)

    print(f"\n{n_splits}-Fold GroupKFold CV ({n_windows} 个窗口)\n")

    all_probs = np.full(len(df), np.nan)
    fold_metrics = []

    for fold, (tr_idx, te_idx) in enumerate(gkf.split(X, y, groups)):
        X_tr, X_te = X.iloc[tr_idx], X.iloc[te_idx]
        y_tr, y_te = y.iloc[tr_idx], y.iloc[te_idx]

        model = lgb.LGBMClassifier(**params)
        model.fit(
            X_tr, y_tr,
            eval_set=[(X_te, y_te)],
            callbacks=[lgb.early_stopping(50, verbose=False)],
        )

        probs = model.predict_proba(X_te)[:, 1]
        all_probs[te_idx] = probs
        preds = (probs > 0.5).astype(int)

        acc = accuracy_score(y_te, preds)
        try:
            auc = roc_auc_score(y_te, probs)
        except ValueError:
            auc = 0.5
        f1 = f1_score(y_te, preds, average="macro")
        n_win = groups.iloc[te_idx].nunique()

        fold_metrics.append({"acc": acc, "auc": auc, "f1": f1, "n": len(te_idx), "w": n_win})
        print(f"  Fold {fold+1}: Acc={acc:.4f}  AUC={auc:.4f}  F1={f1:.4f}  "
              f"(samples={len(te_idx)}, windows={n_win})")

    cv_acc = np.mean([m["acc"] for m in fold_metrics])
    cv_auc = np.mean([m["auc"] for m in fold_metrics])
    cv_f1 = np.mean([m["f1"] for m in fold_metrics])
    acc_std = np.std([m["acc"] for m in fold_metrics])
    auc_std = np.std([m["auc"] for m in fold_metrics])
    print(f"\n  CV 平均: Acc={cv_acc:.4f}±{acc_std:.4f}  AUC={cv_auc:.4f}±{auc_std:.4f}  F1={cv_f1:.4f}")
    print(f"  基准线 (随机): Acc=0.5000  AUC=0.5000")
    lift = (cv_acc - 0.5) / 0.5 * 100
    print(f"  提升: +{lift:.1f}% over random")

    # ── 不同阈值的信号分析 ──
    print(f"\n{'阈值':<8} {'信号率':>8} {'UP%':>7} {'DN%':>7} {'HOLD%':>7} {'信号Acc':>8} {'信号F1':>8}")
    print("-" * 58)

    valid = ~np.isnan(all_probs)
    probs_v = all_probs[valid]
    y_v = y.values[valid]

    best_t, best_metric = threshold, 0
    for t in [0.50, 0.52, 0.55, 0.58, 0.60, 0.62, 0.65, 0.70, 0.75]:
        sig_up = probs_v > t
        sig_dn = probs_v < (1 - t)
        sig = sig_up | sig_dn

        if sig.sum() > 0:
            sig_preds = np.where(sig_up[sig], 1, 0)
            sig_acc = accuracy_score(y_v[sig], sig_preds)
            sig_f1 = f1_score(y_v[sig], sig_preds, average="macro")
        else:
            sig_acc = sig_f1 = 0

        pct_up = sig_up.mean() * 100
        pct_dn = sig_dn.mean() * 100
        pct_hold = (1 - sig.mean()) * 100

        # 综合得分: 信号准确率 × 信号覆盖率的平方根
        score = sig_acc * np.sqrt(sig.mean()) if sig.mean() > 0 else 0
        if score > best_metric:
            best_metric = score
            best_t = t

        marker = " <-- best" if abs(t - best_t) < 0.005 and t == best_t else ""
        print(f"  {t:.2f}   {sig.mean()*100:7.1f}% {pct_up:6.1f}% {pct_dn:6.1f}% "
              f"{pct_hold:6.1f}% {sig_acc:8.4f} {sig_f1:8.4f}{marker}")

    # ── 训练最终模型 ──
    print(f"\n训练最终模型 (全量数据, {len(df)} samples)...")
    final_model = lgb.LGBMClassifier(**params)
    final_model.fit(X, y)

    # ── 特征重要性 ──
    imp = pd.DataFrame({
        "feature": features,
        "importance": final_model.feature_importances_,
    }).sort_values("importance", ascending=False)

    print(f"\nTop 25 特征重要性:")
    print(f"{'特征':<34} {'重要性':>8}")
    print("-" * 44)
    max_imp = imp["importance"].max()
    for _, row in imp.head(25).iterrows():
        bar = "█" * int(row["importance"] / max_imp * 20) if max_imp > 0 else ""
        print(f"  {row['feature']:<32} {row['importance']:>6.0f} {bar}")

    # ── 验证: 模型学到的方向是否正确? ──
    print(f"\n验证: 模型预测 vs BTC 动量方向")
    _verify_direction_logic(df, all_probs)

    return final_model, {
        "cv_acc": cv_acc,
        "cv_auc": cv_auc,
        "cv_f1": cv_f1,
        "best_threshold": best_t,
        "fold_metrics": fold_metrics,
        "all_probs": all_probs,
        "feature_importance": imp,
        "features_used": features,
        "label": label,
        "training_mode": training_mode,
        "df_used": df,  # 训练时实际使用的 df (可能已过滤)
    }


def _verify_direction_logic(df: pd.DataFrame, probs: np.ndarray):
    """验证模型是否学到了正确的趋势跟踪逻辑"""
    valid = ~np.isnan(probs) & df["bn_mom_5s"].notna()
    if valid.sum() < 100:
        print("  (数据不足, 跳过)")
        return

    mom = df.loc[valid, "bn_mom_5s"].values
    p = probs[valid]
    y_true = (df.loc[valid, "side"] == "UP").astype(int).values

    # BTC 上涨时 vs 下跌时
    up_mask = mom > 0.5    # BTC 明显上涨
    dn_mask = mom < -0.5   # BTC 明显下跌

    if up_mask.sum() > 10 and dn_mask.sum() > 10:
        p_up_when_btc_up = p[up_mask].mean()
        p_up_when_btc_dn = p[dn_mask].mean()
        actual_up_when_btc_up = y_true[up_mask].mean()
        actual_up_when_btc_dn = y_true[dn_mask].mean()

        print(f"  BTC涨 (mom>0.5, n={up_mask.sum()}):")
        print(f"    模型 P(UP) = {p_up_when_btc_up:.3f}  |  实际 UP% = {actual_up_when_btc_up:.3f}")
        print(f"  BTC跌 (mom<-0.5, n={dn_mask.sum()}):")
        print(f"    模型 P(UP) = {p_up_when_btc_dn:.3f}  |  实际 UP% = {actual_up_when_btc_dn:.3f}")

        if p_up_when_btc_up > p_up_when_btc_dn:
            print(f"  ✓ 模型正确学到了趋势跟踪模式 (BTC涨→买UP, BTC跌→买DOWN)")
        else:
            print(f"  ✗ 模型方向可能有问题")


# ══════════════════════════════════════════════════════════════
# 5. 信号回测
# ══════════════════════════════════════════════════════════════

def backtest_signals(df: pd.DataFrame, probs: np.ndarray, threshold: float):
    """回测信号质量: 按窗口分析模型信号 vs 0x1d 实际行为"""
    print("\n" + "=" * 72)
    print(f"  信号回测 (阈值={threshold:.2f})")
    print("=" * 72)

    valid = ~np.isnan(probs)
    df_v = df[valid].copy()
    p_v = probs[valid]

    df_v["prob_up"] = p_v
    df_v["signal"] = "HOLD"
    df_v.loc[df_v.prob_up > threshold, "signal"] = "UP"
    df_v.loc[df_v.prob_up < (1 - threshold), "signal"] = "DOWN"
    df_v["correct"] = (
        ((df_v.signal == "UP") & (df_v.side == "UP")) |
        ((df_v.signal == "DOWN") & (df_v.side == "DOWN"))
    )

    signaled = df_v[df_v.signal != "HOLD"]
    held = df_v[df_v.signal == "HOLD"]

    print(f"\n总决策点: {len(df_v)}")
    print(f"  产生信号: {len(signaled)} ({len(signaled)/len(df_v)*100:.1f}%)")
    print(f"  HOLD:     {len(held)} ({len(held)/len(df_v)*100:.1f}%)")

    if len(signaled) > 0:
        sig_acc = signaled.correct.mean()
        print(f"\n信号准确率: {sig_acc:.4f} ({signaled.correct.sum()}/{len(signaled)})")

        # 按信号方向分析
        for sig_dir in ["UP", "DOWN"]:
            sub = signaled[signaled.signal == sig_dir]
            if len(sub) > 0:
                acc = sub.correct.mean()
                avg_conf = sub.prob_up.apply(lambda p: max(p, 1-p)).mean()
                print(f"  {sig_dir} 信号: {len(sub)} 笔, Acc={acc:.4f}, 平均置信度={avg_conf:.3f}")

    # 按窗口分析
    print(f"\n按窗口分析:")
    window_stats = []
    for slug, grp in df_v.groupby("slug"):
        sig = grp[grp.signal != "HOLD"]
        if len(sig) == 0:
            continue
        window_stats.append({
            "slug": slug,
            "total": len(grp),
            "signaled": len(sig),
            "rate": len(sig) / len(grp) * 100,
            "acc": sig.correct.mean(),
            "up_signals": (sig.signal == "UP").sum(),
            "dn_signals": (sig.signal == "DOWN").sum(),
        })

    if window_stats:
        ws = pd.DataFrame(window_stats)
        print(f"  有信号的窗口: {len(ws)}/{df_v.slug.nunique()}")
        print(f"  窗口平均信号率: {ws.rate.mean():.1f}%")
        print(f"  窗口平均准确率: {ws.acc.mean():.4f}")
        print(f"  窗口准确率中位数: {ws.acc.median():.4f}")

        # 显示最好和最差的窗口
        ws_sorted = ws.sort_values("acc", ascending=False)
        print(f"\n  最佳窗口 (Top 5):")
        for _, row in ws_sorted.head(5).iterrows():
            short = row.slug.split("-")[-1] if "-" in row.slug else row.slug
            print(f"    {short}: Acc={row.acc:.3f}, 信号={row.signaled}/{row.total}, "
                  f"UP={row.up_signals} DN={row.dn_signals}")
        print(f"  最差窗口 (Bottom 5):")
        for _, row in ws_sorted.tail(5).iterrows():
            short = row.slug.split("-")[-1] if "-" in row.slug else row.slug
            print(f"    {short}: Acc={row.acc:.3f}, 信号={row.signaled}/{row.total}, "
                  f"UP={row.up_signals} DN={row.dn_signals}")

    # 信号时机分析
    if "elapsed_pct" in signaled.columns and signaled.elapsed_pct.notna().sum() > 0:
        e = signaled.elapsed_pct.dropna()
        print(f"\n信号时机 (elapsed %):")
        print(f"  25th: {e.quantile(0.25):.0f}%  |  中位数: {e.median():.0f}%  |  75th: {e.quantile(0.75):.0f}%")

    # USDC 分析
    sig_usdc = signaled.usdc.sum()
    correct_usdc = signaled[signaled.correct].usdc.sum()
    print(f"\n资金分配:")
    print(f"  信号覆盖 USDC: ${sig_usdc:.0f}")
    print(f"  正确信号 USDC: ${correct_usdc:.0f} ({correct_usdc/sig_usdc*100:.1f}%)")


# ══════════════════════════════════════════════════════════════
# 6. 窗口 PnL 模拟 (核心评估)
# ══════════════════════════════════════════════════════════════

def simulate_window_pnl(df: pd.DataFrame, probs: np.ndarray, threshold: float,
                        settle: dict, bet_mode: str = "fixed",
                        base_bet: float = 10.0):
    """
    ⭐ 核心评估: 按模型信号在每个5分钟窗口内下注, 结算后计算真实PnL.

    流程:
      1. 遍历每个窗口内的决策点
      2. 模型输出 UP/DOWN/HOLD → 有信号则按方向下注
      3. 窗口结算: 赢方 shares × $1 = payout
      4. PnL = payout - total_cost

    bet_mode:
      "fixed"      — 每次信号固定 base_bet 下注 (纯方向预测评估)
      "confidence" — 按模型置信度缩放: base_bet × (conf - 0.5) / 0.5
                     置信度越高下注越大, 刚过阈值只下一点点
    """
    mode_desc = f"{bet_mode} (${base_bet:.0f}/笔)" if bet_mode == "fixed" else f"{bet_mode} (基础${base_bet:.0f})"
    print("\n" + "=" * 72)
    print(f"  ⭐ 窗口 PnL 模拟 (阈值={threshold:.2f}, 下注={mode_desc})")
    print("=" * 72)

    if not settle:
        print("  无结算数据, 跳过")
        return None

    valid = ~np.isnan(probs)
    df_v = df[valid].copy()
    p_v = probs[valid]

    df_v["prob_up"] = p_v
    df_v["signal"] = "HOLD"
    df_v.loc[df_v.prob_up > threshold, "signal"] = "UP"
    df_v.loc[df_v.prob_up < (1 - threshold), "signal"] = "DOWN"

    # ── 逐窗口模拟 ──
    window_results = []
    for slug, grp in df_v.groupby("slug"):
        if slug not in settle:
            continue
        won = settle[slug]["won"]
        if won not in ("UP", "DOWN"):
            continue

        up_shares = 0.0
        dn_shares = 0.0
        up_cost = 0.0
        dn_cost = 0.0
        n_signals = 0

        for _, row in grp.iterrows():
            sig = row["signal"]
            if sig == "HOLD":
                continue

            n_signals += 1
            # 下注金额
            prob = row["prob_up"]
            conf = max(prob, 1 - prob)  # 置信度 [0.5, 1.0]
            if bet_mode == "confidence":
                # 置信度线性缩放: conf=阈值时→小注, conf=1.0时→2×base_bet
                # scale 范围: [0, 2] (阈值处≈0, 满置信度=2x)
                scale = (conf - threshold) / (1.0 - threshold) * 2.0 if conf > threshold else 0
                bet = base_bet * max(scale, 0.1)  # 最少 10% base_bet
            else:
                bet = base_bet

            if sig == "UP":
                price = row.get("up_ask", np.nan)
                if pd.isna(price) or price <= 0 or price >= 1:
                    price = 0.55  # fallback
                shares = bet / price
                up_shares += shares
                up_cost += bet
            else:  # DOWN
                price = row.get("dn_ask", np.nan)
                if pd.isna(price) or price <= 0 or price >= 1:
                    price = 0.55
                shares = bet / price
                dn_shares += shares
                dn_cost += bet

        total_cost = up_cost + dn_cost
        if total_cost == 0:
            continue

        # 结算: 赢方 shares 各兑 $1
        if won == "UP":
            payout = up_shares * 1.0
        else:  # DOWN
            payout = dn_shares * 1.0

        pnl = payout - total_cost

        window_results.append({
            "slug": slug,
            "won": won,
            "up_shares": round(up_shares, 2),
            "dn_shares": round(dn_shares, 2),
            "up_cost": round(up_cost, 2),
            "dn_cost": round(dn_cost, 2),
            "total_cost": round(total_cost, 2),
            "payout": round(payout, 2),
            "pnl": round(pnl, 2),
            "n_signals": n_signals,
            "roi": pnl / total_cost if total_cost > 0 else 0,
            "0x1d_pnl": settle[slug]["pnl"],
        })

    if not window_results:
        print("  无可匹配的窗口")
        return None

    wr = pd.DataFrame(window_results)

    # ── 汇总统计 ──
    total_pnl = wr.pnl.sum()
    total_cost = wr.total_cost.sum()
    total_payout = wr.payout.sum()
    win_rate = (wr.pnl > 0).mean()
    avg_pnl = wr.pnl.mean()
    median_pnl = wr.pnl.median()
    total_roi = total_pnl / total_cost if total_cost > 0 else 0
    actual_total_pnl = wr["0x1d_pnl"].sum()

    print(f"\n  模拟窗口数: {len(wr)}")
    print(f"  {'─' * 50}")
    print(f"  总投入:       ${total_cost:>12,.2f}")
    print(f"  总回收:       ${total_payout:>12,.2f}")
    print(f"  总 PnL:       ${total_pnl:>12,.2f}  (ROI: {total_roi:>+.2%})")
    print(f"  {'─' * 50}")
    print(f"  窗口胜率:     {win_rate:.2%} ({(wr.pnl > 0).sum()}/{len(wr)})")
    print(f"  平均 PnL:     ${avg_pnl:>+.2f}/窗口")
    print(f"  中位数 PnL:   ${median_pnl:>+.2f}/窗口")
    print(f"  最大盈利:     ${wr.pnl.max():>+.2f}")
    print(f"  最大亏损:     ${wr.pnl.min():>+.2f}")

    # 盈利/亏损窗口分别分析
    wins = wr[wr.pnl > 0]
    losses = wr[wr.pnl <= 0]
    if len(wins) > 0 and len(losses) > 0:
        avg_win = wins.pnl.mean()
        avg_loss = losses.pnl.mean()
        profit_factor = wins.pnl.sum() / abs(losses.pnl.sum()) if losses.pnl.sum() != 0 else float('inf')
        print(f"  {'─' * 50}")
        print(f"  盈利窗口均值: ${avg_win:>+.2f}  |  亏损窗口均值: ${avg_loss:>+.2f}")
        print(f"  盈亏比:       {profit_factor:.2f}")

    # ── 与 0x1d 真实 PnL 对比 ──
    print(f"\n  ── 与 0x1d 实际对比 ──")
    print(f"  0x1d 总 PnL:  ${actual_total_pnl:>12,.2f}")
    print(f"  模型总 PnL:   ${total_pnl:>12,.2f}")
    diff = total_pnl - actual_total_pnl
    print(f"  差异:         ${diff:>+12,.2f} ({'模型更优' if diff > 0 else '0x1d更优'})")

    # ── Top/Bottom 窗口明细 ──
    wr_sorted = wr.sort_values("pnl", ascending=False)
    print(f"\n  盈利最多 (Top 5):")
    for _, row in wr_sorted.head(5).iterrows():
        short = row.slug.split("-")[-1] if "-" in str(row.slug) else str(row.slug)
        bias = "UP" if row.up_cost > row.dn_cost else "DOWN"
        print(f"    {short}: PnL=${row.pnl:>+.2f} 下注${row.total_cost:.0f} "
              f"方向={bias} 赢方={row.won} 0x1d=${row['0x1d_pnl']:>+.2f}")
    print(f"  亏损最多 (Bottom 5):")
    for _, row in wr_sorted.tail(5).iterrows():
        short = row.slug.split("-")[-1] if "-" in str(row.slug) else str(row.slug)
        bias = "UP" if row.up_cost > row.dn_cost else "DOWN"
        print(f"    {short}: PnL=${row.pnl:>+.2f} 下注${row.total_cost:.0f} "
              f"方向={bias} 赢方={row.won} 0x1d=${row['0x1d_pnl']:>+.2f}")

    return {
        "total_pnl": total_pnl,
        "total_roi": total_roi,
        "win_rate": win_rate,
        "avg_pnl": avg_pnl,
        "n_windows": len(wr),
        "actual_pnl": actual_total_pnl,
        "window_df": wr,
    }


# ══════════════════════════════════════════════════════════════
# 7. 保存模型
# ══════════════════════════════════════════════════════════════

def save_model(model, eval_results: dict, threshold: float):
    """保存模型 + 配置, 用于生产环境推理"""
    model_path = MODEL_DIR / "signal_model.pkl"
    with open(model_path, "wb") as f:
        pickle.dump(model, f)

    features_used = eval_results.get("features_used", SIGNAL_FEATURES)
    model_label = eval_results.get("label", "全特征")

    config = {
        "features": list(features_used),
        "feature_groups": {
            "bn_momentum": BN_MOMENTUM,
            "bn_volatility": BN_VOLATILITY,
            "bn_advanced": BN_ADVANCED,
            "cl_momentum": CL_MOMENTUM,
            "cl_volatility": CL_VOLATILITY,
            "cl_advanced": CL_ADVANCED,
            "cross_source": CROSS_SOURCE,
            "pm_orderbook": PM_ORDERBOOK,
            "window_context": WINDOW_CONTEXT,
        },
        "threshold": threshold,
        "cv_accuracy": eval_results["cv_acc"],
        "cv_auc": eval_results["cv_auc"],
        "cv_f1": eval_results["cv_f1"],
        "n_features": len(features_used),
        "model_type": model_label,
        "pure_market": "持仓" not in model_label,
        "training_label": eval_results.get("training_mode", "mimicry"),
        "excluded_features": EXCLUDED,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }

    config_path = MODEL_DIR / "signal_config.json"
    with open(config_path, "w") as f:
        json.dump(config, f, indent=2, ensure_ascii=False)

    kb = model_path.stat().st_size / 1024
    print(f"\n模型已保存:")
    print(f"  {model_path} ({kb:.0f} KB)")
    print(f"  {config_path}")

    # 推理示例
    print(f"""
╔══════════════════════════════════════════════════════════════════╗
║  生产推理代码示例                                                ║
╠══════════════════════════════════════════════════════════════════╣
║                                                                  ║
║  import pickle, json, numpy as np                                ║
║  model = pickle.load(open('signal_model.pkl', 'rb'))             ║
║  cfg = json.load(open('signal_config.json'))                     ║
║  threshold = cfg['threshold']                                    ║
║                                                                  ║
║  # 从实时数据采集 (BN WebSocket + CL RTDS + PM API)             ║
║  features = collect_market_features()                            ║
║  x = np.array([[features[f] for f in cfg['features']]])          ║
║                                                                  ║
║  prob_up = model.predict_proba(x)[0, 1]                          ║
║  if prob_up > threshold:                                         ║
║      signal, conf = 'UP', prob_up                                ║
║  elif prob_up < (1 - threshold):                                 ║
║      signal, conf = 'DOWN', 1 - prob_up                          ║
║  else:                                                           ║
║      signal, conf = 'HOLD', 0.5                                  ║
║                                                                  ║
║  print(f'Signal: {{signal}} (confidence: {{conf:.1%}})')         ║
╚══════════════════════════════════════════════════════════════════╝
""")


# ══════════════════════════════════════════════════════════════
# 8. 主函数
# ══════════════════════════════════════════════════════════════

def main():
    # ── 参数解析 ──
    report_only = "--report" in sys.argv
    rich_only = "--rich-only" in sys.argv
    pure_market = "--pure-market" in sys.argv
    compare_mode = "--compare" in sys.argv
    outcome_mode = "--outcome" in sys.argv
    threshold = 0.60
    for i, arg in enumerate(sys.argv):
        if arg == "--threshold" and i + 1 < len(sys.argv):
            threshold = float(sys.argv[i + 1])

    print("=" * 72)
    print("  0x1d 实时信号蒸馏 — Real-time Entry Signal Model")
    if outcome_mode:
        print("  目标: 预测窗口结算赢方, 以 PnL 为核心评估指标")
    else:
        print("  目标: 学习 0x1d 的入场时机和方向, 实时输出 UP/DOWN/HOLD")
    print("=" * 72)
    if rich_only:
        print("  [模式] 仅使用有 ref_ts 的高质量数据")
    if pure_market:
        print("  [模式] 纯市场特征 (排除持仓/行为/burst 自相关特征)")
    if outcome_mode:
        print("  [模式] 结算标签 (训练目标=窗口赢方, 而非模仿0x1d)")
    if compare_mode:
        print("  [模式] 对比: 全特征 vs 纯市场特征")

    # ── 加载数据 ──
    df = load_trades(rich_only=rich_only)

    # ── 加载结算数据 (PnL 评估 + outcome 标签) ──
    settle = load_settlement_labels()

    # ── 突发聚类 ──
    df = deduplicate_bursts(df)

    # ── 特征分析 ──
    feat_results = analyze_features(df)

    if report_only:
        print("\n[--report 模式, 跳过训练]")
        return

    # outcome 模式的标签
    outcome_labels = settle if outcome_mode else None

    # ── 对比模式: 同时训练两个模型 ──
    if compare_mode:
        print("\n" + "#" * 72)
        print("#  对比模式: 全特征 vs 纯市场特征")
        if outcome_mode:
            print("#  训练标签: 窗口结算结果")
        print("#" * 72)

        m_all, r_all = train_signal_model(
            df, threshold=threshold,
            feature_set=SIGNAL_FEATURES, label="全特征(含持仓)",
            outcome_labels=outcome_labels,
        )
        m_pure, r_pure = train_signal_model(
            df, threshold=threshold,
            feature_set=PURE_MARKET_FEATURES, label="纯市场特征",
            outcome_labels=outcome_labels,
        )

        # 对比汇总
        print("\n" + "=" * 72)
        print("  对比结果: 全特征 vs 纯市场特征")
        print("=" * 72)
        print(f"{'指标':<16} {'全特征(含持仓)':>16} {'纯市场特征':>16} {'差异':>10}")
        print("-" * 60)
        for metric, key in [("CV 准确率", "cv_acc"), ("CV AUC", "cv_auc"), ("CV F1", "cv_f1")]:
            v_all = r_all[key]
            v_pure = r_pure[key]
            diff = v_pure - v_all
            print(f"  {metric:<14} {v_all:>16.4f} {v_pure:>16.4f} {diff:>+10.4f}")
        print(f"  {'特征数':<14} {len(SIGNAL_FEATURES):>16} {len(PURE_MARKET_FEATURES):>16}")
        print(f"  {'最佳阈值':<14} {r_all['best_threshold']:>16.2f} {r_pure['best_threshold']:>16.2f}")

        # Top 特征对比
        print(f"\n  全特征 Top-10:")
        for _, row in r_all["feature_importance"].head(10).iterrows():
            leaked = " ⚠ 自相关" if row["feature"] in POSITION_BEHAVIOR + BURST_AGGREGATE else ""
            print(f"    {row['feature']:<32} {row['importance']:>6.0f}{leaked}")
        print(f"\n  纯市场 Top-10:")
        for _, row in r_pure["feature_importance"].head(10).iterrows():
            print(f"    {row['feature']:<32} {row['importance']:>6.0f}")

        # PnL 模拟对比
        print("\n" + "#" * 72)
        print("#  PnL 模拟对比")
        print("#" * 72)
        df_all = r_all.get("df_used", df)
        df_pure = r_pure.get("df_used", df)
        pnl_all = simulate_window_pnl(df_all, r_all["all_probs"], r_all["best_threshold"], settle, "fixed")
        pnl_pure = simulate_window_pnl(df_pure, r_pure["all_probs"], r_pure["best_threshold"], settle, "fixed")

        if pnl_all and pnl_pure:
            print(f"\n  ── PnL 对比总结 (fixed $10/信号) ──")
            print(f"  {'指标':<16} {'全特征':>16} {'纯市场':>16}")
            print(f"  {'-' * 48}")
            print(f"  {'总 PnL':<14} ${pnl_all['total_pnl']:>+14,.2f} ${pnl_pure['total_pnl']:>+14,.2f}")
            print(f"  {'ROI':<14} {pnl_all['total_roi']:>+15.2%} {pnl_pure['total_roi']:>+15.2%}")
            print(f"  {'窗口胜率':<12} {pnl_all['win_rate']:>15.2%} {pnl_pure['win_rate']:>15.2%}")

        print(f"\n  结论:")
        if r_pure["cv_acc"] >= r_all["cv_acc"] - 0.01:
            print(f"  ✓ 纯市场特征效果相当或更好 — 持仓特征是噪声/自相关, 可安全移除")
        elif r_pure["cv_acc"] >= 0.52:
            print(f"  ◐ 纯市场特征有轻微下降但仍有信号 — 建议使用纯市场模型(更可靠)")
        else:
            print(f"  ✗ 纯市场特征效果显著下降 — 当前数据中市场特征区分度不足")
            print(f"    建议: 积累更多数据 / 增加新特征维度 (如 orderbook depth, funding rate)")

        # 保存纯市场模型为主模型
        print(f"\n  保存纯市场模型作为生产模型...")
        save_model(m_pure, r_pure, r_pure["best_threshold"])
        return

    # ── 常规训练 ──
    if pure_market or outcome_mode:
        features = PURE_MARKET_FEATURES
        label = "纯市场特征"
        if outcome_mode and not pure_market:
            print("  [自动] --outcome 模式默认使用纯市场特征 (避免泄漏)")
    else:
        features = SIGNAL_FEATURES
        label = "全特征(含持仓)"

    model, eval_results = train_signal_model(
        df, threshold=threshold, feature_set=features, label=label,
        outcome_labels=outcome_labels,
    )

    # ── 信号回测 (方向准确率, 辅助参考) ──
    best_t = eval_results.get("best_threshold", threshold)
    df_used = eval_results.get("df_used", df)
    backtest_signals(df_used, eval_results["all_probs"], best_t)

    # ── ⭐ PnL 模拟 (核心评估) ──
    pnl_fixed = simulate_window_pnl(df_used, eval_results["all_probs"], best_t, settle, "fixed")
    pnl_conf = simulate_window_pnl(df_used, eval_results["all_probs"], best_t, settle, "confidence")

    # ── 保存 ──
    save_model(model, eval_results, best_t)

    # ── 总结 ──
    print("\n" + "=" * 72)
    print("  训练完成!")
    print(f"  模式:      {label}")
    print(f"  训练标签:  {'窗口结算结果 (outcome)' if outcome_mode else '模仿0x1d方向 (mimicry)'}")
    print(f"  CV 准确率: {eval_results['cv_acc']:.4f} (随机基准: 0.5000)")
    print(f"  CV AUC:    {eval_results['cv_auc']:.4f}")
    print(f"  最佳阈值:  {best_t:.2f}")
    if pnl_fixed:
        print(f"  ── PnL 核心指标 (fixed $10/笔) ──")
        print(f"  模型 PnL:  ${pnl_fixed['total_pnl']:>+,.2f} (ROI: {pnl_fixed['total_roi']:>+.2%})")
        print(f"  窗口胜率:  {pnl_fixed['win_rate']:.2%}")
        print(f"  0x1d PnL:  ${pnl_fixed['actual_pnl']:>+,.2f}")
    if pnl_conf:
        print(f"  ── PnL 核心指标 (confidence缩放) ──")
        print(f"  模型 PnL:  ${pnl_conf['total_pnl']:>+,.2f} (ROI: {pnl_conf['total_roi']:>+.2%})")
        print(f"  窗口胜率:  {pnl_conf['win_rate']:.2%}")
    print(f"  {'─' * 50}")
    print(f"  信号逻辑:  P(UP) > {best_t:.2f} → 买UP")
    print(f"              P(UP) < {1-best_t:.2f} → 买DOWN")
    print(f"              其余 → HOLD (不交易)")
    print("=" * 72)
    if not pure_market and not outcome_mode:
        print("\n  ⚠ 注意: 当前使用全特征(含持仓), Top特征可能是自相关而非市场信号")
        print("  建议运行: python scripts/distill_signal.py --outcome --pure-market")
    if not outcome_mode:
        print("\n  💡 推荐: 使用 --outcome 模式, 以窗口盈亏为训练目标:")
        print("     python scripts/distill_signal.py --outcome --rich-only")
    print("\n  下一步:")
    print("  1. 让 monitor_0x1d.py 持续运行收集更多数据 (目标: 7天+)")
    print("  2. 推荐训练命令: python scripts/distill_signal.py --outcome --rich-only")
    print("  3. 将模型集成到交易机器人, 替代手工规则")


if __name__ == "__main__":
    main()
