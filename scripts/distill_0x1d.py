"""
0x1d 策略蒸馏 (Strategy Distillation)
=====================================
从 monitor_0x1d.py 收集的 trade_snaps + settlements 数据中训练模型,
学习 0x1d 的交易决策模式。

三个独立模型:
  1. 方向模型 (Direction): 窗口内 0x1d 买 UP 还是 DOWN 为主?
  2. 时机模型 (Timing):    窗口 elapsed 多久时开始/加仓?
  3. 仓位模型 (Sizing):    每笔下多少 shares?

用法:
  python scripts/distill_0x1d.py              # 完整训练 + 评估
  python scripts/distill_0x1d.py --report     # 只看数据报告
  python scripts/distill_0x1d.py --predict    # 用最新市场数据预测
"""

import sqlite3
import json
import sys
import os
import pickle
import math
from pathlib import Path
from datetime import datetime, timezone
from collections import defaultdict

import numpy as np
import pandas as pd
from sklearn.model_selection import TimeSeriesSplit, cross_val_score
from sklearn.metrics import (
    accuracy_score, classification_report, mean_absolute_error,
    roc_auc_score, f1_score, confusion_matrix,
)
from sklearn.preprocessing import LabelEncoder
import lightgbm as lgb

# ── 路径 ──
ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "data" / "0x1d_data.db"
MODEL_DIR = ROOT / "data" / "distill_models"
MODEL_DIR.mkdir(parents=True, exist_ok=True)


# ========================================================================
# 1. 数据加载 & 特征工程
# ========================================================================

def load_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    """加载 trade_snaps + settlements, 返回 (snaps_df, settle_df)"""
    db = sqlite3.connect(str(DB_PATH))
    db.row_factory = sqlite3.Row

    # ── trade_snaps ──
    rows = db.execute(
        "SELECT ts, slug, side, shares, price, usdc, features FROM trade_snaps ORDER BY ts"
    ).fetchall()
    records = []
    for r in rows:
        feat = json.loads(r["features"]) if r["features"] else {}
        rec = {
            "ts": r["ts"],
            "slug": r["slug"],
            "side": r["side"],
            "shares": r["shares"],
            "price": r["price"],
            "usdc": r["usdc"],
        }
        rec.update(feat)
        records.append(rec)
    snaps_df = pd.DataFrame(records)

    # ── settlements ──
    settle_rows = db.execute(
        "SELECT * FROM settlements ORDER BY settled_at"
    ).fetchall()
    settle_records = []
    for r in settle_rows:
        rec = {col: r[col] for col in r.keys()}
        settle_records.append(rec)
    settle_df = pd.DataFrame(settle_records)

    db.close()
    print(f"加载: {len(snaps_df)} 笔交易, {len(settle_df)} 个窗口结算")
    return snaps_df, settle_df


def build_window_features(snaps_df: pd.DataFrame, settle_df: pd.DataFrame) -> pd.DataFrame:
    """
    按窗口(slug)聚合交易级特征 → 窗口级特征 + 结算标签。
    每个窗口一行, 用于方向模型训练。
    """
    # 数值型特征列 (排除元数据)
    meta_cols = {"ts", "slug", "side", "shares", "price", "usdc",
                 "ref_ts", "feature_latency", "ts_source",
                 "elapsed", "elapsed_pct", "cum_trades",
                 "cum_up_shares", "cum_dn_shares", "cum_up_cost", "cum_dn_cost",
                 "avg_up_price", "avg_dn_price", "pos_imbalance",
                 "trade_velocity", "time_since_last", "same_side_streak",
                 "burst_seq", "is_burst"}
    num_cols = [c for c in snaps_df.columns
                if c not in meta_cols and snaps_df[c].dtype in ("float64", "int64", "float32")]

    window_records = []
    for slug, group in snaps_df.groupby("slug"):
        if len(group) < 2:
            continue

        rec = {"slug": slug, "n_trades": len(group)}

        # ── 首笔交易特征 (决策入场信号) ──
        first = group.iloc[0]
        for col in num_cols:
            if col in first.index and pd.notna(first[col]):
                rec[f"first_{col}"] = first[col]

        # ── 前 N 笔均值 (早期信号) ──
        early = group.head(min(5, len(group)))
        for col in num_cols:
            if col in early.columns:
                rec[f"early_mean_{col}"] = early[col].mean()

        # ── 全窗口统计 ──
        for col in num_cols:
            if col in group.columns:
                vals = group[col].dropna()
                if len(vals) > 0:
                    rec[f"win_mean_{col}"] = vals.mean()
                    rec[f"win_std_{col}"] = vals.std() if len(vals) > 1 else 0
                    rec[f"win_max_{col}"] = vals.max()
                    rec[f"win_min_{col}"] = vals.min()

        # ── 方向分布 ──
        up_cnt = (group["side"] == "UP").sum()
        dn_cnt = (group["side"] == "DOWN").sum()
        rec["up_ratio"] = up_cnt / len(group)
        rec["dn_ratio"] = dn_cnt / len(group)
        rec["dominant_side"] = "UP" if up_cnt >= dn_cnt else "DOWN"

        # ── 时间分布 ──
        if "elapsed" in group.columns:
            rec["first_entry_elapsed"] = group["elapsed"].iloc[0]
            rec["last_entry_elapsed"] = group["elapsed"].iloc[-1]
            rec["entry_span"] = rec["last_entry_elapsed"] - rec["first_entry_elapsed"]
            if len(group) > 1:
                intervals = group["elapsed"].diff().dropna()
                rec["mean_interval"] = intervals.mean()
                rec["median_interval"] = intervals.median()

        # ── 仓位特征 ──
        if "cum_up_shares" in group.columns:
            last = group.iloc[-1]
            rec["final_up_shares"] = last.get("cum_up_shares", 0)
            rec["final_dn_shares"] = last.get("cum_dn_shares", 0)
            rec["final_gap"] = rec["final_up_shares"] - rec["final_dn_shares"]
            total = max(rec["final_up_shares"], rec["final_dn_shares"], 1)
            rec["final_gap_pct"] = rec["final_gap"] / total

        window_records.append(rec)

    win_df = pd.DataFrame(window_records)

    # ── 合并结算标签 ──
    if len(settle_df) > 0:
        win_df = win_df.merge(
            settle_df[["slug", "won", "pnl", "cost", "payout", "trades",
                        "btc_start", "btc_end", "btc_move", "btc_vol_window"]],
            on="slug", how="inner"
        )
        print(f"窗口特征: {len(win_df)} 个窗口 (有结算标签)")
    else:
        print(f"窗口特征: {len(win_df)} 个窗口 (无结算标签)")

    return win_df


def build_trade_features(snaps_df: pd.DataFrame, settle_df: pd.DataFrame) -> pd.DataFrame:
    """
    为每笔交易构建训练样本, 用于时机/仓位模型。
    标签来自同窗口结算结果。
    """
    if len(settle_df) == 0:
        return snaps_df.copy()

    # 合并结算标签
    settle_label = settle_df[["slug", "won", "pnl"]].copy()
    settle_label = settle_label.rename(columns={"pnl": "window_pnl"})
    df = snaps_df.merge(settle_label, on="slug", how="inner")

    # 每笔交易是否与赢面一致
    df["trade_correct"] = ((df["side"] == df["won"]).astype(int))

    print(f"交易级特征: {len(df)} 笔 (有结算标签)")
    return df


# ========================================================================
# 2. 模型: 方向预测 (窗口级)
# ========================================================================

# 方向模型使用的特征 (仅首笔 + 早期特征, 避免未来泄漏)
DIRECTION_FEATURES = [
    # 首笔入场时的 BTC 状态
    "first_btc_price", "first_btc_delta_ptb", "first_btc_delta_pct",
    "first_btc_vol_10s", "first_btc_vol_30s",
    # 首笔入场时的动量
    "first_mom_1s", "first_mom_3s", "first_mom_5s", "first_mom_10s",
    "first_mom_15s", "first_mom_30s",
    # 首笔 Binance 动量
    "first_bn_mom_1s", "first_bn_mom_3s", "first_bn_mom_5s", "first_bn_mom_10s",
    # 首笔 PM 报价
    "first_up_price", "first_dn_price", "first_pm_edge", "first_pm_spread",
    # 首笔 CL-BN 差异
    "first_cl_bn_spread", "first_cl_bn_mom_diff_5s",
    # 首笔趋势
    "first_cl_trend_30s", "first_cl_trend_60s",
    "first_bn_trend_30s",
    # 首笔百分位
    "first_cl_pctl_60s", "first_cl_pctl_300s",
    # 首笔 Z-score
    "first_cl_mom_z_5s", "first_cl_mom_z_10s",
    # 首笔震荡
    "first_cl_dir_changes_30s", "first_cl_dir_changes_60s",
    # 首笔加速度
    "first_mom_accel_5s", "first_mom_accel_10s",
    # 首笔 BTC vs PTB
    "first_btc_above_ptb",
    # 入场时间
    "first_entry_elapsed",
    # 早期均值
    "early_mean_mom_5s", "early_mean_mom_10s",
    "early_mean_bn_mom_5s", "early_mean_bn_mom_10s",
    "early_mean_btc_vol_30s",
    "early_mean_cl_trend_30s",
]


def train_direction_model(win_df: pd.DataFrame):
    """训练方向预测模型: 给定窗口开始时的市场状态, 预测 0x1d 的主要方向"""
    print("\n" + "=" * 60)
    print("模型 1: 方向预测 (Direction)")
    print("=" * 60)

    if "won" not in win_df.columns:
        print("⚠ 无结算标签, 跳过")
        return None

    # 标签: 0x1d 在该窗口的主导方向
    df = win_df.copy()
    df["label"] = (df["dominant_side"] == "UP").astype(int)

    # 可用特征
    available = [f for f in DIRECTION_FEATURES if f in df.columns]
    missing = [f for f in DIRECTION_FEATURES if f not in df.columns]
    if missing:
        print(f"  缺失特征 ({len(missing)}): {missing[:5]}...")

    X = df[available].fillna(0)
    y = df["label"]

    print(f"  样本: {len(X)}, 特征: {len(available)}")
    print(f"  UP: {y.sum()}, DOWN: {len(y) - y.sum()}")

    if len(X) < 10:
        print("  ⚠ 样本不足, 跳过训练")
        return None

    # LightGBM
    params = {
        "objective": "binary",
        "metric": "binary_logloss",
        "n_estimators": 200,
        "max_depth": 4,
        "learning_rate": 0.05,
        "num_leaves": 15,
        "min_child_samples": 3,
        "subsample": 0.8,
        "colsample_bytree": 0.8,
        "reg_alpha": 0.1,
        "reg_lambda": 1.0,
        "random_state": 42,
        "verbosity": -1,
    }

    model = lgb.LGBMClassifier(**params)

    # 时序交叉验证
    n_splits = min(5, len(X) // 10)
    if n_splits >= 2:
        tscv = TimeSeriesSplit(n_splits=n_splits)
        scores = cross_val_score(model, X, y, cv=tscv, scoring="accuracy")
        print(f"  交叉验证准确率: {scores.mean():.3f} ± {scores.std():.3f}")
        print(f"  各折: {[f'{s:.3f}' for s in scores]}")

    # 全量训练
    model.fit(X, y)

    # 训练集表现
    y_pred = model.predict(X)
    y_proba = model.predict_proba(X)[:, 1]
    print(f"\n  训练集准确率: {accuracy_score(y, y_pred):.3f}")
    if len(set(y)) == 2:
        print(f"  训练集 AUC: {roc_auc_score(y, y_proba):.3f}")

    # 特征重要性
    importance = pd.DataFrame({
        "feature": available,
        "importance": model.feature_importances_
    }).sort_values("importance", ascending=False)
    print(f"\n  Top-10 重要特征:")
    for _, row in importance.head(10).iterrows():
        print(f"    {row['feature']:40s} {row['importance']:>5.0f}")

    # 保存
    model_path = MODEL_DIR / "direction_model.pkl"
    with open(model_path, "wb") as f:
        pickle.dump({"model": model, "features": available, "params": params}, f)
    print(f"\n  模型已保存: {model_path}")

    return model


# ========================================================================
# 3. 模型: 时机预测 (交易级)
# ========================================================================

TIMING_FEATURES = [
    # BTC 状态
    "btc_price", "btc_delta_ptb", "btc_delta_pct",
    "btc_vol_10s", "btc_vol_30s", "btc_vol_60s",
    # 动量
    "mom_1s", "mom_3s", "mom_5s", "mom_10s", "mom_15s", "mom_30s",
    "mom_60s", "mom_120s",
    # Binance 动量
    "bn_mom_1s", "bn_mom_3s", "bn_mom_5s", "bn_mom_10s",
    "bn_mom_30s", "bn_mom_60s",
    # PM 报价
    "up_price", "dn_price", "pm_edge", "pm_spread",
    "up_bid", "up_ask", "dn_bid", "dn_ask",
    "up_ba_spread", "dn_ba_spread",
    # 加速度
    "mom_accel_5s", "mom_accel_10s",
    "bn_mom_accel_5s",
    # 趋势
    "cl_trend_30s", "cl_trend_60s", "cl_trend_120s",
    "bn_trend_30s", "bn_trend_60s",
    # 百分位
    "cl_pctl_60s", "cl_pctl_300s",
    # Z-score
    "cl_mom_z_5s", "cl_mom_z_10s",
    # 震荡
    "cl_dir_changes_30s", "cl_dir_changes_60s",
    # CL-BN
    "cl_bn_spread", "cl_bn_mom_diff_5s", "cl_bn_mom_diff_10s",
    # 信号
    "btc_above_ptb",
    # 窗口进度
    "elapsed", "elapsed_pct",
    # 仓位
    "pos_imbalance", "cum_trades", "trade_velocity",
]


def train_timing_model(trade_df: pd.DataFrame):
    """
    训练时机模型: 给定当前市场状态, 预测 0x1d 是否会在此刻下单。
    思路: 正样本 = 0x1d 实际下单的时刻, 负样本 = 窗口内未下单的时刻 (需要构造)
    简化版: 预测每笔交易是否与最终赢面一致 (trade_correct)
    """
    print("\n" + "=" * 60)
    print("模型 2: 交易质量预测 (Trade Quality)")
    print("=" * 60)

    if "trade_correct" not in trade_df.columns:
        print("  ⚠ 无标签, 跳过")
        return None

    df = trade_df.copy()
    available = [f for f in TIMING_FEATURES if f in df.columns]
    X = df[available].fillna(0)
    y = df["trade_correct"]

    print(f"  样本: {len(X)}, 特征: {len(available)}")
    print(f"  正确交易: {y.sum()} ({y.mean():.1%}), 错误: {len(y) - y.sum()}")

    if len(X) < 20:
        print("  ⚠ 样本不足, 跳过")
        return None

    params = {
        "objective": "binary",
        "n_estimators": 300,
        "max_depth": 5,
        "learning_rate": 0.03,
        "num_leaves": 20,
        "min_child_samples": 5,
        "subsample": 0.7,
        "colsample_bytree": 0.7,
        "reg_alpha": 0.5,
        "reg_lambda": 2.0,
        "random_state": 42,
        "verbosity": -1,
    }

    model = lgb.LGBMClassifier(**params)

    # 时序交叉验证
    n_splits = min(5, len(X) // 50)
    if n_splits >= 2:
        tscv = TimeSeriesSplit(n_splits=n_splits)
        scores = cross_val_score(model, X, y, cv=tscv, scoring="accuracy")
        print(f"  交叉验证准确率: {scores.mean():.3f} ± {scores.std():.3f}")

    model.fit(X, y)
    y_pred = model.predict(X)
    y_proba = model.predict_proba(X)[:, 1]
    print(f"  训练集准确率: {accuracy_score(y, y_pred):.3f}")
    if len(set(y)) == 2:
        print(f"  训练集 AUC: {roc_auc_score(y, y_proba):.3f}")

    importance = pd.DataFrame({
        "feature": available,
        "importance": model.feature_importances_
    }).sort_values("importance", ascending=False)
    print(f"\n  Top-10 重要特征:")
    for _, row in importance.head(10).iterrows():
        print(f"    {row['feature']:40s} {row['importance']:>5.0f}")

    model_path = MODEL_DIR / "trade_quality_model.pkl"
    with open(model_path, "wb") as f:
        pickle.dump({"model": model, "features": available, "params": params}, f)
    print(f"\n  模型已保存: {model_path}")

    return model


# ========================================================================
# 4. 模型: 仓位预测 (交易级)
# ========================================================================

def train_sizing_model(trade_df: pd.DataFrame):
    """训练仓位模型: 预测 0x1d 每笔下单的 shares 数量"""
    print("\n" + "=" * 60)
    print("模型 3: 仓位预测 (Sizing)")
    print("=" * 60)

    df = trade_df.copy()
    available = [f for f in TIMING_FEATURES if f in df.columns]
    # 加入 side 编码
    df["side_enc"] = (df["side"] == "UP").astype(int)
    feat_cols = available + ["side_enc"]

    X = df[feat_cols].fillna(0)
    y = df["shares"]

    print(f"  样本: {len(X)}, 特征: {len(feat_cols)}")
    print(f"  Shares: mean={y.mean():.1f}, median={y.median():.1f}, "
          f"std={y.std():.1f}, min={y.min():.1f}, max={y.max():.1f}")

    if len(X) < 20:
        print("  ⚠ 样本不足, 跳过")
        return None

    params = {
        "objective": "regression",
        "metric": "mae",
        "n_estimators": 300,
        "max_depth": 5,
        "learning_rate": 0.03,
        "num_leaves": 20,
        "min_child_samples": 5,
        "subsample": 0.7,
        "colsample_bytree": 0.7,
        "reg_alpha": 0.5,
        "reg_lambda": 2.0,
        "random_state": 42,
        "verbosity": -1,
    }

    model = lgb.LGBMRegressor(**params)

    n_splits = min(5, len(X) // 50)
    if n_splits >= 2:
        tscv = TimeSeriesSplit(n_splits=n_splits)
        scores = cross_val_score(model, X, y, cv=tscv, scoring="neg_mean_absolute_error")
        print(f"  交叉验证 MAE: {-scores.mean():.2f} ± {scores.std():.2f}")

    model.fit(X, y)
    y_pred = model.predict(X)
    mae = mean_absolute_error(y, y_pred)
    print(f"  训练集 MAE: {mae:.2f} shares")

    importance = pd.DataFrame({
        "feature": feat_cols,
        "importance": model.feature_importances_
    }).sort_values("importance", ascending=False)
    print(f"\n  Top-10 重要特征:")
    for _, row in importance.head(10).iterrows():
        print(f"    {row['feature']:40s} {row['importance']:>5.0f}")

    model_path = MODEL_DIR / "sizing_model.pkl"
    with open(model_path, "wb") as f:
        pickle.dump({"model": model, "features": feat_cols, "params": params}, f)
    print(f"\n  模型已保存: {model_path}")

    return model


# ========================================================================
# 5. 数据报告
# ========================================================================

def print_data_report(snaps_df: pd.DataFrame, settle_df: pd.DataFrame, win_df: pd.DataFrame):
    """打印数据质量报告"""
    print("\n" + "=" * 60)
    print("数据质量报告")
    print("=" * 60)

    # 时间范围
    if "ts" in snaps_df.columns and len(snaps_df) > 0:
        t0 = datetime.fromtimestamp(snaps_df["ts"].min(), tz=timezone.utc)
        t1 = datetime.fromtimestamp(snaps_df["ts"].max(), tz=timezone.utc)
        hours = (snaps_df["ts"].max() - snaps_df["ts"].min()) / 3600
        print(f"\n  时间范围: {t0.strftime('%Y-%m-%d %H:%M')} → {t1.strftime('%Y-%m-%d %H:%M')}")
        print(f"  数据跨度: {hours:.1f} 小时")

    print(f"\n  交易总数: {len(snaps_df)}")
    print(f"  窗口总数: {len(settle_df)}")
    print(f"  每窗口平均交易: {len(snaps_df) / max(len(settle_df), 1):.1f}")

    # 方向分布
    if "side" in snaps_df.columns:
        up = (snaps_df["side"] == "UP").sum()
        dn = (snaps_df["side"] == "DOWN").sum()
        print(f"\n  UP 交易: {up} ({up / len(snaps_df):.1%})")
        print(f"  DOWN 交易: {dn} ({dn / len(snaps_df):.1%})")

    # 结算统计
    if "won" in settle_df.columns and len(settle_df) > 0:
        print(f"\n  结算结果:")
        for won, group in settle_df.groupby("won"):
            print(f"    {won}: {len(group)} 窗口, "
                  f"avg_pnl=${group['pnl'].mean():.2f}, "
                  f"total=${group['pnl'].sum():.2f}")

        # 胜率
        if "pnl" in settle_df.columns:
            win_rate = (settle_df["pnl"] > 0).mean()
            print(f"\n  0x1d 窗口胜率: {win_rate:.1%}")
            print(f"  总 PnL: ${settle_df['pnl'].sum():.2f}")
            print(f"  平均 PnL: ${settle_df['pnl'].mean():.2f}")

    # 特征质量
    if "ref_ts" in snaps_df.columns:
        has_ref = snaps_df["ref_ts"].notna().sum()
        print(f"\n  有时间修正的交易: {has_ref} ({has_ref / len(snaps_df):.1%})")
    if "feature_latency" in snaps_df.columns:
        lat = snaps_df["feature_latency"].dropna()
        if len(lat) > 0:
            print(f"  特征延迟: avg={lat.mean():.1f}s, "
                  f"median={lat.median():.1f}s, max={lat.max():.1f}s")

    # 特征覆盖
    num_cols = snaps_df.select_dtypes(include=[np.number]).columns
    zero_pct = (snaps_df[num_cols] == 0).mean()
    bad_features = zero_pct[zero_pct > 0.8].sort_values(ascending=False)
    if len(bad_features) > 0:
        print(f"\n  ⚠ 高零值特征 (>80% 为零):")
        for feat, pct in bad_features.head(10).items():
            print(f"    {feat}: {pct:.0%} 为零")


# ========================================================================
# 6. 窗口级盈亏预测 (0x1d 会赚还是亏)
# ========================================================================

def train_pnl_model(win_df: pd.DataFrame):
    """预测窗口盈亏方向: 0x1d 在该窗口是赚钱还是亏钱"""
    print("\n" + "=" * 60)
    print("模型 4: 窗口盈亏预测 (Win/Loss)")
    print("=" * 60)

    if "pnl" not in win_df.columns:
        print("  ⚠ 无 PnL 标签, 跳过")
        return None

    df = win_df.copy()
    df["label"] = (df["pnl"] > 0).astype(int)

    # 使用首笔 + 早期特征
    available = [f for f in DIRECTION_FEATURES if f in df.columns]
    X = df[available].fillna(0)
    y = df["label"]

    print(f"  样本: {len(X)}, 特征: {len(available)}")
    print(f"  盈利窗口: {y.sum()} ({y.mean():.1%}), 亏损: {len(y) - y.sum()}")

    if len(X) < 10:
        print("  ⚠ 样本不足, 跳过")
        return None

    params = {
        "objective": "binary",
        "n_estimators": 150,
        "max_depth": 3,
        "learning_rate": 0.05,
        "num_leaves": 8,
        "min_child_samples": 3,
        "subsample": 0.8,
        "colsample_bytree": 0.8,
        "reg_alpha": 0.5,
        "reg_lambda": 2.0,
        "random_state": 42,
        "verbosity": -1,
    }

    model = lgb.LGBMClassifier(**params)

    n_splits = min(5, len(X) // 8)
    if n_splits >= 2:
        tscv = TimeSeriesSplit(n_splits=n_splits)
        scores = cross_val_score(model, X, y, cv=tscv, scoring="accuracy")
        print(f"  交叉验证准确率: {scores.mean():.3f} ± {scores.std():.3f}")

    model.fit(X, y)
    y_pred = model.predict(X)
    print(f"  训练集准确率: {accuracy_score(y, y_pred):.3f}")

    importance = pd.DataFrame({
        "feature": available,
        "importance": model.feature_importances_
    }).sort_values("importance", ascending=False)
    print(f"\n  Top-10 重要特征:")
    for _, row in importance.head(10).iterrows():
        print(f"    {row['feature']:40s} {row['importance']:>5.0f}")

    model_path = MODEL_DIR / "pnl_model.pkl"
    with open(model_path, "wb") as f:
        pickle.dump({"model": model, "features": available, "params": params}, f)
    print(f"\n  模型已保存: {model_path}")

    return model


# ========================================================================
# 7. 主流程
# ========================================================================

def main():
    report_only = "--report" in sys.argv

    print("=" * 60)
    print("0x1d 策略蒸馏 (Strategy Distillation)")
    print(f"数据库: {DB_PATH}")
    print("=" * 60)

    # 加载数据
    snaps_df, settle_df = load_data()

    # 构建窗口特征
    win_df = build_window_features(snaps_df, settle_df)

    # 数据报告
    print_data_report(snaps_df, settle_df, win_df)

    if report_only:
        return

    # 构建交易级特征
    trade_df = build_trade_features(snaps_df, settle_df)

    # ── 训练 4 个模型 ──
    m1 = train_direction_model(win_df)
    m2 = train_timing_model(trade_df)
    m3 = train_sizing_model(trade_df)
    m4 = train_pnl_model(win_df)

    # ── 总结 ──
    print("\n" + "=" * 60)
    print("蒸馏完成")
    print("=" * 60)
    models = [
        ("方向模型", "direction_model.pkl", m1),
        ("交易质量", "trade_quality_model.pkl", m2),
        ("仓位模型", "sizing_model.pkl", m3),
        ("盈亏预测", "pnl_model.pkl", m4),
    ]
    for name, fname, m in models:
        status = "✓" if m else "✗ (样本不足)"
        path = MODEL_DIR / fname
        size = f"{path.stat().st_size / 1024:.0f}KB" if path.exists() else "-"
        print(f"  {name:12s} {status:20s} {size}")


if __name__ == "__main__":
    main()
