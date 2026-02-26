"""
模型动物园 — Model Zoo: 多模型自动训练 + 竞争选择 + 自优化
================================================================

功能:
  1. 多模型并行训练: LightGBM / MLP / TabNet / LogisticRegression
  2. 自动与已有模型对比: 仅 PnL ROI 更优时才替换生产模型
  3. 模型版本管理: 历史模型存档, 可回滚
  4. 增量训练: 新数据到达后自动重训 + 评估

用法:
  python scripts/model_zoo.py                     # 训练所有模型, 自动选最优
  python scripts/model_zoo.py --retrain            # 增量重训 (检查是否有新数据)
  python scripts/model_zoo.py --models lgb,mlp     # 只训练指定模型
  python scripts/model_zoo.py --dry-run            # 只评估不保存
  python scripts/model_zoo.py --force              # 强制替换 (不管是否更优)

与 distill_signal.py 的关系:
  - distill_signal.py: 单模型训练 (LightGBM), 手动运行
  - model_zoo.py: 多模型自动竞争, 可定时运行, 自动升级生产模型
"""

from __future__ import annotations

import json
import os
import pickle
import shutil
import sys
import time
import warnings
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# Windows GBK 兼容
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

import numpy as np
import pandas as pd
from sklearn.model_selection import GroupKFold
from sklearn.metrics import accuracy_score, roc_auc_score, f1_score
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
import lightgbm as lgb

warnings.filterwarnings("ignore")

# ── 路径 ──
ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "data" / "0x1d_data.db"
MODEL_DIR = ROOT / "data" / "distill_models"
ARCHIVE_DIR = MODEL_DIR / "archive"
MODEL_DIR.mkdir(parents=True, exist_ok=True)
ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)

# ── 从 distill_signal.py 复用特征定义和数据加载 ──
sys.path.insert(0, str(ROOT / "scripts"))
from distill_signal import (
    PURE_MARKET_NO_TIME,
    PURE_MARKET_FEATURES,
    SIGNAL_FEATURES,
    load_trades,
    load_settlement_labels,
    deduplicate_bursts,
    simulate_window_pnl,
    temporal_validation,
    _slug_sort_key,
)


# ══════════════════════════════════════════════════════════════
# 1. 模型定义 — 每种模型封装为统一接口
# ══════════════════════════════════════════════════════════════

class BaseModel:
    """模型基类: 统一 fit / predict_proba / save / load 接口"""
    name: str = "base"
    
    def fit(self, X: pd.DataFrame, y: pd.Series, X_val=None, y_val=None):
        raise NotImplementedError
    
    def predict_proba(self, X: pd.DataFrame) -> np.ndarray:
        """返回 P(UP) 的概率数组"""
        raise NotImplementedError
    
    def get_model(self):
        """返回可 pickle 的模型对象"""
        raise NotImplementedError
    
    def n_params(self) -> int:
        """模型参数量 (近似)"""
        return 0


class LGBModel(BaseModel):
    """LightGBM — 当前生产基线"""
    name = "lgb"
    
    def __init__(self):
        self.model = lgb.LGBMClassifier(
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
    
    def fit(self, X, y, X_val=None, y_val=None):
        kwargs = {}
        if X_val is not None:
            kwargs["eval_set"] = [(X_val, y_val)]
            kwargs["callbacks"] = [lgb.early_stopping(50, verbose=False)]
        self.model.fit(X, y, **kwargs)
    
    def predict_proba(self, X):
        return self.model.predict_proba(X)[:, 1]
    
    def get_model(self):
        return self.model
    
    def n_params(self):
        return self.model.n_estimators * self.model.num_leaves


class LRModel(BaseModel):
    """Logistic Regression — 最简基线 (防过拟合参考)"""
    name = "lr"
    
    def __init__(self):
        self.pipeline = Pipeline([
            ("scaler", StandardScaler()),
            ("lr", LogisticRegression(
                C=0.5,
                max_iter=2000,
                solver="lbfgs",
                random_state=42,
            ))
        ])
    
    def fit(self, X, y, X_val=None, y_val=None):
        X_clean = X.fillna(0)
        self.pipeline.fit(X_clean, y)
    
    def predict_proba(self, X):
        X_clean = X.fillna(0)
        return self.pipeline.predict_proba(X_clean)[:, 1]
    
    def get_model(self):
        return self.pipeline
    
    def n_params(self):
        try:
            return len(self.pipeline.named_steps["lr"].coef_.flatten())
        except AttributeError:
            return 0


class MLPModel(BaseModel):
    """MLP (PyTorch) — 简单前馈网络, 方案A"""
    name = "mlp"
    
    def __init__(self, hidden_dims=(128, 64, 32), dropout=0.3, lr=1e-3,
                 epochs=200, batch_size=256, patience=20):
        self.hidden_dims = hidden_dims
        self.dropout = dropout
        self.lr = lr
        self.epochs = epochs
        self.batch_size = batch_size
        self.patience = patience
        self.scaler = StandardScaler()
        self.net = None
        self._n_features = 0
    
    def _build_net(self, n_features: int):
        import torch
        import torch.nn as nn
        
        layers = []
        in_dim = n_features
        for h in self.hidden_dims:
            layers.extend([
                nn.Linear(in_dim, h),
                nn.BatchNorm1d(h),
                nn.ReLU(),
                nn.Dropout(self.dropout),
            ])
            in_dim = h
        layers.append(nn.Linear(in_dim, 1))
        return nn.Sequential(*layers)
    
    def fit(self, X, y, X_val=None, y_val=None):
        import torch
        import torch.nn as nn
        from torch.utils.data import DataLoader, TensorDataset
        
        X_np = self.scaler.fit_transform(X.fillna(0).values)
        y_np = y.values.astype(np.float32)
        self._n_features = X_np.shape[1]
        
        X_t = torch.FloatTensor(X_np)
        y_t = torch.FloatTensor(y_np).unsqueeze(1)
        
        dataset = TensorDataset(X_t, y_t)
        loader = DataLoader(dataset, batch_size=self.batch_size, shuffle=True)
        
        self.net = self._build_net(self._n_features)
        optimizer = torch.optim.AdamW(self.net.parameters(), lr=self.lr, weight_decay=1e-4)
        scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(
            optimizer, patience=10, factor=0.5, verbose=False
        )
        criterion = nn.BCEWithLogitsLoss()
        
        # 验证集
        if X_val is not None:
            X_val_np = self.scaler.transform(X_val.fillna(0).values)
            X_val_t = torch.FloatTensor(X_val_np)
            y_val_t = torch.FloatTensor(y_val.values.astype(np.float32)).unsqueeze(1)
        
        best_val_loss = float("inf")
        best_state = None
        patience_cnt = 0
        
        for epoch in range(self.epochs):
            self.net.train()
            epoch_loss = 0.0
            for xb, yb in loader:
                optimizer.zero_grad()
                logits = self.net(xb)
                loss = criterion(logits, yb)
                loss.backward()
                torch.nn.utils.clip_grad_norm_(self.net.parameters(), 1.0)
                optimizer.step()
                epoch_loss += loss.item() * len(xb)
            epoch_loss /= len(dataset)
            
            # 验证
            if X_val is not None:
                self.net.eval()
                with torch.no_grad():
                    val_logits = self.net(X_val_t)
                    val_loss = criterion(val_logits, y_val_t).item()
                scheduler.step(val_loss)
                
                if val_loss < best_val_loss:
                    best_val_loss = val_loss
                    best_state = {k: v.clone() for k, v in self.net.state_dict().items()}
                    patience_cnt = 0
                else:
                    patience_cnt += 1
                    if patience_cnt >= self.patience:
                        break
            else:
                scheduler.step(epoch_loss)
        
        if best_state is not None:
            self.net.load_state_dict(best_state)
        self.net.eval()
    
    def predict_proba(self, X):
        import torch
        X_np = self.scaler.transform(X.fillna(0).values)
        X_t = torch.FloatTensor(X_np)
        self.net.eval()
        with torch.no_grad():
            logits = self.net(X_t)
            probs = torch.sigmoid(logits).numpy().flatten()
        return probs
    
    def get_model(self):
        return {"net_state": self.net.state_dict(), "scaler": self.scaler,
                "hidden_dims": self.hidden_dims, "dropout": self.dropout,
                "n_features": self._n_features}
    
    def n_params(self):
        if self.net is None:
            return 0
        return sum(p.numel() for p in self.net.parameters())


class TabNetModel(BaseModel):
    """TabNet — 表格数据专用DL架构"""
    name = "tabnet"
    
    def __init__(self, n_d=32, n_a=32, n_steps=5, gamma=1.5,
                 lr=2e-2, epochs=100, patience=15, batch_size=256):
        self.n_d = n_d
        self.n_a = n_a
        self.n_steps = n_steps
        self.gamma = gamma
        self.lr = lr
        self.epochs = epochs
        self.patience = patience
        self.batch_size = batch_size
        self.model = None
        self.scaler = StandardScaler()
    
    def fit(self, X, y, X_val=None, y_val=None):
        from pytorch_tabnet.tab_model import TabNetClassifier
        
        X_np = self.scaler.fit_transform(X.fillna(0).values).astype(np.float32)
        y_np = y.values.astype(np.int64)
        
        self.model = TabNetClassifier(
            n_d=self.n_d, n_a=self.n_a, n_steps=self.n_steps,
            gamma=self.gamma, optimizer_params={"lr": self.lr},
            scheduler_params={"step_size": 30, "gamma": 0.9},
            scheduler_fn=__import__("torch").optim.lr_scheduler.StepLR,
            verbose=0, seed=42,
        )
        
        eval_set = []
        eval_name = []
        if X_val is not None:
            X_val_np = self.scaler.transform(X_val.fillna(0).values).astype(np.float32)
            y_val_np = y_val.values.astype(np.int64)
            eval_set = [(X_val_np, y_val_np)]
            eval_name = ["val"]
        
        self.model.fit(
            X_np, y_np,
            eval_set=eval_set, eval_name=eval_name,
            max_epochs=self.epochs,
            patience=self.patience,
            batch_size=self.batch_size,
        )
    
    def predict_proba(self, X):
        X_np = self.scaler.transform(X.fillna(0).values).astype(np.float32)
        return self.model.predict_proba(X_np)[:, 1]
    
    def get_model(self):
        return {"tabnet": self.model, "scaler": self.scaler}
    
    def n_params(self):
        if self.model is None:
            return 0
        try:
            return sum(p.numel() for p in self.model.network.parameters())
        except Exception:
            return 0


# ══════════════════════════════════════════════════════════════
# 2. 模型注册表
# ══════════════════════════════════════════════════════════════

MODEL_REGISTRY: dict[str, type[BaseModel]] = {
    "lgb": LGBModel,
    "lr": LRModel,
    "mlp": MLPModel,
    "tabnet": TabNetModel,
}


def _check_torch_available() -> bool:
    try:
        import torch
        return True
    except ImportError:
        return False


def _check_tabnet_available() -> bool:
    try:
        from pytorch_tabnet.tab_model import TabNetClassifier
        return True
    except ImportError:
        return False


def get_available_models(requested: list[str] | None = None) -> dict[str, type[BaseModel]]:
    """返回可用模型 (检查依赖是否安装)"""
    available = {"lgb": LGBModel, "lr": LRModel}
    if _check_torch_available():
        available["mlp"] = MLPModel
        if _check_tabnet_available():
            available["tabnet"] = TabNetModel
    
    if requested:
        return {k: v for k, v in available.items() if k in requested}
    return available


# ══════════════════════════════════════════════════════════════
# 3. 统一训练 + 评估 pipeline
# ══════════════════════════════════════════════════════════════

def train_and_evaluate(
    model: BaseModel,
    df: pd.DataFrame,
    features: list[str],
    outcome_labels: dict,
    n_splits: int = 5,
) -> dict:
    """
    训练单个模型并完整评估:
      - GroupKFold CV (按窗口分组)
      - 返回 CV metrics + OOF predictions
    """
    X = df[features].copy()
    groups = df["slug"]
    
    # outcome 标签
    df_w = df.copy()
    df_w["_outcome"] = df_w["slug"].map(
        lambda s: 1 if outcome_labels.get(s, {}).get("won") == "UP"
        else (0 if outcome_labels.get(s, {}).get("won") == "DOWN" else np.nan)
    )
    valid = df_w["_outcome"].notna()
    X = X[valid].reset_index(drop=True)
    groups = groups[valid].reset_index(drop=True)
    df_w = df_w[valid].reset_index(drop=True)
    y = df_w["_outcome"].astype(int)
    
    n_windows = groups.nunique()
    n_splits = min(n_splits, n_windows)
    gkf = GroupKFold(n_splits=n_splits)
    
    oof_probs = np.full(len(df_w), np.nan)
    fold_metrics = []
    
    for fold, (tr_idx, te_idx) in enumerate(gkf.split(X, y, groups)):
        X_tr, X_te = X.iloc[tr_idx], X.iloc[te_idx]
        y_tr, y_te = y.iloc[tr_idx], y.iloc[te_idx]
        
        model_copy = model.__class__()
        model_copy.fit(X_tr, y_tr, X_te, y_te)
        
        probs = model_copy.predict_proba(X_te)
        oof_probs[te_idx] = probs
        preds = (probs > 0.5).astype(int)
        
        acc = accuracy_score(y_te, preds)
        try:
            auc = roc_auc_score(y_te, probs)
        except ValueError:
            auc = 0.5
        f1 = f1_score(y_te, preds, average="macro")
        fold_metrics.append({"acc": acc, "auc": auc, "f1": f1, "n": len(te_idx)})
    
    cv_acc = np.mean([m["acc"] for m in fold_metrics])
    cv_auc = np.mean([m["auc"] for m in fold_metrics])
    cv_f1 = np.mean([m["f1"] for m in fold_metrics])
    acc_std = np.std([m["acc"] for m in fold_metrics])
    
    # 训练最后一个 fold 的模型用于估算参数量
    n_params_est = model_copy.n_params() if fold_metrics else 0
    
    return {
        "model_name": model.name,
        "cv_acc": cv_acc,
        "cv_auc": cv_auc,
        "cv_f1": cv_f1,
        "acc_std": acc_std,
        "oof_probs": oof_probs,
        "fold_metrics": fold_metrics,
        "n_samples": len(df_w),
        "n_features": len(features),
        "n_params": n_params_est,
        "df_used": df_w,
        "features_used": features,
    }


def evaluate_pnl(df: pd.DataFrame, probs: np.ndarray, threshold: float,
                 settle: dict) -> dict | None:
    """评估 PnL (silent mode), 返回核心指标"""
    result = simulate_window_pnl(df, probs, threshold, settle, "confidence", quiet=True)
    if result is None:
        return None
    return {
        "total_pnl": result["total_pnl"],
        "total_roi": result["total_roi"],
        "win_rate": result["win_rate"],
        "n_windows": result["n_windows"],
        "avg_pnl": result["avg_pnl"],
    }


def find_best_threshold(df, probs, settle, thresholds=None):
    """搜索 ROI 最优阈值"""
    thresholds = thresholds or [0.50, 0.52, 0.55, 0.58, 0.60, 0.62, 0.65, 0.70]
    best_t, best_roi = 0.50, -999
    
    for t in thresholds:
        pnl = simulate_window_pnl(df, probs, t, settle, "confidence", quiet=True)
        if pnl and pnl["n_windows"] >= 10 and pnl["total_roi"] > best_roi:
            best_roi = pnl["total_roi"]
            best_t = t
    
    return best_t, best_roi


# ══════════════════════════════════════════════════════════════
# 4. 模型版本管理 + 自动対比
# ══════════════════════════════════════════════════════════════

def load_current_model_config() -> dict | None:
    """加载生产环境当前模型配置"""
    config_path = MODEL_DIR / "signal_config.json"
    if not config_path.exists():
        return None
    try:
        with open(config_path) as f:
            return json.load(f)
    except Exception:
        return None


def load_current_model():
    """加载生产环境当前模型"""
    model_path = MODEL_DIR / "signal_model.pkl"
    if not model_path.exists():
        return None
    try:
        with open(model_path, "rb") as f:
            return pickle.load(f)
    except Exception:
        return None


def archive_current_model(reason: str = "auto_archive"):
    """将当前生产模型存档"""
    model_path = MODEL_DIR / "signal_model.pkl"
    config_path = MODEL_DIR / "signal_config.json"
    
    if not model_path.exists():
        return
    
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    archive_model = ARCHIVE_DIR / f"signal_model_{ts}.pkl"
    archive_config = ARCHIVE_DIR / f"signal_config_{ts}.json"
    
    shutil.copy2(model_path, archive_model)
    if config_path.exists():
        shutil.copy2(config_path, archive_config)
    
    # 清理旧存档 (只保留最近 10 个)
    archives = sorted(ARCHIVE_DIR.glob("signal_model_*.pkl"))
    if len(archives) > 10:
        for old in archives[:-10]:
            old.unlink(missing_ok=True)
            old.with_suffix(".json").unlink(missing_ok=True)
    
    print(f"  [Archive] 旧模型已存档: {archive_model.name} ({reason})")


def save_best_model(model_obj, eval_results: dict, threshold: float,
                    model_name: str, pnl_result: dict | None = None):
    """保存新的生产模型 (含完整元数据)"""
    model_path = MODEL_DIR / "signal_model.pkl"
    config_path = MODEL_DIR / "signal_config.json"
    
    with open(model_path, "wb") as f:
        pickle.dump(model_obj, f)
    
    features_used = eval_results.get("features_used", PURE_MARKET_NO_TIME)
    
    config = {
        "features": list(features_used),
        "threshold": threshold,
        "cv_accuracy": eval_results["cv_acc"],
        "cv_auc": eval_results["cv_auc"],
        "cv_f1": eval_results["cv_f1"],
        "n_features": len(features_used),
        "n_samples": eval_results.get("n_samples", 0),
        "model_type": model_name,
        "model_class": eval_results.get("model_name", "lgb"),
        "pure_market": True,
        "training_label": "outcome",
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    
    if pnl_result:
        config["pnl_roi"] = pnl_result["total_roi"]
        config["pnl_total"] = pnl_result["total_pnl"]
        config["pnl_win_rate"] = pnl_result["win_rate"]
        config["pnl_n_windows"] = pnl_result["n_windows"]
    
    with open(config_path, "w") as f:
        json.dump(config, f, indent=2, ensure_ascii=False)
    
    kb = model_path.stat().st_size / 1024
    print(f"\n  [Save] 生产模型已更新: {model_path.name} ({kb:.0f} KB)")
    print(f"         模型类型: {model_name} | 阈值: {threshold:.2f}")
    if pnl_result:
        print(f"         PnL ROI: {pnl_result['total_roi']:+.2%} | "
              f"胜率: {pnl_result['win_rate']:.2%}")


def should_update_model(new_pnl: dict | None, old_config: dict | None,
                        force: bool = False) -> bool:
    """判断是否应该更新生产模型"""
    if force:
        return True
    if old_config is None:
        return True  # 无旧模型, 直接保存
    if new_pnl is None:
        return False  # 新模型无法评估
    
    old_roi = old_config.get("pnl_roi", -999)
    new_roi = new_pnl["total_roi"]
    
    # 新模型 ROI 必须 >= 旧模型 ROI (允许微小回退 0.1%)
    improvement = new_roi - old_roi
    if improvement >= -0.001:
        return True
    
    # ROI 下降但胜率大幅提升 (alternative check)
    old_wr = old_config.get("pnl_win_rate", 0)
    new_wr = new_pnl.get("win_rate", 0)
    if new_wr - old_wr >= 0.05 and new_roi > 0:
        return True
    
    return False


# ══════════════════════════════════════════════════════════════
# 5. 时序验证 (独立实现, 避免数据泄漏)
# ══════════════════════════════════════════════════════════════

def temporal_eval(model_cls: type[BaseModel], df: pd.DataFrame,
                  features: list[str], outcome_labels: dict,
                  settle: dict, threshold: float) -> dict | None:
    """前70%窗口训练 → 后30%窗口测试, 评估真实泛化"""
    X = df[features].copy()
    groups = df["slug"].copy()
    
    df_w = df.copy()
    df_w["_outcome"] = df_w["slug"].map(
        lambda s: 1 if outcome_labels.get(s, {}).get("won") == "UP"
        else (0 if outcome_labels.get(s, {}).get("won") == "DOWN" else np.nan)
    )
    valid = df_w["_outcome"].notna()
    X = X[valid].reset_index(drop=True)
    groups = groups[valid].reset_index(drop=True)
    df_w = df_w[valid].reset_index(drop=True)
    y = df_w["_outcome"].astype(int)
    
    unique_slugs = sorted(groups.unique(), key=_slug_sort_key)
    n_total = len(unique_slugs)
    n_train = int(n_total * 0.7)
    
    if n_total - n_train < 15:
        return None
    
    train_slugs = set(unique_slugs[:n_train])
    test_mask = groups.isin(set(unique_slugs[n_train:]))
    train_mask = groups.isin(train_slugs)
    
    X_train, X_test = X[train_mask], X[test_mask]
    y_train, y_test = y[train_mask], y[test_mask]
    
    model = model_cls()
    model.fit(X_train, y_train, X_test, y_test)
    probs = model.predict_proba(X_test)
    preds = (probs > 0.5).astype(int)
    
    acc = accuracy_score(y_test, preds)
    try:
        auc = roc_auc_score(y_test, probs)
    except ValueError:
        auc = 0.5
    
    # 测试集 PnL
    df_test = df_w[test_mask].reset_index(drop=True)
    pnl = simulate_window_pnl(df_test, probs, threshold, settle, "confidence", quiet=True)
    
    return {
        "acc": acc,
        "auc": auc,
        "n_test_windows": n_total - n_train,
        "n_test_samples": test_mask.sum(),
        "test_pnl": pnl,
    }


# ══════════════════════════════════════════════════════════════
# 6. 主流程 — 多模型竞争
# ══════════════════════════════════════════════════════════════

def run_model_zoo(
    model_names: list[str] | None = None,
    features: list[str] | None = None,
    rich_only: bool = True,
    dry_run: bool = False,
    force: bool = False,
):
    """
    主流程:
      1. 加载数据
      2. 训练所有候选模型
      3. CV + PnL 评估
      4. 选择最优 → 与现有生产模型对比 → 有提升则替换
    """
    print("=" * 72)
    print("  Model Zoo — 多模型自动训练 + 竞争选择")
    print("=" * 72)
    
    features = features or PURE_MARKET_NO_TIME
    available = get_available_models(model_names)
    
    print(f"\n  候选模型: {list(available.keys())}")
    print(f"  特征数量: {len(features)}")
    if rich_only:
        print(f"  数据模式: 仅高质量 (rich_only)")
    
    # ── 加载数据 ──
    print("\n[1/5] 加载数据...")
    df = load_trades(rich_only=rich_only)
    settle = load_settlement_labels()
    df = deduplicate_bursts(df)
    
    n_windows = df["slug"].nunique()
    print(f"  决策样本: {len(df)} | 窗口: {n_windows}")
    
    if n_windows < 30:
        print(f"\n  ⚠ 窗口数不足 ({n_windows} < 30), 建议继续采集数据")
        print(f"  当前不建议频繁重训, 数据量可能导致过拟合")
    
    # ── 加载旧模型配置 ──
    old_config = load_current_model_config()
    if old_config:
        print(f"\n  [当前生产模型]")
        print(f"    类型: {old_config.get('model_type', 'unknown')}")
        print(f"    CV AUC: {old_config.get('cv_auc', 0):.4f}")
        print(f"    PnL ROI: {old_config.get('pnl_roi', 0):+.2%}")
        print(f"    训练样本: {old_config.get('n_samples', '?')}")
        print(f"    创建时间: {old_config.get('created_at', '?')}")
    else:
        print(f"\n  [无现有生产模型, 将保存最优候选]")
    
    # ── 检查增量: 新数据是否足够多 ──
    old_n_samples = old_config.get("n_samples", 0) if old_config else 0
    if old_n_samples > 0 and len(df) <= old_n_samples * 1.05 and not force:
        print(f"\n  ⚠ 新数据增量不足 ({len(df)} vs 旧{old_n_samples}), 跳过重训")
        print(f"  使用 --force 强制重训")
        return
    
    # ── 训练所有候选模型 ──
    print(f"\n[2/5] 训练候选模型...")
    results = {}
    
    for name, model_cls in available.items():
        print(f"\n  {'─' * 50}")
        print(f"  训练 [{name.upper()}]...")
        t0 = time.time()
        
        try:
            model_inst = model_cls()
            eval_res = train_and_evaluate(model_inst, df, features, settle)
            elapsed = time.time() - t0
            
            print(f"    CV Acc: {eval_res['cv_acc']:.4f}±{eval_res['acc_std']:.4f} | "
                  f"AUC: {eval_res['cv_auc']:.4f} | F1: {eval_res['cv_f1']:.4f} | "
                  f"耗时: {elapsed:.1f}s")
            
            results[name] = eval_res
        except Exception as e:
            print(f"    ✗ 训练失败: {e}")
            import traceback
            traceback.print_exc()
    
    if not results:
        print("\n  ✗ 所有模型训练失败")
        return
    
    # ── PnL 评估 + 阈值搜索 ──
    print(f"\n[3/5] PnL 评估 + 阈值搜索...")
    pnl_results = {}
    
    for name, res in results.items():
        df_used = res["df_used"]
        oof = res["oof_probs"]
        
        best_t, best_roi = find_best_threshold(df_used, oof, settle)
        pnl = evaluate_pnl(df_used, oof, best_t, settle)
        
        if pnl:
            pnl["threshold"] = best_t
            pnl_results[name] = pnl
            print(f"  [{name.upper()}] 阈值={best_t:.2f} | "
                  f"ROI={pnl['total_roi']:+.2%} | "
                  f"PnL=${pnl['total_pnl']:+,.2f} | "
                  f"胜率={pnl['win_rate']:.2%} | "
                  f"窗口={pnl['n_windows']}")
        else:
            print(f"  [{name.upper()}] PnL 无法评估")
    
    # ── 排名 ──
    print(f"\n[4/5] 模型排名 (按 PnL ROI)...")
    print(f"\n  {'排名':<4} {'模型':<8} {'CV AUC':>8} {'CV Acc':>8} {'PnL ROI':>9} "
          f"{'胜率':>7} {'总PnL':>11} {'阈值':>5}")
    print(f"  {'─' * 65}")
    
    ranked = sorted(
        results.keys(),
        key=lambda n: pnl_results.get(n, {}).get("total_roi", -999),
        reverse=True,
    )
    
    for rank, name in enumerate(ranked, 1):
        res = results[name]
        pnl = pnl_results.get(name)
        roi_str = f"{pnl['total_roi']:+.2%}" if pnl else "N/A"
        wr_str = f"{pnl['win_rate']:.2%}" if pnl else "N/A"
        pnl_str = f"${pnl['total_pnl']:+,.2f}" if pnl else "N/A"
        t_str = f"{pnl['threshold']:.2f}" if pnl else "N/A"
        marker = " ◀ best" if rank == 1 else ""
        print(f"  {rank:<4} {name.upper():<8} {res['cv_auc']:>8.4f} {res['cv_acc']:>8.4f} "
              f"{roi_str:>9} {wr_str:>7} {pnl_str:>11} {t_str:>5}{marker}")
    
    # ── 时序验证 (最优模型) ──
    best_name = ranked[0]
    best_pnl = pnl_results.get(best_name)
    best_threshold = best_pnl["threshold"] if best_pnl else 0.60
    
    print(f"\n  时序验证 [{best_name.upper()}]...")
    temporal = temporal_eval(
        available[best_name], df, features, settle, settle, best_threshold
    )
    if temporal:
        print(f"    后30% Acc: {temporal['acc']:.4f} | AUC: {temporal['auc']:.4f}")
        cv_auc = results[best_name]["cv_auc"]
        if temporal["auc"] < cv_auc - 0.10:
            print(f"    ⚠ 时序验证 AUC 大幅下降 ({temporal['auc']:.4f} vs CV {cv_auc:.4f})")
            print(f"    模型可能过拟合, 建议使用更简单的模型或更多数据")
        elif temporal["auc"] >= cv_auc - 0.05:
            print(f"    ✓ 时序验证通过 (差距 {temporal['auc'] - cv_auc:+.4f})")
        if temporal["test_pnl"]:
            tp = temporal["test_pnl"]
            print(f"    测试集 PnL: ${tp['total_pnl']:+,.2f} (ROI: {tp['total_roi']:+.2%})")
    
    # ── 决定是否更新生产模型 ──
    print(f"\n[5/5] 模型更新决策...")
    
    if dry_run:
        print(f"  [Dry Run] 不保存模型")
        return
    
    if should_update_model(best_pnl, old_config, force):
        # 全量训练最优模型
        print(f"\n  ✓ 训练全量 [{best_name.upper()}] 模型...")
        final_model = available[best_name]()
        
        df_w = results[best_name]["df_used"]
        X = df_w[features].copy()
        y = df_w["_outcome"].astype(int)
        final_model.fit(X, y)
        
        # 存档旧模型
        if old_config:
            archive_current_model("zoo_upgrade")
        
        # 保存新模型
        save_best_model(
            final_model.get_model(),
            results[best_name],
            best_threshold,
            f"zoo_{best_name}",
            best_pnl,
        )
        
        # 对比汇总
        if old_config and best_pnl:
            old_roi = old_config.get("pnl_roi", 0)
            new_roi = best_pnl["total_roi"]
            print(f"\n  ── 模型升级汇总 ──")
            print(f"  旧模型 ROI: {old_roi:+.2%}")
            print(f"  新模型 ROI: {new_roi:+.2%}")
            print(f"  提升:       {(new_roi - old_roi):+.2%}")
    else:
        print(f"\n  ✗ 新模型未达到替换标准, 保留现有生产模型")
        if best_pnl and old_config:
            print(f"    新模型 ROI: {best_pnl['total_roi']:+.2%} vs "
                  f"旧模型 ROI: {old_config.get('pnl_roi', 0):+.2%}")
    
    print(f"\n{'=' * 72}")
    print(f"  Model Zoo 完成")
    print(f"{'=' * 72}")


# ══════════════════════════════════════════════════════════════
# 7. AUTO-RETRAIN: 定时重训入口
# ══════════════════════════════════════════════════════════════

def auto_retrain():
    """
    自动重训入口 — 可由 cron/scheduler 定时调用:
      1. 检查是否有足够新数据
      2. 训练所有可用模型
      3. 仅在有提升时更新生产模型
    """
    print(f"\n{'=' * 72}")
    print(f"  Auto-Retrain 触发 ({datetime.now().isoformat()})")
    print(f"{'=' * 72}")
    
    try:
        run_model_zoo(rich_only=True, dry_run=False, force=False)
    except Exception as e:
        print(f"\n  ✗ Auto-Retrain 失败: {e}")
        import traceback
        traceback.print_exc()


# ══════════════════════════════════════════════════════════════
# 8. CLI
# ══════════════════════════════════════════════════════════════

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Model Zoo: 多模型自动训练 + 竞争选择")
    parser.add_argument("--models", type=str, default=None,
                        help="指定模型 (逗号分隔): lgb,mlp,tabnet,lr")
    parser.add_argument("--retrain", action="store_true",
                        help="自动重训模式 (检查新数据)")
    parser.add_argument("--dry-run", action="store_true",
                        help="只评估不保存")
    parser.add_argument("--force", action="store_true",
                        help="强制替换 (不管是否更优)")
    parser.add_argument("--rich-only", action="store_true", default=True,
                        help="仅使用高质量数据 (默认开启)")
    parser.add_argument("--all-data", action="store_true",
                        help="使用全部数据")
    
    args = parser.parse_args()
    
    if args.retrain:
        auto_retrain()
        return
    
    model_names = args.models.split(",") if args.models else None
    rich = not args.all_data
    
    run_model_zoo(
        model_names=model_names,
        rich_only=rich,
        dry_run=args.dry_run,
        force=args.force,
    )


if __name__ == "__main__":
    main()
