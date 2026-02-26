"""
Distill Signal — ML 蒸馏模型信号提供者

将 distill_signal.py / model_zoo.py 训练产出的模型集成到策略信号框架中。
支持:
  - 加载 signal_model.pkl + signal_config.json
  - 从 Context 提取特征 → 模型推理 → 输出 UP/DOWN/HOLD
  - 自动 hot-reload (文件变更时重新加载模型)
  - confidence-based sizing 建议
"""

from __future__ import annotations

import json
import pickle
import time
from pathlib import Path
from typing import Any

import numpy as np
from loguru import logger

from src.core.context import Context
from src.strategy.signals.base import SignalProvider, SignalResult


# 模型文件路径
_MODEL_DIR = Path(__file__).resolve().parent.parent.parent.parent / "data" / "distill_models"
_MODEL_PATH = _MODEL_DIR / "signal_model.pkl"
_CONFIG_PATH = _MODEL_DIR / "signal_config.json"


class DistillSignal(SignalProvider):
    """
    ML 蒸馏模型信号

    功能:
      1. 加载训练好的模型 (LightGBM / MLP / TabNet)
      2. 从 Context.market 提取特征向量
      3. 模型推理 → P(UP)
      4. 阈值判定 → UP / DOWN / HOLD

    集成方式:
      strategy 中添加:
        from src.strategy.signals.distill_signal import DistillSignal
        self._distill = DistillSignal()
        result = self._distill.calculate(ctx)
        if result.direction != "NEUTRAL":
            # 按 result.score 和 result.confidence 下单
    """

    def __init__(
        self,
        model_dir: Path | None = None,
        reload_interval_s: float = 60.0,
    ):
        self._model_dir = model_dir or _MODEL_DIR
        self._model_path = self._model_dir / "signal_model.pkl"
        self._config_path = self._model_dir / "signal_config.json"
        self._reload_interval = reload_interval_s

        self._model = None
        self._config: dict = {}
        self._features: list[str] = []
        self._threshold: float = 0.60
        self._model_class: str = "lgb"

        self._last_load_ts: float = 0
        self._last_mtime: float = 0
        self._load_count: int = 0
        self._inference_count: int = 0
        self._last_prob: float = 0.5

        # 特征提取状态 (需要外部注入原始特征, 因为 Context 不含所有蒸馏特征)
        self._feature_cache: dict[str, float] = {}

        self._try_load()

    def name(self) -> str:
        return "distill"

    # ================================================================
    #  模型加载 / Hot-reload
    # ================================================================

    def _try_load(self) -> bool:
        """尝试加载模型, 返回是否成功"""
        try:
            if not self._model_path.exists() or not self._config_path.exists():
                logger.warning(f"[Distill] 模型文件不存在: {self._model_path}")
                return False

            mtime = self._model_path.stat().st_mtime
            if mtime == self._last_mtime and self._model is not None:
                return True  # 无变化

            with open(self._config_path) as f:
                self._config = json.load(f)

            with open(self._model_path, "rb") as f:
                self._model = pickle.load(f)

            self._features = self._config.get("features", [])
            self._threshold = self._config.get("threshold", 0.60)
            self._model_class = self._config.get("model_class", "lgb")

            self._last_mtime = mtime
            self._last_load_ts = time.time()
            self._load_count += 1

            logger.info(
                f"[Distill] 模型已加载 (#{self._load_count}): "
                f"type={self._model_class}, "
                f"features={len(self._features)}, "
                f"threshold={self._threshold:.2f}, "
                f"AUC={self._config.get('cv_auc', '?')}"
            )
            return True

        except Exception as e:
            logger.error(f"[Distill] 加载模型失败: {e}")
            return False

    def _maybe_reload(self):
        """检查是否需要热加载"""
        now = time.time()
        if now - self._last_load_ts > self._reload_interval:
            self._try_load()

    # ================================================================
    #  特征提取
    # ================================================================

    def inject_features(self, features: dict[str, float]):
        """
        注入原始特征 (由外部数据采集模块调用)

        Context 对象只包含基础行情, 蒸馏模型需要的完整特征 (BN动量/CL趋势/...)
        需要由数据采集层计算后注入.

        用法:
          distill_signal.inject_features({
              "bn_mom_1s": 0.5, "bn_mom_3s": 1.2, ...
              "btc_vol_60s": 15.3, "cl_bn_spread": 0.02, ...
          })
        """
        self._feature_cache.update(features)

    def _extract_features_from_context(self, ctx: Context) -> dict[str, float]:
        """
        从 Context 提取基础特征 (能提取的部分).
        完整特征应通过 inject_features() 注入.
        """
        mkt = ctx.market
        features = dict(self._feature_cache)  # 先用注入的

        # 补充 Context 能直接提供的
        if mkt.btc_price > 0 and mkt.pm_window_start_price > 0:
            ptb = mkt.pm_window_start_price
            delta = mkt.btc_price - ptb
            features.setdefault("btc_delta_pct", delta / ptb * 100 if ptb > 0 else 0)
            features.setdefault("btc_delta_ptb", delta)

        features.setdefault("up_bid", mkt.pm_yes_bid)
        features.setdefault("up_ask", mkt.pm_yes_ask)
        features.setdefault("dn_bid", mkt.pm_no_bid)
        features.setdefault("dn_ask", mkt.pm_no_ask)
        features.setdefault("up_ba_spread", mkt.pm_yes_ask - mkt.pm_yes_bid)
        features.setdefault("dn_ba_spread", mkt.pm_no_ask - mkt.pm_no_bid)
        features.setdefault("pm_spread", (mkt.pm_yes_ask + mkt.pm_no_ask) - 1.0)
        features.setdefault("pm_edge", 1.0 - (mkt.pm_yes_ask + mkt.pm_no_ask))

        if mkt.pm_window_seconds_left >= 0:
            elapsed = 300 - mkt.pm_window_seconds_left
            features.setdefault("elapsed", elapsed)
            features.setdefault("elapsed_pct", elapsed / 300.0)

        return features

    def _build_feature_vector(self, ctx: Context) -> np.ndarray | None:
        """构建模型输入向量"""
        if not self._features:
            return None

        feats = self._extract_features_from_context(ctx)
        vec = []
        missing = 0
        for f in self._features:
            val = feats.get(f)
            if val is None:
                vec.append(np.nan)
                missing += 1
            else:
                vec.append(float(val))

        # 如果缺失率 > 50%, 放弃推理
        if missing > len(self._features) * 0.5:
            return None

        return np.array([vec])

    # ================================================================
    #  推理
    # ================================================================

    def _predict(self, X: np.ndarray) -> float:
        """模型推理, 返回 P(UP)"""
        try:
            if self._model_class in ("lgb", "lr"):
                # LightGBM / LogisticRegression pipeline
                return float(self._model.predict_proba(X)[:, 1][0])
            elif self._model_class == "mlp":
                # PyTorch MLP (dict with net_state + scaler)
                import torch
                import torch.nn as nn

                state = self._model
                scaler = state["scaler"]
                n_feat = state["n_features"]
                hidden = state.get("hidden_dims", (128, 64, 32))
                dropout = state.get("dropout", 0.3)

                # 重建网络
                layers = []
                in_dim = n_feat
                for h in hidden:
                    layers.extend([
                        nn.Linear(in_dim, h), nn.BatchNorm1d(h),
                        nn.ReLU(), nn.Dropout(dropout),
                    ])
                    in_dim = h
                layers.append(nn.Linear(in_dim, 1))
                net = nn.Sequential(*layers)
                net.load_state_dict(state["net_state"])
                net.eval()

                X_scaled = scaler.transform(np.nan_to_num(X, 0))
                with torch.no_grad():
                    logits = net(torch.FloatTensor(X_scaled))
                    prob = torch.sigmoid(logits).item()
                return prob
            elif self._model_class == "tabnet":
                state = self._model
                scaler = state["scaler"]
                tabnet = state["tabnet"]
                X_scaled = scaler.transform(np.nan_to_num(X, 0)).astype(np.float32)
                return float(tabnet.predict_proba(X_scaled)[:, 1][0])
            else:
                # 通用 fallback
                return float(self._model.predict_proba(X)[:, 1][0])
        except Exception as e:
            logger.error(f"[Distill] 推理失败: {e}")
            return 0.5

    # ================================================================
    #  SignalProvider 接口
    # ================================================================

    def calculate(self, ctx: Context) -> SignalResult:
        """
        计算蒸馏模型信号

        Returns:
            SignalResult:
              score ∈ [-1, 1]: 正=看涨(UP), 负=看跌(DOWN)
              confidence ∈ [0, 1]: 模型置信度
              details["prob_up"]: P(UP) 原始概率
              details["suggested_sizing"]: 建议仓位系数 [0, 2]
        """
        self._maybe_reload()

        if self._model is None:
            return SignalResult(reason="模型未加载")

        X = self._build_feature_vector(ctx)
        if X is None:
            return SignalResult(reason="特征不足")

        prob_up = self._predict(X)
        self._last_prob = prob_up
        self._inference_count += 1

        # ── 信号判定 ──
        threshold = self._threshold
        if prob_up > threshold:
            direction = "UP"
            conf = prob_up
            score = (prob_up - 0.5) * 2  # 映射到 [0, 1]
        elif prob_up < (1 - threshold):
            direction = "DOWN"
            conf = 1 - prob_up
            score = -((1 - prob_up) - 0.5) * 2  # 映射到 [-1, 0]
        else:
            direction = "HOLD"
            conf = max(prob_up, 1 - prob_up)
            score = 0

        # ── confidence-based sizing ──
        # 置信度线性映射到下注系数:
        #   刚过阈值 → sizing ≈ 0.1 (最小仓位)
        #   满置信度 → sizing = 2.0 (最大仓位)
        if direction != "HOLD":
            sizing_scale = (conf - threshold) / (1.0 - threshold) * 2.0
            sizing = max(sizing_scale, 0.1)
        else:
            sizing = 0.0

        reason = (
            f"ML[{self._model_class}] {direction} | "
            f"P(UP)={prob_up:.3f} | "
            f"conf={conf:.3f} | "
            f"sizing={sizing:.2f}"
        )

        return SignalResult(
            score=score,
            confidence=conf,
            reason=reason,
            details={
                "prob_up": round(prob_up, 4),
                "threshold": threshold,
                "model_class": self._model_class,
                "suggested_sizing": round(sizing, 3),
                "features_available": len([v for v in self._feature_cache.values() if v is not None]),
                "features_required": len(self._features),
                "inference_count": self._inference_count,
            },
        )

    def get_params(self) -> dict[str, Any]:
        return {
            "model_class": self._model_class,
            "threshold": self._threshold,
            "n_features": len(self._features),
            "cv_auc": self._config.get("cv_auc", 0),
            "pnl_roi": self._config.get("pnl_roi", 0),
            "load_count": self._load_count,
            "inference_count": self._inference_count,
        }

    def reset(self):
        self._feature_cache.clear()
        self._inference_count = 0
        self._last_prob = 0.5
