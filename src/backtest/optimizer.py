"""
Parameter Optimizer - 策略参数优化

支持网格搜索、随机搜索和贝叶斯优化。
"""

from __future__ import annotations

import asyncio
import itertools
import random
from dataclasses import dataclass
from typing import Any, Callable

import pandas as pd
from loguru import logger

from src.backtest.engine import BacktestConfig, BacktestEngine, BacktestResult


@dataclass
class ParamSpace:
    """参数空间定义"""
    name: str
    values: list[Any] | None = None       # 离散值列表
    low: float | None = None               # 连续范围下界
    high: float | None = None              # 连续范围上界
    step: float | None = None              # 步长 (网格搜索)


@dataclass
class OptimizationResult:
    """优化结果"""
    best_params: dict[str, Any]
    best_score: float
    all_results: pd.DataFrame
    metric: str


class GridSearchOptimizer:
    """网格搜索优化器"""

    def __init__(
        self,
        strategy_factory: Callable[[dict], Any],
        klines: pd.DataFrame,
        config: BacktestConfig | None = None,
        metric: str = "sharpe_ratio",
    ) -> None:
        self.strategy_factory = strategy_factory
        self.klines = klines
        self.config = config or BacktestConfig()
        self.metric = metric

    async def optimize(self, param_spaces: list[ParamSpace]) -> OptimizationResult:
        """
        执行网格搜索

        Args:
            param_spaces: 参数空间列表

        Returns:
            OptimizationResult
        """
        # 构建参数网格
        param_names = [p.name for p in param_spaces]
        param_values = []
        for p in param_spaces:
            if p.values is not None:
                param_values.append(p.values)
            elif p.low is not None and p.high is not None and p.step is not None:
                import numpy as np
                param_values.append(list(np.arange(p.low, p.high + p.step, p.step)))

        combinations = list(itertools.product(*param_values))
        logger.info(f"Grid search: {len(combinations)} combinations")

        results = []
        best_score = float("-inf")
        best_params = {}

        for i, combo in enumerate(combinations):
            params = dict(zip(param_names, combo))

            try:
                # 创建策略实例
                strategy = self.strategy_factory(params)

                # 运行回测
                engine = BacktestEngine(self.config)
                engine.load_data(self.klines.copy())
                engine.set_strategy(strategy)
                result = await engine.run()

                # 提取评估指标
                score = getattr(result, self.metric, 0.0)
                results.append({**params, "score": score, **result.summary()})

                if score > best_score:
                    best_score = score
                    best_params = params

                if (i + 1) % 10 == 0:
                    logger.info(f"Progress: {i+1}/{len(combinations)}, best={best_score:.4f}")

            except Exception as e:
                logger.error(f"Params {params} failed: {e}")
                results.append({**params, "score": float("nan"), "error": str(e)})

        results_df = pd.DataFrame(results)
        logger.info(f"Grid search complete. Best: {best_params} -> {self.metric}={best_score:.4f}")

        return OptimizationResult(
            best_params=best_params,
            best_score=best_score,
            all_results=results_df,
            metric=self.metric,
        )


class RandomSearchOptimizer:
    """随机搜索优化器"""

    def __init__(
        self,
        strategy_factory: Callable[[dict], Any],
        klines: pd.DataFrame,
        config: BacktestConfig | None = None,
        metric: str = "sharpe_ratio",
        n_trials: int = 50,
    ) -> None:
        self.strategy_factory = strategy_factory
        self.klines = klines
        self.config = config or BacktestConfig()
        self.metric = metric
        self.n_trials = n_trials

    async def optimize(self, param_spaces: list[ParamSpace]) -> OptimizationResult:
        """执行随机搜索"""
        results = []
        best_score = float("-inf")
        best_params = {}

        for i in range(self.n_trials):
            params = {}
            for p in param_spaces:
                if p.values is not None:
                    params[p.name] = random.choice(p.values)
                elif p.low is not None and p.high is not None:
                    if p.step and p.step >= 1:
                        params[p.name] = random.randint(int(p.low), int(p.high))
                    else:
                        params[p.name] = random.uniform(p.low, p.high)

            try:
                strategy = self.strategy_factory(params)
                engine = BacktestEngine(self.config)
                engine.load_data(self.klines.copy())
                engine.set_strategy(strategy)
                result = await engine.run()

                score = getattr(result, self.metric, 0.0)
                results.append({**params, "score": score})

                if score > best_score:
                    best_score = score
                    best_params = params

                if (i + 1) % 10 == 0:
                    logger.info(f"Random search: {i+1}/{self.n_trials}, best={best_score:.4f}")

            except Exception as e:
                logger.error(f"Trial {i} failed: {e}")

        return OptimizationResult(
            best_params=best_params,
            best_score=best_score,
            all_results=pd.DataFrame(results),
            metric=self.metric,
        )


class WalkForwardOptimizer:
    """
    滚动窗口前推优化器 (Walk-Forward Analysis)

    将数据分为多个 in-sample / out-of-sample 窗口，
    在 in-sample 上优化参数，在 out-of-sample 上验证。
    """

    def __init__(
        self,
        strategy_factory: Callable[[dict], Any],
        klines: pd.DataFrame,
        config: BacktestConfig | None = None,
        metric: str = "sharpe_ratio",
        n_splits: int = 5,
        train_ratio: float = 0.7,
    ) -> None:
        self.strategy_factory = strategy_factory
        self.klines = klines
        self.config = config or BacktestConfig()
        self.metric = metric
        self.n_splits = n_splits
        self.train_ratio = train_ratio

    async def optimize(self, param_spaces: list[ParamSpace]) -> dict[str, Any]:
        """
        执行 Walk-Forward 分析

        Returns:
            包含每个窗口最优参数和 OOS 表现的结果
        """
        total_len = len(self.klines)
        window_size = total_len // self.n_splits
        train_size = int(window_size * self.train_ratio)

        wf_results = []

        for i in range(self.n_splits):
            start = i * window_size
            train_end = start + train_size
            test_end = min(start + window_size, total_len)

            train_data = self.klines.iloc[start:train_end].copy()
            test_data = self.klines.iloc[train_end:test_end].copy()

            if train_data.empty or test_data.empty:
                continue

            logger.info(f"WF Split {i+1}/{self.n_splits}: train={len(train_data)}, test={len(test_data)}")

            # 在训练集上优化
            optimizer = GridSearchOptimizer(
                self.strategy_factory, train_data, self.config, self.metric
            )
            opt_result = await optimizer.optimize(param_spaces)

            # 在测试集上验证
            best_strategy = self.strategy_factory(opt_result.best_params)
            engine = BacktestEngine(self.config)
            engine.load_data(test_data)
            engine.set_strategy(best_strategy)
            test_result = await engine.run()

            oos_score = getattr(test_result, self.metric, 0.0)

            wf_results.append({
                "split": i + 1,
                "best_params": opt_result.best_params,
                "in_sample_score": opt_result.best_score,
                "out_of_sample_score": oos_score,
                "oos_return": test_result.total_return,
            })

            logger.info(
                f"  IS={opt_result.best_score:.4f}, OOS={oos_score:.4f}, "
                f"OOS Return={test_result.total_return:.2%}"
            )

        return {
            "splits": wf_results,
            "avg_oos_score": sum(r["out_of_sample_score"] for r in wf_results) / len(wf_results) if wf_results else 0,
            "overfit_ratio": self._calculate_overfit_ratio(wf_results),
        }

    def _calculate_overfit_ratio(self, results: list[dict]) -> float:
        """计算过拟合比率 (IS score vs OOS score 的偏差)"""
        if not results:
            return 0.0
        is_scores = [r["in_sample_score"] for r in results]
        oos_scores = [r["out_of_sample_score"] for r in results]
        avg_is = sum(is_scores) / len(is_scores)
        avg_oos = sum(oos_scores) / len(oos_scores)
        if avg_is == 0:
            return 0.0
        return 1 - (avg_oos / avg_is)
