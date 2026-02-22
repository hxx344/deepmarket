"""
Signal Base - 信号提供者基类

所有交易信号模块必须继承 SignalProvider，提供统一的计算接口。
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

from src.core.context import Context


@dataclass
class SignalResult:
    """
    信号计算结果

    Attributes:
        score:      方向强度分 ∈ [-1, 1]。 >0 看涨(买UP)，<0 看跌(买DOWN)
        confidence: 信心度 ∈ [0, 1]。用于加权和过滤
        reason:     人可读的信号触发原因（用于日志和 Dashboard 展示）
        details:    额外明细数据（指标值等）
    """

    score: float = 0.0
    confidence: float = 0.0
    reason: str = ""
    details: dict[str, Any] = field(default_factory=dict)

    @property
    def direction(self) -> str:
        """'UP' / 'DOWN' / 'NEUTRAL'"""
        if self.score > 0:
            return "UP"
        elif self.score < 0:
            return "DOWN"
        return "NEUTRAL"

    @property
    def abs_score(self) -> float:
        return abs(self.score)

    def to_dict(self) -> dict[str, Any]:
        return {
            "score": round(self.score, 4),
            "confidence": round(self.confidence, 4),
            "direction": self.direction,
            "reason": self.reason,
            "details": self.details,
        }


class SignalProvider(ABC):
    """
    信号提供者基类

    每个子类代表一个独立的信号维度（动量 / 订单流 / 技术指标等）。
    策略通过 `calculate()` 获取当前信号，再做加权融合决策。

    Usage:
        class MySignal(SignalProvider):
            def name(self) -> str: return "my_signal"
            def calculate(self, ctx: Context) -> SignalResult: ...
    """

    @abstractmethod
    def name(self) -> str:
        """信号唯一名称"""
        ...

    @abstractmethod
    def calculate(self, ctx: Context) -> SignalResult:
        """
        计算当前信号

        Args:
            ctx: 全局上下文（可访问行情、订单簿、账户等所有状态）

        Returns:
            SignalResult  score ∈ [-1, 1]
        """
        ...

    def reset(self) -> None:
        """重置内部状态（新窗口起始时调用）"""
        pass

    def get_params(self) -> dict[str, Any]:
        """返回当前参数（序列化/优化用）"""
        return {}
