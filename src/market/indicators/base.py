"""
Indicator Base - 指标插件基类

所有技术指标、情绪指标、微观结构指标都继承此基类，
通过 PluginManager 自动发现和加载。
"""

from __future__ import annotations

from abc import abstractmethod
from typing import Any

import pandas as pd

from src.core.plugin import PluginBase


class IndicatorPlugin(PluginBase):
    """
    指标插件基类

    所有自定义指标必须实现此接口。
    指标通过 calculate() 方法接收 DataFrame，返回计算结果。

    Usage:
        class RSIIndicator(IndicatorPlugin):
            def __init__(self, period: int = 14):
                self._period = period

            def name(self) -> str: return f"RSI_{self._period}"
            def version(self) -> str: return "1.0"
            def required_fields(self) -> list[str]: return ["close"]
            def params(self) -> dict: return {"period": self._period}

            def calculate(self, data: pd.DataFrame) -> pd.Series:
                delta = data["close"].diff()
                gain = delta.clip(lower=0).rolling(self._period).mean()
                loss = (-delta.clip(upper=0)).rolling(self._period).mean()
                rs = gain / loss
                return 100 - (100 / (1 + rs))
    """

    @abstractmethod
    def required_fields(self) -> list[str]:
        """计算所需的列名列表 (如 ['close', 'volume'])"""
        ...

    @abstractmethod
    def calculate(self, data: pd.DataFrame) -> pd.Series | pd.DataFrame:
        """
        计算指标

        Args:
            data: 包含 required_fields 所需列的 DataFrame

        Returns:
            Series (单值指标) 或 DataFrame (多值指标如 Bollinger Bands)
        """
        ...

    @abstractmethod
    def params(self) -> dict[str, Any]:
        """指标参数"""
        ...

    def validate(self, data: pd.DataFrame) -> bool:
        """校验输入数据是否满足要求"""
        required = set(self.required_fields())
        available = set(data.columns)
        return required.issubset(available)
