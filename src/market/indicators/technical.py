"""
Technical Indicators - 内置技术指标集合

提供常用技术分析指标：SMA, EMA, RSI, MACD, Bollinger Bands, ATR, VWAP, OBV 等。
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from src.market.indicators.base import IndicatorPlugin


class SMAIndicator(IndicatorPlugin):
    """简单移动平均线"""

    def __init__(self, period: int = 20) -> None:
        self._period = period

    def name(self) -> str:
        return f"SMA_{self._period}"

    def version(self) -> str:
        return "1.0"

    def required_fields(self) -> list[str]:
        return ["close"]

    def params(self) -> dict[str, Any]:
        return {"period": self._period}

    def calculate(self, data: pd.DataFrame) -> pd.Series:
        return data["close"].rolling(window=self._period).mean()


class EMAIndicator(IndicatorPlugin):
    """指数移动平均线"""

    def __init__(self, period: int = 20) -> None:
        self._period = period

    def name(self) -> str:
        return f"EMA_{self._period}"

    def version(self) -> str:
        return "1.0"

    def required_fields(self) -> list[str]:
        return ["close"]

    def params(self) -> dict[str, Any]:
        return {"period": self._period}

    def calculate(self, data: pd.DataFrame) -> pd.Series:
        return data["close"].ewm(span=self._period, adjust=False).mean()


class RSIIndicator(IndicatorPlugin):
    """相对强弱指标"""

    def __init__(self, period: int = 14) -> None:
        self._period = period

    def name(self) -> str:
        return f"RSI_{self._period}"

    def version(self) -> str:
        return "1.0"

    def required_fields(self) -> list[str]:
        return ["close"]

    def params(self) -> dict[str, Any]:
        return {"period": self._period}

    def calculate(self, data: pd.DataFrame) -> pd.Series:
        delta = data["close"].diff()
        gain = delta.clip(lower=0).rolling(window=self._period).mean()
        loss = (-delta.clip(upper=0)).rolling(window=self._period).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi


class MACDIndicator(IndicatorPlugin):
    """MACD 指标"""

    def __init__(self, fast: int = 12, slow: int = 26, signal: int = 9) -> None:
        self._fast = fast
        self._slow = slow
        self._signal = signal

    def name(self) -> str:
        return f"MACD_{self._fast}_{self._slow}_{self._signal}"

    def version(self) -> str:
        return "1.0"

    def required_fields(self) -> list[str]:
        return ["close"]

    def params(self) -> dict[str, Any]:
        return {"fast": self._fast, "slow": self._slow, "signal": self._signal}

    def calculate(self, data: pd.DataFrame) -> pd.DataFrame:
        ema_fast = data["close"].ewm(span=self._fast, adjust=False).mean()
        ema_slow = data["close"].ewm(span=self._slow, adjust=False).mean()
        macd_line = ema_fast - ema_slow
        signal_line = macd_line.ewm(span=self._signal, adjust=False).mean()
        histogram = macd_line - signal_line
        return pd.DataFrame({
            "macd": macd_line,
            "signal": signal_line,
            "histogram": histogram,
        })


class BollingerBandsIndicator(IndicatorPlugin):
    """布林带"""

    def __init__(self, period: int = 20, std_dev: float = 2.0) -> None:
        self._period = period
        self._std_dev = std_dev

    def name(self) -> str:
        return f"BBANDS_{self._period}_{self._std_dev}"

    def version(self) -> str:
        return "1.0"

    def required_fields(self) -> list[str]:
        return ["close"]

    def params(self) -> dict[str, Any]:
        return {"period": self._period, "std_dev": self._std_dev}

    def calculate(self, data: pd.DataFrame) -> pd.DataFrame:
        sma = data["close"].rolling(window=self._period).mean()
        std = data["close"].rolling(window=self._period).std()
        return pd.DataFrame({
            "bb_upper": sma + self._std_dev * std,
            "bb_middle": sma,
            "bb_lower": sma - self._std_dev * std,
            "bb_width": (2 * self._std_dev * std) / sma,
        })


class ATRIndicator(IndicatorPlugin):
    """平均真实波动幅度"""

    def __init__(self, period: int = 14) -> None:
        self._period = period

    def name(self) -> str:
        return f"ATR_{self._period}"

    def version(self) -> str:
        return "1.0"

    def required_fields(self) -> list[str]:
        return ["high", "low", "close"]

    def params(self) -> dict[str, Any]:
        return {"period": self._period}

    def calculate(self, data: pd.DataFrame) -> pd.Series:
        high = data["high"]
        low = data["low"]
        prev_close = data["close"].shift(1)
        tr = pd.concat([
            high - low,
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ], axis=1).max(axis=1)
        return tr.rolling(window=self._period).mean()


class VWAPIndicator(IndicatorPlugin):
    """成交量加权平均价"""

    def __init__(self) -> None:
        pass

    def name(self) -> str:
        return "VWAP"

    def version(self) -> str:
        return "1.0"

    def required_fields(self) -> list[str]:
        return ["high", "low", "close", "volume"]

    def params(self) -> dict[str, Any]:
        return {}

    def calculate(self, data: pd.DataFrame) -> pd.Series:
        typical_price = (data["high"] + data["low"] + data["close"]) / 3
        cumulative_tp_vol = (typical_price * data["volume"]).cumsum()
        cumulative_vol = data["volume"].cumsum()
        return cumulative_tp_vol / cumulative_vol


class OBVIndicator(IndicatorPlugin):
    """能量潮指标"""

    def __init__(self) -> None:
        pass

    def name(self) -> str:
        return "OBV"

    def version(self) -> str:
        return "1.0"

    def required_fields(self) -> list[str]:
        return ["close", "volume"]

    def params(self) -> dict[str, Any]:
        return {}

    def calculate(self, data: pd.DataFrame) -> pd.Series:
        direction = np.sign(data["close"].diff())
        return (direction * data["volume"]).cumsum()


class MomentumIndicator(IndicatorPlugin):
    """动量指标"""

    def __init__(self, period: int = 10) -> None:
        self._period = period

    def name(self) -> str:
        return f"MOM_{self._period}"

    def version(self) -> str:
        return "1.0"

    def required_fields(self) -> list[str]:
        return ["close"]

    def params(self) -> dict[str, Any]:
        return {"period": self._period}

    def calculate(self, data: pd.DataFrame) -> pd.Series:
        return data["close"] - data["close"].shift(self._period)


# 便捷函数：创建所有默认指标实例
def create_default_indicators() -> list[IndicatorPlugin]:
    """创建一组默认参数的指标实例"""
    return [
        SMAIndicator(5),
        SMAIndicator(10),
        SMAIndicator(20),
        EMAIndicator(5),
        EMAIndicator(10),
        EMAIndicator(20),
        RSIIndicator(14),
        MACDIndicator(12, 26, 9),
        BollingerBandsIndicator(20, 2.0),
        ATRIndicator(14),
        VWAPIndicator(),
        OBVIndicator(),
        MomentumIndicator(10),
    ]
