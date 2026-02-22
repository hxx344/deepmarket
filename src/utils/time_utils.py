"""
Time Utils - 时间工具

处理时间戳转换、市场时间判断等。
"""

from __future__ import annotations

import time
from datetime import datetime, timezone


def now_ts() -> float:
    """当前 Unix 时间戳 (秒)"""
    return time.time()


def now_ms() -> int:
    """当前 Unix 时间戳 (毫秒)"""
    return int(time.time() * 1000)


def ts_to_str(ts: float, fmt: str = "%Y-%m-%d %H:%M:%S") -> str:
    """时间戳转字符串 (UTC)"""
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime(fmt)


def str_to_ts(s: str, fmt: str = "%Y-%m-%d %H:%M:%S") -> float:
    """字符串转时间戳"""
    dt = datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
    return dt.timestamp()


def date_to_ts(date_str: str) -> float:
    """日期字符串 (YYYY-MM-DD) 转时间戳"""
    return str_to_ts(f"{date_str} 00:00:00")


def ts_to_date(ts: float) -> str:
    """时间戳转日期字符串"""
    return ts_to_str(ts, "%Y-%m-%d")


def align_to_interval(ts: float, interval_seconds: int) -> float:
    """将时间戳对齐到某个间隔的起始"""
    return (int(ts) // interval_seconds) * interval_seconds


def interval_to_seconds(interval: str) -> int:
    """将间隔字符串转为秒数: '1m' -> 60, '5m' -> 300"""
    mapping = {
        "1s": 1, "5s": 5, "10s": 10, "30s": 30,
        "1m": 60, "3m": 180, "5m": 300, "15m": 900, "30m": 1800,
        "1h": 3600, "4h": 14400, "1d": 86400,
    }
    return mapping.get(interval, 60)


def is_market_hours() -> bool:
    """
    Polymarket/加密货币市场 24/7 运行，始终返回 True。
    保留此函数接口用于未来扩展（如限定交易时段）。
    """
    return True


def seconds_until_next_5min() -> float:
    """距离下一个 5分钟 K线开始的秒数"""
    now = time.time()
    next_bar = ((int(now) // 300) + 1) * 300
    return next_bar - now
