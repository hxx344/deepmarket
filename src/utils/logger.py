"""
Logger - 日志配置

基于 loguru 的统一日志系统。
"""

from __future__ import annotations

import sys
from pathlib import Path

from loguru import logger


def setup_logger(
    log_dir: str | Path = "logs",
    level: str = "INFO",
    rotation: str = "10 MB",
    retention: str = "30 days",
) -> None:
    """
    配置全局日志

    Args:
        log_dir: 日志文件目录
        level: 日志级别
        rotation: 文件轮转大小
        retention: 保留时长
    """
    log_dir = Path(log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)

    # 移除默认 handler
    logger.remove()

    # 控制台输出（带颜色）
    logger.add(
        sys.stderr,
        level=level,
        format=(
            "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
            "<level>{level:8s}</level> | "
            "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
            "<level>{message}</level>"
        ),
        colorize=True,
    )

    # 文件输出（全量日志）
    logger.add(
        str(log_dir / "pm_bot_{time:YYYY-MM-DD}.log"),
        level="DEBUG",
        format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level:8s} | {name}:{function}:{line} | {message}",
        rotation=rotation,
        retention=retention,
        encoding="utf-8",
    )

    # 错误日志（单独文件）
    logger.add(
        str(log_dir / "errors_{time:YYYY-MM-DD}.log"),
        level="ERROR",
        format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level:8s} | {name}:{function}:{line} | {message}",
        rotation=rotation,
        retention=retention,
        encoding="utf-8",
    )

    # 交易日志（单独文件）
    logger.add(
        str(log_dir / "trades_{time:YYYY-MM-DD}.log"),
        level="INFO",
        format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {message}",
        filter=lambda record: "trade" in record["extra"].get("tags", []),
        rotation="1 day",
        retention="90 days",
        encoding="utf-8",
    )

    logger.info(f"Logger initialized: level={level}, dir={log_dir}")
