"""
Alerting System - 告警系统

多渠道告警：Telegram / Discord / 控制台 / 文件日志
"""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

import aiohttp
from loguru import logger


class AlertLevel(str, Enum):
    P0_CRITICAL = "P0"    # 紧急：资金安全
    P1_ALERT = "P1"       # 严重：风控触发
    P2_WARNING = "P2"     # 警告：接近阈值
    P3_INFO = "P3"        # 信息：常规事件


@dataclass
class Alert:
    """告警对象"""
    level: AlertLevel
    title: str
    message: str
    source: str = ""
    data: dict[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)


class AlertChannel:
    """告警渠道基类"""

    async def send(self, alert: Alert) -> bool:
        raise NotImplementedError


class ConsoleChannel(AlertChannel):
    """控制台告警"""

    async def send(self, alert: Alert) -> bool:
        icons = {"P0": "🔴", "P1": "🟠", "P2": "🟡", "P3": "🔵"}
        icon = icons.get(alert.level.value, "⚪")

        if alert.level in (AlertLevel.P0_CRITICAL, AlertLevel.P1_ALERT):
            logger.critical(f"{icon} [{alert.level.value}] {alert.title}: {alert.message}")
        elif alert.level == AlertLevel.P2_WARNING:
            logger.warning(f"{icon} [{alert.level.value}] {alert.title}: {alert.message}")
        else:
            logger.info(f"{icon} [{alert.level.value}] {alert.title}: {alert.message}")
        return True


class TelegramChannel(AlertChannel):
    """Telegram Bot 告警"""

    def __init__(self, bot_token: str, chat_id: str) -> None:
        self.bot_token = bot_token
        self.chat_id = chat_id
        self._api_base = f"https://api.telegram.org/bot{bot_token}"

    async def send(self, alert: Alert) -> bool:
        if not self.bot_token or not self.chat_id:
            return False

        icons = {"P0": "🔴", "P1": "🟠", "P2": "🟡", "P3": "🔵"}
        icon = icons.get(alert.level.value, "⚪")

        text = (
            f"{icon} *{alert.level.value} - {alert.title}*\n\n"
            f"{alert.message}\n\n"
            f"_Source: {alert.source}_\n"
            f"_Time: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(alert.timestamp))}_"
        )

        try:
            async with aiohttp.ClientSession() as session:
                await session.post(
                    f"{self._api_base}/sendMessage",
                    json={
                        "chat_id": self.chat_id,
                        "text": text,
                        "parse_mode": "Markdown",
                    },
                    timeout=aiohttp.ClientTimeout(total=10),
                )
            return True
        except Exception as e:
            logger.error(f"Telegram alert failed: {e}")
            return False


class DiscordChannel(AlertChannel):
    """Discord Webhook 告警"""

    def __init__(self, webhook_url: str) -> None:
        self.webhook_url = webhook_url

    async def send(self, alert: Alert) -> bool:
        if not self.webhook_url:
            return False

        color_map = {"P0": 0xFF0000, "P1": 0xFF8800, "P2": 0xFFFF00, "P3": 0x0088FF}
        color = color_map.get(alert.level.value, 0x808080)

        payload = {
            "embeds": [{
                "title": f"[{alert.level.value}] {alert.title}",
                "description": alert.message,
                "color": color,
                "footer": {"text": f"Source: {alert.source}"},
                "timestamp": time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(alert.timestamp)),
            }]
        }

        try:
            async with aiohttp.ClientSession() as session:
                await session.post(
                    self.webhook_url,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=10),
                )
            return True
        except Exception as e:
            logger.error(f"Discord alert failed: {e}")
            return False


class AlertManager:
    """
    告警管理器

    管理多个告警渠道，根据告警等级路由到不同渠道。

    Usage:
        manager = AlertManager()
        manager.add_channel(ConsoleChannel())
        manager.add_channel(TelegramChannel(token, chat_id), min_level=AlertLevel.P1_ALERT)

        await manager.send(Alert(
            level=AlertLevel.P1_ALERT,
            title="风控触发",
            message="策略 momentum 日亏损超限",
            source="risk_engine",
        ))
    """

    def __init__(self) -> None:
        self._channels: list[tuple[AlertChannel, AlertLevel]] = []
        self._alert_history: list[Alert] = []
        self._max_history = 1000
        self._rate_limiter: dict[str, float] = {}  # 防止重复告警
        self._rate_limit_seconds = 30  # 同一告警最小间隔

    def add_channel(
        self,
        channel: AlertChannel,
        min_level: AlertLevel = AlertLevel.P3_INFO,
    ) -> None:
        """
        添加告警渠道

        Args:
            channel: 渠道实例
            min_level: 最低告警等级 (只发送 >= 此等级的告警)
        """
        self._channels.append((channel, min_level))
        logger.info(f"Alert channel added: {channel.__class__.__name__} (min_level={min_level.value})")

    async def send(self, alert: Alert) -> None:
        """发送告警"""
        # 防抖: 同一告警短时间内不重复发送
        alert_key = f"{alert.level.value}:{alert.title}"
        last_sent = self._rate_limiter.get(alert_key, 0)
        if time.time() - last_sent < self._rate_limit_seconds:
            return
        self._rate_limiter[alert_key] = time.time()

        # 记录历史
        self._alert_history.append(alert)
        if len(self._alert_history) > self._max_history:
            self._alert_history = self._alert_history[-self._max_history // 2:]

        # 分发到各渠道
        level_order = [AlertLevel.P0_CRITICAL, AlertLevel.P1_ALERT, AlertLevel.P2_WARNING, AlertLevel.P3_INFO]
        alert_idx = level_order.index(alert.level)

        for channel, min_level in self._channels:
            min_idx = level_order.index(min_level)
            if alert_idx <= min_idx:  # P0 < P1 < P2 < P3
                try:
                    await channel.send(alert)
                except Exception as e:
                    logger.error(f"Channel {channel.__class__.__name__} failed: {e}")

    async def send_quick(
        self,
        level: AlertLevel,
        title: str,
        message: str,
        source: str = "",
    ) -> None:
        """快捷发送告警"""
        await self.send(Alert(
            level=level,
            title=title,
            message=message,
            source=source,
        ))

    def get_history(self, level: AlertLevel | None = None, limit: int = 50) -> list[Alert]:
        """获取告警历史"""
        alerts = self._alert_history
        if level:
            alerts = [a for a in alerts if a.level == level]
        return alerts[-limit:]
