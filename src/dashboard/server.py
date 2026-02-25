"""
Dashboard Server — aiohttp Web 服务器

提供:
- Jinja2 HTML 页面渲染
- REST API (/api/v1/...)
- WebSocket 实时推送 (/ws)
- 静态文件服务 (/static)
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import aiohttp.web
import aiohttp_jinja2
import jinja2
from loguru import logger

from src.core.event_bus import EventBus, get_event_bus
from src.core.context import Context

from .ws_handler import WebSocketManager
from .api_routes import register_api_routes

# ───────────────────── paths ─────────────────────
_DASHBOARD_DIR = Path(__file__).resolve().parent
_TEMPLATES_DIR = _DASHBOARD_DIR / "templates"
_STATIC_DIR = _DASHBOARD_DIR / "static"


class DashboardServer:
    """
    Dashboard Web 服务器

    Usage:
        server = DashboardServer(context=ctx, event_bus=bus)
        await server.start(host="0.0.0.0", port=8080)
        # ...
        await server.stop()
    """

    def __init__(
        self,
        context: Context,
        event_bus: EventBus | None = None,
        host: str = "0.0.0.0",
        port: int = 8080,
    ) -> None:
        self.context = context
        self.event_bus = event_bus or get_event_bus()
        self.host = host
        self.port = port

        self.app = aiohttp.web.Application()
        self.ws_manager = WebSocketManager(event_bus=self.event_bus, context=self.context)
        self.runner: aiohttp.web.AppRunner | None = None

        self._setup()

    def _setup(self) -> None:
        """初始化路由、模板引擎、静态文件等。"""
        # ── Jinja2 模板引擎 ──
        aiohttp_jinja2.setup(
            self.app,
            loader=jinja2.FileSystemLoader(str(_TEMPLATES_DIR)),
        )

        # ── 将共享对象存到 app 上 ──
        self.app["context"] = self.context
        self.app["event_bus"] = self.event_bus
        self.app["ws_manager"] = self.ws_manager

        # ── 静态文件 ──
        self.app.router.add_static("/static", _STATIC_DIR, name="static")

        # ── HTML 页面路由 ──
        self.app.router.add_get("/", self._handle_index)
        self.app.router.add_get("/overview", self._handle_overview)
        self.app.router.add_get("/market", self._handle_trades)   # 重定向到行情中心
        self.app.router.add_get("/orderbook", self._handle_trades)  # 重定向到行情中心
        self.app.router.add_get("/strategy", self._handle_strategy)
        self.app.router.add_get("/backtest", self._handle_backtest)
        self.app.router.add_get("/trades", self._handle_trades)
        self.app.router.add_get("/risk", self._handle_risk)
        self.app.router.add_get("/settings", self._handle_settings)

        # ── WebSocket 端点 ──
        self.app.router.add_get("/ws", self.ws_manager.handle)

        # ── REST API ──
        register_api_routes(self.app)

        # ── 生命周期 ──
        self.app.on_startup.append(self._on_startup)
        self.app.on_shutdown.append(self._on_shutdown)

    # ────────────────── 生命周期 ──────────────────

    async def _on_startup(self, app: aiohttp.web.Application) -> None:
        """服务器启动时初始化 WebSocket 管理器。"""
        await self.ws_manager.start()
        logger.info(f"Dashboard WebSocket 管理器已启动")

    async def _on_shutdown(self, app: aiohttp.web.Application) -> None:
        """服务器关闭时清理。"""
        await self.ws_manager.stop()
        logger.info("Dashboard WebSocket 管理器已停止")

    async def start(self) -> None:
        """启动 HTTP 服务器（非阻塞）。端口被占用时自动尝试下一个端口（最多 10 次）。"""
        self.runner = aiohttp.web.AppRunner(self.app)
        await self.runner.setup()

        max_attempts = 10
        port = self.port
        for attempt in range(max_attempts):
            try:
                site = aiohttp.web.TCPSite(self.runner, self.host, port)
                await site.start()
                self.port = port  # 记录实际使用的端口
                logger.info(f"Dashboard 已启动: http://{self.host}:{port}")
                return
            except OSError as e:
                if e.errno in (10048, 98):  # Windows WSAEADDRINUSE / Linux EADDRINUSE
                    logger.warning(f"端口 {port} 已被占用, 尝试 {port + 1}...")
                    port += 1
                else:
                    raise
        # 全部失败 — 不让 Dashboard 影响主系统
        logger.error(f"Dashboard 启动失败: 端口 {self.port}-{port - 1} 均被占用, 系统继续运行")
        await self.runner.cleanup()
        self.runner = None

    async def stop(self) -> None:
        """停止 HTTP 服务器。"""
        if self.runner:
            await self.runner.cleanup()
            self.runner = None
        logger.info("Dashboard 已停止")

    # ────────────────── 页面处理 ──────────────────

    @aiohttp_jinja2.template("overview.html")
    async def _handle_index(self, request: aiohttp.web.Request) -> dict[str, Any]:
        return self._common_context("overview")

    @aiohttp_jinja2.template("overview.html")
    async def _handle_overview(self, request: aiohttp.web.Request) -> dict[str, Any]:
        return self._common_context("overview")

    @aiohttp_jinja2.template("strategy.html")
    async def _handle_strategy(self, request: aiohttp.web.Request) -> dict[str, Any]:
        return self._common_context("strategy")

    @aiohttp_jinja2.template("backtest.html")
    async def _handle_backtest(self, request: aiohttp.web.Request) -> dict[str, Any]:
        return self._common_context("backtest")

    @aiohttp_jinja2.template("trades.html")
    async def _handle_trades(self, request: aiohttp.web.Request) -> dict[str, Any]:
        return self._common_context("trades")


    @aiohttp_jinja2.template("risk.html")
    async def _handle_risk(self, request: aiohttp.web.Request) -> dict[str, Any]:
        return self._common_context("risk")

    @aiohttp_jinja2.template("settings.html")
    async def _handle_settings(self, request: aiohttp.web.Request) -> dict[str, Any]:
        return self._common_context("settings")

    # ────────────────── 模板上下文 ──────────────────

    def _common_context(self, active_page: str) -> dict[str, Any]:
        """生成所有页面共享的模板上下文。"""
        ctx = self.context
        return {
            "active_page": active_page,
            "run_mode": ctx.run_mode.value,
            "trading_mode": ctx.trading_mode.value,
            "account": {
                "balance": ctx.account.balance,
                "total_equity": ctx.account.total_equity,
                "daily_pnl": ctx.account.daily_pnl,
                "total_pnl": ctx.account.total_pnl,
                "initial_balance": ctx.account.initial_balance,
            },
            "market": {
                "btc_price": ctx.market.btc_price,
                "pm_yes_price": ctx.market.pm_yes_price,
                "pm_no_price": ctx.market.pm_no_price,
                "pm_market_question": ctx.market.pm_market_question,
                "pm_window_start_price": ctx.market.pm_window_start_price,
            },
            "ws_url": "",  # 由前端 JS 根据 window.location 自动构造
        }
