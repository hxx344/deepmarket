"""
REST API Routes — Dashboard 数据接口

所有 API 统一前缀 /api/v1/，返回 JSON。
"""

from __future__ import annotations

import json
import time
from typing import Any

import aiohttp.web
from loguru import logger

from src.core.context import Context
from src.core.event_bus import EventBus


def register_api_routes(app: aiohttp.web.Application) -> None:
    """注册所有 REST API 路由。"""
    app.router.add_get("/api/v1/status", handle_status)
    app.router.add_get("/api/v1/account", handle_account)
    app.router.add_get("/api/v1/market", handle_market)
    app.router.add_get("/api/v1/strategies", handle_strategies)
    app.router.add_post("/api/v1/strategies/{name}/toggle", handle_strategy_toggle)
    app.router.add_get("/api/v1/trades", handle_trades)
    app.router.add_get("/api/v1/risk", handle_risk)
    app.router.add_get("/api/v1/orderbook", handle_orderbook_api)
    app.router.add_get("/api/v1/klines", handle_klines)
    app.router.add_get("/api/v1/alerts", handle_alerts)
    app.router.add_get("/api/v1/settings", handle_get_settings)
    app.router.add_put("/api/v1/settings", handle_update_settings)
    app.router.add_get("/api/v1/backtest/reports", handle_backtest_reports)


# ────────── 辅助 ──────────

def _json(data: Any, status: int = 200) -> aiohttp.web.Response:
    return aiohttp.web.json_response(data, status=status)


def _ctx(request: aiohttp.web.Request) -> Context:
    return request.app["context"]


def _bus(request: aiohttp.web.Request) -> EventBus:
    return request.app["event_bus"]


# ────────── 系统状态 ──────────

async def handle_status(request: aiohttp.web.Request) -> aiohttp.web.Response:
    ctx = _ctx(request)
    ws_mgr = request.app["ws_manager"]
    return _json({
        "status": "running",
        "run_mode": ctx.run_mode.value,
        "trading_mode": ctx.trading_mode.value,
        "uptime": time.time() - ctx.start_time,
        "ws_clients": ws_mgr.client_count,
        "btc_price": ctx.market.btc_price,
    })


# ────────── 账户 ──────────

async def handle_account(request: aiohttp.web.Request) -> aiohttp.web.Response:
    ctx = _ctx(request)
    acct = ctx.account
    return _json({
        "balance": acct.balance,
        "available": acct.available,
        "total_equity": acct.total_equity,
        "daily_pnl": acct.daily_pnl,
        "total_pnl": acct.total_pnl,
        "initial_balance": acct.initial_balance,
        "open_positions": len(acct.open_positions),
        "return_pct": (
            (acct.total_equity - acct.initial_balance) / acct.initial_balance * 100
            if acct.initial_balance > 0 else 0
        ),
    })


# ────────── 市场行情 ──────────

async def handle_market(request: aiohttp.web.Request) -> aiohttp.web.Response:
    ctx = _ctx(request)
    m = ctx.market
    return _json({
        "btc_price": m.btc_price,
        "pm_yes_price": m.pm_yes_price,
        "pm_no_price": m.pm_no_price,
        "pm_volume_5m": m.pm_volume_5m,
        "pm_market_active": m.pm_market_active,
        "last_update": m.last_update,
    })


# ────────── 策略列表 ──────────

async def handle_strategies(request: aiohttp.web.Request) -> aiohttp.web.Response:
    ctx = _ctx(request)
    strategies = ctx.get("strategies", [])
    items = []
    for s in strategies:
        items.append({
            "name": s.name() if callable(getattr(s, "name", None)) else str(s),
            "status": getattr(s, "_status", "unknown"),
            "pnl": getattr(s, "_pnl", 0.0),
            "trades": getattr(s, "_trade_count", 0),
            "win_rate": getattr(s, "_win_rate", 0.0),
        })
    return _json({"strategies": items})


async def handle_strategy_toggle(request: aiohttp.web.Request) -> aiohttp.web.Response:
    name = request.match_info["name"]
    ctx = _ctx(request)
    strategies = ctx.get("strategies", [])
    for s in strategies:
        s_name = s.name() if callable(getattr(s, "name", None)) else ""
        if s_name == name:
            # Toggle enabled state
            current = getattr(s, "_enabled", True)
            s._enabled = not current
            return _json({"name": name, "enabled": s._enabled})
    return _json({"error": f"策略 '{name}' 未找到"}, status=404)


# ────────── 交易记录 ──────────

async def handle_trades(request: aiohttp.web.Request) -> aiohttp.web.Response:
    ctx = _ctx(request)
    storage = ctx.get("storage")
    trades = []
    if storage:
        try:
            # 尝试从 SQLite 读取最近交易
            db = storage.get_sqlite()
            if db:
                cursor = db.execute(
                    "SELECT * FROM trades ORDER BY timestamp DESC LIMIT 100"
                )
                cols = [desc[0] for desc in cursor.description] if cursor.description else []
                trades = [dict(zip(cols, row)) for row in cursor.fetchall()]
        except Exception:
            pass
    return _json({"trades": trades, "total": len(trades)})


# ────────── 风控状态 ──────────

async def handle_risk(request: aiohttp.web.Request) -> aiohttp.web.Response:
    ctx = _ctx(request)
    risk_engine = ctx.get("risk_engine")
    data: dict[str, Any] = {
        "status": "normal",
        "circuit_breaker": False,
        "rules": [],
        "recent_alerts": [],
    }
    if risk_engine:
        data["circuit_breaker"] = getattr(risk_engine, "_circuit_breaker_active", False)
        data["status"] = "breaker" if data["circuit_breaker"] else "normal"
        # 获取规则状态
        for rule in getattr(risk_engine, "_rules", []):
            data["rules"].append({
                "id": getattr(rule, "rule_id", ""),
                "name": getattr(rule, "name", ""),
                "enabled": getattr(rule, "enabled", True),
            })
    return _json(data)


# ────────── 订单簿 ──────────

def _serialize_orderbook(ob) -> dict[str, Any]:
    """将 OrderBook 序列化为 API 响应格式"""
    if not ob:
        return {"bids": [], "asks": [], "spread": 0}
    try:
        state = ob.get_state()
        return {
            "bids": [{"price": lvl.price, "quantity": lvl.size} for lvl in state.bids[:20]],
            "asks": [{"price": lvl.price, "quantity": lvl.size} for lvl in state.asks[:20]],
            "spread": state.spread,
        }
    except Exception:
        return {"bids": [], "asks": [], "spread": 0}


async def handle_orderbook_api(request: aiohttp.web.Request) -> aiohttp.web.Response:
    ctx = _ctx(request)
    ob_up = ctx.get("orderbook_up")
    ob_down = ctx.get("orderbook_down")

    # 兼容: 如果没有 up/down，回退到旧的单一 orderbook
    if not ob_up and not ob_down:
        ob_up = ctx.get("orderbook")

    data: dict[str, Any] = {
        "up": _serialize_orderbook(ob_up),
        "down": _serialize_orderbook(ob_down),
        # 兼容旧格式: 顶层也保留 up 的数据
        **_serialize_orderbook(ob_up),
    }
    return _json(data)


# ────────── K 线数据 ──────────

async def handle_klines(request: aiohttp.web.Request) -> aiohttp.web.Response:
    ctx = _ctx(request)
    aggregator = ctx.get("aggregator")
    limit = int(request.query.get("limit", "200"))
    interval = request.query.get("interval", "5m")

    klines: list[dict] = []
    if aggregator:
        try:
            raw = aggregator.get_klines(interval=interval, limit=limit)
            for k in raw:
                klines.append({
                    "time": k.get("timestamp", 0),
                    "open": k.get("open", 0),
                    "high": k.get("high", 0),
                    "low": k.get("low", 0),
                    "close": k.get("close", 0),
                    "volume": k.get("volume", 0),
                })
        except Exception:
            pass
    return _json({"klines": klines, "interval": interval})


# ────────── 告警历史 ──────────

async def handle_alerts(request: aiohttp.web.Request) -> aiohttp.web.Response:
    ctx = _ctx(request)
    alert_mgr = ctx.get("alert_manager")
    alerts: list[dict] = []
    if alert_mgr:
        history = getattr(alert_mgr, "_history", [])
        for a in history[-50:]:
            alerts.append({
                "level": str(getattr(a, "level", "")),
                "message": getattr(a, "message", ""),
                "timestamp": getattr(a, "timestamp", 0),
            })
    return _json({"alerts": alerts})


# ────────── 设置 ──────────

async def handle_get_settings(request: aiohttp.web.Request) -> aiohttp.web.Response:
    ctx = _ctx(request)
    cfg = ctx.get("config", {})
    # 只返回安全的配置项 (不含密钥)
    safe = {
        "datasources": {
            "chainlink": {
                k: v
                for k, v in cfg.get("datasources", {}).get("chainlink", {}).items()
                if k not in ("client_secret",)
            },
        },
        "trading": {
            "mode": cfg.get("trading", {}).get("mode", "paper"),
        },
        "risk": cfg.get("risk_rules_config", {}),
    }
    return _json(safe)


async def handle_update_settings(request: aiohttp.web.Request) -> aiohttp.web.Response:
    try:
        body = await request.json()
    except json.JSONDecodeError:
        return _json({"error": "无效 JSON"}, status=400)

    ctx = _ctx(request)
    cfg = ctx.get("config", {})

    # 目前只支持更新部分安全配置
    if "trading" in body:
        cfg.setdefault("trading", {}).update(body["trading"])
    if "risk" in body:
        cfg["risk_rules_config"] = body["risk"]

    return _json({"ok": True, "message": "配置已更新 (运行时生效, 重启后需手动保存)"})


# ────────── 回测报告列表 ──────────

async def handle_backtest_reports(request: aiohttp.web.Request) -> aiohttp.web.Response:
    from pathlib import Path
    reports_dir = Path("reports")
    reports = []
    if reports_dir.exists():
        for f in sorted(reports_dir.glob("*.html"), reverse=True):
            reports.append({
                "name": f.name,
                "size": f.stat().st_size,
                "modified": f.stat().st_mtime,
                "url": f"/static/reports/{f.name}",
            })
    return _json({"reports": reports[:20]})
