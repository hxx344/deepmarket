"""
Risk Engine - 风控引擎

四层风控架构：订单级 → 策略级 → 账户级 → 熔断
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable

from loguru import logger

from src.core.context import Context
from src.core.event_bus import Event, EventBus, EventType, get_event_bus


class RiskLevel(str, Enum):
    INFO = "info"           # P3
    WARNING = "warning"     # P2
    ALERT = "alert"         # P1
    CRITICAL = "critical"   # P0


class RuleResult(str, Enum):
    PASS = "pass"
    WARN = "warn"
    REJECT = "reject"
    BREAKER = "breaker"     # 触发熔断


@dataclass
class RiskCheckResult:
    """风控检查结果"""
    rule_id: str
    rule_name: str
    result: RuleResult
    message: str = ""
    level: RiskLevel = RiskLevel.INFO
    data: dict[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)


@dataclass
class RiskRule:
    """风控规则定义"""
    id: str
    name: str
    layer: int               # 1=订单级, 2=策略级, 3=账户级, 4=熔断
    enabled: bool = True
    check_fn: Callable[..., RiskCheckResult] | None = None
    params: dict[str, Any] = field(default_factory=dict)


class RiskEngine:
    """
    风控引擎

    Usage:
        risk = RiskEngine(context)
        risk.load_default_rules()

        # 订单预检
        result = risk.check_order(order_request)
        if not result.passed:
            reject order

        # 定时巡检
        risk.run_periodic_check()
    """

    def __init__(
        self,
        context: Context,
        event_bus: EventBus | None = None,
    ) -> None:
        self.context = context
        self.event_bus = event_bus or get_event_bus()

        self._rules: dict[str, RiskRule] = {}
        self._check_history: list[RiskCheckResult] = []
        self._breaker_active = False
        self._breaker_until: float = 0.0

        # 策略级统计
        self._strategy_daily_pnl: dict[str, float] = {}
        self._strategy_consecutive_loss: dict[str, int] = {}
        self._strategy_positions: dict[str, float] = {}

        # 账户级统计
        self._daily_total_loss: float = 0.0
        self._total_open_exposure: float = 0.0
        self._order_count_minute: list[float] = []  # 最近 1 分钟的下单时间戳

    def load_default_rules(self) -> None:
        """加载默认风控规则"""

        # === Layer 1: 订单级 ===
        self.add_rule(RiskRule(
            id="R-101", name="单笔金额上限", layer=1,
            params={"max_amount": 500.0},
        ))
        self.add_rule(RiskRule(
            id="R-102", name="价格合理性检查", layer=1,
            params={"max_deviation": 0.05},
        ))
        self.add_rule(RiskRule(
            id="R-103", name="频率限制", layer=1,
            params={"max_orders_per_min": 120},
        ))
        self.add_rule(RiskRule(
            id="R-104", name="最小收益率检查", layer=1,
            params={"min_expected_return": 0.001},
        ))
        self.add_rule(RiskRule(
            id="R-105", name="流动性检查", layer=1,
            params={"min_depth": 50.0},
        ))
        self.add_rule(RiskRule(
            id="R-106", name="市场到期检查", layer=1,
            params={"min_seconds_to_expiry": 30},
        ))

        # === Layer 2: 策略级 ===
        self.add_rule(RiskRule(
            id="R-201", name="策略日亏损限额", layer=2,
            params={"max_daily_loss": -2000.0},
        ))
        self.add_rule(RiskRule(
            id="R-202", name="策略连续亏损", layer=2,
            params={"max_consecutive_losses": 20},
        ))
        self.add_rule(RiskRule(
            id="R-203", name="策略持仓上限", layer=2,
            params={"max_position": 10000.0},
        ))
        self.add_rule(RiskRule(
            id="R-204", name="策略回撤限制", layer=2,
            params={"max_drawdown": -0.10},
        ))

        # === Layer 3: 账户级 ===
        self.add_rule(RiskRule(
            id="R-301", name="总资产保护", layer=3,
            params={"min_equity_ratio": 0.50},
        ))
        self.add_rule(RiskRule(
            id="R-302", name="日亏损总限额", layer=3,
            params={"max_daily_total_loss": -3000.0},
        ))
        self.add_rule(RiskRule(
            id="R-303", name="最大总敞口", layer=3,
            params={"max_total_exposure": 10000.0},
        ))
        self.add_rule(RiskRule(
            id="R-304", name="集中度风险", layer=3,
            params={"max_concentration": 0.30},
        ))

        # === Layer 4: 熔断 ===
        self.add_rule(RiskRule(
            id="R-401", name="市场异常熔断", layer=4,
            params={"max_5m_volatility": 0.05, "cooldown_seconds": 60},
        ))
        self.add_rule(RiskRule(
            id="R-402", name="系统故障熔断", layer=4,
            params={"max_data_delay_seconds": 10},
        ))
        self.add_rule(RiskRule(
            id="R-403", name="Polymarket异常", layer=4,
            params={},
        ))
        self.add_rule(RiskRule(
            id="R-404", name="资金异常熔断", layer=4,
            params={"max_balance_change_pct": 0.20},
        ))

        logger.info(f"Loaded {len(self._rules)} default risk rules")

    def add_rule(self, rule: RiskRule) -> None:
        """添加风控规则"""
        self._rules[rule.id] = rule

    def remove_rule(self, rule_id: str) -> None:
        """移除风控规则"""
        self._rules.pop(rule_id, None)

    def enable_rule(self, rule_id: str) -> None:
        if rule_id in self._rules:
            self._rules[rule_id].enabled = True

    def disable_rule(self, rule_id: str) -> None:
        if rule_id in self._rules:
            self._rules[rule_id].enabled = False

    # ========== 订单预检 ==========

    def check_order(self, order: Any) -> tuple[bool, str]:
        """
        订单风控预检 (Layer 1 + Layer 2 + Layer 3)

        Returns:
            (passed, reason)
        """
        # 检查熔断器
        if self._breaker_active and time.time() < self._breaker_until:
            return False, "Circuit breaker active"

        all_results = []

        # Layer 1: 订单级
        all_results.extend(self._check_order_level(order))

        # Layer 2: 策略级
        all_results.extend(self._check_strategy_level(order))

        # Layer 3: 账户级
        all_results.extend(self._check_account_level(order))

        # 记录检查历史
        self._check_history.extend(all_results)

        # 判断最终结果
        for r in all_results:
            if r.result == RuleResult.BREAKER:
                asyncio_safe_publish = self._trigger_breaker(r)
                return False, f"BREAKER: {r.message}"
            elif r.result == RuleResult.REJECT:
                return False, f"{r.rule_id}: {r.message}"

        return True, "All checks passed"

    def _check_order_level(self, order: Any) -> list[RiskCheckResult]:
        """Layer 1 订单级检查"""
        results = []
        order_size = getattr(order, "size", 0)
        order_price = getattr(order, "price", 0)

        # R-101: 单笔金额
        rule = self._rules.get("R-101")
        if rule and rule.enabled:
            max_amt = rule.params.get("max_amount", 100)
            if order_size > max_amt:
                results.append(RiskCheckResult(
                    rule_id="R-101", rule_name=rule.name,
                    result=RuleResult.REJECT,
                    message=f"Order size {order_size} exceeds max {max_amt}",
                    level=RiskLevel.WARNING,
                ))
            else:
                results.append(RiskCheckResult(
                    rule_id="R-101", rule_name=rule.name,
                    result=RuleResult.PASS,
                ))

        # R-102: 价格合理性
        rule = self._rules.get("R-102")
        if rule and rule.enabled and order_price > 0:
            if order_price < 0.01 or order_price > 0.99:
                results.append(RiskCheckResult(
                    rule_id="R-102", rule_name=rule.name,
                    result=RuleResult.REJECT,
                    message=f"Price {order_price} out of valid range [0.01, 0.99]",
                    level=RiskLevel.WARNING,
                ))
            else:
                results.append(RiskCheckResult(
                    rule_id="R-102", rule_name=rule.name,
                    result=RuleResult.PASS,
                ))

        # R-103: 频率限制
        rule = self._rules.get("R-103")
        if rule and rule.enabled:
            now = time.time()
            self._order_count_minute = [t for t in self._order_count_minute if now - t < 60]
            max_per_min = rule.params.get("max_orders_per_min", 10)
            if len(self._order_count_minute) >= max_per_min:
                results.append(RiskCheckResult(
                    rule_id="R-103", rule_name=rule.name,
                    result=RuleResult.REJECT,
                    message=f"Rate limit: {len(self._order_count_minute)} orders in last minute",
                    level=RiskLevel.WARNING,
                ))
            else:
                self._order_count_minute.append(now)
                results.append(RiskCheckResult(
                    rule_id="R-103", rule_name=rule.name,
                    result=RuleResult.PASS,
                ))

        return results

    def _check_strategy_level(self, order: Any) -> list[RiskCheckResult]:
        """Layer 2 策略级检查"""
        results = []
        strategy_id = getattr(order, "strategy_id", "default")

        # R-201: 策略日亏损
        rule = self._rules.get("R-201")
        if rule and rule.enabled:
            daily_pnl = self._strategy_daily_pnl.get(strategy_id, 0)
            max_loss = rule.params.get("max_daily_loss", -200)
            if daily_pnl <= max_loss:
                results.append(RiskCheckResult(
                    rule_id="R-201", rule_name=rule.name,
                    result=RuleResult.REJECT,
                    message=f"Strategy {strategy_id} daily PnL {daily_pnl:.2f} <= {max_loss}",
                    level=RiskLevel.ALERT,
                ))
            else:
                results.append(RiskCheckResult(
                    rule_id="R-201", rule_name=rule.name,
                    result=RuleResult.PASS,
                ))

        # R-202: 策略连续亏损
        rule = self._rules.get("R-202")
        if rule and rule.enabled:
            consec = self._strategy_consecutive_loss.get(strategy_id, 0)
            max_consec = rule.params.get("max_consecutive_losses", 5)
            if consec >= max_consec:
                results.append(RiskCheckResult(
                    rule_id="R-202", rule_name=rule.name,
                    result=RuleResult.REJECT,
                    message=f"Strategy {strategy_id} consecutive losses: {consec}",
                    level=RiskLevel.ALERT,
                ))

        return results

    def _check_account_level(self, order: Any) -> list[RiskCheckResult]:
        """Layer 3 账户级检查"""
        results = []

        # R-301: 总资产保护
        rule = self._rules.get("R-301")
        if rule and rule.enabled:
            equity = self.context.account.total_equity
            initial = self.context.account.initial_balance
            min_ratio = rule.params.get("min_equity_ratio", 0.70)
            if initial > 0 and equity / initial < min_ratio:
                results.append(RiskCheckResult(
                    rule_id="R-301", rule_name=rule.name,
                    result=RuleResult.BREAKER,
                    message=f"Equity {equity:.2f} < {min_ratio:.0%} of initial {initial:.2f}",
                    level=RiskLevel.CRITICAL,
                ))

        # R-302: 日亏损总限额
        rule = self._rules.get("R-302")
        if rule and rule.enabled:
            max_loss = rule.params.get("max_daily_total_loss", -500)
            if self._daily_total_loss <= max_loss:
                results.append(RiskCheckResult(
                    rule_id="R-302", rule_name=rule.name,
                    result=RuleResult.REJECT,
                    message=f"Daily total loss {self._daily_total_loss:.2f} <= {max_loss}",
                    level=RiskLevel.CRITICAL,
                ))

        # R-303: 最大总敞口
        rule = self._rules.get("R-303")
        if rule and rule.enabled:
            max_exp = rule.params.get("max_total_exposure", 2000)
            order_size = getattr(order, "size", 0)
            if self._total_open_exposure + order_size > max_exp:
                results.append(RiskCheckResult(
                    rule_id="R-303", rule_name=rule.name,
                    result=RuleResult.REJECT,
                    message=f"Total exposure {self._total_open_exposure + order_size:.2f} > {max_exp}",
                    level=RiskLevel.ALERT,
                ))

        return results

    # ========== 熔断器 ==========

    def _trigger_breaker(self, check: RiskCheckResult) -> None:
        """触发熔断"""
        cooldown = 60  # 默认冷却 60s
        if check.rule_id in self._rules:
            cooldown = self._rules[check.rule_id].params.get("cooldown_seconds", 60)

        self._breaker_active = True
        self._breaker_until = time.time() + cooldown

        logger.critical(f"🔴 CIRCUIT BREAKER TRIGGERED: {check.rule_id} - {check.message}")
        logger.critical(f"   Trading suspended for {cooldown}s")

    def emergency_stop(self) -> None:
        """紧急停止 (R-405)"""
        self._breaker_active = True
        self._breaker_until = time.time() + 86400  # 暂停 24h
        logger.critical("🔴 EMERGENCY STOP - All trading suspended for 24h")

    def reset_breaker(self) -> None:
        """重置熔断器"""
        self._breaker_active = False
        self._breaker_until = 0.0
        logger.warning("Circuit breaker reset")

    @property
    def is_breaker_active(self) -> bool:
        if self._breaker_active and time.time() >= self._breaker_until:
            self._breaker_active = False
            logger.info("Circuit breaker expired, trading resumed")
        return self._breaker_active

    # ========== 状态更新 ==========

    def update_trade_result(self, strategy_id: str, pnl: float) -> None:
        """更新交易结果（供风控统计使用）"""
        # 更新策略级
        self._strategy_daily_pnl[strategy_id] = \
            self._strategy_daily_pnl.get(strategy_id, 0) + pnl

        if pnl < 0:
            self._strategy_consecutive_loss[strategy_id] = \
                self._strategy_consecutive_loss.get(strategy_id, 0) + 1
        else:
            self._strategy_consecutive_loss[strategy_id] = 0

        # 更新账户级
        if pnl < 0:
            self._daily_total_loss += pnl

    def update_exposure(self, total_exposure: float) -> None:
        """更新总敞口"""
        self._total_open_exposure = total_exposure

    def reset_daily(self) -> None:
        """每日重置"""
        self._strategy_daily_pnl.clear()
        self._daily_total_loss = 0.0
        logger.info("Risk engine daily stats reset")

    # ========== 查询 ==========

    def get_status(self) -> dict[str, Any]:
        """获取风控状态"""
        return {
            "breaker_active": self.is_breaker_active,
            "breaker_until": self._breaker_until,
            "daily_total_loss": self._daily_total_loss,
            "total_exposure": self._total_open_exposure,
            "strategy_pnl": dict(self._strategy_daily_pnl),
            "strategy_consec_loss": dict(self._strategy_consecutive_loss),
            "rules_count": len(self._rules),
            "rules_enabled": sum(1 for r in self._rules.values() if r.enabled),
            "recent_checks": len(self._check_history),
        }

    def get_check_history(self, limit: int = 50) -> list[RiskCheckResult]:
        return self._check_history[-limit:]
