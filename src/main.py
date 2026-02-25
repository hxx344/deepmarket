"""
Polymarket BTC 5-min Prediction Betting System
================================================
主入口文件 — 将所有模块编排到一起。

用法:
    python -m src.main live     # 实盘/模拟盘运行
    python -m src.main paper    # 纯纸交易模式
    python -m src.main backtest # 回测模式
"""

from __future__ import annotations

import argparse
import asyncio
import signal
import sys
import time
from pathlib import Path

# 自动加载项目根目录的 .env 文件
try:
    from dotenv import load_dotenv
    load_dotenv(override=False)
except ImportError:
    pass

import yaml

# ── 项目根目录 ─────────────────────────────────────
ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

# ── 内部模块 ───────────────────────────────────────
from src.core.event_bus import Event, EventBus, EventType, get_event_bus  # noqa: E402
from src.core.context import Context, RunMode, TradingMode, AccountState  # noqa: E402
from src.core.storage import StorageManager  # noqa: E402
from src.market.datasources.chainlink_ds import ChainlinkDataSource, ChainlinkConfig  # noqa: E402
from src.market.datasources.polymarket_ds import PolymarketDataSource  # noqa: E402
from src.market.aggregator import MarketAggregator  # noqa: E402
from src.orderbook.book import OrderBook  # noqa: E402
from src.orderbook.analyzer import OrderBookAnalyzer  # noqa: E402
from src.orderbook.snapshot import SnapshotManager  # noqa: E402
from src.trading.executor import OrderExecutor  # noqa: E402
from src.trading.wallet import WalletManager  # noqa: E402
from src.risk.engine import RiskEngine  # noqa: E402
from src.risk.alerting import AlertManager, ConsoleChannel, AlertLevel  # noqa: E402
from src.strategy.base import Strategy  # noqa: E402
from src.backtest.engine import BacktestEngine, BacktestConfig  # noqa: E402
from src.backtest.report import ReportGenerator  # noqa: E402
from src.utils.logger import setup_logger  # noqa: E402
from src.utils.trade_logger import TradeLogger  # noqa: E402
from src.dashboard.server import DashboardServer  # noqa: E402

# ── 全局实例 ───────────────────────────────────────
logger = None  # 延迟初始化


# ================================================================
#  配置加载
# ================================================================
def load_config(config_dir: Path = None) -> dict:
    """加载并合并所有 YAML 配置。"""
    if config_dir is None:
        config_dir = ROOT_DIR / "config"

    settings_path = config_dir / "settings.yaml"
    if not settings_path.exists():
        raise FileNotFoundError(f"配置文件不存在: {settings_path}")

    with open(settings_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}

    # 可选: 加载指标配置
    indicators_path = config_dir / "indicators.yaml"
    if indicators_path.exists():
        with open(indicators_path, "r", encoding="utf-8") as f:
            cfg["indicators_config"] = yaml.safe_load(f) or {}

    # 可选: 加载风控规则
    risk_path = config_dir / "risk_rules.yaml"
    if risk_path.exists():
        with open(risk_path, "r", encoding="utf-8") as f:
            cfg["risk_rules_config"] = yaml.safe_load(f) or {}

    # 可选: 加载策略配置
    strategies_dir = config_dir / "strategies"
    if strategies_dir.exists():
        cfg["strategies_config"] = {}
        for p in strategies_dir.glob("*.yaml"):
            with open(p, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
                name = data.get("strategy", {}).get("name", p.stem)
                cfg["strategies_config"][name] = data

    return cfg


# ================================================================
#  上下文构建
# ================================================================
def build_context(cfg: dict, mode: str) -> Context:
    """根据命令行模式 + 配置构建 Context。"""
    run_mode_map = {
        "live": RunMode.LIVE,
        "paper": RunMode.PAPER,
        "backtest": RunMode.BACKTEST,
    }
    trading_mode_map = {
        "auto": TradingMode.AUTO,
        "semi_auto": TradingMode.SEMI_AUTO,
        "manual": TradingMode.MANUAL,
        "paper": TradingMode.PAPER,
    }

    run_mode = run_mode_map.get(mode, RunMode.PAPER)

    # 纸交易模式强制使用 PAPER trading mode
    if mode == "paper":
        trading_mode = TradingMode.PAPER
    else:
        # 优先 app.trading_mode, 兼容旧 trading.mode
        tm_str = cfg.get("app", {}).get("trading_mode", "") \
              or cfg.get("trading", {}).get("mode", "paper")
        trading_mode = trading_mode_map.get(tm_str, TradingMode.PAPER)

    # 账户初始状态
    acct_cfg = cfg.get("trading", {}).get("paper_account", {})
    ctx = Context(
        run_mode=run_mode,
        trading_mode=trading_mode,
    )
    initial_bal = acct_cfg.get("initial_balance", 100.0)
    ctx.account.balance = initial_bal
    ctx.account.available = initial_bal
    ctx.account.initial_balance = initial_bal
    ctx.account.total_equity = initial_bal

    # 把整个配置放到 context 的 data store 里供各模块访问
    ctx.set("config", cfg)

    return ctx


# ================================================================
#  策略加载
# ================================================================
def load_strategies(cfg: dict) -> list[Strategy]:
    """从 config/strategies/ 加载启用的策略实例。"""
    import importlib

    strategies: list[Strategy] = []
    strategies_cfg = cfg.get("strategies_config", {})

    for name, scfg in strategies_cfg.items():
        s_info = scfg.get("strategy", {})
        if not s_info.get("enabled", False):
            continue

        module_path = s_info.get("module", "")
        class_name = s_info.get("class", "")
        params = s_info.get("params", {})

        if not module_path or not class_name:
            continue

        try:
            mod = importlib.import_module(module_path)
            cls = getattr(mod, class_name)
            # 尝试用 **params 初始化 (策略构造函数接受具体参数)
            try:
                instance: Strategy = cls(**params) if params else cls()
            except TypeError:
                instance = cls()
            strategies.append(instance)
            if logger:
                logger.info(f"已加载策略: {name} ({class_name})")
        except Exception as e:
            if logger:
                logger.error(f"加载策略 {name} 失败: {e}")

    return strategies


# ================================================================
#  Live / Paper 主循环
# ================================================================
async def run_live(cfg: dict, ctx: Context):
    """实盘 / 纸交易主循环。"""
    bus = get_event_bus()

    # ── 初始化存储 ────
    storage_cfg = cfg.get("storage", {})
    storage = StorageManager(
        data_dir=storage_cfg.get("data_dir", "data"),
    )
    ctx.set("storage", storage)

    # ── 初始化交易日志记录器 ────
    trade_logger = TradeLogger(
        data_dir=storage_cfg.get("data_dir", "data"),
        run_mode=ctx.run_mode.value,
        strategy_name=",".join(
            s_info.get("strategy", {}).get("name", k)
            for k, s_info in cfg.get("strategies_config", {}).items()
            if s_info.get("strategy", {}).get("enabled", False)
        ),
        config=cfg,
    )
    ctx.set("trade_logger", trade_logger)

    # ── 初始化风控 ────
    risk_engine = RiskEngine(context=ctx, event_bus=bus)
    risk_engine.load_default_rules()
    ctx.set("risk_engine", risk_engine)

    # ── 初始化报警 ────
    alert_manager = AlertManager()
    alert_manager.add_channel(ConsoleChannel())
    # 可选: 添加 Telegram / Discord 渠道
    alert_cfg = cfg.get("alerting", {})
    tg_cfg = alert_cfg.get("telegram", {})
    if tg_cfg.get("bot_token") and tg_cfg.get("chat_id"):
        from src.risk.alerting import TelegramChannel
        alert_manager.add_channel(
            TelegramChannel(tg_cfg["bot_token"], tg_cfg["chat_id"]),
            min_level=AlertLevel.P1_ALERT,
        )
    dc_cfg = alert_cfg.get("discord", {})
    if dc_cfg.get("webhook_url"):
        from src.risk.alerting import DiscordChannel
        alert_manager.add_channel(
            DiscordChannel(dc_cfg["webhook_url"]),
            min_level=AlertLevel.P2_WARNING,
        )
    ctx.set("alert_manager", alert_manager)

    # ── 初始化钱包 ────
    wallet = None
    if ctx.trading_mode in (TradingMode.AUTO, TradingMode.SEMI_AUTO):
        try:
            wallet = WalletManager(read_only=False)
            if wallet.initialize_from_env():
                logger.info(f"钱包已加载: {wallet.address}")
            else:
                logger.warning("钱包初始化失败 (将以纸交易模式运行)")
                ctx.trading_mode = TradingMode.PAPER
                wallet = None
        except Exception as e:
            logger.warning(f"钱包加载失败 (将以纸交易模式运行): {e}")
            ctx.trading_mode = TradingMode.PAPER

    # ── 初始化交易执行器 ────
    executor = OrderExecutor(
        context=ctx,
        event_bus=bus,
        storage=storage,
        risk_checker=risk_engine.check_order,
        wallet=wallet,
    )
    await executor.initialize()
    ctx.set("executor", executor)

    # ── 实盘模式: 同步链上 USDC 余额 ────
    if wallet and wallet.is_initialized and ctx.trading_mode in (
        TradingMode.AUTO, TradingMode.SEMI_AUTO,
    ):
        try:
            import os
            # 优先查询 Proxy 钱包余额 (Polymarket 资金在 proxy 里)
            proxy_addr = os.environ.get("PM_PROXY_ADDRESS", "")
            if proxy_addr:
                real_balance = await wallet.get_usdc_balance(address=proxy_addr)
                balance_source = f"Proxy({proxy_addr[:8]}...)"
            else:
                real_balance = await wallet.get_usdc_balance()
                balance_source = f"EOA({wallet.address[:8]}...)"

            if real_balance > 0:
                ctx.account.balance = real_balance
                ctx.account.available = real_balance
                ctx.account.initial_balance = real_balance
                ctx.account.total_equity = real_balance
                logger.info(f"链上 USDC 余额已同步: ${real_balance:.2f} ({balance_source})")
            else:
                logger.warning(f"链上 USDC 余额为 0 ({balance_source})，请确认地址和 Polygon 网络")
        except Exception as e:
            logger.warning(f"链上余额同步失败 (使用配置默认值): {e}")

    # ── 初始化订单簿 (UP + DOWN) ────
    ds_cfg = cfg.get("datasources", {})
    pm_markets = ds_cfg.get("polymarket", {}).get("market_ids", [])
    market_id = pm_markets[0] if pm_markets else "default"

    orderbook_up = OrderBook(market_id=f"{market_id}_up")
    orderbook_down = OrderBook(market_id=f"{market_id}_down")
    # 保持兼容: orderbook 指向 up
    orderbook = orderbook_up
    analyzer = OrderBookAnalyzer(
        large_order_threshold=cfg.get("orderbook", {}).get("large_order_threshold", 500),
    )
    snapshot_mgr = SnapshotManager(
        data_dir=storage_cfg.get("data_dir", "data") + "/orderbook_snapshots",
    )
    ctx.set("orderbook", orderbook)
    ctx.set("orderbook_up", orderbook_up)
    ctx.set("orderbook_down", orderbook_down)
    ctx.set("orderbook_analyzer", analyzer)

    # ── 初始化行情聚合器 ────
    aggregator = MarketAggregator(
        event_bus=bus,
        storage=storage,
    )
    aggregator.load_default_indicators()
    ctx.set("aggregator", aggregator)

    # ── 初始化数据源 ────
    chainlink_cfg = ds_cfg.get("chainlink", {})
    chainlink_ds = ChainlinkDataSource(
        event_bus=bus,
        config=ChainlinkConfig(
            rtds_ws=chainlink_cfg.get(
                "rtds_ws", "wss://ws-live-data.polymarket.com",
            ),
            client_id=chainlink_cfg.get("client_id", ""),
            client_secret=chainlink_cfg.get("client_secret", ""),
            feed_id=chainlink_cfg.get(
                "feed_id",
                "0x00027bbaff688c906a3e20a34fe951715d1018d262a5b66e38edd64e0b0c69",
            ),
            binance_ws=chainlink_cfg.get(
                "binance_ws",
                "wss://stream.binance.com:9443/ws/btcusdt@trade",
            ),
            binance_rest=chainlink_cfg.get(
                "binance_rest",
                "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT",
            ),
            polygon_rpc=chainlink_cfg.get(
                "polygon_rpc", "https://polygon-bor-rpc.publicnode.com",
            ),
            poll_interval=chainlink_cfg.get("poll_interval", 2.0),
            mode=chainlink_cfg.get("mode", "auto"),
            symbols=chainlink_cfg.get("symbols", ["btc/usd", "eth/usd", "xrp/usd"]),
        ),
    )
    pm_cfg = ds_cfg.get("polymarket", {})
    polymarket_ds = PolymarketDataSource(
        event_bus=bus,
        market_ids=pm_cfg.get("market_ids", []),
        search_query=pm_cfg.get("search_query", "Bitcoin"),
        poll_interval=pm_cfg.get("poll_interval", 5.0),
        symbols=pm_cfg.get("symbols", ["btc", "eth", "xrp"]),
    )

    # ── 加载策略 ────
    strategies = load_strategies(cfg)
    ctx.set("strategies", strategies)  # Dashboard API 需要
    if not strategies:
        logger.warning("没有启用的策略, 系统将仅进行数据采集")

    # ── 启动事件总线 ────
    bus_task = asyncio.create_task(bus.start())
    logger.info("事件总线已启动")

    # ── Context 行情状态同步 ────
    # 将数据源事件同步到 ctx.market，供 Dashboard / API / 快照使用

    # 带时间戳的 BTC 价格环形缓冲区 (用于回溯 5-min 窗口基准价格)
    _price_buffer: list[tuple[float, float]] = []  # [(timestamp, price), ...]
    _PRICE_BUFFER_MAX = 600  # 保留约 10 分钟 (按 ~1 tick/s)

    async def _sync_btc_price(event: Event):
        """CHAINLINK_PRICE → ctx.markets[symbol].price + PTB 捕获 (多币种)"""
        price = event.data.get("price", 0)
        symbol = event.data.get("symbol", "btc")  # btc / eth / xrp
        if price <= 0:
            return

        mkt = ctx.markets.get(symbol)
        if not mkt:
            return

        # 通用字段更新
        if abs(price - mkt.price) > 0.001:
            mkt.price_updated_at = event.timestamp
        mkt.price = price
        mkt.last_update = event.timestamp

        # BTC 向后兼容 (legacy 字段 + 全局缓冲区)
        if symbol == "btc":
            if abs(price - mkt.btc_price) > 0.01:
                mkt.btc_price_updated_at = event.timestamp
            mkt.btc_price = price
            # BTC 价格缓冲区 (PTB 回溯用)
            _price_buffer.append((event.timestamp, price))
            if len(_price_buffer) > _PRICE_BUFFER_MAX:
                _price_buffer[:] = _price_buffer[-_PRICE_BUFFER_MAX:]

        # ━━ 基准价格 (PTB) 精确捕获 ━━
        # RTDS 的 payload.timestamp = Chainlink observationsTimestamp
        # 当 observationsTimestamp % 300 == 0 时 = 5-min 窗口边界
        rtds_ts = event.data.get("timestamp", 0)
        if rtds_ts > 0:
            rtds_ts_int = int(rtds_ts)
            if rtds_ts_int % 300 == 0:
                window_start_ts = rtds_ts_int
                old_ptb = mkt.pm_window_start_price
                mkt.pm_window_start_price = price
                mkt.pm_window_start_ts = window_start_ts
                logger.info(
                    f"⚡ [{symbol.upper()}] PTB 捕获 (observationsTimestamp={rtds_ts_int}): "
                    f"${price:,.4f} (旧=${old_ptb:,.4f})"
                )

    def _lookup_price_at(target_ts: float) -> float:
        """在缓冲区中找到最接近 target_ts 的价格 (优先取 <= target_ts 的最新值)"""
        if not _price_buffer:
            return 0.0
        best_price = 0.0
        best_diff = float("inf")
        for ts, price in reversed(_price_buffer):
            diff = abs(ts - target_ts)
            if diff < best_diff:
                best_diff = diff
                best_price = price
            # 缓冲区倒序遍历，一旦越过 target_ts 太远就停止
            if ts < target_ts - 10:
                break
        return best_price

    # ── WS Book 回调工厂 (供初始注册 + 窗口切换时重新注册) ────
    def _make_book_ws_cb(ob: OrderBook, label: str):
        """创建 WS Book 回调闭包, 将 WS 数据同步到 OrderBook 对象"""
        def _cb(bids_or_changes, asks, is_snapshot):
            if is_snapshot:
                ob.apply_snapshot(bids_or_changes, asks)
                logger.debug(
                    f"WS OB {label} snapshot: "
                    f"{len(bids_or_changes)} bids, {len(asks)} asks, "
                    f"spread={ob.get_state().spread:.4f}"
                )
            else:
                # 增量更新: bids_or_changes 是 changes 列表
                for chg in bids_or_changes:
                    price = float(chg.get("price", 0))
                    size = float(chg.get("size", 0))
                    raw_side = chg.get("side", "").upper()
                    # CLOB WS: BUY → bid, SELL → ask
                    side = "bid" if raw_side == "BUY" else "ask"
                    ob.apply_update(side, price, size)
        return _cb

    async def _sync_pm_price(event: Event):
        """PM_PRICE → ctx.markets[symbol] 的 PM 字段 (多币种)"""
        data = event.data or {}
        symbol = data.get("symbol", "btc")
        mkt = ctx.markets.get(symbol)
        if not mkt:
            return

        if "yes_price" in data:
            mkt.pm_yes_price = data["yes_price"]
        if "no_price" in data:
            mkt.pm_no_price = data["no_price"]
        if "yes_bid" in data:
            mkt.pm_yes_bid = data["yes_bid"]
        if "yes_ask" in data:
            mkt.pm_yes_ask = data["yes_ask"]
        if "no_bid" in data:
            mkt.pm_no_bid = data["no_bid"]
        if "no_ask" in data:
            mkt.pm_no_ask = data["no_ask"]
        if "question" in data:
            mkt.pm_market_question = data["question"]
            mkt.pm_market_active = True
        if "condition_id" in data:
            mkt.pm_market_id = data["condition_id"]
            mkt.pm_condition_id = data["condition_id"]
        if "neg_risk" in data:
            mkt.pm_neg_risk = data["neg_risk"]
        # 同步 CLOB token IDs (下单必须)
        tids = data.get("token_ids", [])
        if len(tids) >= 1:
            mkt.pm_yes_token_id = tids[0]
        if len(tids) >= 2:
            mkt.pm_no_token_id = tids[1]

        # BTC: WS Book 回调 + CLOB 缓存预热 (目前仅 BTC 需要 orderbook)
        if symbol == "btc":
            if tids and executor._clob and executor._clob.is_ready:
                neg_risk = data.get("neg_risk", True)
                asyncio.create_task(executor._clob.warmup_cache(tids[:2], neg_risk))
            if tids:
                _new_ob_map: dict[str, OrderBook] = {}
                if len(tids) >= 1:
                    _new_ob_map[tids[0]] = orderbook_up
                if len(tids) >= 2:
                    _new_ob_map[tids[1]] = orderbook_down
                polymarket_ds._book_callbacks.clear()
                for _tid, _ob in _new_ob_map.items():
                    _lbl = "UP" if _ob is orderbook_up else "DOWN"
                    polymarket_ds.register_book_callback(
                        _tid, _make_book_ws_cb(_ob, _lbl)
                    )

        if "seconds_left" in data:
            mkt.pm_window_seconds_left = data["seconds_left"]

        # 检测 5-min 窗口切换 → 用缓冲区回溯基准价格 (Price to Beat)
        wst = data.get("window_start_ts", 0)
        if wst and wst != mkt.pm_window_start_ts:
            mkt.pm_window_start_ts = wst
            # 如果 RTDS observationsTimestamp 尚未设置过, 则从缓冲区回溯 (仅 BTC)
            if mkt.pm_window_start_price <= 0 and symbol == "btc":
                ptb = _lookup_price_at(float(wst))
                if ptb <= 0:
                    ptb = mkt.btc_price
                if ptb > 0:
                    mkt.pm_window_start_price = ptb
                    logger.info(
                        f"[{symbol.upper()}] 新 5-min 窗口 ts={wst}, "
                        f"基准价格(PTB/辅助)=${ptb:,.4f}"
                    )
            # 非 BTC: 从通用 price 字段取 PTB
            elif mkt.pm_window_start_price <= 0:
                ptb = mkt.price
                if ptb > 0:
                    mkt.pm_window_start_price = ptb
                    logger.info(
                        f"[{symbol.upper()}] 新 5-min 窗口 ts={wst}, "
                        f"基准价格(PTB)=${ptb:,.4f}"
                    )
        mkt.last_update = event.timestamp

    bus.subscribe(EventType.CHAINLINK_PRICE, _sync_btc_price, priority=100)
    bus.subscribe(EventType.PM_PRICE, _sync_pm_price, priority=100)
    logger.info("Context 行情状态同步已注册 (PTB 基于 Chainlink observationsTimestamp 精确捕获)")

    # 注: _window_boundary_timer 已移除, PTB 现在直接在 _sync_btc_price 中
    # 通过检测 RTDS observationsTimestamp % 300 == 0 来捕获
    # 这是最精确的方式: 直接用 Chainlink 观测时间戳,
    # 不受网络延迟/前端时钟影响

    # ── 注册策略到事件总线 (同步 subscribe, 策略方法是同步的) ────
    for strat in strategies:
        strat.on_init(ctx)

        def _make_kline_cb(s=strat):
            async def _h(e):
                r = s.on_market_data(ctx, e.data)
                if asyncio.iscoroutine(r):
                    await r
            return _h

        def _make_book_cb(s=strat):
            async def _h(e):
                s.on_order_book(ctx, e.data)
            return _h

        def _make_order_cb(s=strat):
            async def _h(e):
                s.on_order_update(ctx, e.data)
            return _h

        bus.subscribe(EventType.MARKET_KLINE, _make_kline_cb())
        bus.subscribe(EventType.ORDERBOOK_UPDATE, _make_book_cb())
        bus.subscribe(EventType.ORDER_FILLED, _make_order_cb())

        # 实时 BTC 价格 tick + PM 价格驱动 (需要逐 tick 驱动的策略)
        if strat.name() in ("btc5min_taker", "bn_rtds_spread", "tail_reversal"):
            def _make_tick_cb(s=strat):
                async def _h(e):
                    r = s.on_market_data(ctx, e.data)
                    if asyncio.iscoroutine(r):
                        await r
                return _h

            def _make_pm_cb(s=strat):
                async def _h(e):
                    r = s.on_market_data(ctx, e.data)
                    if asyncio.iscoroutine(r):
                        await r
                return _h

            bus.subscribe(EventType.CHAINLINK_PRICE, _make_tick_cb(), priority=50)
            bus.subscribe(EventType.PM_PRICE, _make_pm_cb(), priority=50)
            logger.info(f"策略 {strat.name()} 已订阅 BTC tick + PM 价格实时驱动")

        logger.info(f"策略 {strat.name()} 已初始化并订阅事件")

    # ── 启动聚合器 ────
    await aggregator.start()

    # ── 连接数据源 ────
    tasks = [bus_task]

    try:
        await chainlink_ds.connect()
        # 立即同步初始价格到 ctx (稍后 RTDS 流式推送会持续更新)
        if chainlink_ds._last_price and chainlink_ds._last_price > 0:
            ctx.market.btc_price = chainlink_ds._last_price
            ctx.market.price = chainlink_ds._last_price
            logger.info(f"初始 BTC 价格: ${chainlink_ds._last_price:,.2f}")
        logger.info("Chainlink BTC/USD 数据源已连接")
        tasks.append(asyncio.create_task(chainlink_ds.start_streaming()))
    except Exception as e:
        logger.error(f"Chainlink 连接失败: {e}")

    try:
        await polymarket_ds.connect()
        logger.info("Polymarket 数据源已连接")

        # ── 注册 WS Book 回调: 实时推送 OB 数据到 OrderBook 对象 (仅 BTC) ────
        pm_mkt = polymarket_ds._active_markets.get("btc")
        if pm_mkt and pm_mkt.token_ids:
            _ob_map = {}  # token_id → OrderBook
            if len(pm_mkt.token_ids) >= 1:
                _ob_map[pm_mkt.token_ids[0]] = orderbook_up
            if len(pm_mkt.token_ids) >= 2:
                _ob_map[pm_mkt.token_ids[1]] = orderbook_down

            for tid, ob_obj in _ob_map.items():
                label = "UP" if ob_obj is orderbook_up else "DOWN"
                polymarket_ds.register_book_callback(
                    tid, _make_book_ws_cb(ob_obj, label)
                )
            logger.info(
                f"WS Book 回调已注册 ({len(_ob_map)} tokens) — "
                f"订单簿将通过 WebSocket 实时更新"
            )

        tasks.append(asyncio.create_task(polymarket_ds.start_streaming()))

        # ── CLOB SDK 缓存预热 (P0 延迟优化) ────
        # 提前将已知 token_ids 的 tick_size / neg_risk 写入 SDK 内部缓存,
        # 避免首笔下单时 create_order 的隐式网络调用 (~200-400ms)
        if executor._clob and executor._clob.is_ready:
            if pm_mkt and pm_mkt.token_ids:
                await executor._clob.warmup_cache(
                    token_ids=pm_mkt.token_ids[:2],
                    neg_risk=pm_mkt.neg_risk,
                )
                logger.info(f"CLOB SDK 缓存已预热 ({len(pm_mkt.token_ids[:2])} tokens)")
    except Exception as e:
        logger.error(f"Polymarket 连接失败: {e}")

    # ── 订单簿数据拉取 + 快照循环 ────
    ob_poll_interval = cfg.get("orderbook", {}).get("snapshot_interval", 1.0)

    async def _orderbook_feed_loop():
        """
        REST 降级轮询 — 仅在 WS Book 断开时生效。

        当 WS Book 正常连接时, OB 数据由 WebSocket 实时推送,
        本循环跳过 REST 请求, 仅维持心跳检测。
        WS 断开后自动恢复 REST 轮询, 确保 OB 数据不中断。
        """
        _ws_was_connected = False
        while True:
            try:
                # WS Book 正常时跳过 REST 轮询
                if polymarket_ds.ws_book_connected:
                    if not _ws_was_connected:
                        logger.info(
                            "OB REST 轮询已暂停 — WS Book 实时推送生效"
                        )
                        _ws_was_connected = True
                    await asyncio.sleep(ob_poll_interval)
                    continue

                # WS 刚断开, 打印降级日志
                if _ws_was_connected:
                    logger.warning(
                        "WS Book 断开 — OB 降级为 REST 轮询 "
                        f"(间隔 {ob_poll_interval}s)"
                    )
                    _ws_was_connected = False

                mkt = polymarket_ds._active_market
                if mkt and mkt.token_ids and polymarket_ds._session:
                    token_ids = mkt.token_ids
                    has_up = len(token_ids) >= 1
                    has_dn = len(token_ids) >= 2

                    # 并发拉取 UP + DOWN 订单簿 (原来串行 ~200-400ms → 现在 ~100-200ms)
                    ob_tasks = []
                    if has_up:
                        ob_tasks.append(polymarket_ds.get_orderbook(token_ids[0]))
                    if has_dn:
                        ob_tasks.append(polymarket_ds.get_orderbook(token_ids[1]))

                    ob_results = await asyncio.gather(*ob_tasks, return_exceptions=True)

                    # 处理 UP 结果
                    if has_up and not isinstance(ob_results[0], Exception):
                        raw_book = ob_results[0]
                        bids = raw_book.get("bids", [])
                        asks = raw_book.get("asks", [])
                        if bids or asks:
                            orderbook_up.apply_snapshot(bids, asks)
                            logger.debug(
                                f"OB UP 更新: {len(bids)} bids, {len(asks)} asks, "
                                f"spread={orderbook_up.get_state().spread:.4f}"
                            )
                    elif has_up and isinstance(ob_results[0], Exception):
                        logger.debug(f"OB UP 拉取失败: {ob_results[0]}")

                    # 处理 DOWN 结果
                    dn_idx = 1 if has_up else 0
                    if has_dn and not isinstance(ob_results[dn_idx], Exception):
                        raw_book = ob_results[dn_idx]
                        bids = raw_book.get("bids", [])
                        asks = raw_book.get("asks", [])
                        if bids or asks:
                            orderbook_down.apply_snapshot(bids, asks)
                            logger.debug(
                                f"OB DOWN 更新: {len(bids)} bids, {len(asks)} asks, "
                                f"spread={orderbook_down.get_state().spread:.4f}"
                            )
                    elif has_dn and isinstance(ob_results[dn_idx], Exception):
                        logger.debug(f"OB DOWN 拉取失败: {ob_results[dn_idx]}")
            except Exception as e:
                logger.debug(f"OB 拉取失败: {e}")
            await asyncio.sleep(ob_poll_interval)

    async def _snapshot_loop():
        """定期快照 + 分析 + 发布事件"""
        while True:
            try:
                state = orderbook.get_state()
                if state.bids or state.asks:  # 有数据才做快照/分析
                    snapshot_mgr.capture(state)
                    analysis = analyzer.update(state)
                    await bus.publish_async(Event(
                        type=EventType.ORDERBOOK_UPDATE,
                        data=analysis,
                        source="orderbook_analyzer",
                    ))
            except Exception as e:
                logger.error(f"Snapshot loop error: {e}")
            await asyncio.sleep(ob_poll_interval)

    tasks.append(asyncio.create_task(_orderbook_feed_loop()))
    tasks.append(asyncio.create_task(_snapshot_loop()))

    # ── 二级数据源: Binance BTC/USDT (用于 lead-lag spread 策略) ────
    # 无论主数据源是 RTDS 还是其他, 始终维护一个独立的 Binance 价格流
    # 供 bn_rtds_spread 等策略读取 ctx.market.binance_price
    bn_ws_url = chainlink_cfg.get(
        "binance_ws", "wss://stream.binance.com:9443/ws/btcusdt@trade"
    )

    async def _binance_secondary_feed():
        """Binance WebSocket 二级价格流 (独立于主 RTDS 数据源)。"""
        import aiohttp as _aio
        while True:
            try:
                async with _aio.ClientSession() as _sess:
                    async with _sess.ws_connect(
                        bn_ws_url, heartbeat=30,
                        timeout=_aio.ClientTimeout(total=15),
                    ) as ws:
                        logger.info("Binance 二级价格流已连接 (lead-lag)")
                        async for msg in ws:
                            if msg.type == _aio.WSMsgType.TEXT:
                                data = msg.json()
                                price = float(data.get("p", 0))
                                ts = data.get("T", time.time() * 1000) / 1000.0
                                if price > 0:
                                    ctx.market.binance_price = price
                                    ctx.market.binance_price_ts = ts
                            elif msg.type in (
                                _aio.WSMsgType.ERROR,
                                _aio.WSMsgType.CLOSED,
                            ):
                                break
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.debug(f"Binance 二级价格流断开: {e}, 5s 后重连...")
                await asyncio.sleep(5)

    # 检查是否有 bn_rtds_spread 策略启用, 按需启动 Binance 二级流
    _need_binance_feed = any(
        s.name() == "bn_rtds_spread" for s in strategies
    )
    if _need_binance_feed:
        tasks.append(asyncio.create_task(_binance_secondary_feed()))
        logger.info("已启动 Binance 二级价格流 (供 bn_rtds_spread 策略使用)")

    # ── 1-min 定时器: 喂 K 线给策略技术指标 ────
    async def _strategy_timer_loop():
        """每 60 秒触发一次 on_timer("kline_1m")，喂 BTC 价格给技术指标。"""
        while True:
            await asyncio.sleep(60)
            try:
                for strat in strategies:
                    strat.on_timer(ctx, "kline_1m")
            except Exception as e:
                logger.debug(f"Strategy timer error: {e}")

    if strategies:
        tasks.append(asyncio.create_task(_strategy_timer_loop()))

    # ── 优雅退出 ────
    stop_event = asyncio.Event()

    def _signal_handler():
        logger.info("收到退出信号, 正在停止...")
        stop_event.set()

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _signal_handler)
        except NotImplementedError:
            # Windows 不支持 add_signal_handler
            pass

    # ── 启动 Dashboard (可选) ────
    dashboard_server = None
    dashboard_enabled = cfg.get("dashboard", {}).get("enabled", True)
    dashboard_port = cfg.get("dashboard", {}).get("port", 8080)
    # CLI 覆盖
    cli_port = ctx.get("cli_dashboard_port", 0)
    if cli_port:
        dashboard_port = cli_port
    cli_no_dash = ctx.get("cli_no_dashboard", False)
    if cli_no_dash:
        dashboard_enabled = False

    if dashboard_enabled:
        try:
            dashboard_server = DashboardServer(
                context=ctx,
                event_bus=bus,
                host=cfg.get("dashboard", {}).get("host", "0.0.0.0"),
                port=dashboard_port,
            )
            await dashboard_server.start()
            # start() 可能自动切换到其他端口
            dashboard_port = dashboard_server.port
            if dashboard_server.runner:
                logger.info(f"Dashboard 已启动: http://localhost:{dashboard_port}")
            else:
                logger.warning("Dashboard 启动失败: 所有端口均被占用, 系统继续运行")
                dashboard_server = None
        except Exception as e:
            logger.warning(f"Dashboard 启动失败 (系统继续运行): {e}")
            dashboard_server = None

    # ── 打印启动信息 ────
    logger.info("=" * 60)
    logger.info("  Polymarket BTC 5-min Prediction System")
    logger.info(f"  运行模式: {ctx.run_mode.value}")
    logger.info(f"  交易模式: {ctx.trading_mode.value}")
    logger.info(f"  活跃策略: {[s.name() for s in strategies]}")
    logger.info(f"  账户余额: ${ctx.account.balance:.2f}")
    if dashboard_server:
        logger.info(f"  Dashboard: http://localhost:{dashboard_port}")
    logger.info("=" * 60)

    # ── 主循环 ────
    try:
        # Windows 兼容: 用轮询代替信号
        while not stop_event.is_set():
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=1.0)
            except asyncio.TimeoutError:
                pass
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt, 正在退出...")
    finally:
        # ── 清理 ────
        logger.info("正在停止所有模块...")

        for strat in strategies:
            try:
                strat.on_stop(ctx)
            except Exception as e:
                logger.error(f"停止策略 {strat.name()} 出错: {e}")

        snapshot_mgr.flush()
        await aggregator.stop()

        try:
            await chainlink_ds.disconnect()
        except Exception:
            pass
        try:
            await polymarket_ds.disconnect()
        except Exception:
            pass

        # 停止 Dashboard
        if dashboard_server:
            try:
                await dashboard_server.stop()
            except Exception:
                pass

        await bus.stop()

        # ── 写入交易日志摘要并关闭 ────
        try:
            strat_obj = strategies[0] if strategies else None
            trade_logger.finalize(
                final_balance=ctx.account.balance,
                initial_balance=ctx.account.initial_balance,
                win_count=getattr(strat_obj, '_win_count', 0),
                loss_count=getattr(strat_obj, '_loss_count', 0),
            )
            trade_logger.close()
        except Exception as e:
            logger.error(f"TradeLogger finalize 失败: {e}")

        storage.close()

        # 取消后台任务
        for t in tasks:
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass

        logger.info("系统已安全退出 ✓")


# ================================================================
#  回测模式
# ================================================================
async def run_backtest(cfg: dict, ctx: Context):
    """
    执行回测。

    数据流:
    1. 根据 config 中的 token_id / condition_id 从 Polymarket 拉取历史数据
    2. 如果未指定 token_id, 先搜索 BTC 相关市场让用户选择
    3. 历史数据自动缓存到本地 Parquet, 下次运行直接使用缓存
    """
    from src.market.datasources.polymarket_history import PolymarketHistoryFetcher

    bt_cfg_raw = cfg.get("backtest", {})
    pm_cfg = cfg.get("datasources", {}).get("polymarket", {})

    # 获取 token_id (优先 CLI --token, 其次 config)
    token_id = ctx.get("cli_token_id", "") or bt_cfg_raw.get("token_id", "")
    condition_id = ctx.get("cli_condition_id", "") or bt_cfg_raw.get("condition_id", "")

    # 如果缺 token_id, 尝试自动搜索市场
    if not token_id:
        logger.info("未指定 token_id, 正在搜索 BTC 相关市场...")
        fetcher = PolymarketHistoryFetcher(
            data_dir=bt_cfg_raw.get("data_dir", "data/polymarket"),
        )
        try:
            markets = await fetcher.search_btc_markets(
                query=pm_cfg.get("search_query", "Bitcoin"),
                active_only=False,  # 回测也需要已结束的市场
            )
            if markets:
                logger.info("找到以下 BTC 相关市场:")
                for i, m in enumerate(markets[:10]):
                    tokens_str = ", ".join(
                        f"{t['outcome']}={t.get('token_id', '')[:12]}..."
                        for t in m.tokens
                    )
                    logger.info(
                        f"  [{i}] {m.question[:80]}"
                        f"\n      volume=${m.volume:,.0f}  "
                        f"active={m.active}  tokens=[{tokens_str}]"
                    )
                logger.info(
                    "请在 config/settings.yaml 的 backtest 部分设置 "
                    "token_id 和 condition_id, 然后重新运行。"
                )
                logger.info("示例:")
                if markets[0].tokens:
                    example_token = markets[0].tokens[0].get("token_id", "")
                    logger.info(f"  token_id: \"{example_token}\"")
                    logger.info(f"  condition_id: \"{markets[0].condition_id}\"")
            else:
                logger.warning("未找到任何 BTC 相关市场")
        finally:
            await fetcher.close()
        return

    # 构建 BacktestConfig
    bt_config = BacktestConfig(
        start_date=bt_cfg_raw.get("start_date", "2024-01-01"),
        end_date=bt_cfg_raw.get("end_date", "2025-12-31"),
        initial_balance=bt_cfg_raw.get("initial_balance", 1000.0),
        fee_rate=bt_cfg_raw.get("fee_rate", 0.002),
        slippage_bps=bt_cfg_raw.get("slippage_bps", 5),
        latency_ms=bt_cfg_raw.get("latency_ms", 100),
        data_interval=bt_cfg_raw.get("interval", "5m"),
        token_id=token_id,
        condition_id=condition_id,
        data_source="polymarket",
        data_dir=bt_cfg_raw.get("data_dir", "data/polymarket"),
        market_question=bt_cfg_raw.get("market_question", ""),
    )

    # 加载策略
    strategies = load_strategies(cfg)
    if not strategies:
        logger.error("没有可用的策略, 无法回测")
        return

    for strat in strategies:
        logger.info(f"开始回测策略: {strat.name()}")
        logger.info(f"  数据源: Polymarket (token={token_id[:20]}...)")
        logger.info(f"  时间范围: {bt_config.start_date} → {bt_config.end_date}")
        logger.info(f"  K线周期: {bt_config.data_interval}")
        logger.info(f"  初始资金: ${bt_config.initial_balance:.2f}")

        engine = BacktestEngine(config=bt_config)
        engine.set_strategy(strat)

        try:
            # engine.run() 内部会自动从 Polymarket 拉取数据
            result = await engine.run()
        except ValueError as e:
            logger.error(f"回测失败: {e}")
            continue

        # 打印结果
        logger.info("=" * 50)
        logger.info(f"  策略: {strat.name()}")
        logger.info(f"  数据: {len(engine._klines)} 根 K 线")
        logger.info(f"  总收益: {result.total_return:.2%}")
        logger.info(f"  Sharpe: {result.sharpe_ratio:.3f}")
        logger.info(f"  最大回撤: {result.max_drawdown:.2%}")
        logger.info(f"  胜率: {result.win_rate:.2%}")
        logger.info(f"  盈亏比: {result.profit_factor:.2f}")
        logger.info(f"  交易次数: {result.trade_count}")
        logger.info("=" * 50)

        # 生成报告
        report_dir = ROOT_DIR / "reports"
        report_dir.mkdir(exist_ok=True)

        generator = ReportGenerator(output_dir=str(report_dir))
        report_path = generator.generate(result)
        if report_path:
            logger.info(f"报告已生成: {report_path}")


# ================================================================
#  CLI 入口
# ================================================================
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Polymarket BTC 5-min Prediction Betting System",
    )
    parser.add_argument(
        "mode",
        choices=["live", "paper", "backtest"],
        default="paper",
        nargs="?",
        help="运行模式: live(实盘), paper(纸交易), backtest(回测)",
    )
    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="配置目录路径 (默认: ./config)",
    )
    parser.add_argument(
        "--strategy",
        type=str,
        default=None,
        help="只运行指定名称的策略",
    )
    parser.add_argument(
        "--market",
        type=str,
        default=None,
        help="Polymarket 市场 condition ID",
    )
    parser.add_argument(
        "--token",
        type=str,
        default=None,
        help="Polymarket token ID (回测数据源, YES/NO token)",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="详细日志输出",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=0,
        help="Dashboard 端口 (默认: 8080, 0 则使用配置文件值)",
    )
    parser.add_argument(
        "--no-dashboard",
        action="store_true",
        help="禁用 Dashboard Web 界面",
    )
    return parser.parse_args()


async def async_main():
    global logger

    args = parse_args()

    # 初始化日志
    log_level = "DEBUG" if args.verbose else "INFO"
    setup_logger(level=log_level, log_dir=str(ROOT_DIR / "logs"))

    from loguru import logger as _logger
    logger = _logger

    logger.info(f"启动模式: {args.mode}")

    # 加载配置
    config_dir = Path(args.config) if args.config else None
    cfg = load_config(config_dir)

    # CLI 覆盖
    if args.strategy:
        # 只保留指定策略
        sc = cfg.get("strategies_config", {})
        filtered = {k: v for k, v in sc.items() if k == args.strategy}
        if not filtered:
            logger.error(f"策略 '{args.strategy}' 不存在于配置中")
            sys.exit(1)
        cfg["strategies_config"] = filtered

    if args.market:
        # 将 market ID 注入所有策略配置
        for scfg in cfg.get("strategies_config", {}).values():
            scfg.setdefault("strategy", {})["market_id"] = args.market

    # 构建上下文
    ctx = build_context(cfg, args.mode)

    # CLI 注入 token / condition_id (用于回测)
    if args.token:
        ctx.set("cli_token_id", args.token)
    if args.market:
        ctx.set("cli_condition_id", args.market)

    # CLI → Dashboard 控制
    if args.port:
        ctx.set("cli_dashboard_port", args.port)
    if args.no_dashboard:
        ctx.set("cli_no_dashboard", True)

    # 分发到不同模式
    if args.mode == "backtest":
        await run_backtest(cfg, ctx)
    else:
        await run_live(cfg, ctx)


def main():
    """同步入口点。"""
    try:
        asyncio.run(async_main())
    except KeyboardInterrupt:
        print("\n已退出。")


if __name__ == "__main__":
    main()
