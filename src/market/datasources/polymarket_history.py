"""
Polymarket History Fetcher - Polymarket 历史数据抓取器

从 Polymarket CLOB API 和 Gamma API 拉取历史数据,
将原始成交记录聚合成 K 线 (OHLCV) 格式供回测引擎使用。

数据源:
  - CLOB API: https://clob.polymarket.com   (trades / prices-history / timeseries)
  - Gamma API: https://gamma-api.polymarket.com  (markets / events metadata)

注意: Polymarket 的 "价格" 是概率, 范围 [0, 1], 代表该结果发生的概率。
"""

from __future__ import annotations

import asyncio
import math
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import aiohttp
import pandas as pd
from loguru import logger


# ================================================================
#  数据结构
# ================================================================

@dataclass
class MarketInfo:
    """Polymarket 市场元信息"""
    condition_id: str               # 市场唯一 ID
    question: str = ""              # 市场问题 (e.g. "Will BTC be above $50k?")
    description: str = ""
    tokens: list[dict] = field(default_factory=list)  # [{token_id, outcome}]
    active: bool = False
    closed: bool = False
    end_date: str = ""
    volume: float = 0.0
    liquidity: float = 0.0
    category: str = ""
    created_at: str = ""


@dataclass
class HistoryCandle:
    """聚合后的 K 线 (概率价格)"""
    timestamp: float            # 开盘时间 (Unix seconds)
    open: float                 # 开盘概率价格
    high: float                 # 最高
    low: float                  # 最低
    close: float                # 收盘
    volume: float               # 成交量 (USDC)
    trade_count: int = 0        # 成交笔数
    vwap: float = 0.0           # 成交量加权平均价


# ================================================================
#  核心: 历史数据抓取器
# ================================================================

class PolymarketHistoryFetcher:
    """
    Polymarket 历史数据抓取器

    Usage:
        fetcher = PolymarketHistoryFetcher()
        # 1. 搜索 BTC 相关市场
        markets = await fetcher.search_btc_markets()
        # 2. 拉取历史数据
        df = await fetcher.fetch_market_history(
            token_id="...", start_date="2024-01-01", end_date="2024-06-01"
        )
        # 3. 聚合为 K 线
        candles = await fetcher.fetch_candles(
            token_id="...", interval="5m", start_date="2024-01-01"
        )
    """

    CLOB_REST = "https://clob.polymarket.com"
    GAMMA_API = "https://gamma-api.polymarket.com"

    # API 频率限制: 保守
    RATE_LIMIT_DELAY = 0.25  # 每次请求间隔 (秒)

    def __init__(
        self,
        clob_url: str | None = None,
        gamma_url: str | None = None,
        data_dir: str = "data/polymarket",
    ) -> None:
        self.clob_url = clob_url or self.CLOB_REST
        self.gamma_url = gamma_url or self.GAMMA_API
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self._session: aiohttp.ClientSession | None = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30),
                headers={"Accept": "application/json"},
            )
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    # ================================================================
    #  市场搜索
    # ================================================================

    async def search_btc_markets(
        self,
        query: str = "Bitcoin",
        active_only: bool = False,
    ) -> list[MarketInfo]:
        """
        搜索 BTC 相关的预测市场

        Returns:
            匹配的市场列表, 按成交量排序
        """
        session = await self._get_session()
        markets: list[MarketInfo] = []
        next_cursor = "MA=="

        try:
            # 分页拉取 (Polymarket 有分页)
            while next_cursor:
                url = f"{self.gamma_url}/markets"
                params = {"next_cursor": next_cursor}
                async with session.get(url, params=params) as resp:
                    if resp.status != 200:
                        logger.warning(f"Gamma API 返回 {resp.status}")
                        break
                    data = await resp.json()

                items = data if isinstance(data, list) else data.get("data", [])
                next_cursor = data.get("next_cursor") if isinstance(data, dict) else None

                for m in items:
                    q = m.get("question", "")
                    desc = m.get("description", "")
                    # 过滤含 BTC / Bitcoin 的市场
                    text = f"{q} {desc}".lower()
                    if query.lower() not in text:
                        continue
                    if active_only and not m.get("active", False):
                        continue

                    tokens = []
                    for t in m.get("tokens", []):
                        tokens.append({
                            "token_id": t.get("token_id", ""),
                            "outcome": t.get("outcome", ""),
                            "price": float(t.get("price", 0)),
                        })

                    info = MarketInfo(
                        condition_id=m.get("condition_id", ""),
                        question=q,
                        description=desc,
                        tokens=tokens,
                        active=m.get("active", False),
                        closed=m.get("closed", False),
                        end_date=m.get("end_date_iso", ""),
                        volume=float(m.get("volume", 0)),
                        liquidity=float(m.get("liquidity", 0)),
                        category=m.get("category", ""),
                        created_at=m.get("created_at", ""),
                    )
                    markets.append(info)

                if not items or not next_cursor:
                    break

                await asyncio.sleep(self.RATE_LIMIT_DELAY)

        except Exception as e:
            logger.error(f"搜索市场失败: {e}")

        # 按成交量降序排列
        markets.sort(key=lambda m: m.volume, reverse=True)
        logger.info(f"找到 {len(markets)} 个 BTC 相关市场")
        return markets

    async def get_market_info(self, condition_id: str) -> MarketInfo | None:
        """获取单个市场的详细信息"""
        session = await self._get_session()
        try:
            url = f"{self.gamma_url}/markets/{condition_id}"
            async with session.get(url) as resp:
                if resp.status != 200:
                    return None
                m = await resp.json()

            tokens = []
            for t in m.get("tokens", []):
                tokens.append({
                    "token_id": t.get("token_id", ""),
                    "outcome": t.get("outcome", ""),
                    "price": float(t.get("price", 0)),
                })

            return MarketInfo(
                condition_id=m.get("condition_id", ""),
                question=m.get("question", ""),
                description=m.get("description", ""),
                tokens=tokens,
                active=m.get("active", False),
                closed=m.get("closed", False),
                end_date=m.get("end_date_iso", ""),
                volume=float(m.get("volume", 0)),
                liquidity=float(m.get("liquidity", 0)),
                category=m.get("category", ""),
                created_at=m.get("created_at", ""),
            )
        except Exception as e:
            logger.error(f"获取市场信息失败: {e}")
            return None

    # ================================================================
    #  历史成交数据
    # ================================================================

    async def fetch_trades(
        self,
        condition_id: str,
        start_ts: float | None = None,
        end_ts: float | None = None,
        max_records: int = 10000,
    ) -> pd.DataFrame:
        """
        拉取历史成交记录

        Args:
            condition_id: 市场 condition ID
            start_ts: 起始时间 (Unix seconds)
            end_ts: 结束时间
            max_records: 最大记录数

        Returns:
            DataFrame: [timestamp, price, size, side, fee, market_id, ...]
        """
        session = await self._get_session()
        all_trades: list[dict] = []
        cursor = None
        fetched = 0

        logger.info(f"开始拉取市场 {condition_id[:12]}... 的历史成交")

        while fetched < max_records:
            try:
                url = f"{self.clob_url}/trades"
                params: dict[str, Any] = {
                    "condition_id": condition_id,
                    "limit": min(500, max_records - fetched),
                }
                if cursor:
                    params["next_cursor"] = cursor
                if start_ts:
                    params["after"] = int(start_ts)
                if end_ts:
                    params["before"] = int(end_ts)

                async with session.get(url, params=params) as resp:
                    if resp.status != 200:
                        logger.warning(f"Trades API 返回 {resp.status}")
                        break
                    data = await resp.json()

                trades = data if isinstance(data, list) else data.get("data", data.get("trades", []))
                cursor = data.get("next_cursor") if isinstance(data, dict) else None

                if not trades:
                    break

                for t in trades:
                    ts = t.get("timestamp", t.get("match_time", 0))
                    # 处理 ISO 格式时间
                    if isinstance(ts, str):
                        try:
                            ts = datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp()
                        except Exception:
                            ts = time.time()

                    all_trades.append({
                        "timestamp": float(ts),
                        "price": float(t.get("price", 0)),
                        "size": float(t.get("size", t.get("amount", 0))),
                        "side": t.get("side", ""),
                        "outcome": t.get("outcome", ""),
                        "fee_rate_bps": float(t.get("fee_rate_bps", 0)),
                        "market_id": condition_id,
                        "trade_id": t.get("id", ""),
                    })

                fetched += len(trades)
                if not cursor:
                    break

                await asyncio.sleep(self.RATE_LIMIT_DELAY)

            except Exception as e:
                logger.error(f"拉取成交数据出错: {e}")
                break

        if not all_trades:
            logger.warning(f"市场 {condition_id[:12]}... 无成交数据")
            return pd.DataFrame()

        df = pd.DataFrame(all_trades)
        df = df.sort_values("timestamp").reset_index(drop=True)

        # 过滤时间范围
        if start_ts:
            df = df[df["timestamp"] >= start_ts]
        if end_ts:
            df = df[df["timestamp"] <= end_ts]

        logger.info(f"共获取 {len(df)} 条成交记录")
        return df

    # ================================================================
    #  CLOB Prices History (时间序列价格)
    # ================================================================

    async def fetch_prices_history(
        self,
        token_id: str,
        interval: str = "5m",
        fidelity: int = 5,
        start_ts: int | None = None,
        end_ts: int | None = None,
    ) -> pd.DataFrame:
        """
        从 CLOB API 的 /prices-history 端点获取价格时间序列。

        这是获取 Polymarket 历史价格最直接的方式。

        Args:
            token_id: 代币 ID (从 market.tokens 中获取)
            interval: K 线周期 (用于后续聚合, API 返回原始点)
            fidelity: 数据精度 (分钟): 1 / 5 / 60 / 1440 等
            start_ts: 起始时间 (Unix seconds)
            end_ts: 结束时间 (Unix seconds)

        Returns:
            DataFrame: [timestamp, price]
        """
        session = await self._get_session()

        url = f"{self.clob_url}/prices-history"
        params: dict[str, Any] = {
            "market": token_id,
            "interval": "max",     # all | 1d | 1w | 1m | max
            "fidelity": fidelity,  # 数据精度 (分钟)
        }
        if start_ts:
            params["startTs"] = int(start_ts)
        if end_ts:
            params["endTs"] = int(end_ts)

        try:
            async with session.get(url, params=params) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    logger.warning(f"prices-history 返回 {resp.status}: {text[:200]}")
                    return pd.DataFrame()
                data = await resp.json()
        except Exception as e:
            logger.error(f"获取 prices-history 失败: {e}")
            return pd.DataFrame()

        # API 返回 {"history": [{"t": timestamp, "p": price}, ...]}
        history = data if isinstance(data, list) else data.get("history", [])

        if not history:
            logger.warning(f"token {token_id[:12]}... 无价格历史数据")
            return pd.DataFrame()

        records = []
        for point in history:
            ts = point.get("t", 0)
            price = point.get("p", 0)
            records.append({
                "timestamp": float(ts),
                "price": float(price),
            })

        df = pd.DataFrame(records)
        df = df.sort_values("timestamp").reset_index(drop=True)

        if start_ts:
            df = df[df["timestamp"] >= start_ts]
        if end_ts:
            df = df[df["timestamp"] <= end_ts]

        logger.info(f"获取 {len(df)} 条价格历史点")
        return df

    # ================================================================
    #  Gamma API 时间序列
    # ================================================================

    async def fetch_timeseries(
        self,
        token_id: str,
        start_date: str = "",
        end_date: str = "",
    ) -> pd.DataFrame:
        """
        从 Gamma API /timeseries 端点获取价格时间序列 (备选方案)。

        Args:
            token_id: 代币 ID
            start_date: YYYY-MM-DD
            end_date: YYYY-MM-DD

        Returns:
            DataFrame: [timestamp, price]
        """
        session = await self._get_session()

        url = f"{self.gamma_url}/timeseries"
        params: dict[str, Any] = {"market": token_id}
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date

        try:
            async with session.get(url, params=params) as resp:
                if resp.status != 200:
                    return pd.DataFrame()
                data = await resp.json()
        except Exception as e:
            logger.error(f"获取 Gamma timeseries 失败: {e}")
            return pd.DataFrame()

        history = data if isinstance(data, list) else data.get("history", [])
        if not history:
            return pd.DataFrame()

        records = []
        for p in history:
            ts = p.get("t", p.get("timestamp", 0))
            price = p.get("p", p.get("price", 0))
            if isinstance(ts, str):
                try:
                    ts = datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp()
                except Exception:
                    continue
            records.append({"timestamp": float(ts), "price": float(price)})

        df = pd.DataFrame(records).sort_values("timestamp").reset_index(drop=True)
        logger.info(f"Gamma timeseries: {len(df)} 条记录")
        return df

    # ================================================================
    #  聚合: 原始数据 → K 线
    # ================================================================

    @staticmethod
    def aggregate_to_candles(
        price_df: pd.DataFrame,
        interval: str = "5m",
        volume_df: pd.DataFrame | None = None,
    ) -> pd.DataFrame:
        """
        将原始价格点聚合为 OHLCV K 线。

        Args:
            price_df: DataFrame with [timestamp, price]
            interval: K 线周期 ('1m', '5m', '15m', '1h', '4h', '1d')
            volume_df: 可选的成交量数据 [timestamp, size]

        Returns:
            DataFrame: [timestamp, open, high, low, close, volume, trade_count, vwap]
        """
        if price_df.empty:
            return pd.DataFrame()

        # 解析周期到秒
        interval_seconds = _parse_interval(interval)

        df = price_df.copy()
        df["timestamp"] = pd.to_numeric(df["timestamp"])
        df = df.sort_values("timestamp")

        # 按时间窗口分桶
        df["bucket"] = (df["timestamp"] // interval_seconds) * interval_seconds

        # 如果有成交量数据, 合并
        if volume_df is not None and not volume_df.empty:
            vol = volume_df.copy()
            vol["timestamp"] = pd.to_numeric(vol["timestamp"])
            vol["bucket"] = (vol["timestamp"] // interval_seconds) * interval_seconds
            vol_agg = vol.groupby("bucket").agg(
                volume=("size", "sum"),
                trade_count=("size", "count"),
            ).reset_index()
        else:
            vol_agg = None

        # OHLC 聚合
        candles = df.groupby("bucket").agg(
            open=("price", "first"),
            high=("price", "max"),
            low=("price", "min"),
            close=("price", "last"),
            data_points=("price", "count"),
        ).reset_index()

        candles = candles.rename(columns={"bucket": "timestamp"})

        # 合并成交量
        if vol_agg is not None:
            candles = candles.merge(vol_agg, on="timestamp", how="left")
            candles["volume"] = candles["volume"].fillna(0)
            candles["trade_count"] = candles["trade_count"].fillna(0).astype(int)
        else:
            candles["volume"] = 0.0
            candles["trade_count"] = candles["data_points"]

        # VWAP (如果有成交量)
        if "volume" in candles.columns and candles["volume"].sum() > 0:
            # 近似: 用收盘价 * 成交量
            candles["vwap"] = candles["close"]  # 简化处理
        else:
            candles["vwap"] = candles["close"]

        candles = candles.drop(columns=["data_points"], errors="ignore")
        candles = candles.sort_values("timestamp").reset_index(drop=True)

        logger.info(
            f"聚合完成: {len(candles)} 根 {interval} K 线, "
            f"价格范围: [{candles['low'].min():.4f}, {candles['high'].max():.4f}]"
        )

        return candles

    @staticmethod
    def trades_to_candles(
        trades_df: pd.DataFrame,
        interval: str = "5m",
    ) -> pd.DataFrame:
        """
        将成交记录聚合为 K 线 (包含真实成交量)。

        Args:
            trades_df: DataFrame with [timestamp, price, size, ...]

        Returns:
            DataFrame: [timestamp, open, high, low, close, volume, trade_count, vwap]
        """
        if trades_df.empty:
            return pd.DataFrame()

        interval_seconds = _parse_interval(interval)

        df = trades_df.copy()
        df["timestamp"] = pd.to_numeric(df["timestamp"])
        df["size"] = pd.to_numeric(df.get("size", 0))
        df = df.sort_values("timestamp")

        df["bucket"] = (df["timestamp"] // interval_seconds) * interval_seconds

        # 成交量加权平均价
        df["pv"] = df["price"] * df["size"]

        candles = df.groupby("bucket").agg(
            open=("price", "first"),
            high=("price", "max"),
            low=("price", "min"),
            close=("price", "last"),
            volume=("size", "sum"),
            trade_count=("price", "count"),
            pv_sum=("pv", "sum"),
        ).reset_index()

        candles = candles.rename(columns={"bucket": "timestamp"})

        # VWAP
        candles["vwap"] = candles.apply(
            lambda r: r["pv_sum"] / r["volume"] if r["volume"] > 0 else r["close"],
            axis=1,
        )
        candles = candles.drop(columns=["pv_sum"])
        candles = candles.sort_values("timestamp").reset_index(drop=True)

        logger.info(
            f"成交聚合: {len(candles)} 根 {interval} K 线, "
            f"总成交量: {candles['volume'].sum():.2f} USDC"
        )

        return candles

    # ================================================================
    #  高阶: 一键获取回测 K 线数据
    # ================================================================

    async def fetch_candles(
        self,
        token_id: str,
        interval: str = "5m",
        start_date: str = "",
        end_date: str = "",
        condition_id: str = "",
    ) -> pd.DataFrame:
        """
        一键获取指定代币的 K 线数据 (完整流程)。

        优先使用 CLOB prices-history, 无数据时回退到 trades 聚合。

        Args:
            token_id: 代币 ID
            interval: K 线周期
            start_date: YYYY-MM-DD (可选)
            end_date: YYYY-MM-DD (可选)
            condition_id: 市场 condition ID (用于 trades 回退)

        Returns:
            DataFrame: [timestamp, open, high, low, close, volume, trade_count, vwap]
        """
        start_ts = _date_to_ts(start_date) if start_date else None
        end_ts = _date_to_ts(end_date, end_of_day=True) if end_date else None
        fidelity = max(1, _parse_interval(interval) // 60)  # API fidelity 以分钟计

        # 方案 1: CLOB prices-history
        logger.info(f"尝试从 CLOB prices-history 获取数据 (fidelity={fidelity}min)...")
        price_df = await self.fetch_prices_history(
            token_id=token_id,
            fidelity=fidelity,
            start_ts=int(start_ts) if start_ts else None,
            end_ts=int(end_ts) if end_ts else None,
        )

        trades_df = pd.DataFrame()

        # 如果有 condition_id, 也拉取 trades 获得成交量
        if condition_id:
            logger.info("同时拉取成交数据以获取成交量...")
            trades_df = await self.fetch_trades(
                condition_id=condition_id,
                start_ts=start_ts,
                end_ts=end_ts,
            )

        if not price_df.empty:
            # 用 prices-history 的价格 + trades 的成交量聚合
            vol_df = trades_df[["timestamp", "size"]].copy() if not trades_df.empty else None
            candles = self.aggregate_to_candles(price_df, interval, vol_df)
            if not candles.empty:
                return candles

        # 方案 2: 回退到 trades 聚合
        if not trades_df.empty:
            logger.info("使用成交数据聚合 K 线...")
            return self.trades_to_candles(trades_df, interval)

        # 方案 3: Gamma timeseries 兜底
        logger.info("回退到 Gamma timeseries API...")
        ts_df = await self.fetch_timeseries(token_id, start_date, end_date)
        if not ts_df.empty:
            return self.aggregate_to_candles(ts_df, interval)

        logger.warning(f"所有数据源均无历史数据: token_id={token_id[:16]}...")
        return pd.DataFrame()

    # ================================================================
    #  数据缓存 (Parquet 本地缓存)
    # ================================================================

    async def fetch_candles_cached(
        self,
        token_id: str,
        interval: str = "5m",
        start_date: str = "",
        end_date: str = "",
        condition_id: str = "",
        force_refresh: bool = False,
    ) -> pd.DataFrame:
        """
        带本地 Parquet 缓存的 K 线获取。
        已缓存的数据直接读取, 只增量拉取新数据。

        Args:
            ... (同 fetch_candles)
            force_refresh: 强制重新拉取

        Returns:
            DataFrame: K 线数据
        """
        cache_key = f"{token_id[:16]}_{interval}"
        cache_path = self.data_dir / f"{cache_key}.parquet"

        # 读取缓存
        cached_df = pd.DataFrame()
        if cache_path.exists() and not force_refresh:
            try:
                cached_df = pd.read_parquet(cache_path)
                logger.info(f"从缓存读取 {len(cached_df)} 根 K 线: {cache_path.name}")
            except Exception as e:
                logger.warning(f"缓存读取失败: {e}")

        # 确定需要拉取的时间范围
        fetch_start = start_date
        if not cached_df.empty and not force_refresh:
            last_ts = cached_df["timestamp"].max()
            # 从最后一根 K 线开始增量拉取
            fetch_start = datetime.fromtimestamp(last_ts, tz=timezone.utc).strftime("%Y-%m-%d")

        # 拉取新数据
        new_df = await self.fetch_candles(
            token_id=token_id,
            interval=interval,
            start_date=fetch_start,
            end_date=end_date,
            condition_id=condition_id,
        )

        # 合并
        if not cached_df.empty and not new_df.empty:
            combined = pd.concat([cached_df, new_df]).drop_duplicates(
                subset=["timestamp"], keep="last"
            )
            combined = combined.sort_values("timestamp").reset_index(drop=True)
        elif not new_df.empty:
            combined = new_df
        else:
            combined = cached_df

        # 过滤时间范围
        if start_date:
            start_ts = _date_to_ts(start_date)
            combined = combined[combined["timestamp"] >= start_ts]
        if end_date:
            end_ts = _date_to_ts(end_date, end_of_day=True)
            combined = combined[combined["timestamp"] <= end_ts]

        combined = combined.reset_index(drop=True)

        # 写缓存
        if not combined.empty:
            try:
                combined.to_parquet(cache_path, index=False, compression="snappy")
                logger.info(f"缓存已更新: {len(combined)} 根 K 线 → {cache_path.name}")
            except Exception as e:
                logger.warning(f"写缓存失败: {e}")

        return combined

    # ================================================================
    #  批量拉取多个市场 (用于发现和回测选择)
    # ================================================================

    async def fetch_multiple_markets(
        self,
        market_infos: list[MarketInfo],
        interval: str = "5m",
        start_date: str = "",
        end_date: str = "",
        outcome: str = "Yes",
    ) -> dict[str, pd.DataFrame]:
        """
        批量拉取多个市场的历史数据。

        Args:
            market_infos: 市场列表 (从 search_btc_markets 获取)
            interval: K 线周期
            start_date / end_date: 时间范围
            outcome: 拉取哪个 outcome 的 token ("Yes" / "No")

        Returns:
            dict: {condition_id: candles_df}
        """
        results: dict[str, pd.DataFrame] = {}

        for market in market_infos:
            # 找到指定 outcome 的 token_id
            token_id = ""
            for t in market.tokens:
                if t.get("outcome", "").lower() == outcome.lower():
                    token_id = t.get("token_id", "")
                    break

            if not token_id:
                logger.warning(f"市场 {market.condition_id[:12]}... 未找到 {outcome} token")
                continue

            logger.info(f"拉取: {market.question[:60]}...")
            candles = await self.fetch_candles_cached(
                token_id=token_id,
                interval=interval,
                start_date=start_date,
                end_date=end_date,
                condition_id=market.condition_id,
            )

            if not candles.empty:
                results[market.condition_id] = candles

            await asyncio.sleep(self.RATE_LIMIT_DELAY)

        logger.info(f"批量拉取完成: {len(results)}/{len(market_infos)} 个市场有数据")
        return results


# ================================================================
#  辅助函数
# ================================================================

def _parse_interval(interval: str) -> int:
    """解析 K 线周期字符串为秒数"""
    mapping = {
        "1m": 60,
        "3m": 180,
        "5m": 300,
        "15m": 900,
        "30m": 1800,
        "1h": 3600,
        "2h": 7200,
        "4h": 14400,
        "6h": 21600,
        "12h": 43200,
        "1d": 86400,
    }
    if interval in mapping:
        return mapping[interval]

    # 尝试解析 "Xm" / "Xh" / "Xd"
    try:
        if interval.endswith("m"):
            return int(interval[:-1]) * 60
        elif interval.endswith("h"):
            return int(interval[:-1]) * 3600
        elif interval.endswith("d"):
            return int(interval[:-1]) * 86400
    except ValueError:
        pass

    return 300  # 默认 5 分钟


def _date_to_ts(date_str: str, end_of_day: bool = False) -> float:
    """日期字符串转 Unix 时间戳"""
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        if end_of_day:
            dt = dt + timedelta(days=1) - timedelta(seconds=1)
        return dt.timestamp()
    except ValueError:
        return 0.0
