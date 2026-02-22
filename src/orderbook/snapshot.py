"""
Order Book Snapshot Manager - 订单簿快照存储与回放

每秒存储一次增量快照，支持任意时刻回放。
使用 Parquet 压缩存储。
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import pandas as pd
from loguru import logger

from src.orderbook.book import OrderBook, OrderBookState


class SnapshotManager:
    """
    订单簿快照管理器

    - 定期存储订单簿快照 (增量/全量)
    - 支持历史回放
    - 存储使用 Parquet (LZ4 压缩)
    """

    def __init__(
        self,
        data_dir: str | Path = "data/orderbook_snapshots",
        snapshot_interval: float = 1.0,  # 秒
    ) -> None:
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.snapshot_interval = snapshot_interval
        self._last_snapshot_time: float = 0.0
        self._buffer: list[dict] = []
        self._buffer_limit = 300  # 每 300 条 flush 一次 (~5 min)

    def should_snapshot(self) -> bool:
        """检查是否应该拍快照"""
        return time.time() - self._last_snapshot_time >= self.snapshot_interval

    def capture(self, book_state: OrderBookState) -> None:
        """
        捕获订单簿快照

        Args:
            book_state: 当前订单簿状态
        """
        if not self.should_snapshot():
            return

        snapshot = {
            "timestamp": book_state.timestamp,
            "market_id": book_state.market_id,
            "best_bid": book_state.best_bid,
            "best_ask": book_state.best_ask,
            "mid_price": book_state.mid_price,
            "spread": book_state.spread,
            "bid_depth_5": book_state.total_bid_size(5),
            "ask_depth_5": book_state.total_ask_size(5),
            # 存储前 10 档深度作为 JSON
            "bids_json": json.dumps(
                [{"p": l.price, "s": l.size} for l in book_state.bids[:10]]
            ),
            "asks_json": json.dumps(
                [{"p": l.price, "s": l.size} for l in book_state.asks[:10]]
            ),
        }

        self._buffer.append(snapshot)
        self._last_snapshot_time = time.time()

        if len(self._buffer) >= self._buffer_limit:
            self.flush()

    def flush(self) -> None:
        """将缓冲区写入 Parquet"""
        if not self._buffer:
            return

        df = pd.DataFrame(self._buffer)
        date_str = pd.Timestamp.now().strftime("%Y-%m-%d")
        hour_str = pd.Timestamp.now().strftime("%H")

        # 按日期+小时分区
        partition_dir = self.data_dir / f"date={date_str}" / f"hour={hour_str}"
        partition_dir.mkdir(parents=True, exist_ok=True)

        file_name = f"snap_{int(time.time())}.parquet"
        file_path = partition_dir / file_name

        df.to_parquet(file_path, compression="lz4", index=False)
        logger.debug(f"Flushed {len(self._buffer)} snapshots to {file_path}")
        self._buffer.clear()

    def load_snapshots(
        self,
        date_str: str,
        start_hour: int = 0,
        end_hour: int = 23,
    ) -> pd.DataFrame:
        """
        加载历史快照

        Args:
            date_str: 日期 (YYYY-MM-DD)
            start_hour: 起始小时
            end_hour: 结束小时

        Returns:
            快照 DataFrame
        """
        frames = []
        date_dir = self.data_dir / f"date={date_str}"
        if not date_dir.exists():
            return pd.DataFrame()

        for h in range(start_hour, end_hour + 1):
            hour_dir = date_dir / f"hour={h:02d}"
            if not hour_dir.exists():
                continue
            for f in sorted(hour_dir.glob("*.parquet")):
                frames.append(pd.read_parquet(f))

        if not frames:
            return pd.DataFrame()

        df = pd.concat(frames, ignore_index=True)
        return df.sort_values("timestamp").reset_index(drop=True)

    def replay(self, date_str: str) -> list[OrderBookState]:
        """
        回放某天的订单簿序列

        Returns:
            OrderBookState 列表（按时间排序）
        """
        df = self.load_snapshots(date_str)
        if df.empty:
            return []

        states = []
        for _, row in df.iterrows():
            from src.orderbook.book import PriceLevel
            bids = [PriceLevel(price=b["p"], size=b["s"]) for b in json.loads(row["bids_json"])]
            asks = [PriceLevel(price=a["p"], size=a["s"]) for a in json.loads(row["asks_json"])]
            states.append(OrderBookState(
                bids=bids,
                asks=asks,
                timestamp=row["timestamp"],
                market_id=row["market_id"],
            ))

        return states

    def cleanup(self, keep_days: int = 30) -> int:
        """清理过期快照，返回删除的文件数"""
        import shutil
        cutoff = pd.Timestamp.now() - pd.Timedelta(days=keep_days)
        cutoff_str = cutoff.strftime("%Y-%m-%d")
        deleted = 0

        for date_dir in self.data_dir.iterdir():
            if not date_dir.name.startswith("date="):
                continue
            date_val = date_dir.name.replace("date=", "")
            if date_val < cutoff_str:
                shutil.rmtree(date_dir)
                deleted += 1
                logger.info(f"Cleaned up snapshots: {date_dir.name}")

        return deleted
