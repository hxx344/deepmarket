"""
Storage - 轻量级数据存储层 (方案B)

使用 SQLite + Parquet + diskcache 替代重型数据库。
"""

from __future__ import annotations

import json
import re
import sqlite3
import time
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from diskcache import Cache
from loguru import logger


class ParquetStore:
    """
    Parquet 列式存储 - 用于行情/K线等时序数据

    按日期分区存储，使用 DuckDB 做高效查询。
    """

    def __init__(self, base_dir: str | Path) -> None:
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def _partition_path(self, table_name: str, date_str: str) -> Path:
        """获取分区文件路径: base_dir/table_name/date=YYYY-MM-DD/data.parquet"""
        path = self.base_dir / table_name / f"date={date_str}"
        path.mkdir(parents=True, exist_ok=True)
        return path / "data.parquet"

    def append(self, table_name: str, df: pd.DataFrame, date_str: str | None = None) -> None:
        """
        追加数据到 Parquet 文件

        Args:
            table_name: 表名
            df: 数据
            date_str: 日期分区键 (YYYY-MM-DD)，为 None 则自动取当天
        """
        if df.empty:
            return

        if date_str is None:
            date_str = pd.Timestamp.now().strftime("%Y-%m-%d")

        path = self._partition_path(table_name, date_str)
        table = pa.Table.from_pandas(df)

        if path.exists():
            existing = pq.read_table(path)
            table = pa.concat_tables([existing, table])

        pq.write_table(table, path, compression="lz4")
        logger.debug(f"Appended {len(df)} rows to {table_name}/{date_str}")

    def query(
        self,
        table_name: str,
        sql: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> pd.DataFrame:
        """
        查询 Parquet 数据

        Args:
            table_name: 表名
            sql: DuckDB SQL 查询 (使用 {table} 作为占位符)
            start_date: 起始日期
            end_date: 结束日期
        """
        table_dir = self.base_dir / table_name
        if not table_dir.exists():
            return pd.DataFrame()

        glob_pattern = str(table_dir / "**" / "*.parquet")

        if sql:
            sql = sql.replace("{table}", f"read_parquet('{glob_pattern}', hive_partitioning=true)")
        else:
            where_parts = []
            if start_date:
                # 日期格式校验防止注入
                if not re.match(r'^\d{4}-\d{2}-\d{2}$', start_date):
                    raise ValueError(f"Invalid start_date format: {start_date}")
                where_parts.append(f"date >= '{start_date}'")
            if end_date:
                if not re.match(r'^\d{4}-\d{2}-\d{2}$', end_date):
                    raise ValueError(f"Invalid end_date format: {end_date}")
                where_parts.append(f"date <= '{end_date}'")

            where_clause = f" WHERE {' AND '.join(where_parts)}" if where_parts else ""
            sql = f"SELECT * FROM read_parquet('{glob_pattern}', hive_partitioning=true){where_clause} ORDER BY timestamp"

        try:
            conn = duckdb.connect()
            result = conn.execute(sql).fetchdf()
            conn.close()
            return result
        except Exception as e:
            logger.error(f"Parquet query error: {e}")
            return pd.DataFrame()

    def list_dates(self, table_name: str) -> list[str]:
        """列出某表的所有日期分区"""
        table_dir = self.base_dir / table_name
        if not table_dir.exists():
            return []
        dates = []
        for d in sorted(table_dir.iterdir()):
            if d.is_dir() and d.name.startswith("date="):
                dates.append(d.name.replace("date=", ""))
        return dates


class SQLiteStore:
    """
    SQLite 存储 - 用于业务数据（订单、策略配置、风控记录等）
    """

    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn: sqlite3.Connection | None = None
        self._init_db()

    def _init_db(self) -> None:
        """初始化数据库表"""
        conn = self._get_conn()

        conn.executescript("""
            -- 订单表
            CREATE TABLE IF NOT EXISTS orders (
                id TEXT PRIMARY KEY,
                strategy_id TEXT NOT NULL,
                market_id TEXT NOT NULL,
                side TEXT NOT NULL,          -- 'YES' or 'NO'
                order_type TEXT NOT NULL,     -- 'market', 'limit'
                price REAL,
                size REAL NOT NULL,
                filled_size REAL DEFAULT 0,
                status TEXT DEFAULT 'pending',
                created_at REAL NOT NULL,
                updated_at REAL,
                meta TEXT                    -- JSON 扩展字段
            );

            -- 成交表
            CREATE TABLE IF NOT EXISTS trades (
                id TEXT PRIMARY KEY,
                order_id TEXT NOT NULL,
                price REAL NOT NULL,
                size REAL NOT NULL,
                fee REAL DEFAULT 0,
                timestamp REAL NOT NULL,
                FOREIGN KEY (order_id) REFERENCES orders(id)
            );

            -- 策略运行记录
            CREATE TABLE IF NOT EXISTS strategy_runs (
                id TEXT PRIMARY KEY,
                strategy_id TEXT NOT NULL,
                mode TEXT NOT NULL,
                started_at REAL NOT NULL,
                stopped_at REAL,
                config TEXT,                 -- JSON
                status TEXT DEFAULT 'running'
            );

            -- 风控事件
            CREATE TABLE IF NOT EXISTS risk_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                rule_id TEXT NOT NULL,
                level TEXT NOT NULL,          -- 'warning', 'alert', 'breaker'
                message TEXT NOT NULL,
                data TEXT,                    -- JSON
                timestamp REAL NOT NULL,
                acknowledged INTEGER DEFAULT 0
            );

            -- 系统日志
            CREATE TABLE IF NOT EXISTS system_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                level TEXT NOT NULL,
                module TEXT,
                message TEXT NOT NULL,
                timestamp REAL NOT NULL
            );

            -- 创建索引
            CREATE INDEX IF NOT EXISTS idx_orders_strategy ON orders(strategy_id);
            CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
            CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON trades(timestamp);
            CREATE INDEX IF NOT EXISTS idx_risk_events_ts ON risk_events(timestamp);
        """)
        conn.commit()

    def _get_conn(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(str(self.db_path))
            self._conn.row_factory = sqlite3.Row
        return self._conn

    def execute(self, sql: str, params: tuple = ()) -> sqlite3.Cursor:
        conn = self._get_conn()
        cursor = conn.execute(sql, params)
        conn.commit()
        return cursor

    def query(self, sql: str, params: tuple = ()) -> list[dict]:
        cursor = self.execute(sql, params)
        return [dict(row) for row in cursor.fetchall()]

    def insert(self, table: str, data: dict[str, Any]) -> None:
        # 防止 SQL 注入: 只允许字母、数字、下划线作为表名和列名
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', table):
            raise ValueError(f"Invalid table name: {table}")
        for col in data.keys():
            if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', col):
                raise ValueError(f"Invalid column name: {col}")
        columns = ", ".join(data.keys())
        placeholders = ", ".join(["?"] * len(data))
        sql = f"INSERT OR REPLACE INTO {table} ({columns}) VALUES ({placeholders})"
        self.execute(sql, tuple(data.values()))

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None


class CacheStore:
    """
    diskcache 缓存 - 替代 Redis，用于实时数据缓存
    """

    def __init__(self, cache_dir: str | Path) -> None:
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self._cache = Cache(str(self.cache_dir))

    def get(self, key: str, default: Any = None) -> Any:
        return self._cache.get(key, default)

    def set(self, key: str, value: Any, ttl: float | None = None) -> None:
        """
        设置缓存

        Args:
            key: 缓存键
            value: 缓存值
            ttl: 过期时间（秒），None 表示不过期
        """
        self._cache.set(key, value, expire=ttl)

    def delete(self, key: str) -> bool:
        return self._cache.delete(key)

    def get_json(self, key: str) -> dict | None:
        data = self._cache.get(key)
        if data is None:
            return None
        return json.loads(data) if isinstance(data, str) else data

    def set_json(self, key: str, value: dict, ttl: float | None = None) -> None:
        self._cache.set(key, json.dumps(value), expire=ttl)

    def push_list(self, key: str, value: Any, max_len: int = 1000) -> None:
        """向列表追加元素（模拟 Redis LPUSH + LTRIM）"""
        lst = self._cache.get(key, [])
        lst.append(value)
        if len(lst) > max_len:
            lst = lst[-max_len:]
        self._cache.set(key, lst)

    def get_list(self, key: str, start: int = 0, end: int = -1) -> list:
        lst = self._cache.get(key, [])
        if end == -1:
            return lst[start:]
        return lst[start:end + 1]

    def close(self) -> None:
        self._cache.close()


class StorageManager:
    """统一存储管理器"""

    def __init__(self, data_dir: str | Path = "data") -> None:
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)

        self.parquet = ParquetStore(self.data_dir / "timeseries")
        self.db = SQLiteStore(self.data_dir / "pm_bot.db")
        self.cache = CacheStore(self.data_dir / "cache")

        logger.info(f"StorageManager initialized at {self.data_dir}")

    def close(self) -> None:
        self.db.close()
        self.cache.close()
        logger.info("StorageManager closed")
