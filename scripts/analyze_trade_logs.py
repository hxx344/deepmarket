"""
Trade Log Analyzer — 交易日志离线分析工具

从 TradeLogger 产生的 SQLite 数据库中加载历史交易记录，
生成策略优化所需的统计分析。

用法:
    python scripts/analyze_trade_logs.py                    # 分析最近一次运行
    python scripts/analyze_trade_logs.py --run-id <id>      # 分析指定运行
    python scripts/analyze_trade_logs.py --list              # 列出所有运行记录
    python scripts/analyze_trade_logs.py --all               # 分析所有运行
    python scripts/analyze_trade_logs.py --last 5            # 分析最近 5 次运行
    python scripts/analyze_trade_logs.py --export csv        # 导出 CSV
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path

import pandas as pd

DB_PATH = Path("data/trade_logs.db")


def get_conn(db_path: Path = DB_PATH) -> sqlite3.Connection:
    if not db_path.exists():
        print(f"错误: 数据库不存在 — {db_path}")
        print("请先运行 paper 或 live 模式以产生交易日志。")
        sys.exit(1)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


# ================================================================
#  列出运行记录
# ================================================================

def list_runs(conn: sqlite3.Connection, limit: int = 20) -> pd.DataFrame:
    df = pd.read_sql_query(
        "SELECT run_id, mode, strategy, "
        "datetime(start_time, 'unixepoch', 'localtime') as start, "
        "datetime(end_time, 'unixepoch', 'localtime') as end, "
        "total_orders, total_fills, total_settlements, "
        "win_count, loss_count, "
        "printf('%.1f%%', win_rate * 100) as win_rate, "
        "printf('%.4f', total_pnl) as pnl, "
        "status "
        "FROM runs ORDER BY start_time DESC LIMIT ?",
        conn,
        params=(limit,),
    )
    return df


# ================================================================
#  单次运行分析
# ================================================================

def analyze_run(conn: sqlite3.Connection, run_id: str) -> None:
    """详细分析一次运行的交易日志。"""
    # ── 运行摘要 ──
    run = conn.execute(
        "SELECT * FROM runs WHERE run_id = ?", (run_id,)
    ).fetchone()
    if not run:
        print(f"运行记录不存在: {run_id}")
        return

    print("=" * 70)
    print(f"  运行 ID:    {run['run_id']}")
    print(f"  模式:       {run['mode']}")
    print(f"  策略:       {run['strategy']}")
    print(f"  状态:       {run['status']}")
    if run['start_time']:
        import datetime
        start = datetime.datetime.fromtimestamp(run['start_time'])
        print(f"  开始时间:   {start.strftime('%Y-%m-%d %H:%M:%S')}")
    if run['end_time']:
        end = datetime.datetime.fromtimestamp(run['end_time'])
        duration = run['end_time'] - run['start_time']
        print(f"  结束时间:   {end.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"  运行时长:   {duration:.0f}s ({duration/60:.1f}min)")
    print(f"  初始余额:   ${run['initial_balance']:.2f}" if run['initial_balance'] else "")
    print(f"  最终余额:   ${run['final_balance']:.2f}" if run['final_balance'] else "")
    print("=" * 70)

    # ── 订单统计 ──
    orders_df = pd.read_sql_query(
        "SELECT * FROM order_logs WHERE run_id = ? ORDER BY timestamp",
        conn,
        params=(run_id,),
    )

    if orders_df.empty:
        print("\n  (无订单记录)")
    else:
        print(f"\n📊 订单统计 ({len(orders_df)} 笔)")
        print("-" * 50)

        # 按状态统计
        status_counts = orders_df["status"].value_counts()
        for status, count in status_counts.items():
            print(f"  {status}: {count}")

        # 按方向统计
        filled = orders_df[orders_df["status"] == "filled"]
        if not filled.empty:
            print(f"\n  成交订单: {len(filled)}")
            for direction in ["UP", "DOWN"]:
                d_orders = filled[filled["direction"] == direction]
                if not d_orders.empty:
                    avg_price = d_orders["filled_price"].mean()
                    total_shares = d_orders["filled_shares"].sum()
                    total_cost = (d_orders["filled_shares"] * d_orders["filled_price"]).sum()
                    print(
                        f"    {direction}: {len(d_orders)} 笔 | "
                        f"均价={avg_price:.4f} | "
                        f"shares={total_shares:.2f} | "
                        f"cost=${total_cost:.2f}"
                    )

            # 下单时间分布
            print(f"\n  窗口内下单时间分布 (secs_left):")
            secs = filled["window_seconds_left"]
            if secs.notna().any() and (secs > 0).any():
                valid = secs[secs > 0]
                print(f"    min={valid.min():.0f}s  max={valid.max():.0f}s  "
                      f"mean={valid.mean():.0f}s  std={valid.std():.1f}s")

            # 动量分布
            print(f"\n  动量分布:")
            mom = filled["momentum"]
            if mom.notna().any():
                print(f"    min={mom.min():.2f}  max={mom.max():.2f}  "
                      f"mean={mom.mean():.2f}  std={mom.std():.2f}")

            # Edge 分布
            print(f"\n  Edge 分布:")
            edg = filled["edge"]
            if edg.notna().any():
                print(f"    min={edg.min():.4f}  max={edg.max():.4f}  "
                      f"mean={edg.mean():.4f}")

            # BTC 价格范围
            print(f"\n  BTC 价格范围:")
            btc = filled["btc_price"]
            if btc.notna().any() and (btc > 0).any():
                valid_btc = btc[btc > 0]
                print(f"    min=${valid_btc.min():,.2f}  max=${valid_btc.max():,.2f}")

    # ── 结算统计 ──
    settle_df = pd.read_sql_query(
        "SELECT * FROM settlement_logs WHERE run_id = ? ORDER BY timestamp",
        conn,
        params=(run_id,),
    )

    if settle_df.empty:
        print("\n  (无结算记录)")
    else:
        print(f"\n📊 结算统计 ({len(settle_df)} 个窗口)")
        print("-" * 50)

        wins = settle_df[settle_df["result"] == "WIN"]
        losses = settle_df[settle_df["result"] == "LOSE"]
        print(f"  胜: {len(wins)}  负: {len(losses)}  "
              f"胜率: {len(wins)/len(settle_df)*100:.1f}%")

        total_pnl = settle_df["net_pnl"].sum()
        avg_pnl = settle_df["net_pnl"].mean()
        print(f"  总 PnL: {total_pnl:+.4f} USDC")
        print(f"  平均 PnL/窗口: {avg_pnl:+.4f} USDC")

        if not wins.empty:
            avg_win = wins["net_pnl"].mean()
            print(f"  平均盈利: {avg_win:+.4f} USDC")
        if not losses.empty:
            avg_loss = losses["net_pnl"].mean()
            print(f"  平均亏损: {avg_loss:+.4f} USDC")

        # Edge 分析
        print(f"\n  实际 Edge 分析:")
        edge = settle_df["actual_edge"]
        if edge.notna().any():
            print(f"    均值: {edge.mean():.4f}  中位数: {edge.median():.4f}")
            print(f"    正Edge: {(edge > 0).sum()}/{len(edge)}  "
                  f"({(edge > 0).mean()*100:.1f}%)")

        # Gap 分析
        print(f"\n  Gap (UP-DN shares 差异) 分析:")
        gap = settle_df["gap_shares"]
        gap_pct = settle_df["gap_pct"]
        if gap.notna().any():
            print(f"    shares: mean={gap.mean():+.2f}  std={gap.std():.2f}")
            print(f"    pct:    mean={gap_pct.mean():.1f}%  max={gap_pct.max():.1f}%")

        # 亏损窗口特征分析
        if not losses.empty and len(losses) >= 2:
            print(f"\n  亏损窗口特征:")
            print(f"    平均 entries: {losses['entries'].mean():.1f}")
            print(f"    平均 gap:     {losses['gap_pct'].mean():.1f}%")
            print(f"    平均 edge:    {losses['actual_edge'].mean():.4f}")

        # PnL 曲线
        print(f"\n  PnL 轨迹:")
        cumsum = settle_df["net_pnl"].cumsum()
        peak = cumsum.cummax()
        drawdown = cumsum - peak
        max_dd = drawdown.min()
        print(f"    最大回撤: {max_dd:+.4f} USDC")
        print(f"    最高点:   {peak.max():+.4f} USDC")

    # ── 信号统计 ──
    signal_df = pd.read_sql_query(
        "SELECT * FROM signal_logs WHERE run_id = ? ORDER BY timestamp",
        conn,
        params=(run_id,),
    )

    if not signal_df.empty:
        print(f"\n📊 信号统计 ({len(signal_df)} 个)")
        print("-" * 50)
        exec_count = signal_df["executed"].sum()
        print(f"  已执行: {exec_count}  跳过: {len(signal_df) - exec_count}")
        if "momentum" in signal_df.columns:
            mom = signal_df["momentum"]
            if mom.notna().any():
                print(f"  信号动量: mean={mom.mean():.2f}  std={mom.std():.2f}")


# ================================================================
#  多次运行对比
# ================================================================

def compare_runs(conn: sqlite3.Connection, run_ids: list[str]) -> None:
    """对比多次运行的关键指标。"""
    rows = []
    for rid in run_ids:
        run = conn.execute(
            "SELECT * FROM runs WHERE run_id = ?", (rid,)
        ).fetchone()
        if not run:
            continue

        settle_df = pd.read_sql_query(
            "SELECT net_pnl, actual_edge, gap_pct, entries "
            "FROM settlement_logs WHERE run_id = ?",
            conn,
            params=(rid,),
        )

        duration = 0
        if run['end_time'] and run['start_time']:
            duration = run['end_time'] - run['start_time']

        rows.append({
            "run_id": rid[:30],
            "mode": run["mode"],
            "strategy": (run["strategy"] or "")[:20],
            "duration_min": round(duration / 60, 1),
            "orders": run["total_orders"],
            "fills": run["total_fills"],
            "settlements": run["total_settlements"],
            "win_rate": f"{(run['win_rate'] or 0) * 100:.1f}%",
            "total_pnl": round(run["total_pnl"] or 0, 4),
            "avg_pnl": round(settle_df["net_pnl"].mean(), 4) if not settle_df.empty else 0,
            "avg_edge": round(settle_df["actual_edge"].mean(), 4) if not settle_df.empty else 0,
            "avg_gap%": round(settle_df["gap_pct"].mean(), 1) if not settle_df.empty else 0,
        })

    if rows:
        df = pd.DataFrame(rows)
        print("\n📊 运行对比")
        print("=" * 100)
        print(df.to_string(index=False))
    else:
        print("没有找到匹配的运行记录")


# ================================================================
#  导出
# ================================================================

def export_run(conn: sqlite3.Connection, run_id: str, fmt: str = "csv") -> None:
    """导出指定运行的数据。"""
    export_dir = Path("data/trade_logs/exports")
    export_dir.mkdir(parents=True, exist_ok=True)

    for table in ["order_logs", "settlement_logs", "signal_logs"]:
        df = pd.read_sql_query(
            f"SELECT * FROM {table} WHERE run_id = ? ORDER BY timestamp",
            conn,
            params=(run_id,),
        )
        if df.empty:
            continue

        if fmt == "csv":
            path = export_dir / f"{run_id}_{table}.csv"
            df.to_csv(path, index=False)
        elif fmt == "parquet":
            path = export_dir / f"{run_id}_{table}.parquet"
            df.to_parquet(path, index=False)
        else:
            path = export_dir / f"{run_id}_{table}.json"
            df.to_json(path, orient="records", lines=True)

        print(f"  已导出: {path}")


# ================================================================
#  JSONL 加载辅助
# ================================================================

def load_jsonl(path: str | Path) -> pd.DataFrame:
    """加载 JSONL 文件为 DataFrame (用于更细粒度分析)。"""
    records = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return pd.DataFrame(records)


# ================================================================
#  CLI
# ================================================================

def main():
    parser = argparse.ArgumentParser(
        description="交易日志分析工具 — 分析 paper/live 运行记录以优化策略",
    )
    parser.add_argument("--db", type=str, default="data/trade_logs.db",
                        help="数据库路径")
    parser.add_argument("--list", action="store_true",
                        help="列出所有运行记录")
    parser.add_argument("--run-id", type=str, default="",
                        help="分析指定运行 ID")
    parser.add_argument("--all", action="store_true",
                        help="分析所有运行")
    parser.add_argument("--last", type=int, default=0,
                        help="分析/对比最近 N 次运行")
    parser.add_argument("--export", type=str, choices=["csv", "parquet", "json"],
                        help="导出运行数据")

    args = parser.parse_args()
    conn = get_conn(Path(args.db))

    if args.list:
        df = list_runs(conn, limit=50)
        if df.empty:
            print("暂无运行记录。")
        else:
            print("\n📋 运行记录")
            print("=" * 120)
            print(df.to_string(index=False))
        conn.close()
        return

    if args.run_id:
        if args.export:
            export_run(conn, args.run_id, args.export)
        else:
            analyze_run(conn, args.run_id)
        conn.close()
        return

    if args.last > 0:
        runs = conn.execute(
            "SELECT run_id FROM runs ORDER BY start_time DESC LIMIT ?",
            (args.last,),
        ).fetchall()
        run_ids = [r["run_id"] for r in runs]
        if len(run_ids) == 1:
            analyze_run(conn, run_ids[0])
        elif len(run_ids) > 1:
            compare_runs(conn, run_ids)
            print("\n" + "=" * 70)
            print("要查看单次运行详情, 请使用: --run-id <id>")
        else:
            print("暂无运行记录。")
        conn.close()
        return

    if args.all:
        runs = conn.execute(
            "SELECT run_id FROM runs ORDER BY start_time DESC"
        ).fetchall()
        run_ids = [r["run_id"] for r in runs]
        if run_ids:
            compare_runs(conn, run_ids)
        else:
            print("暂无运行记录。")
        conn.close()
        return

    # 默认: 分析最近一次运行
    latest = conn.execute(
        "SELECT run_id FROM runs ORDER BY start_time DESC LIMIT 1"
    ).fetchone()
    if latest:
        run_id = latest["run_id"]
        if args.export:
            export_run(conn, run_id, args.export)
        else:
            analyze_run(conn, run_id)
    else:
        print("暂无运行记录。请先运行 paper 或 live 模式。")

    conn.close()


if __name__ == "__main__":
    main()
