"""
Backtest Report - 回测绩效报告生成

生成可视化 HTML 报告，包含净值曲线、回撤图、交易分布等。
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
from loguru import logger


class ReportGenerator:
    """
    回测报告生成器

    使用 Plotly 生成交互式 HTML 报告。
    """

    def __init__(self, output_dir: str | Path = "reports") -> None:
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def generate(
        self,
        result,  # BacktestResult
        report_name: str = "backtest_report",
    ) -> Path:
        """
        生成 HTML 报告

        Args:
            result: BacktestResult 对象
            report_name: 报告文件名

        Returns:
            报告文件路径
        """
        try:
            import plotly.graph_objects as go
            from plotly.subplots import make_subplots
        except ImportError:
            logger.error("plotly not installed, skipping report generation")
            return self._generate_text_report(result, report_name)

        fig = make_subplots(
            rows=4, cols=1,
            shared_xaxes=True,
            row_heights=[0.4, 0.2, 0.2, 0.2],
            subplot_titles=("账户净值", "回撤", "BTC 价格", "交易分布"),
            vertical_spacing=0.05,
        )

        equity = result.equity_curve
        if not equity.empty:
            timestamps = pd.to_datetime(equity["timestamp"], unit="s")

            # 1. 净值曲线
            fig.add_trace(
                go.Scatter(
                    x=timestamps, y=equity["equity"],
                    name="净值", line=dict(color="blue", width=2),
                ),
                row=1, col=1,
            )
            fig.add_hline(
                y=result.initial_balance, line_dash="dash",
                line_color="gray", row=1, col=1,
            )

            # 2. 回撤曲线
            peak = equity["equity"].expanding().max()
            drawdown = (equity["equity"] - peak) / peak * 100
            fig.add_trace(
                go.Scatter(
                    x=timestamps, y=drawdown,
                    name="回撤 %", fill="tozeroy",
                    line=dict(color="red", width=1),
                ),
                row=2, col=1,
            )

            # 3. BTC 价格
            if "btc_price" in equity.columns:
                fig.add_trace(
                    go.Scatter(
                        x=timestamps, y=equity["btc_price"],
                        name="BTC", line=dict(color="orange", width=1),
                    ),
                    row=3, col=1,
                )

        # 4. 交易分布
        trades = result.trades
        if not trades.empty and "pnl" in trades.columns:
            colors = ["green" if p > 0 else "red" for p in trades["pnl"]]
            fig.add_trace(
                go.Bar(
                    x=list(range(len(trades))),
                    y=trades["pnl"],
                    name="交易 PnL",
                    marker_color=colors,
                ),
                row=4, col=1,
            )

        # 布局
        fig.update_layout(
            title=f"回测报告 - {report_name}",
            height=1000,
            showlegend=True,
            template="plotly_white",
        )

        # 添加绩效摘要注解
        summary = result.summary()
        annotation_text = "<br>".join([f"{k}: {v}" for k, v in summary.items()])
        fig.add_annotation(
            text=annotation_text,
            xref="paper", yref="paper",
            x=1.02, y=1.0,
            showarrow=False,
            font=dict(size=10, family="monospace"),
            align="left",
            bgcolor="lightyellow",
            bordercolor="gray",
            borderwidth=1,
        )

        # 保存
        file_path = self.output_dir / f"{report_name}.html"
        fig.write_html(str(file_path))
        logger.info(f"Report saved: {file_path}")

        # 同时保存摘要 JSON
        import json
        summary_path = self.output_dir / f"{report_name}_summary.json"
        with open(summary_path, "w") as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)

        return file_path

    def _generate_text_report(self, result, report_name: str) -> Path:
        """降级为文本报告"""
        summary = result.summary()
        report_lines = [
            f"{'='*50}",
            f"  回测报告: {report_name}",
            f"{'='*50}",
        ]
        for k, v in summary.items():
            report_lines.append(f"  {k:20s}: {v}")
        report_lines.append(f"{'='*50}")

        file_path = self.output_dir / f"{report_name}.txt"
        with open(file_path, "w", encoding="utf-8") as f:
            f.write("\n".join(report_lines))

        logger.info(f"Text report saved: {file_path}")
        return file_path
