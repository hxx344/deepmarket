# 项目记忆 (Copilot Context)
> 本文件记录关键技术决策和项目状态，供 AI 助手快速恢复上下文。

---

## 项目概述
- **名称**: Polymarket BTC 5-min Prediction Betting System
- **目标**: 在 Polymarket 的 BTC 5分钟涨跌预测市场上自动交易
- **参考账号**: 0x1d (地址 0x1d0034134e339a309700ff2d34e99fa2d48b0313)，高胜率交易者，作为蒸馏学习的"老师"
- **最终目标**: 训练黑盒 ML 模型，输入实时市场数据 → 输出 UP/DOWN/HOLD 信号，替代手工规则

## 技术栈
- Python 3.9+, asyncio + aiohttp + websockets
- SQLite + Parquet + DuckDB + diskcache
- scikit-learn, LightGBM
- Polymarket CLOB API, Chainlink RTDS WS, Binance WS

## 关键文件

### 核心代码
| 文件 | 说明 | 版本 |
|------|------|------|
| `monitor_0x1d.py` | 实时监控 0x1d 交易 + 数据采集 | ~2100行, 88+特征 |
| `src/strategy/templates/btc5min_taker.py` | BTC 5min 双边下注策略 | v5.3 |
| `config/strategies/btc5min_taker.yaml` | 策略参数配置 | v5.3 |
| `scripts/distill_signal.py` | 实时信号蒸馏训练 | 新版 |
| `scripts/distill_0x1d.py` | 旧版窗口级蒸馏 (已弃用) | — |
| `src/utils/trade_logger.py` | 交易日志 SQLite+JSONL | — |

### 数据
| 文件 | 说明 |
|------|------|
| `data/0x1d_data.db` | 监控数据: trade_snaps + settlements |
| `data/distill_models/signal_model.pkl` | 训练产出的信号模型 |
| `data/distill_models/signal_config.json` | 模型特征列表 + 阈值 |

## 策略演进 (btc5min_taker)
1. **v5.1**: 基础双边下注
2. **v5.2**: 增加 `_time_gap_control()` — 幂函数衰减控制下单金额
3. **v5.3**: 增加三层 choppiness guard + price guard (edge gate + 价格恶化检测)

## 监控特征体系 (monitor_0x1d.py)

### ref_ts 时间回溯
- 所有特征回溯到 0x1d 的实际决策时间点 (CLOB 撮合时间)，而非 monitor 轮询时间
- 6 个静态计算方法都支持 `ref_ts` 参数
- 辅助方法: `_price_at()`, `_pm_state_at()`

### 特征分组 (共 ~94 个)
- **BN动量** (8): bn_mom_1s ~ bn_mom_120s
- **BN波动率** (3): bn_vol_10s/30s/60s
- **BN高级** (6): 加速度、趋势、z-score、百分位
- **CL动量** (8): mom_1s ~ mom_120s
- **CL波动率** (3): btc_vol_10s/30s/60s
- **CL高级** (11): 趋势、百分位、z-score、方向变化
- **跨源** (9): CL-BN价差、动量差异、趋势一致性
- **PM盘口** (8): bid/ask/spread/edge
- **窗口** (2): elapsed, elapsed_pct
- **时间** (6): hour_utc, minute_utc, day_of_week, us_session, asia_session, euro_session
- **持仓/元数据** (若干): cum_shares, streak, burst 等 (蒸馏时排除)

### Binance 域名
- 默认使用 `data-stream.binance.vision` / `data-api.binance.vision`
- 无地区限制，腾讯云 VPS 可用
- 可通过环境变量 `BN_HOST` / `BN_API` 覆盖

## 蒸馏模型

### 旧版 (distill_0x1d.py) — 已弃用
- 按窗口训练 (76个样本)，样本太少无法学习
- 4个模型: 方向/质量/仓位/PnL

### 新版 (distill_signal.py) — 当前使用
- **思路**: 每笔交易 = 一个决策样本，学习交易方向
- **样本**: 13,403 笔交易 → 突发聚类 → ~2,600 决策点
- **标签**: side (UP=1, DOWN=0)
- **特征**: 仅纯市场状态 (58个，排除持仓/累计)
- **验证**: GroupKFold 按窗口分组
- **结果** (5小时完整数据):
  - CV Acc = 57.2%, AUC = 0.606
  - 阈值 0.60 → Acc 66.4%, 覆盖 25.6%
  - 阈值 0.65 → Acc 71.0%, 覆盖 10.1%
  - 模型正确学到趋势跟踪模式
- **关键发现**: 65.6% 的数据是残缺的 (2月20日BN/CL全0)，仅5小时完整数据

### 数据质量
- 完整数据仅占 34.4% (4614/13403 笔)
- 02-20 16:00-17:00: 仅有PM盘口，BN/CL全0 (PARTIAL)
- 02-22 10:00-14:00: 全部特征完整 (FULL)
- 使用 `--rich-only` 参数只用完整数据训练效果更好

## 0x1d 交易模式
- **每个窗口都双边交易** (81/81 窗口)
- **趋势跟踪**: BTC涨→买UP, BTC跌→买DOWN
- 69.7% 窗口胜率, +$2,848 总 PnL (76个窗口)
- DOWN 方向更赚 ($59/窗口) vs UP ($19/窗口)

## 部署
- **开发机**: Windows, D:\porject-pm
- **VPS**: 腾讯云 Ubuntu (root@VM-0-7-ubuntu)
- **推荐**: AWS us-east-1 (离 Polymarket 最近)
- **Git**: https://github.com/hxx344/deepmarket.git

## 下一步
1. **持续采集**: monitor_0x1d.py 在 VPS 24/7 运行，积累 7天+ 完整数据
2. **重新训练**: 数据充足后 `python scripts/distill_signal.py --rich-only`
3. **模型集成**: 将信号模型集成到 btc5min_taker 策略，替代手工规则
4. **黑盒交易**: 最终目标是模型直接输出 (方向, 仓位, 置信度)

---
*最后更新: 2026-02-23*
