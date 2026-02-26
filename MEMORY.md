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
- **持仓/行为** (17): net_shares, cum_shares, pos_imbalance, streak, burst, velocity, price_vs_fair 等
- **Burst聚合** (4): burst_n_fills, burst_total_shares, burst_total_usdc, burst_avg_price

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
- **特征**: 78个，分为 12 组:
  - 市场状态 (58): BN动量/波动率/高级 + CL动量/波动率/高级 + 跨源 + PM盘口 + 窗口 + 时间
  - 持仓行为 (16): net_shares, cum_trades/shares/cost, avg_price, pos_imbalance, streak, velocity, burst_seq/is_burst, price_vs_fair
  - Burst聚合 (4): burst_n_fills, burst_total_shares, burst_total_usdc, burst_avg_price
- **Burst 去重逻辑**:
  - 市场特征取首笔 (决策瞬间的市场快照)
  - 持仓特征取末笔 (扫单完成后的持仓状态)
  - 新增 burst_n_fills/total_shares/total_usdc/avg_price (反映扫单力度)
  - 核心: 连续同向多笔可能是一个信号扫多个档口，不是多次信号
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

### 纯市场特征子集 (PURE_MARKET_NO_TIME)
- 58 个特征，去除时间特征 (hour/minute/dow/session) 避免过拟合
- 这是 Model Zoo 默认使用的特征集

## Model Zoo 自优化系统 (2026-02-26)

### 架构
| 文件 | 说明 |
|------|------|
| `scripts/model_zoo.py` | 多模型竞赛 + 自动选优 (961行) |
| `src/strategy/signals/distill_signal.py` | ML 信号提供者 (策略集成) |
| `data/distill_models/archive/` | 模型版本归档目录 |

### 支持模型
| 模型 | 类名 | 依赖 | 状态 |
|------|------|------|------|
| LightGBM | `LGBModel` | lightgbm | ✅ 可用 |
| Logistic Regression | `LRModel` | sklearn | ✅ 可用 |
| MLP (PyTorch) | `MLPModel` | torch | ⬜ 需安装 |
| TabNet | `TabNetModel` | pytorch-tabnet | ⬜ 需安装 |

### 模型选择逻辑
1. 各模型 GroupKFold CV (5折，按窗口分组)
2. 对每个模型做 PnL 回测 (模拟真实交易)
3. **主排序**: PnL ROI (投资回报率)
4. **次排序**: CV AUC
5. 与当前已部署模型比较 ROI，新模型更优才替换
6. 旧模型自动归档到 `archive/` 目录

### 最新训练结果 (2026-02-26)
- **数据**: 1,617 决策点 (rich_only + burst去重)，60 个窗口
- **LGB**: CV Acc=0.639, AUC=0.713, **ROI=+45.48%**, 胜率=63.4%
- **LR**: CV Acc=0.677, AUC=0.749, ROI=+27.22%, 胜率=60.0%
- **胜出**: LGB (ROI 最高)，temporal AUC=0.662
- 注意: LR 的 AUC 更高但 ROI 更低，说明概率校准不等于收益最优

### CLI 用法
```bash
# 多模型竞赛
python scripts/model_zoo.py --models lgb,lr --force
# 自动定时重训 (VPS cron)
python scripts/model_zoo.py --retrain
# 试运行不保存
python scripts/model_zoo.py --dry-run
```

### DistillSignal 信号提供者
- 继承 `SignalProvider` → 输出 `SignalResult`
- 支持模型热加载 (文件变化自动重载)
- 输出: score∈[-1,1], confidence, suggested_sizing∈[0,2]
- 集成方式: 在策略中实例化后调用 `calculate(ctx)`

## Raw Tick 存储 (2026-02-26)
- monitor_0x1d.py 新增 `raw_ticks` 表
- 存储 Binance + Chainlink RTDS 原始 tick 数据
- 字段: ts, source, price, slug, extra
- 缓冲写入 (每200条flush)，减少 I/O
- 为后续序列模型 (LSTM/Transformer) 积累原始数据

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
1. **持续采集**: monitor_0x1d.py 在 VPS 24/7 运行，积累原始 tick + 交易快照
2. **定时重训**: VPS cron 运行 `python scripts/model_zoo.py --retrain`，数据增长后自动更新模型
3. **安装 PyTorch**: `pip install torch pytorch-tabnet` 启用 MLP/TabNet 模型参与竞赛
4. **策略集成**: 将 DistillSignal 接入 btc5min_taker 策略，替代手工规则
5. **序列模型**: 当 raw_ticks 积累到 5000+ 窗口后，尝试 LSTM/Transformer 方案
6. **黑盒交易**: 最终目标是模型直接输出 (方向, 仓位, 置信度)

---
*最后更新: 2026-02-26*
