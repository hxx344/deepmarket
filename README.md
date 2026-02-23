# Polymarket BTC 5-min Prediction Betting System

> 基于 Polymarket 预测市场的 BTC 5 分钟行情预测与自动下注系统。

## 📋 概览

本系统通过实时监控 BTC 5 分钟 K 线行情，结合技术指标分析和订单流分析，在 Polymarket 预测市场上进行自动/半自动下注。

### 核心模块

| 模块 | 说明 |
|------|------|
| **行情引擎** | PM RTDS Chainlink BTC/USD 免费实时价格，Polymarket 市场数据 |
| **订单簿引擎** | 买卖盘维护、流动性分析、大单追踪、Spoofing 检测 |
| **回测引擎** | 事件驱动回测、滑点模拟、网格/随机/Walk-Forward 优化器 |
| **交易引擎** | 订单生命周期管理、Polygon 链上签名、纸交易 |
| **风控引擎** | 四层风控（订单→仓位→账户→系统）、熔断器、多通道告警 |
| **策略框架** | 插件化策略、内置动量/均值回归/订单流/混合投票模板 |

### 技术栈

- **语言**: Python 3.9+
- **异步**: asyncio + aiohttp + websockets
- **存储**: SQLite（业务数据）+ Parquet（时序数据）+ DuckDB（查询）+ diskcache（缓存）
- **区块链**: web3 + eth-account (Polygon)
- **可视化**: Plotly

> ⚡ **零外部依赖存储方案** — 无需安装 PostgreSQL、Redis 等数据库软件。

---

## 🚀 快速开始

### 1. 环境准备

```bash
# 克隆项目
git clone <repo-url>
cd porject-pm

# 创建虚拟环境
python -m venv .venv

# 激活 (Windows)
.venv\Scripts\activate

# 激活 (Linux/macOS)
source .venv/bin/activate

# 安装依赖
pip install -r requirements.txt
```

### 2. 配置

编辑 `config/settings.yaml`，主要需要配置：

```yaml
# 数据源
datasources:
  chainlink:
    mode: "auto"                   # auto / rtds / streams / binance / onchain
    # auto 模式优先级: PM RTDS(免费) → Data Streams → Binance → 链上
    # 默认 auto 即可, 无需任何 API key!
    # 可选: Chainlink Data Streams API (PM 赞助通道: https://pm-ds-request.streams.chain.link/)
    # client_id: "your-client-id"     # 或环境变量 CHAINLINK_CLIENT_ID
    # client_secret: "your-secret"    # 或环境变量 CHAINLINK_CLIENT_SECRET
  polymarket:
    market_ids: ["你的市场ID"]

# 交易模式 (先用 paper 测试)
trading:
  mode: "paper"
  paper_account:
    initial_balance: 100.0
```

如需实盘交易，还需配置钱包：

```bash
# 设置环境变量 (推荐)
set POLYMARKET_PRIVATE_KEY=你的私钥

# 或使用加密密钥文件
# 参见 config/settings.yaml 中 wallet 部分
```

### 3. 运行

```bash
# 纸交易模式 (推荐先用此模式测试)
python -m src.main paper

# 实盘模式
python -m src.main live

# 回测模式
python -m src.main backtest

# 指定策略运行
python -m src.main paper --strategy momentum_5min

# 指定市场
python -m src.main paper --market <market-id>

# 详细日志
python -m src.main paper -v
```

### 4. 0x1d 实时监控面板

实时监控 BTC 行情、Polymarket 报价、0x1d 账号下单及持仓盈亏情况的 Web 可视化面板。

```bash
# 启动监控 (默认端口 8888)
python monitor_0x1d.py

# 指定端口
python monitor_0x1d.py --port 9999
```

启动后打开浏览器访问 `http://localhost:8888` 即可看到面板。

**面板功能：**

| 模块 | 说明 |
|------|------|
| **BTC 行情** | Binance WebSocket 实时 BTC/USDT 价格 + 5s 动量 |
| **PM 报价** | UP/DOWN 中间价、Bid/Ask、Edge (1 - UP - DN) |
| **窗口状态** | 当前 5-min 窗口名、基准价 (PTB)、BTC 方向、进度条 |
| **0x1d 交易** | 实时订单流、累计 UP/DN Shares、Gap 偏差 |
| **Burst 检测** | 识别集中下单行为 (≥3 笔/轮询) |
| **持仓盈亏** | MTM 估值、预期 PnL、若 UP/DN 赢的 PnL、平均成本 |
| **历史结算** | 累计 PnL、胜率、每窗口结算明细 |

---

## 📁 项目结构

```
porject-pm/
├── config/                     # 配置文件
│   ├── settings.yaml           # 全局配置
│   ├── indicators.yaml         # 指标配置
│   ├── risk_rules.yaml         # 风控规则
│   └── strategies/             # 策略配置
│       ├── momentum.yaml
│       ├── mean_reversion.yaml
│       ├── orderflow.yaml
│       └── hybrid.yaml
├── src/                        # 源码
│   ├── main.py                 # 主入口
│   ├── core/                   # 核心框架
│   │   ├── event_bus.py        # 事件总线 (pub/sub)
│   │   ├── context.py          # 运行上下文
│   │   ├── plugin.py           # 插件管理器
│   │   └── storage.py          # 存储管理 (SQLite/Parquet/Cache)
│   ├── market/                 # 行情引擎
│   │   ├── datasources/        # 数据源
│   │   │   ├── chainlink_ds.py # Chainlink Data Streams BTC/USD
│   │   │   ├── binance_ds.py   # Binance WebSocket (备用)
│   │   │   └── polymarket_ds.py# Polymarket CLOB API
│   │   ├── indicators/         # 技术指标
│   │   │   ├── base.py         # 指标插件基类
│   │   │   └── technical.py    # 9 种内置指标
│   │   └── aggregator.py       # 行情聚合器
│   ├── orderbook/              # 订单簿引擎
│   │   ├── book.py             # 订单簿维护
│   │   ├── analyzer.py         # 盘口分析
│   │   └── snapshot.py         # 快照管理
│   ├── backtest/               # 回测引擎
│   │   ├── engine.py           # 回测核心
│   │   ├── report.py           # 报告生成 (Plotly HTML)
│   │   └── optimizer.py        # 参数优化器
│   ├── trading/                # 交易引擎
│   │   ├── executor.py         # 订单执行器
│   │   └── wallet.py           # 钱包管理 (Polygon)
│   ├── risk/                   # 风控引擎
│   │   ├── engine.py           # 风控核心 (16 条规则)
│   │   └── alerting.py         # 告警管理
│   ├── strategy/               # 策略框架
│   │   ├── base.py             # 策略基类
│   │   └── templates/
│   │       └── builtin.py      # 4 种内置策略模板
│   └── utils/                  # 工具
│       ├── logger.py           # 日志 (loguru)
│       └── time_utils.py       # 时间工具
├── data/                       # 数据存储 (自动创建)
│   ├── parquet/                # 时序数据
│   ├── sqlite/                 # 业务数据
│   └── cache/                  # 缓存
├── logs/                       # 日志文件 (自动创建)
├── reports/                    # 回测报告 (自动创建)
├── docs/
│   └── PRD.md                  # 产品需求文档
├── pyproject.toml              # 项目配置
├── requirements.txt            # 依赖列表
└── README.md                   # 本文件
```

---

## 🧩 内置策略

### 1. 动量策略 (MomentumStrategy)
- 基于价格动量 + RSI 过滤
- 适用场景：趋势明显的市场

### 2. 均值回归策略 (MeanReversionStrategy)
- 基于布林带
- 适用场景：震荡市场

### 3. 订单流策略 (OrderFlowStrategy)
- 基于 CVD + 盘口压力
- 适用场景：有深度订单簿数据时

### 4. 混合投票策略 (HybridStrategy)
- 加权投票，综合多策略信号
- 适用场景：提高信号稳定性

---

## 🛡️ 风控体系

四层风控保护：

| 层级 | 规则 | 动作 |
|------|------|------|
| 订单级 | 单笔金额、赔率范围、滑点 | 拒绝 |
| 仓位级 | 持仓上限、敞口占比、方向集中度 | 拒绝/警告 |
| 账户级 | 日亏损、余额最低、连续亏损 | 熔断 |
| 系统级 | API频率、数据新鲜度、错误率 | 拒绝/熔断 |

所有风控规则可在 `config/risk_rules.yaml` 中配置。

---

## 📊 回测

回测数据**直接从 Polymarket 拉取**（概率价格时间序列），无需手动准备数据文件。

### 步骤 1: 发现可用市场

```bash
# 不设置 token_id 运行，会自动搜索并列出 BTC 相关市场
python -m src.main backtest
```

输出示例：
```
找到以下 BTC 相关市场:
  [0] Will Bitcoin be above $100k on March 1?
      volume=$250,000  active=True  tokens=[Yes=abc123..., No=def456...]
  [1] Bitcoin price at end of February?
      volume=$180,000  active=False  tokens=[Yes=ghi789..., No=jkl012...]
```

### 步骤 2: 配置目标市场

将找到的 token_id 和 condition_id 写入 `config/settings.yaml`：

```yaml
backtest:
  token_id: "abc123..."          # YES token ID
  condition_id: "xyz789..."      # 市场 condition ID
  start_date: "2024-01-01"
  end_date: "2025-12-31"
  interval: "5m"
```

或通过命令行参数指定：

```bash
python -m src.main backtest --token <token_id> --market <condition_id>
```

### 步骤 3: 执行回测

```bash
# 回测 (自动从 Polymarket 拉取数据并缓存到本地)
python -m src.main backtest

# 指定策略
python -m src.main backtest --strategy momentum_5min
```

回测报告 (HTML) 自动保存到 `reports/` 目录，包含：
- 权益曲线
- 回撤图
- 概率价格走势叠加
- 交易 PnL 分布
- 绩效指标摘要

> 历史数据首次拉取后自动缓存为 Parquet 文件（`data/polymarket/`），后续运行只做增量更新。

---

## ⚙️ 自定义策略

继承 `Strategy` 基类即可创建自定义策略：

```python
from src.strategy.base import Strategy

class MyStrategy(Strategy):
    async def on_init(self, ctx):
        self.ctx = ctx

    async def on_market_data(self, data: dict):
        # data 包含: klines, indicators, price 等
        price = data.get("close")
        indicators = data.get("indicators", {})

        # 你的逻辑...
        if should_buy:
            executor = self.ctx.get("executor")
            await executor.submit_order(...)

    async def on_stop(self):
        pass
```

然后在 `config/strategies/` 下添加对应 YAML 配置文件。

---

## 🧠 策略蒸馏 (Strategy Distillation)

通过监控面板 (`monitor_0x1d.py`) 持续采集 0x1d 的交易快照和结算数据，用机器学习模型逆向学习其交易决策模式。提供两套互补的蒸馏程序：

### 前置条件

1. **数据采集**：确保 `monitor_0x1d.py` 已运行并积累足够数据（推荐 ≥7 天）
2. **数据来源**：SQLite 数据库 `data/0x1d_data.db`（表 `trade_snaps` + `settlements`）
3. **依赖安装**：

```bash
pip install lightgbm scikit-learn pandas numpy
```

### 程序 1: 窗口级蒸馏 (`distill_0x1d.py`)

从窗口维度学习 0x1d 的决策，训练 **4 个独立模型**：

| 模型 | 文件 | 说明 |
|------|------|------|
| 方向模型 | `direction_model.pkl` | 预测 0x1d 在窗口内主买 UP 还是 DOWN |
| 交易质量 | `trade_quality_model.pkl` | 每笔交易是否与最终赢面方向一致 |
| 仓位模型 | `sizing_model.pkl` | 预测每笔下单的 shares 数量 |
| 盈亏预测 | `pnl_model.pkl` | 预测窗口盈亏方向（赚/亏） |

```bash
# 完整训练 + 评估 (4 个模型)
python scripts/distill_0x1d.py

# 只查看数据质量报告 (不训练)
python scripts/distill_0x1d.py --report
```

**输出示例**：
```
模型 1: 方向预测 (Direction)
  样本: 120, 特征: 35
  交叉验证准确率: 0.683 ± 0.045
  Top-10 重要特征:
    first_bn_mom_5s                          142
    first_cl_trend_30s                       128
    ...
蒸馏完成
  方向模型     ✓                    12KB
  交易质量     ✓                    18KB
  仓位模型     ✓                    15KB
  盈亏预测     ✓                     9KB
```

### 程序 2: 实时信号蒸馏 (`distill_signal.py`)

从每笔交易维度学习 0x1d 的入场时机和方向，生成一个**实时信号生成器**：

- **输入**：当前市场状态（BTC 动量/波动率/趋势 + CL-BN 价差 + PM 盘口）
- **输出**：`UP` / `DOWN` / `HOLD` 信号 + 置信度

```bash
# 完整训练 + 评估 + 信号回测
python scripts/distill_signal.py

# 只看特征分析报告
python scripts/distill_signal.py --report

# 自定义信号阈值 (默认 0.60)
python scripts/distill_signal.py --threshold 0.65

# 仅用有 ref_ts 的高质量数据 (时间戳更精确)
python scripts/distill_signal.py --rich-only
```

**产出文件**：

| 文件 | 说明 |
|------|------|
| `data/distill_models/signal_model.pkl` | LightGBM 模型 |
| `data/distill_models/signal_config.json` | 特征列表 + 信号阈值 |

**训练流程**：
1. 加载交易数据 → 突发聚类（Burst Dedup: 1s 内同向交易合并为一个决策点）
2. 特征分析（Cohen's d 区分度排序）
3. GroupKFold CV 训练（按窗口分组，无时间泄漏）
4. 多阈值信号分析 → 自动选最优阈值
5. 信号回测 + 与真实结算对比

**推理代码示例**：
```python
import pickle, json, numpy as np

model = pickle.load(open('data/distill_models/signal_model.pkl', 'rb'))
cfg = json.load(open('data/distill_models/signal_config.json'))
threshold = cfg['threshold']

# 从实时数据源采集特征
features = collect_market_features()  # BN WebSocket + CL RTDS + PM API
x = np.array([[features[f] for f in cfg['features']]])

prob_up = model.predict_proba(x)[0, 1]
if prob_up > threshold:
    signal, conf = 'UP', prob_up
elif prob_up < (1 - threshold):
    signal, conf = 'DOWN', 1 - prob_up
else:
    signal, conf = 'HOLD', 0.5

print(f'Signal: {signal} (confidence: {conf:.1%})')
```

### 两套蒸馏的区别

| 维度 | `distill_0x1d.py` | `distill_signal.py` |
|------|--------------------|----------------------|
| 粒度 | 窗口级（每 5-min 窗口一个样本） | 交易级（每笔交易一个样本） |
| 样本量 | 较少（~百级） | 较多（~千级） |
| 用途 | 策略逆向分析、盈亏预测 | **实时信号生成**、替代手工规则 |
| 核心产出 | 4 个分析模型 | 1 个生产可用的信号模型 |

### 推荐工作流

```
1. 运行 monitor_0x1d.py 持续采集数据（≥7 天）
2. python scripts/distill_0x1d.py --report    # 查看数据质量
3. python scripts/distill_0x1d.py             # 训练窗口级模型
4. python scripts/distill_signal.py           # 训练实时信号模型
5. 数据增长后定期重新训练
6. 将 signal_model 集成到交易机器人
```

模型文件统一保存在 `data/distill_models/` 目录下。

---

## ⚠️ 免责声明

- 本系统仅供学习和研究用途
- 预测市场交易存在风险，可能导致资金损失
- 请在充分了解风险后，使用小额资金进行测试
- 开发者不对任何交易损失承担责任

---

## 📄 许可证

MIT License
