# PRD 未实现功能清单

> 更新日期: 2026-02-15
> PRD 整体完成度: ~45%

---

## 🔴 P0 — Dashboard 前端面板 (0%) — **当前优先**

- [ ] 总览页 — 账户净值、当日 PnL、活跃策略数、风控状态
- [ ] 行情中心 — BTC K线 + PM 市场价格 + 指标面板 (多图表联动)
- [ ] 订单簿页 — 实时深度图、成交流、大单追踪
- [ ] 策略管理 — 策略列表、运行状态、实时 PnL、启停控制
- [ ] 回测中心 — 回测任务管理、报告查看、参数优化
- [ ] 交易记录 — 历史订单、成交明细、执行质量
- [ ] 风控面板 — 风控状态、告警历史、规则配置
- [ ] 系统设置 — 数据源配置、策略参数、风控阈值

---

## 🟠 P1 — 执行算法 (0%)

- [ ] TWAP — 时间加权分拆下单
- [ ] Iceberg — 冰山单 (隐藏大单)
- [ ] Smart Router — 根据深度智能选择
- [ ] Sniper — 等待特定价位快速成交
- [ ] `trading/algorithms/` 目录当前为空壳

---

## 🟠 P1 — 缺失策略 (3/7 缺失 + 竞技系统)

- [ ] Sentiment 策略 — 情绪驱动 (依赖舆情数据源)
- [ ] Arbitrage 策略 — YES+NO 价差套利
- [ ] ML Ensemble 策略 — 多特征融合 ML 预测
- [ ] Strategy Arena (arena.py) — 多策略 Paper Trading 竞赛/排行榜
- [ ] Hybrid 策略融合投票逻辑完善 (标记 TODO)

---

## 🟡 P2 — 缺失数据源 (4 个)

- [ ] 社交媒体/舆情 — Twitter/X 情绪采集 (5s 轮询)
- [ ] Fear & Greed Index — 恐贪指数
- [ ] Funding Rate — 各交易所资金费率
- [ ] 宏观数据 — TradingView/FRED (利率、CPI 等)

---

## 🟡 P2 — 缺失指标 (6 个情绪/链上)

- [ ] `fear_greed_index` — 恐贪指数
- [ ] `social_sentiment` — 社交媒体情绪评分
- [ ] `whale_movement` — 大户异动
- [ ] `open_interest_delta` — 未平仓合约变化
- [ ] `funding_rate_avg` — 综合资金费率
- [ ] `trade_flow_toxicity` (VPIN) — 交易流毒性

---

## 🟡 P2 — 回测引擎增强 (3 项)

- [ ] Order Book Replay 模式 — 基于订单簿快照回放的最高精度回测
- [ ] 贝叶斯优化 (Optuna) — 依赖已列但代码未实现
- [ ] 过拟合检测 — White's Reality Check

---

## 🟡 P2 — 订单簿引擎补全

- [ ] `orderbook/flow.py` — 独立订单流分析模块 (CVD/OFI/Sweep Detection)

---

## 🟡 P2 — 风控补全

- [ ] `risk/circuit_breaker.py` — 熔断器独立模块 (当前嵌入 engine.py)
- [ ] `risk/rules/` — 独立规则文件 (当前为空壳)
- [ ] Email 告警渠道
- [ ] 短信告警渠道

---

## 🟡 P2 — 交易引擎补全

- [ ] `trading/polymarket_api.py` — PM API 独立封装
- [ ] Gas 优化
- [ ] Nonce 管理

---

## 🔵 P3 — 测试 (0%)

- [ ] `tests/unit/` — 单元测试
- [ ] `tests/integration/` — 集成测试
- [ ] `tests/e2e/` — 端到端测试

---

## 🔵 P3 — 部署/运维 (0%)

- [ ] Docker 配置 (Dockerfile + docker-compose.yml)
- [ ] 运维脚本 (`scripts/`)
- [ ] 系统监控 (Prometheus + Grafana)
- [ ] 自动恢复机制

---

## 🔵 P3 — 市场情报模块 (0%)

- [ ] 市场日历 — 重要经济事件、BTC 期权到期日
- [ ] 波动率预测 — 历史 + 隐含波动率
- [ ] 智能资金追踪 — PM 大户下注行为
- [ ] 市场异常检测

---

## 🔵 P3 — 其他

- [ ] `utils/crypto.py` — 加密工具模块
- [ ] 消息队列 (Redis Streams / NATS) — 当前用内存 EventBus
