# NLP-Driven Reinforcement Learning Trading Platform

本项目是一个面向金融科技课程与论文写作的端到端实验平台，用于研究：

```text
financial news + OHLCV market data
-> peer-trained NLP sentiment / market-impact signals
-> DQN trading state
-> model-upgrade DQN trading decisions
-> diagnostics, dashboard, and report-ready artifacts
```

核心研究问题是：

```text
在严格排除目标股票自身新闻训练的前提下，
由 peer stocks 训练得到的 NLP signals
是否能增强目标股票 DQN 交易模型的表现？
```

当前 dashboard 和报告主结果使用 **latest model-upgrade experiment**，即：

```text
outputs/model_upgrade/*.csv
```

旧的 official DQN 输出仍可用于追溯，但不再作为主 dashboard 曲线、主对比表或论文结果解释的数据源。

---

## 1. Research Design

以目标股票 `A` 为例：

```text
target stock A = 002475 立讯精密
sector = electronics
peer stocks = same-sector stocks excluding target A
```

目标股票永远不进入自己的 NLP 训练语料：

```text
target stock news is excluded from NLP training
target stock future returns are excluded from NLP labelling
```

实验比较组：

| Role | Experiment | Meaning |
|---|---|---|
| Benchmark | buy_and_hold | 只作为 benchmark，不计入 DQN 组 |
| Control | dqn_without_nlp | market features + zero NLP signal |
| NLP group | dqn_with_sector_sentiment_nlp | sector peer sentiment signal |
| NLP group | dqn_with_sector_impact_nlp | sector peer market-impact signal |
| Add-on group | dqn_with_marketwide_sentiment_nlp | marketwide peer sentiment signal |
| Add-on group | dqn_with_marketwide_impact_nlp | marketwide peer market-impact signal |

论文解释时应注意：

```text
trade activity alone does not prove NLP value
NLP improvement should be claimed only when no-NLP baseline comparisons support it
medium-test is diagnostic; full-grid is stronger evidence
```

---

## 2. End-To-End Pipeline

完整流程：

```text
1. User inputs target symbol and company name
2. Resolve target sector from stock-sector mapping
3. Check local stock cache
4. Fetch missing market/news data through existing scraper
5. Save integrated finance_text CSV under outputs/stocks/<symbol>/data/
6. Identify same-sector peers and optional marketwide peers
7. Exclude target stock from all peer NLP training corpora
8. Build peer sentiment corpus
9. Build peer market-impact labelled corpus
10. Score held-out target stock news
11. Aggregate daily NLP signals
12. Merge market features + NLP features
13. Run model-upgrade DQN groups
14. Record portfolio curves, action counts, exposure, env dynamics, and Q-values
15. Generate dashboard visualizations
16. Generate diagnostic README and compact analysis summary
```

Dashboard 中会显示每个阶段的状态：

```text
pending -> running -> completed / skipped / failed
```

默认研究区间：

```text
DATA_START_DATE = 2024-01-01
DATA_END_DATE   = 2026-04-30
```

---

## 3. Stock Crawling And Cache Logic

本项目使用已经集成的 A 股 scraper 逻辑。入口由 dashboard 或 CLI 调用，不需要手工直接操作底层 scraper。

主缓存目录：

```text
outputs/stocks/<symbol>/data/
```

每只股票维护一个 master timeline：

```text
outputs/stocks/<symbol>/data/<symbol>_finance_text_master.csv
```

请求某只股票、某个日期区间时，系统先检查本地缓存：

```text
if master covers requested date range:
    reuse local master
    slice requested interval
else:
    fetch missing interval
    merge into master
    deduplicate
    materialize requested interval
```

这意味着：

```text
local cache first
network scraping only when local data is missing
same stock data is not repeatedly duplicated
```

旧目录：

```text
data/integrated/
```

只作为 legacy fallback。当前正式数据判断应以：

```text
outputs/stocks/<symbol>/data/
```

为准。

---

## 4. Data Storage Layer

本项目有两层存储。

第一层是主要审计产物：

```text
CSV / Markdown / HTML files under outputs/
```

这些文件是 dashboard、论文表格、结果分析和复现实验的主要依据。

第二层是可选 SQLite 同步存储：

```text
outputs/database/trading_platform.db
```

SQLite schema 位于：

```text
src/storage/schema.sql
```

当前核心表：

```text
news_table
market_table
sentiment_table
nlp_signal_table
experiment_metrics_table
trading_log_table
```

兼容 views：

```text
news_articles
market_bars
sentiment_signals
nlp_signals
trade_logs
```

SQLite 用途：

```text
persist core news / market / NLP signal / metric / trade-log data
```

重要限制：

```text
latest model-upgrade diagnostics are stored primarily as CSV / Markdown / HTML artifacts
```

也就是说，当前项目具备核心 SQLite 架构，但最新 model-upgrade 的完整诊断体系仍以文件产物为主。

---

## 5. Peer NLP Signal Generation

### 5.1 Target Exclusion

目标股票 `A` 的新闻不会进入自己的 peer NLP 训练：

```text
target A news not used for training
target A future returns not used for market-impact labels
```

Peer stocks 来自：

```text
same-sector peers excluding A
optional marketwide peers excluding A
```

如果本地 peer 数据不足，dashboard 可触发补抓：

```text
allow_fetch_missing_sector_peers = true
```

### 5.2 Sentiment Signal

对 peer 新闻文本：

```text
x = title + content
```

Peer sentiment scorer 输出：

```text
P(pos | x), P(neu | x), P(neg | x)
```

sentiment score：

```text
sentiment_score(x) = P(pos | x) - P(neg | x)
```

目标股票日度 sector sentiment：

```text
S_sector(t) =
    0, if target has no mapped news on day t
    mean(sentiment_score_sector(x)) for target news mapped to day t
```

Marketwide sentiment 同理，只是 peer corpus 变为跨板块 marketwide peers。

### 5.3 Market-Impact Signal

Market-impact NLP 不直接问文本语气，而是用 peer 股票未来收益构造 pseudo label。

对 peer stock `j` 的新闻 `x_{j,i}`，映射到交易日 `tau(i)` 后：

```text
R_{j,tau(i),h} = (C_{j,tau(i)+h} - C_{j,tau(i)}) / C_{j,tau(i)}
```

默认：

```text
h = 3
positive_threshold = 0.015
negative_threshold = -0.015
```

标签：

```text
bullish_impact, if R > 0.015
bearish_impact, if R < -0.015
neutral_impact, otherwise
```

impact score：

```text
impact_score(x) = P(bullish | x) - P(bearish | x)
```

目标股票的未来收益不参与目标股票自己的 NLP label 或 scorer 训练。

---

## 6. DQN Experiment: Latest Model-Upgrade Version

旧 official DQN 使用 `discrete_trade` 三动作环境：

```text
0 = Hold
1 = Buy
2 = Sell
```

它在实验中出现过明显问题：

```text
too many Hold actions
flat or nearly flat portfolio curves
low exposure
hard to interpret whether NLP changed decisions
```

因此当前主实验使用 model-upgrade 版本。

### 6.1 State Feature Mode

旧 official baseline 保留 8D raw state 用于追溯：

```text
official_8d = [
    price,
    MA50,
    MA200,
    RSI,
    MACD,
    position,
    cash,
    nlp_signal_score
]
```

当前主实验推荐：

```text
state_feature_mode = normalized_plus
```

特征：

```text
daily_return
price_ma50_gap
price_ma200_gap
ma50_ma200_gap
RSI_normalized
MACD_zscore
position_ratio
cash_ratio
nlp_signal_score
abs_nlp_signal
nlp_signal_3day_mean
nlp_signal_change
```

归一化统计量只从训练窗口计算，再应用到测试窗口，避免 test leakage。

### 6.2 Action Mode

当前 model-upgrade 默认：

```text
action_mode = target_position
```

动作含义：

```text
action0 = 0% exposure
action1 = 25% exposure
action2 = 50% exposure
action3 = 75% exposure
action4 = 100% exposure
```

执行逻辑：

```text
portfolio_before = cash_before + shares_before * price_t
current_stock_value = shares_before * price_t
target_stock_value = target_exposure * portfolio_before
trade_value = target_stock_value - current_stock_value
```

如果 `trade_value > 0`，买入到目标仓位；如果 `trade_value < 0`，卖出到目标仓位。系统不允许负现金或负持仓。

Dashboard 仍把动作映射成：

```text
target_exposure > current_exposure_before + 0.01 -> Buy
target_exposure < current_exposure_before - 0.01 -> Sell
otherwise -> Hold
```

### 6.3 DQN Variants

支持：

```text
vanilla_dqn
double_dqn
dueling_dqn
double_dueling_dqn
```

Double DQN target：

```text
a_star = argmax_a Q_online(s_{t+1}, a)
y_t = r_t + gamma * Q_target(s_{t+1}, a_star)
```

Dueling DQN：

```text
Q(s,a) = V(s) + A(s,a) - mean_a A(s,a)
```

默认训练设置：

```text
loss_type = huber
grad_clip_norm = 1.0
epsilon_start = 1.0
epsilon_end = 0.10
epsilon_decay_ratio = 0.85
```

### 6.4 Reward Variants

支持：

```text
one_day_return
three_day_return
five_day_return
risk_adjusted_return
```

默认 next-day portfolio return：

```text
reward_t = (V_{t+1} - V_t) / V_t
```

训练期可启用 flat-position penalty，避免模型长期空仓；早期训练可启用 trade exploration bonus，促进 Buy/Sell 探索。

---

## 7. Evaluation Metrics

每个 DQN group 会记录：

```text
final_equity
cumulative_return
annualized_return
annualized_volatility
sharpe_ratio
max_drawdown
number_of_trades
win_rate
exposure_ratio
average_exposure
turnover
unique_portfolio_values
hold_ratio
action_buy_count
action_sell_count
action_hold_count
q_value_margin
```

累计收益：

```text
cumulative_return = V_T / V_0 - 1
```

最大回撤：

```text
Drawdown_t = V_t / max_{k<=t}(V_k) - 1
MaxDrawdown = min_t Drawdown_t
```

NLP 组是否优于 no-NLP control，应优先看同股票、同 run mode、同 model variant、同 reward、同 state/action mode 下的对比。

---

## 8. Main Output Files

### 8.1 Per-Stock Official / Signal Outputs

每只股票：

```text
outputs/stocks/<symbol>/
    data/
    reports/
    results/
    models/
```

关键 signal 文件：

```text
outputs/stocks/<symbol>/results/peer_nlp_daily_sentiment.csv
outputs/stocks/<symbol>/results/peer_market_impact_daily_signal.csv
```

这些文件是 model-upgrade grid 的输入之一。

### 8.2 Latest Model-Upgrade Outputs

主实验结果：

```text
outputs/model_upgrade/model_upgrade_summary.csv
outputs/model_upgrade/seed_level_metrics.csv
outputs/model_upgrade/env_dynamics_diagnostics.csv
outputs/model_upgrade/action_distribution_diagnostics.csv
outputs/model_upgrade/q_value_diagnostics.csv
outputs/model_upgrade/exploration_diagnostics.csv
outputs/model_upgrade/portfolio_curves.csv
outputs/model_upgrade/reward_variant_comparison.csv
outputs/model_upgrade/state_feature_diagnostics.csv
outputs/model_upgrade/stock_complete_performance.csv
outputs/model_upgrade/STOCK_COMPLETE_PERFORMANCE.md
outputs/model_upgrade/README_diagnostics.md
outputs/model_upgrade/CHATGPT_DIAGNOSTIC_SUMMARY.md
outputs/model_upgrade/UPGRADE_REASON_README.md
```

当前报告写作建议优先使用：

```text
outputs/model_upgrade/STOCK_COMPLETE_PERFORMANCE.md
outputs/model_upgrade/stock_complete_performance.csv
outputs/model_upgrade/README_diagnostics.md
outputs/model_upgrade/CHATGPT_DIAGNOSTIC_SUMMARY.md
```

`CHATGPT_DIAGNOSTIC_SUMMARY.md` 是轻量分析摘要；它不改变实验流程，也不是核心训练管线的一部分。

---

## 9. Installation

```bash
cd /Users/luxinyu/Desktop/Fintech/fintechgp
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Jupyter kernel：

```bash
.venv/bin/python -m ipykernel install --user --name fintechgp --display-name "Python (.venv fintechgp)"
```

---

## 10. Run Dashboard

```bash
python3 -m streamlit run src/dashboard/streamlit_app.py \
  --server.port 8501 \
  --server.address 127.0.0.1 \
  --server.headless true
```

Dashboard tabs：

```text
Single Stock Basics
Scraping Density
Peer Cross Analysis
Market Impact Analysis
Model Upgrade Diagnostics
```

当前 dashboard 的结果 tab 使用 latest model-upgrade outputs：

```text
Peer Cross Analysis:
    Latest Model-Upgrade DQN Groups
    portfolio curves
    Buy / Sell / Hold counts
    seed-level metrics

Market Impact Analysis:
    Latest Model-Upgrade Sentiment / Impact Comparison
    Q-value diagnostics
    state feature diagnostics
    reward variant comparison

Model Upgrade Diagnostics:
    target_symbol selector
    trade-by-trade behavior
    portfolio + actions
    exposure + actions
    Q-value decision view
    detailed trade log
    diagnostic README and compact summary preview
```

Dashboard 对 `outputs/model_upgrade/*.csv` 严格按当前 `target_symbol` 过滤。如果当前股票没有 model-upgrade 结果，不会回退展示其他股票数据。

---

## 11. Run Model-Upgrade Grid

Quick test，只验证代码路径：

```bash
python scripts/run_model_upgrade_grid.py --quick-test
```

Medium test，用于交互式 dashboard 后的行为诊断：

```bash
python scripts/run_model_upgrade_grid.py --medium-test
```

指定单只股票：

```bash
python scripts/run_model_upgrade_grid.py \
  --target-symbol 002475 \
  --target-company 立讯精密 \
  --medium-test
```

批量股票：

```bash
python scripts/run_model_upgrade_grid.py \
  --batch-file outputs/model_upgrade/consumer_electronics_batch.csv \
  --medium-test
```

批量 CSV 格式：

```csv
target_symbol,target_company
002475,立讯精密
601138,工业富联
```

生成诊断 README：

```bash
python scripts/generate_model_upgrade_diagnostic_readme.py
```

---

## 12. Run Legacy CLI

旧 CLI 仍可用于复现实验流程和生成 official/signal artifacts：

```bash
.venv/bin/python main.py \
  --symbol 002475 \
  --company-name 立讯精密 \
  --start-date 2024-01-01 \
  --end-date 2026-04-30 \
  --run-ingestion \
  --run-nlp \
  --run-rl \
  --run-ablation \
  --run-market-impact-nlp \
  --allow-fetch-missing-sector-peers \
  --use-sqlite \
  --episodes 200
```

说明：

```text
legacy official outputs are useful for tracing the pipeline
latest dashboard/report conclusions should use model-upgrade outputs
```

---

## 13. Project Structure

```text
config/
docs/
notebooks/
program/
outputs/
reports/
scripts/
src/data_ingestion/
src/storage/
src/nlp/
src/features/
src/rl/
src/evaluation/
src/reporting/
src/dashboard/
tests/
```

关键模块：

| Path | Responsibility |
|---|---|
| `src/data_ingestion/ingestion.py` | cache check, scraper orchestration, integrated finance_text CSV |
| `src/storage/schema.sql` | optional SQLite schema |
| `src/storage/database.py` | SQLite initialization and persistence helpers |
| `src/nlp/peer_sentiment.py` | peer sentiment corpus and target scoring |
| `src/nlp/market_impact.py` | future-return-labelled market-impact NLP |
| `src/evaluation/model_upgrade.py` | latest model-upgrade grid |
| `src/evaluation/model_upgrade_diagnostics.py` | diagnostic discovery and report generation |
| `src/rl/trading_env.py` | chronological trading environment |
| `src/rl/dqn_agent.py` | PyTorch DQN variants |
| `src/dashboard/streamlit_app.py` | dashboard |
| `scripts/run_model_upgrade_grid.py` | single-stock / batch model-upgrade runs |
---

## 14. Validation Commands

```bash
python -m compileall src scripts tests
pytest -q
python scripts/generate_model_upgrade_diagnostic_readme.py
```

Dashboard launch:

```bash
python3 -m streamlit run src/dashboard/streamlit_app.py \
  --server.port 8501 \
  --server.address 127.0.0.1 \
  --server.headless true
```

---

## 15. Reliability Checks

系统会检查：

```text
target stock exclusion
same-sector peer count
marketwide peer diversity
training news count
market-impact labelled news count
high-density window coverage
lagged state features
same-day leakage
DQN trade count
flat portfolio curves
hold-heavy policy
target-position exposure behavior
Q-value margin
seed-level stability
```

常见状态：

```text
READY_FOR_PRESENTATION
READY_WITH_WARNINGS
NOT_READY
```

---

## 16. Known Limitations

1. A 股新闻接口远端历史新闻密度低，因此当前实验重点使用近端高密度窗口。
2. Market-impact label 是 pseudo label，不是真实人工标注。
3. `medium-test` 是行为诊断，不是最终论文结论。
4. `full-grid` with more seeds 更适合正式报告结论。
5. `reward_diagnostics.csv` 和 `nlp_incremental_effect.csv` 可能缺失；缺失时不能强行声称 reward attribution 或 NLP incremental effect 已充分证明。
6. 旧 official 曲线和最新 model-upgrade 曲线不能直接逐点比较，因为 action space、state features、reward variant、seeds 和 portfolio timing 已经不同。
7. SQLite 当前覆盖核心数据持久化；最新 model-upgrade 全量诊断仍以 CSV/Markdown/HTML 为主。

---

## 17. Report-Writing Guidance

写论文结果部分时，优先参考：

```text
outputs/model_upgrade/STOCK_COMPLETE_PERFORMANCE.md
outputs/model_upgrade/README_diagnostics.md
outputs/model_upgrade/CHATGPT_DIAGNOSTIC_SUMMARY.md
outputs/model_upgrade/stock_complete_performance.csv
```

安全结论：

```text
The platform evaluates whether peer-trained NLP signals improve DQN trading under target-stock exclusion.
The latest model-upgrade experiment improves interpretability through target-position actions,
normalized state features, environment dynamics, and Q-value diagnostics.
NLP improvement should be discussed as evidence-based and conditional, not assumed.
```
