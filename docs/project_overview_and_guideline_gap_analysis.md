# NLP-RL Trading Platform 项目全景与 Guideline 缺口分析

生成日期：2026-05-01  
项目路径：`fintechgp`

## 1. 结论先讲

这个项目已经做出了一个比较完整的端到端原型：从 A 股股票代码出发，抓取/复用行情和新闻事件数据，生成日级 NLP 情绪信号与资金流 proxy，再把情绪信号接入 DQN 交易环境，最后输出消融实验、风险指标、图表、报告草稿和 Streamlit dashboard。

按 `Project Guidelines.docx` 的评分项看，代码层面已经覆盖了五大模块：Data Ingestion、NLP Pipeline、Data Storage、RL Trading Engine、Front-End Dashboard；也已经有 DQN with NLP vs DQN without NLP vs Buy-and-Hold 的 ablation。但最终交付还不稳的地方也很明确：FinBERT 当前多数运行是 `skipped`，NLP F1 主要是 pseudo-label 结果，不是人工标注真值；walk-forward 目前更像 split diagnostics，不是真正多窗口重训评估；25-50 页正式 report 和 25 分钟 presentation/live demo 还没有整理成最终交付物；GitHub repo 当前本地目录不是 git repo。

## 2. 我检索到的项目文件范围

本次检索覆盖了项目根目录下的源代码、配置、文档、notebook、测试、脚本、outputs 和 reports。`.venv`、`__pycache__`、`.pytest_cache`、`.DS_Store` 属于环境/缓存文件，不作为项目功能交付物分析。

项目中 `.venv` 之外约有 1055 个文件，其中 `src/` 下有 49 个 Python 源文件。主要目录如下：

- `README.md`：项目使用说明、运行方式、输出结构、已知限制。
- `Project Guidelines.docx` 与 `docs/Project_Guidelines.docx`：课程项目要求，两份内容一致。
- `config/`：默认运行参数和股票代码-公司名称映射。
- `program/`：底层爬虫和低层数据采集 runner。
- `src/`：平台主体代码，包括 ingestion、NLP、features、RL、evaluation、storage、dashboard、reporting。
- `notebooks/`：`full_report_pipeline.ipynb`，用于 notebook/report 工作流。
- `tests/`：单元测试，覆盖 metrics、replay buffer、walk-forward split。
- `outputs/`：按股票保存数据、模型、结果、图表、报告草稿和系统级 cross-stock/audit 输出。
- `reports/`：早期/根目录报告输出，和 `outputs/stocks/<symbol>/reports/` 有重复性质。
- `docs/statement/`：数据源说明、爬虫说明。
- `scripts/`：整理和清理冗余输出的辅助脚本。

## 3. 项目现在到底做了什么

### 3.1 总体工作流

项目的主线是：

1. 用户输入股票代码、公司名称、日期范围、news cap、DQN episodes 等参数。
2. 系统优先检查本地 master CSV 是否覆盖请求区间。
3. 如果本地数据不足，并且允许抓取，就调用 `program/run_scraper.py` 和 `program/finance_text_scraper.py` 抓取行情和文本事件。
4. 得到日级 integrated CSV 后，NLP 模块对新闻标题/正文进行预处理、情绪打分和日级聚合。
5. feature 模块生成 MA50、MA200、RSI、MACD、position、cash、sentiment_score 等 RL state。
6. RL 模块用 DQN 训练并评估交易策略。
7. evaluation 模块比较 buy-and-hold、DQN without NLP、DQN with NLP，并输出 Sharpe、MDD、return、trade logs、portfolio curves。
8. reporting 和 dashboard 模块把结果变成图表、CSV、Markdown 草稿和交互式界面。

代码入口主要有两个：

- `main.py`：命令行完整 pipeline 入口。
- `src/dashboard/streamlit_app.py`：Streamlit 交互式 dashboard 入口。

## 4. 模块级说明：现在怎么做的

### 4.1 配置与路径

相关文件：

- `config/default.json`
- `config/stock_aliases.json`
- `src/config/paths.py`
- `src/config/settings.py`

`config/default.json` 保存默认 symbol、company name、日期、source、news count、episodes、initial cash、transaction cost 和 SQLite 开关。`config/stock_aliases.json` 维护股票代码和公司名称映射，近期已经修正过 `301607 -> 富特科技`、`002475 -> 立讯精密`、`688802 -> 沐曦股份` 等映射，避免 dashboard 或爬虫用错公司名。

`src/config/paths.py` 负责把输出固定到 `outputs/stocks/<symbol>/data|reports|results|models` 和 `outputs/system`。这让单股票和多股票结果比较容易追踪。

### 4.2 Data Ingestion 数据采集

相关文件：

- `src/data_ingestion/ingestion.py`
- `src/data_ingestion/cache.py`
- `src/data_ingestion/market_ingestor.py`
- `src/data_ingestion/news_ingestor.py`
- `src/data_ingestion/scheduler.py`
- `src/data_ingestion/scrapers/existing_finance_text_adapter.py`
- `program/run_scraper.py`
- `program/finance_text_scraper.py`
- `docs/statement/data_source_statement.md`

数据采集层做了三件事。

第一，cache-aware ingestion。`src/data_ingestion/cache.py` 会维护每只股票的 master CSV，例如 `outputs/stocks/301607/data/301607_finance_text_master.csv`。当 dashboard 或 CLI 请求某个日期区间时，系统会先判断 master 是否覆盖请求区间，覆盖就直接切片复用，不覆盖才抓取缺失区间。

第二，底层抓取。`program/finance_text_scraper.py` 支持 Yahoo、Eastmoney、Tencent 和 auto/fallback 思路。当前 A 股流程主要稳定使用 Tencent 行情，并补充 Eastmoney、CNINFO、Sina 等公告/新闻端口。Yahoo 相关逻辑保留，但在当前网络环境曾经被 rate-limit，所以不是稳定主力来源。

第三，诊断和透明 fallback。爬虫会输出 diagnostic JSON，记录每个 source 的 command、stdout、stderr、returncode 和失败原因。若某些交易日没有外部新闻，系统会生成带有 `程序生成行情文本摘要` 标签的 OHLCV 日摘要，保证行情日期连续，但报告里必须说明这种文本不是外部新闻。

`src/data_ingestion/scheduler.py` 目前只是提供 `APScheduler` 的 interval job builder，可以创建定时任务，但没有做成完整的生产级自动采集服务。因此 guideline 里的 “schedule automated collection” 目前只能算部分完成。

### 4.3 新闻密度与近期爬取状态

近期围绕 `301607 富特科技` 做过数据密度测试：

- 临时每日 20 条限制测试文件：`outputs/stocks/301607/data/301607_finance_text_2024-09-04_2026-04-27_density_check.csv`
- 解除限制后的 10000 news cap 测试文件：`outputs/stocks/301607/data/301607_finance_text_2024-09-04_2026-04-27_uncapped_news10000.csv`

解除每日 20 条限制后，`301607` 在 `2024-09-04` 到 `2026-04-27` 期间约有 601 个自然日、4442 条事件，其中外部事件约 4195 条，生成摘要约 247 条。问题是分布严重不均：2026 年 4 月附近单日可达数百条，但更远日期仍然稀疏。这说明 `news_count=10000` 能提升总量，但不会自动解决远端历史新闻密度不平均的问题。最终训练时应考虑按日聚合、winsorize/news_count cap、按日期平衡采样，或者把近端高密度新闻单独作为 case study，而不是直接让模型被近端新闻淹没。

### 4.4 NLP Pipeline

相关文件：

- `src/nlp/preprocess.py`
- `src/nlp/lexicon_sentiment.py`
- `src/nlp/logistic_sentiment.py`
- `src/nlp/finbert_sentiment.py`
- `src/nlp/aggregate_sentiment.py`

NLP 模块流程如下：

1. `preprocess.py` 做 tokenization，保留中文片段、英文词和数字。
2. 英文 stopwords 会被移除。
3. `lemmatize_token` 提供轻量 dependency-free stemmer/lemmatizer；中文 token 保持原样。
4. `lexicon_sentiment.py` 用中英文金融正负词典打 baseline 情绪分。
5. `logistic_sentiment.py` 用 TF-IDF + Logistic Regression 做第二种方法。如果输入没有人工标签，会使用 lexicon pseudo-label 训练/评估。
6. `finbert_sentiment.py` 接入 `ProsusAI/finbert`，但默认 local-only；除非本地已有模型或设置 `FINBERT_ALLOW_DOWNLOAD=1`，否则会跳过。
7. `aggregate_sentiment.py` 把 item-level 新闻对齐到交易日，并输出 daily sentiment、item sentiment、NLP evaluation 和 method comparison。

当前重要风险：现有 `*_nlp_evaluation.csv` 显示 FinBERT 多数为 `skipped`，Logistic F1 多数是 `pseudo_label_eval`，例如 `301607` 的最新区间里 `finbert_status=skipped`，`logistic_tfidf` 的 F1 为 0.0 且状态是 pseudo-label，不是人工标注真值。这是 NLP 评分里最需要补强的一块。

### 4.5 Feature Engineering 与反泄露

相关文件：

- `src/features/technical_indicators.py`
- `src/features/money_flow.py`
- `src/evaluation/diagnostics.py`
- `src/evaluation/signals.py`

feature 模块构造 guideline 指定的 RL state vector：

```text
[price, MA50, MA200, RSI, MACD, position, cash, sentiment_score]
```

`technical_indicators.py` 会生成 MA50、MA200、RSI、MACD，并把行情特征 shift 到上一交易日，避免同日/未来信息进入交易决策。`diagnostics.py` 会检查 `feature_available_until` 是否早于当前 date，并输出 `lookahead_bias_detected`、`feature_shift_correctness` 等诊断。

`money_flow.py` 会优先使用真实资金流字段；如果没有，就用 OHLCV 和成交额构造解释性 proxy。报告里需要明确：proxy 可以用于解释和辅助信号诊断，但不能当成真实主力资金流。

### 4.6 Data Storage

相关文件：

- `src/storage/schema.sql`
- `src/storage/database.py`
- `src/storage/models.py`
- `src/storage/repositories.py`
- `outputs/database/trading_platform.db`

SQLite/PostgreSQL DAL 已经有基础实现。`schema.sql` 定义了：

- `news_table`
- `market_table`
- `sentiment_table`
- `trading_log_table`

并提供旧命名兼容 views：`news_articles`、`market_bars`、`sentiment_signals`、`trade_logs`。

`database.py` 提供初始化、upsert news/market/sentiment、append trading logs、load table 等函数。当前 pipeline 默认写 CSV，SQLite 通过 `--use-sqlite` 或配置开关启用。因此 guideline 的数据库要求是“代码已具备，最终 demo/report 需要展示一次实际写库和查询结果”。

### 4.7 RL Trading Engine

相关文件：

- `src/rl/trading_env.py`
- `src/rl/dqn_agent.py`
- `src/rl/replay_buffer.py`
- `src/rl/train.py`

RL 部分已经比较符合 guideline：

- `trading_env.py` 定义 `FinancialTradingEnv`。
- action space 是 Hold / Buy / Sell。
- state 包含价格指标、仓位、现金和情绪分数。
- reward 使用 portfolio value 的相邻交易期收益，并扣 transaction cost。
- `dqn_agent.py` 是 PyTorch 从 scratch 的 DQN，包含 QNetwork、epsilon-greedy、replay buffer、target network。
- `replay_buffer.py` 自己实现经验回放。
- `train.py` 负责训练、评估、保存 rewards、logs、models。

默认 episodes 是 200，符合 minimum requirement。当前需要补的是 report 里的 reward justification 和 convergence analysis，也就是解释 reward 为什么这样设计，并展示 training reward 曲线/多 seed 稳定性。

### 4.8 Evaluation、Ablation 与 Walk-Forward

相关文件：

- `src/evaluation/ablation.py`
- `src/evaluation/metrics.py`
- `src/evaluation/walk_forward.py`
- `src/evaluation/cross_stock.py`
- `src/evaluation/feasibility_audit.py`

`ablation.py` 做了三组对比：

- `buy_and_hold`
- `dqn_without_nlp`
- `dqn_with_nlp`

DQN 默认多 seed：`42, 123, 2024, 2025, 3407`。输出包括：

- `ablation_metrics.csv`
- `ablation_metrics_by_seed.csv`
- `portfolio_curves.csv`
- `drawdown_curves.csv`
- `trading_logs.csv`
- `training_rewards_all_seeds.csv`

`metrics.py` 覆盖 Sharpe、max drawdown、annualized return、annualized volatility、Sortino、Calmar、VaR、profit factor 等。guideline 要求的 Sharpe、MDD、buy-and-hold comparison 已经覆盖。

`walk_forward.py` 和相关输出 `*_walk_forward_splits.csv` 能证明时间切分没有 overlap，但目前更像 split table/diagnostics。严格来说，guideline 说 “Apply walk-forward validation”，最好补一个真正 rolling window 的训练-测试循环，而不是只输出窗口表。

### 4.9 Reporting

相关文件：

- `src/reporting/artifacts.py`
- `notebooks/full_report_pipeline.ipynb`
- `outputs/stocks/<symbol>/reports/*_report_draft.md`
- `outputs/stocks/<symbol>/reports/*.svg`
- `outputs/system/reports/*.md|*.csv`

`artifacts.py` 会生成：

- system architecture Mermaid
- close price 图
- daily sentiment 图
- daily news count 图
- daily net flow 图
- portfolio curves
- drawdown curves
- action distribution
- risk-return scatter
- signal-return correlation
- report draft markdown

这已经是正式 report 的素材库，但还不是 25-50 页最终 written report。最终报告需要把这些素材组织成完整论文式结构，并且补充方法解释、实验讨论、局限性、引用和 appendix。

### 4.10 Dashboard

相关文件：

- `src/dashboard/streamlit_app.py`
- `src/dashboard/app.py`

Streamlit dashboard 已经能作为平台前端：

- 配置单股票/多股票任务。
- 设置日期、news cap、DQN episodes、是否 run ingestion、是否复用本地 master、是否 require news、是否写 SQLite。
- 跑 preflight audit。
- 调用完整 workflow。
- 展示 live stock status、run summary、sentiment、money flow、portfolio、ablation metrics、diagnostics。
- 导出 dashboard run bundle。

这符合 front-end dashboard 要求。最终 presentation 时建议用它做 live demo，并提前固定一个稳定股票和日期范围，避免现场重新爬取被网络影响。

### 4.11 Cross-Stock 与 Feasibility Audit

相关文件：

- `src/evaluation/cross_stock.py`
- `src/evaluation/feasibility_audit.py`
- `outputs/system/cross_stock_summary.csv`
- `outputs/system/cross_stock_discussion.md`
- `outputs/system/reports/feasibility_audit.md`

cross-stock 模块会从每个股票的 reports/results 中汇总：

- market regime
- buy-and-hold / DQN without NLP / DQN with NLP final equity
- cumulative return
- Sharpe
- max drawdown
- sentiment coverage
- NLP effect label

当前 `outputs/system/cross_stock_summary.csv` 显示不同股票结果不一致：有的股票 NLP improves，有的 NLP hurts，有的 mixed effect。这是 report 里 critical discussion 的好材料。

feasibility audit 能判断 local cache 是否覆盖请求日期、sentiment coverage 是否足够、RL state rows 是否可用、ablation artifacts 是否存在。最近的 audit 对 `301607` 给出 cross-stock status `NOT_RELIABLE`，原因不是单只股票完全不能跑，而是 cross-stock 共同窗口和可比性还需要更谨慎。

### 4.12 Tests 与质量控制

相关文件：

- `tests/test_metrics.py`
- `tests/test_replay_buffer.py`
- `tests/test_walk_forward.py`
- `requirements.txt`

现有测试覆盖：

- max drawdown 和 Sharpe 返回行为。
- replay buffer sample。
- walk-forward windows 无 overlap。

测试数量还比较少。对最终项目来说，建议补充至少三类测试：state vector column/order 和 no-lookahead、storage roundtrip、dashboard-independent pipeline smoke test。

## 5. Guideline 逐项对照

### 5.1 Data Ingestion

要求：fetch financial news + OHLCV，schedule automated collection，log data flow。  
当前状态：部分到基本完成。

已完成：

- 行情和文本事件抓取已实现。
- 本地 master CSV 复用和缺失区间抓取已实现。
- 每次低层抓取有 diagnostic JSON。
- dashboard 有 run log/live status。

不足：

- 自动定时采集只有 `build_scheduler`，没有完整展示脚本/daemon/notebook demo。
- Yahoo 当前不稳定；A 股主要依赖 Tencent/Eastmoney/CNINFO/Sina。
- 远端历史新闻密度仍不均衡。

建议补充：

- 写一个 `scripts/run_scheduled_ingestion_demo.py` 或 README section，展示 scheduler 每 N 分钟调用 ingestion job。
- 在 report 里明确数据源、fallback 顺序、生成摘要标签和新闻密度限制。
- 对最终实验数据使用固定 frozen CSV，不要现场重新爬取。

### 5.2 NLP Pipeline

要求：preprocess text；至少两种 sentiment 方法；FinBERT 作为其中之一；报告 F1。  
当前状态：部分完成，但这是最大扣分风险之一。

已完成：

- Tokenize、stopword removal、lemmatize/stemmer 已有。
- Lexicon 和 Logistic Regression 已有。
- FinBERT 代码已接入。
- Daily sentiment aggregation 已有。

不足：

- FinBERT 当前运行多为 `skipped`，因为本地没有模型且不能连接 Hugging Face。
- Logistic 的 F1 多数来自 pseudo-label，不是人工标注 gold labels。
- 现有 `*_nlp_evaluation.csv` 中 F1 常为 0.0 或空值，不能很好支撑 NLP Pipeline 25% 评分。

建议补充：

- 在联网环境下载并缓存 FinBERT，或把模型 cache 放进可复现实验环境，然后 rerun。
- 手工标注一小批新闻，例如 300-500 条，标签为 positive/neutral/negative。
- 用这批 gold labels 对 lexicon、logistic、FinBERT 都算 accuracy/precision/recall/F1。
- 把 pseudo-label eval 改名/解释为 fallback，不作为主 F1 证据。

### 5.3 Data Storage

要求：设计 DB schema；SQLite/PostgreSQL data access layer。  
当前状态：基本完成。

已完成：

- `schema.sql` 已有 news、market、sentiment、trading log 表。
- `database.py` 有初始化、保存和读取函数。
- `outputs/database/trading_platform.db` 已存在。

不足：

- 默认流程主要写 CSV，SQLite 是可选。
- report/presentation 里还没有明显展示数据库 schema 和一次 query/roundtrip。

建议补充：

- 用 `--use-sqlite` 跑一次正式样例。
- 在 final report 放 schema 表格和 SQLite 截图/查询结果。
- 补一个 storage roundtrip test。

### 5.4 RL Trading Engine

要求：FinancialTradingEnv，state 包含价格指标和 sentiment，actions Buy/Sell/Hold，DQN from scratch，200 episodes，train/evaluate。  
当前状态：基本完成。

已完成：

- `FinancialTradingEnv` 已有。
- state vector 满足 guideline。
- Buy/Sell/Hold 已有。
- DQN from scratch、replay buffer、target network 已有。
- episodes 默认 200。
- 多 seed ablation 已有。

不足：

- 需要在报告里更清楚地解释 reward function、transaction cost、为什么使用这些 state。
- convergence analysis 需要从 `training_rewards_all_seeds.csv` 组织成图和文字。

建议补充：

- Final report 增加 “Reward Design” 和 “Convergence” 小节。
- 对每个策略展示 final equity、Sharpe、MDD 和 action distribution。

### 5.5 Front-End Dashboard

要求：展示 sentiment trends、trading decisions、portfolio performance、system health。  
当前状态：基本完成。

已完成：

- Streamlit dashboard 已有。
- 支持 workflow 配置、运行、live status、结果展示和导出。
- 能展示 sentiment、net flow、portfolio、ablation、diagnostics。

不足：

- 需要准备固定 demo script，避免现场网络或训练时间过长。
- Dashboard 的运行结果 bundle 很多，需要挑一个最终版本放进 presentation。

建议补充：

- 录屏或截图 4 个页面：configuration、workflow status、portfolio/ablation、diagnostics。
- Presentation 时 demo 用本地 cached CSV，不现场爬虫。

### 5.6 Core Integration

要求：sentiment_score included in RL state vector；做 with NLP vs without NLP ablation。  
当前状态：完成。

证据：

- `src/features/technical_indicators.py` 的 state columns 包含 `sentiment_score`。
- `src/evaluation/ablation.py` 输出三组实验。
- `outputs/stocks/<symbol>/reports/*_state_vector_compliance.csv` 和 `*_ablation_metrics.csv` 是证据。

需要注意：

- Report 中要解释 without NLP 具体是怎样置零/移除 sentiment 的，否则 reader 看不出 ablation 是否公平。

### 5.7 Evaluation

要求：Sharpe、MDD、buy-and-hold、walk-forward、no look-ahead。  
当前状态：多数完成，walk-forward 部分完成。

已完成：

- Sharpe 和 MDD 已实现并输出。
- Buy-and-hold baseline 已有。
- No-lookahead diagnostics 已有。
- Chronological train/test split 已有。

不足：

- True rolling walk-forward training/evaluation 还没有完整落地。
- Cross-stock status 当前有 `NOT_RELIABLE`，说明多股票比较需要更多一致数据或更谨慎解释。

建议补充：

- 最好实现一个 rolling walk-forward experiment：每个窗口 train DQN，再在后续窗口 test，汇总每个窗口的 Sharpe/MDD/return。
- 如果时间不够，至少在 report 里明确当前是 chronological holdout + walk-forward split diagnostics，不夸大成完整 walk-forward validation。

### 5.8 Written Report 25-50 Pages

要求：正式 25-50 页报告，含 ablation、Sharpe/MDD、critical discussion。  
当前状态：素材完成，最终报告未完成。

已完成：

- 每个股票有 `*_report_draft.md`。
- 图表、metrics、diagnostics 已生成。
- notebook workflow 存在。

不足：

- 没有一个合并后的 25-50 页 final report。
- 引用、方法解释、实验设置、结果讨论、局限性和 appendix 需要整理。

建议 final report 结构：

1. Introduction and research question
2. System architecture
3. Data collection and data quality
4. NLP methods and labelled evaluation
5. RL environment and DQN implementation
6. Experiment design
7. Single-stock results
8. Cross-stock robustness
9. Ablation: with NLP vs without NLP
10. Risk metrics: Sharpe, MDD, drawdown, VaR
11. Limitations: news density, generated summaries, FinBERT availability, proxy net flow
12. Conclusion
13. Appendix: schema, commands, dashboard screenshots

### 5.9 GitHub Repo

要求：GitHub repo with README, requirements, module docs。  
当前状态：本地代码有 README/requirements/module structure，但当前目录不是 git repo。

证据：

- `git status --short` 返回 `fatal: not a git repository`。
- `.gitignore` 已存在，忽略 `.venv`、outputs、cache、env 等。

建议补充：

- 初始化 git 或把当前目录复制到正式 GitHub repo。
- 确认不要提交 `.venv`、outputs 大文件、数据库、缓存。
- 提交核心代码、README、requirements、docs、notebooks、少量示例图/结果。
- README 增加 one-command demo 和 expected outputs。

### 5.10 Presentation 25 min + Q&A + Live Demo

要求：25 分钟 presentation，Q&A，live demo。  
当前状态：未完成。

建议 slide structure：

1. Problem and motivation
2. Guideline mapping / system overview
3. Data sources and ingestion flow
4. News density issue and data quality controls
5. NLP pipeline: lexicon, logistic, FinBERT
6. RL state and trading environment
7. DQN architecture from scratch
8. Experiment setup and no-lookahead design
9. Results: with NLP vs without NLP
10. Risk metrics and drawdowns
11. Cross-stock robustness
12. Dashboard live demo
13. Limitations and future work
14. Conclusion

Live demo 建议：

- 使用 cached data。
- 选择一个已跑通且图表完整的股票，例如 `002475`、`300750` 或 `600519`。
- 准备备用截图/录屏，防止现场训练或网络出问题。

## 6. 当前最需要补的优先级

### P0：必须补，否则很可能扣大分

1. FinBERT 需要真正跑通，不能只停留在 skipped。
2. NLP F1 需要人工标注 gold labels，不能只靠 pseudo-label。
3. 最终 25-50 页 report 需要合并生成。
4. Presentation deck 和 live demo script 需要做。
5. GitHub repo 需要正式初始化/提交。

### P1：强烈建议补，会显著提高质量

1. 真正 rolling walk-forward validation。
2. SQLite demo 和 storage roundtrip test。
3. 更多 unit tests：state vector、no-lookahead、pipeline smoke。
4. 对新闻密度不均衡做训练前处理，并在 report 里解释。
5. Cross-stock common window 重新整理，避免 `NOT_RELIABLE` 被误读。

### P2：时间够再补

1. Scheduler demo 脚本。
2. 更完整的 module docs。
3. Dashboard screenshots/export bundle 精简。
4. 数据源失败原因和 fallback path 的 appendix。

## 7. 建议接下来怎么做

### 第一步：固定最终实验样本

选择 3-5 只数据较完整的股票，固定日期区间和 frozen CSV。不要一边写报告一边改数据，否则结果会变。

建议候选：

- `002475` 立讯精密
- `300750` 宁德时代
- `600519` 贵州茅台
- `301607` 富特科技可以作为“新股/新闻密度不均衡”案例，但不一定适合作为主实验样本

### 第二步：补 NLP gold-label evaluation

从 `item_sentiment.csv` 或 integrated CSV 里抽样新闻，人工标注 positive/neutral/negative。然后让 lexicon、logistic、FinBERT 都在同一批样本上算 F1。这个动作对 NLP 25% 的评分非常关键。

### 第三步：跑一次最终 pipeline

用统一参数重新跑主样本，并保存：

- integrated CSV
- daily sentiment
- NLP evaluation
- state vector compliance
- leakage diagnostics
- ablation metrics
- training rewards
- portfolio curves
- drawdown curves
- report draft

### 第四步：写正式 report

不要只交每只股票的 draft。需要合并成一个总报告，重点回答：

- NLP signal 是否提升 RL trading performance？
- 在哪些股票/市场状态下有效？
- 为什么有些股票 NLP hurts？
- 数据源和新闻密度会怎样影响结论？
- 如何避免 look-ahead bias？

### 第五步：准备 presentation 和 demo

Dashboard 是很好的 live demo 前端，但不要现场重训 200 episodes。建议 demo cached results，然后用 1 个短区间点击展示 workflow 状态即可。

## 8. 可直接引用的项目证据文件

核心代码：

- `main.py`
- `program/run_scraper.py`
- `program/finance_text_scraper.py`
- `src/data_ingestion/ingestion.py`
- `src/data_ingestion/cache.py`
- `src/nlp/aggregate_sentiment.py`
- `src/nlp/lexicon_sentiment.py`
- `src/nlp/logistic_sentiment.py`
- `src/nlp/finbert_sentiment.py`
- `src/features/technical_indicators.py`
- `src/rl/trading_env.py`
- `src/rl/dqn_agent.py`
- `src/evaluation/ablation.py`
- `src/evaluation/metrics.py`
- `src/dashboard/streamlit_app.py`
- `src/reporting/artifacts.py`

配置和文档：

- `README.md`
- `requirements.txt`
- `config/default.json`
- `config/stock_aliases.json`
- `docs/PROJECT_STRUCTURE.md`
- `docs/statement/data_source_statement.md`

代表性输出：

- `outputs/stocks/301607/data/301607_finance_text_2024-09-04_2026-04-27_uncapped_news10000.csv`
- `outputs/stocks/301607/data/301607_diagnostic_20260501_195439.json`
- `outputs/stocks/301607/reports/301607_finance_text_2025-01-01_2026-04-27_nlp_evaluation.csv`
- `outputs/stocks/301607/reports/301607_finance_text_2025-01-01_2026-04-27_ablation_metrics.csv`
- `outputs/system/cross_stock_summary.csv`
- `outputs/system/reports/feasibility_audit.md`

## 9. 最终判断

这个项目不是“没做完代码”，而是“代码原型和实验素材很多，但最终课程交付证据还没整理到位”。最关键的补强方向不是继续无限爬更多新闻，而是把评分 rubric 需要看的证据补齐：真实 FinBERT、真实 NLP F1、真正或诚实说明的 walk-forward validation、正式报告、presentation、GitHub repo。

如果只剩有限时间，优先补 P0；如果还有 1-2 天，补 P1 中的 walk-forward 和 storage demo。这样项目会从“能跑的工程原型”变成“能拿去评分的完整课程项目”。
