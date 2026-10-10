# 从金融新闻到交易决策

本项目是 Data Analysis and FinTech Fundamentals 小组课程作业，研究同行新闻
训练出的 NLP 信号是否能为目标股票的 DQN 交易提供额外信息。

保留采集、词典/LR/FinBERT、手写 DQN、SQLite 和 Streamlit，以及十二维状态、
目标仓位和 DQN 变种。固定核心比较使用八维状态、训练期缩放、三动作、同费用
与同种子，比较 market-only、sector sentiment、sector impact 和 Buy-and-Hold。

本轮修复当日收盘进入状态、费用与奖励不一致、账户估值时点、FinBERT 标签截断
和 gold-label 训练/测试混用。新增前缀拟合及逐折重训入口，运行清单记录输入、
代码、截止日期、种子和校验值。默认界面是四页只读演示；原研究控件从 Advanced
打开。

**真实实验已完成：**002475、601138 的两折、三种子、三组 DQN 共 36 个模型，
每个训练 200 episodes，保留 2,520 行测试账本和 7,200 行训练记录。NLP 收益差异
随股票、折和种子变化，11 个模型没有测试交易，不能据此声称稳定增益。

**FinBERT 已实际评估：**固定中文检查点，在 CFBenchmark 外部人工共识标签的
共同 149 条测试集上，macro F1 为 0.391；TF-IDF/LR 为 0.605，词典为 0.453。
作者未公开检查点训练文本，无法核验与评估集是否重叠；这不代表已发现重叠，
但不能把分数当成已证明无污染的全新文本表现。我们自己的 LR 训练/测试已隔离。

数据为腾讯前复权行情和东方财富公告标题。历史公告只有公开日期，按整日结束后
再进入下一开盘推断可用时间；这不是已观测的历史发布时间。行情调整、A 股执行
约束、弱标签和回顾性数据的限制均写入报告。FinBERT 评估与 DQN 弱标签信号分开。
默认四页只读展示已选定 002475 正式结果，也可选择 601138。

- [英语主页](README.md)
- [真实数据、正式实验与全部结果](docs/real_experiments.md)
- [中文 FinBERT 评估与重叠限制说明](docs/finbert_evaluation.md)
- [方法与时间隔离](docs/methodology.md)
- [数据输入与覆盖](docs/data.md)
- [安装、Demo / Smoke / Full](docs/reproduction.md)
- [项目演进与两则案例](docs/project_journey.md)
- [修订报告](reports/refinement_report.md)
- [团队贡献待核实项](docs/team.md)
