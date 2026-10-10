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

**证据边界：**离线测试和合成数据 Smoke 已执行。正式行情与带可用时间的新闻
快照、人工 gold labels、适用的 FinBERT checkpoint 尚不齐全，因此没有新增正式
收益结论。旧诊断文件保留作历史材料，需要按新口径重跑。

- [英语主页](README.md)
- [方法与时间隔离](docs/methodology.md)
- [数据输入与覆盖](docs/data.md)
- [安装、Demo / Smoke / Full](docs/reproduction.md)
- [项目演进与两则案例](docs/project_journey.md)
- [修订报告](reports/refinement_report.md)
- [团队贡献待核实项](docs/team.md)
