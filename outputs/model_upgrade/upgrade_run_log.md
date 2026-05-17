# Model Upgrade Run Log

- target_symbol: 002475
- target_company: 立讯精密
- window: 2024-01-01 -> 2026-04-30
- quick_test: True

- model_variants: ['vanilla_dqn', 'double_dueling_dqn']
- reward_variants: ['one_day_return']
- state_feature_modes: ['official_8d']
- seeds: [1, 2]
- episodes: 5

- 002475_vanilla_dqn_one_day_return_official_8d_seed1: final_equity=842361.7848799999, trades=2, exposure=0.09090909090909091
- 002475_vanilla_dqn_one_day_return_official_8d_seed1: final_equity=842361.7848799999, trades=2, exposure=0.09090909090909091
- 002475_vanilla_dqn_one_day_return_official_8d_seed1: final_equity=1342369.00564, trades=3, exposure=0.30578512396694213
- 002475_vanilla_dqn_one_day_return_official_8d_seed1: final_equity=880157.7389400001, trades=3, exposure=0.03305785123966942
- 002475_vanilla_dqn_one_day_return_official_8d_seed1: final_equity=1000000.0, trades=0, exposure=0.0
- 002475_vanilla_dqn_one_day_return_official_8d_seed2: final_equity=1000000.0, trades=0, exposure=0.0
- 002475_vanilla_dqn_one_day_return_official_8d_seed2: final_equity=1105608.2580199998, trades=11, exposure=0.39669421487603307
- 002475_vanilla_dqn_one_day_return_official_8d_seed2: final_equity=1000000.0, trades=0, exposure=0.0
- 002475_vanilla_dqn_one_day_return_official_8d_seed2: final_equity=1102335.26248, trades=10, exposure=0.18181818181818182
- 002475_vanilla_dqn_one_day_return_official_8d_seed2: final_equity=1024532.37893, trades=14, exposure=0.8925619834710744
- 002475_double_dueling_dqn_one_day_return_official_8d_seed1: final_equity=993641.8525800002, trades=4, exposure=0.04132231404958678
- 002475_double_dueling_dqn_one_day_return_official_8d_seed1: final_equity=1000000.0, trades=0, exposure=0.0
- 002475_double_dueling_dqn_one_day_return_official_8d_seed1: final_equity=1000000.0, trades=0, exposure=0.0
- 002475_double_dueling_dqn_one_day_return_official_8d_seed1: final_equity=1000000.0, trades=0, exposure=0.0
- 002475_double_dueling_dqn_one_day_return_official_8d_seed1: final_equity=1127598.6780700004, trades=3, exposure=0.0743801652892562
- 002475_double_dueling_dqn_one_day_return_official_8d_seed2: final_equity=1014995.8652, trades=8, exposure=0.049586776859504134
- 002475_double_dueling_dqn_one_day_return_official_8d_seed2: final_equity=911647.75175, trades=14, exposure=0.06611570247933884
- 002475_double_dueling_dqn_one_day_return_official_8d_seed2: final_equity=935383.4349700002, trades=6, exposure=0.024793388429752067
- 002475_double_dueling_dqn_one_day_return_official_8d_seed2: final_equity=1038645.08956, trades=2, exposure=0.008264462809917356
- 002475_double_dueling_dqn_one_day_return_official_8d_seed2: final_equity=1203982.2088399995, trades=13, exposure=0.1652892561983471

## Validation Status

- python -m compileall src scripts tests: PASSED
- python scripts/run_model_upgrade_grid.py --quick-test: PASSED
- dashboard launch at http://127.0.0.1:8501: PASSED
- pytest -q: PASSED (26 passed)
