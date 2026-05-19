# Model Upgrade Diagnostic Report

## 1. Run Overview
- generated timestamp: 20260518_223047
- run_mode: medium_test, quick_test
- episodes: 5, 50
- max_seeds: 2, 5
- target_symbol list: 002475
- target_company list: UNKNOWN
- action_mode list: target_position
- model_variant list: double_dueling_dqn, vanilla_dqn
- reward_variant list: one_day_return, three_day_return
- state_feature_mode list: normalized_plus
- data source paths used: 9 file(s) loaded
- validation status: see upgrade_run_log.md if available

## 2. Data Availability Check
| file | exists | path | rows | columns | targets |
| --- | --- | --- | --- | --- | --- |
| model_upgrade_summary.csv | yes | outputs/model_upgrade/model_upgrade_summary.csv | 30 | 24 | 002475 |
| seed_level_metrics.csv | yes | outputs/model_upgrade/seed_level_metrics.csv | 120 | 29 | 002475 |
| env_dynamics_diagnostics.csv | yes | outputs/model_upgrade/env_dynamics_diagnostics.csv | 14520 | 40 | 002475 |
| action_distribution_diagnostics.csv | yes | outputs/model_upgrade/action_distribution_diagnostics.csv | 1094 | 26 | 002475 |
| q_value_diagnostics.csv | yes | outputs/model_upgrade/q_value_diagnostics.csv | 14520 | 25 | 002475 |
| exploration_diagnostics.csv | yes | outputs/model_upgrade/exploration_diagnostics.csv | 5100 | 21 | 002475 |
| reward_diagnostics.csv | no |  | 0 | 0 |  |
| portfolio_curves.csv | yes | outputs/model_upgrade/portfolio_curves.csv | 14520 | 18 | 002475 |
| nlp_incremental_effect.csv | no |  | 0 | 0 |  |
| nlp_incremental_effect_summary.csv | no |  | 0 | 0 |  |
| reward_variant_comparison.csv | yes | outputs/model_upgrade/reward_variant_comparison.csv | 30 | 17 | 002475 |
| state_feature_diagnostics.csv | yes | outputs/model_upgrade/state_feature_diagnostics.csv | 720 | 20 | 002475 |

## 3. Cross-Stock Summary
| target_symbol | runs | experiments | best_final_equity_experiment | best_sharpe_experiment | average_exposure | average_trades | hold_ratio | trade_diagnostics | q_diagnostics |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 002475 | 120 | dqn_with_marketwide_impact_nlp, dqn_with_marketwide_sentiment_nlp, dqn_with_sector_impact_nlp, dqn_with_sector_sentiment_nlp, dqn_without_nlp | dqn_with_sector_sentiment_nlp | dqn_with_sector_sentiment_nlp | 0.47418 | 37.6333 | 0.688981 | yes | yes |

## 4. Model Upgrade Summary
| target_symbol | experiment | model_variant | reward_variant | state_feature_mode | action_mode | seed_count | mean_final_equity | mean_cumulative_return | mean_sharpe_ratio | mean_max_drawdown | mean_number_of_trades | mean_average_exposure | mean_unique_portfolio_values |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 002475 | dqn_with_sector_sentiment_nlp | vanilla_dqn | one_day_return | normalized_plus | target_position | 2 | 1.66033e+06 | 0.660331 | 3.28578 | 0.102489 | 54 | 0.403545 | 107.5 |
| 002475 | dqn_with_marketwide_sentiment_nlp | vanilla_dqn | one_day_return | normalized_plus | target_position | 5 | 1.53148e+06 | 0.531477 | 2.96934 | 0.0872543 | 43.4 | 0.365475 | 86.8 |
| 002475 | dqn_with_marketwide_impact_nlp | double_dueling_dqn | one_day_return | normalized_plus | target_position | 2 | 1.5058e+06 | 0.505795 | 3.10622 | 0.127314 | 49 | 0.396826 | 78 |
| 002475 | dqn_with_marketwide_sentiment_nlp | double_dueling_dqn | one_day_return | normalized_plus | target_position | 2 | 1.47338e+06 | 0.473376 | 3.58372 | 0.074924 | 62 | 0.408172 | 93.5 |
| 002475 | dqn_with_sector_sentiment_nlp | vanilla_dqn | one_day_return | normalized_plus | target_position | 5 | 1.44604e+06 | 0.446036 | 2.72464 | 0.148195 | 41.2 | 0.536441 | 92 |
| 002475 | dqn_with_sector_impact_nlp | vanilla_dqn | one_day_return | normalized_plus | target_position | 2 | 1.44548e+06 | 0.445477 | 3.06107 | 0.0743923 | 49.5 | 0.419963 | 109.5 |
| 002475 | dqn_without_nlp | vanilla_dqn | one_day_return | normalized_plus | target_position | 2 | 1.41518e+06 | 0.415178 | 2.50048 | 0.138982 | 58 | 0.453929 | 96.5 |
| 002475 | dqn_with_marketwide_impact_nlp | vanilla_dqn | one_day_return | normalized_plus | target_position | 2 | 1.40938e+06 | 0.409376 | 2.625 | 0.120689 | 55 | 0.513083 | 107.5 |
| 002475 | dqn_with_marketwide_sentiment_nlp | double_dueling_dqn | one_day_return | normalized_plus | target_position | 5 | 1.40749e+06 | 0.407489 | 2.36077 | 0.140465 | 33.2 | 0.514477 | 80 |
| 002475 | dqn_without_nlp | double_dueling_dqn | one_day_return | normalized_plus | target_position | 2 | 1.40168e+06 | 0.401675 | 3.32118 | 0.0900381 | 65.5 | 0.397359 | 118 |

## 5. Hold-Heavy Policy Diagnosis
| target_symbol | experiment | model_variant | reward_variant | state_feature_mode | action_mode | avg_hold_ratio | avg_buy_count | avg_sell_count | avg_exposure | avg_unique_portfolio_values | label |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 002475 | dqn_with_marketwide_impact_nlp | double_dueling_dqn | one_day_return | normalized_plus | target_position | 0.726092 | 16 | 17.1429 | 0.500946 | 78.2857 | NOT_HOLD_HEAVY |
| 002475 | dqn_with_marketwide_impact_nlp | double_dueling_dqn | three_day_return | normalized_plus | target_position | 0.765289 | 14.6 | 13.8 | 0.543441 | 82.2 | NOT_HOLD_HEAVY |
| 002475 | dqn_with_marketwide_impact_nlp | vanilla_dqn | one_day_return | normalized_plus | target_position | 0.655254 | 21.1429 | 20.5714 | 0.505528 | 93.1429 | NOT_HOLD_HEAVY |
| 002475 | dqn_with_marketwide_impact_nlp | vanilla_dqn | three_day_return | normalized_plus | target_position | 0.715702 | 18.2 | 16.2 | 0.321838 | 76 | NOT_HOLD_HEAVY |
| 002475 | dqn_with_marketwide_sentiment_nlp | double_dueling_dqn | one_day_return | normalized_plus | target_position | 0.657615 | 21.2857 | 20.1429 | 0.484104 | 83.8571 | NOT_HOLD_HEAVY |
| 002475 | dqn_with_marketwide_sentiment_nlp | double_dueling_dqn | three_day_return | normalized_plus | target_position | 0.809917 | 12.2 | 10.8 | 0.680187 | 93.2 | NOT_HOLD_HEAVY |
| 002475 | dqn_with_marketwide_sentiment_nlp | vanilla_dqn | one_day_return | normalized_plus | target_position | 0.600945 | 24 | 24.2857 | 0.435212 | 92.2857 | NOT_HOLD_HEAVY |
| 002475 | dqn_with_marketwide_sentiment_nlp | vanilla_dqn | three_day_return | normalized_plus | target_position | 0.712397 | 18.6 | 16.2 | 0.4999 | 85.6 | NOT_HOLD_HEAVY |
| 002475 | dqn_with_sector_impact_nlp | double_dueling_dqn | one_day_return | normalized_plus | target_position | 0.658796 | 20.7143 | 20.5714 | 0.584143 | 92.1429 | NOT_HOLD_HEAVY |
| 002475 | dqn_with_sector_impact_nlp | double_dueling_dqn | three_day_return | normalized_plus | target_position | 0.771901 | 13.6 | 14 | 0.485192 | 71.6 | NOT_HOLD_HEAVY |
| 002475 | dqn_with_sector_impact_nlp | vanilla_dqn | one_day_return | normalized_plus | target_position | 0.618654 | 23.5714 | 22.5714 | 0.467765 | 92.5714 | NOT_HOLD_HEAVY |
| 002475 | dqn_with_sector_impact_nlp | vanilla_dqn | three_day_return | normalized_plus | target_position | 0.712397 | 18.2 | 16.6 | 0.389987 | 74 | NOT_HOLD_HEAVY |
| 002475 | dqn_with_sector_sentiment_nlp | double_dueling_dqn | one_day_return | normalized_plus | target_position | 0.639906 | 22.2857 | 21.2857 | 0.496969 | 91.8571 | NOT_HOLD_HEAVY |
| 002475 | dqn_with_sector_sentiment_nlp | double_dueling_dqn | three_day_return | normalized_plus | target_position | 0.646281 | 22.2 | 20.6 | 0.559991 | 90.8 | NOT_HOLD_HEAVY |
| 002475 | dqn_with_sector_sentiment_nlp | vanilla_dqn | one_day_return | normalized_plus | target_position | 0.62928 | 23 | 21.8571 | 0.498471 | 96.4286 | NOT_HOLD_HEAVY |
| 002475 | dqn_with_sector_sentiment_nlp | vanilla_dqn | three_day_return | normalized_plus | target_position | 0.690909 | 20.8 | 16.6 | 0.343289 | 92.8 | NOT_HOLD_HEAVY |
| 002475 | dqn_without_nlp | double_dueling_dqn | one_day_return | normalized_plus | target_position | 0.675325 | 20.5714 | 18.7143 | 0.539452 | 94.8571 | NOT_HOLD_HEAVY |
| 002475 | dqn_without_nlp | double_dueling_dqn | three_day_return | normalized_plus | target_position | 0.804959 | 11.8 | 11.8 | 0.333084 | 52.6 | NOT_HOLD_HEAVY |
| 002475 | dqn_without_nlp | vanilla_dqn | one_day_return | normalized_plus | target_position | 0.662338 | 21.1429 | 19.7143 | 0.476841 | 85.4286 | NOT_HOLD_HEAVY |
| 002475 | dqn_without_nlp | vanilla_dqn | three_day_return | normalized_plus | target_position | 0.771901 | 14.6 | 13 | 0.238197 | 54.8 | NOT_HOLD_HEAVY |

## 6. Trade Behavior Summary
| target_symbol | experiment | model_variant | reward_variant | state_feature_mode | action_mode | buy_count | sell_count | hold_count | avg_exposure | max_exposure | min_exposure | avg_transaction_cost | final_portfolio_value | total_return | number_of_trades |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 002475 | dqn_with_marketwide_impact_nlp | double_dueling_dqn | one_day_return | normalized_plus | target_position | 112 | 120 | 615 | 0.500946 | 1 | 0 | 220.174 | 880985 | -0.119015 | 232 |
| 002475 | dqn_with_marketwide_impact_nlp | double_dueling_dqn | three_day_return | normalized_plus | target_position | 73 | 69 | 463 | 0.543441 | 1 | 0 | 152.434 | 1.18766e+06 | 0.187662 | 142 |
| 002475 | dqn_with_marketwide_impact_nlp | vanilla_dqn | one_day_return | normalized_plus | target_position | 148 | 144 | 555 | 0.505528 | 1 | 0 | 187.082 | 936192 | -0.0638076 | 292 |
| 002475 | dqn_with_marketwide_impact_nlp | vanilla_dqn | three_day_return | normalized_plus | target_position | 91 | 81 | 433 | 0.321838 | 1 | 0 | 142.63 | 1.09471e+06 | 0.0947125 | 172 |
| 002475 | dqn_with_marketwide_sentiment_nlp | double_dueling_dqn | one_day_return | normalized_plus | target_position | 149 | 141 | 557 | 0.484104 | 1 | 0 | 270.444 | 1.6416e+06 | 0.641595 | 290 |
| 002475 | dqn_with_marketwide_sentiment_nlp | double_dueling_dqn | three_day_return | normalized_plus | target_position | 61 | 54 | 490 | 0.680187 | 1 | 0 | 149.162 | 1.31262e+06 | 0.312617 | 115 |
| 002475 | dqn_with_marketwide_sentiment_nlp | vanilla_dqn | one_day_return | normalized_plus | target_position | 168 | 170 | 509 | 0.435212 | 1 | 0 | 266.853 | 1.50075e+06 | 0.500751 | 338 |
| 002475 | dqn_with_marketwide_sentiment_nlp | vanilla_dqn | three_day_return | normalized_plus | target_position | 93 | 81 | 431 | 0.4999 | 1 | 0 | 152.964 | 1.00341e+06 | 0.00341196 | 174 |
| 002475 | dqn_with_sector_impact_nlp | double_dueling_dqn | one_day_return | normalized_plus | target_position | 145 | 144 | 558 | 0.584143 | 1 | 0 | 209.518 | 983182 | -0.0168177 | 289 |
| 002475 | dqn_with_sector_impact_nlp | double_dueling_dqn | three_day_return | normalized_plus | target_position | 68 | 70 | 467 | 0.485192 | 1 | 0 | 134.879 | 1.04452e+06 | 0.0445212 | 138 |
| 002475 | dqn_with_sector_impact_nlp | vanilla_dqn | one_day_return | normalized_plus | target_position | 165 | 158 | 524 | 0.467765 | 1 | 0 | 217.378 | 1.12343e+06 | 0.123428 | 323 |
| 002475 | dqn_with_sector_impact_nlp | vanilla_dqn | three_day_return | normalized_plus | target_position | 91 | 83 | 431 | 0.389987 | 1 | 0 | 148.377 | 1.39034e+06 | 0.390339 | 174 |
| 002475 | dqn_with_sector_sentiment_nlp | double_dueling_dqn | one_day_return | normalized_plus | target_position | 156 | 149 | 542 | 0.496969 | 1 | 0 | 256.484 | 1.0156e+06 | 0.0156032 | 305 |
| 002475 | dqn_with_sector_sentiment_nlp | double_dueling_dqn | three_day_return | normalized_plus | target_position | 111 | 103 | 391 | 0.559991 | 1 | 0 | 226.051 | 1.27127e+06 | 0.27127 | 214 |
| 002475 | dqn_with_sector_sentiment_nlp | vanilla_dqn | one_day_return | normalized_plus | target_position | 161 | 153 | 533 | 0.498471 | 1 | 0 | 268.264 | 1.60568e+06 | 0.605677 | 314 |
| 002475 | dqn_with_sector_sentiment_nlp | vanilla_dqn | three_day_return | normalized_plus | target_position | 104 | 83 | 418 | 0.343289 | 1 | 0 | 161.025 | 1.46232e+06 | 0.462318 | 187 |
| 002475 | dqn_without_nlp | double_dueling_dqn | one_day_return | normalized_plus | target_position | 144 | 131 | 572 | 0.539452 | 1 | 0 | 212.904 | 988726 | -0.0112736 | 275 |
| 002475 | dqn_without_nlp | double_dueling_dqn | three_day_return | normalized_plus | target_position | 59 | 59 | 487 | 0.333084 | 1 | 0 | 156.896 | 1.37037e+06 | 0.370368 | 118 |
| 002475 | dqn_without_nlp | vanilla_dqn | one_day_return | normalized_plus | target_position | 148 | 138 | 561 | 0.476841 | 1 | 0 | 174.799 | 1.16674e+06 | 0.166744 | 286 |
| 002475 | dqn_without_nlp | vanilla_dqn | three_day_return | normalized_plus | target_position | 73 | 65 | 467 | 0.238197 | 1 | 0 | 104.28 | 1.28684e+06 | 0.286845 | 138 |

## 7. Q-Value Decision Diagnosis
| target_symbol | experiment | model_variant | reward_variant | state_feature_mode | action_mode | avg_selected_q_value | avg_q_value_margin | action0_ratio | label |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 002475 | dqn_with_marketwide_impact_nlp | double_dueling_dqn | one_day_return | normalized_plus | target_position | 0.563803 | 0.0260025 | 0.446281 | Q_VALUE_CONFIDENT |
| 002475 | dqn_with_marketwide_impact_nlp | double_dueling_dqn | three_day_return | normalized_plus | target_position | 1.24651 | 0.0256289 | 0.398347 | Q_VALUE_CONFIDENT |
| 002475 | dqn_with_marketwide_impact_nlp | vanilla_dqn | one_day_return | normalized_plus | target_position | 0.993401 | 0.0279139 | 0.316411 | Q_VALUE_CONFIDENT |
| 002475 | dqn_with_marketwide_impact_nlp | vanilla_dqn | three_day_return | normalized_plus | target_position | 1.84733 | 0.04898 | 0.46281 | Q_VALUE_CONFIDENT |
| 002475 | dqn_with_marketwide_sentiment_nlp | double_dueling_dqn | one_day_return | normalized_plus | target_position | 0.480218 | 0.0223357 | 0.422668 | Q_VALUE_CONFIDENT |
| 002475 | dqn_with_marketwide_sentiment_nlp | double_dueling_dqn | three_day_return | normalized_plus | target_position | 0.939112 | 0.0285364 | 0.299174 | Q_VALUE_CONFIDENT |
| 002475 | dqn_with_marketwide_sentiment_nlp | vanilla_dqn | one_day_return | normalized_plus | target_position | 0.976951 | 0.0488134 | 0.350649 | Q_VALUE_CONFIDENT |
| 002475 | dqn_with_marketwide_sentiment_nlp | vanilla_dqn | three_day_return | normalized_plus | target_position | 1.66526 | 0.052733 | 0.381818 | Q_VALUE_CONFIDENT |
| 002475 | dqn_with_sector_impact_nlp | double_dueling_dqn | one_day_return | normalized_plus | target_position | 0.605457 | 0.0172776 | 0.341204 | Q_VALUE_CONFIDENT |
| 002475 | dqn_with_sector_impact_nlp | double_dueling_dqn | three_day_return | normalized_plus | target_position | 1.17398 | 0.0272871 | 0.464463 | Q_VALUE_CONFIDENT |
| 002475 | dqn_with_sector_impact_nlp | vanilla_dqn | one_day_return | normalized_plus | target_position | 1.08572 | 0.0364724 | 0.33412 | Q_VALUE_CONFIDENT |
| 002475 | dqn_with_sector_impact_nlp | vanilla_dqn | three_day_return | normalized_plus | target_position | 1.57688 | 0.0749836 | 0.477686 | Q_VALUE_CONFIDENT |
| 002475 | dqn_with_sector_sentiment_nlp | double_dueling_dqn | one_day_return | normalized_plus | target_position | 0.490197 | 0.0177651 | 0.362456 | Q_VALUE_CONFIDENT |
| 002475 | dqn_with_sector_sentiment_nlp | double_dueling_dqn | three_day_return | normalized_plus | target_position | 1.21343 | 0.0144544 | 0.347107 | Q_VALUE_CONFIDENT |
| 002475 | dqn_with_sector_sentiment_nlp | vanilla_dqn | one_day_return | normalized_plus | target_position | 1.00091 | 0.0343873 | 0.312869 | Q_VALUE_CONFIDENT |
| 002475 | dqn_with_sector_sentiment_nlp | vanilla_dqn | three_day_return | normalized_plus | target_position | 1.66635 | 0.0656318 | 0.322314 | Q_VALUE_CONFIDENT |
| 002475 | dqn_without_nlp | double_dueling_dqn | one_day_return | normalized_plus | target_position | 0.655602 | 0.0221059 | 0.308146 | Q_VALUE_CONFIDENT |
| 002475 | dqn_without_nlp | double_dueling_dqn | three_day_return | normalized_plus | target_position | 1.17095 | 0.0313609 | 0.647934 | Q_VALUE_CONFIDENT |
| 002475 | dqn_without_nlp | vanilla_dqn | one_day_return | normalized_plus | target_position | 0.912247 | 0.0443355 | 0.373081 | Q_VALUE_CONFIDENT |
| 002475 | dqn_without_nlp | vanilla_dqn | three_day_return | normalized_plus | target_position | 1.7432 | 0.0521118 | 0.62314 | Q_VALUE_CONFIDENT |

## 8. Reward Diagnosis
Reward diagnostics are missing. Reward attribution cannot be fully analyzed.

## 9. NLP Incremental Effect Diagnosis
NLP_EFFECT_DIAGNOSTICS_MISSING

## 10. Benchmark Comparison
Benchmark curve or benchmark metrics are unavailable for model-upgrade diagnostics.

## 11. Stability Across Seeds
| target_symbol | experiment | model_variant | reward_variant | state_feature_mode | seed_count | std_final_equity | std_sharpe_ratio | coefficient_of_variation | stability_label |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 002475 | dqn_with_marketwide_impact_nlp | double_dueling_dqn | one_day_return | normalized_plus | 2 | 259763 | 0.956341 | 0.172509 | INSUFFICIENT_SEEDS |
| 002475 | dqn_with_marketwide_impact_nlp | vanilla_dqn | one_day_return | normalized_plus | 2 | 232448 | 1.11707 | 0.16493 | INSUFFICIENT_SEEDS |
| 002475 | dqn_with_marketwide_sentiment_nlp | double_dueling_dqn | one_day_return | normalized_plus | 2 | 148384 | 0.102744 | 0.10071 | INSUFFICIENT_SEEDS |
| 002475 | dqn_with_marketwide_sentiment_nlp | vanilla_dqn | one_day_return | normalized_plus | 2 | 201553 | 1.38132 | 0.161815 | INSUFFICIENT_SEEDS |
| 002475 | dqn_with_sector_impact_nlp | double_dueling_dqn | one_day_return | normalized_plus | 2 | 3647.88 | 0.0485644 | 0.00266405 | INSUFFICIENT_SEEDS |
| 002475 | dqn_with_sector_impact_nlp | vanilla_dqn | one_day_return | normalized_plus | 2 | 111836 | 0.14764 | 0.0773694 | INSUFFICIENT_SEEDS |
| 002475 | dqn_with_sector_sentiment_nlp | double_dueling_dqn | one_day_return | normalized_plus | 2 | 289913 | 1.54229 | 0.229035 | INSUFFICIENT_SEEDS |
| 002475 | dqn_with_sector_sentiment_nlp | vanilla_dqn | one_day_return | normalized_plus | 2 | 402479 | 1.09148 | 0.242409 | INSUFFICIENT_SEEDS |
| 002475 | dqn_without_nlp | double_dueling_dqn | one_day_return | normalized_plus | 2 | 222690 | 0.870755 | 0.158874 | INSUFFICIENT_SEEDS |
| 002475 | dqn_without_nlp | vanilla_dqn | one_day_return | normalized_plus | 2 | 178594 | 0.679926 | 0.126199 | INSUFFICIENT_SEEDS |
| 002475 | dqn_with_marketwide_impact_nlp | double_dueling_dqn | one_day_return | normalized_plus | 5 | 331277 | 1.53256 | 0.268333 | UNSTABLE |
| 002475 | dqn_with_marketwide_impact_nlp | double_dueling_dqn | three_day_return | normalized_plus | 5 | 191963 | 0.899168 | 0.164555 | STABLE |
| 002475 | dqn_with_marketwide_impact_nlp | vanilla_dqn | one_day_return | normalized_plus | 5 | 186796 | 1.03301 | 0.150793 | STABLE |
| 002475 | dqn_with_marketwide_impact_nlp | vanilla_dqn | three_day_return | normalized_plus | 5 | 214310 | 1.32106 | 0.174255 | STABLE |
| 002475 | dqn_with_marketwide_sentiment_nlp | double_dueling_dqn | one_day_return | normalized_plus | 5 | 232402 | 1.25351 | 0.165118 | STABLE |
| 002475 | dqn_with_marketwide_sentiment_nlp | double_dueling_dqn | three_day_return | normalized_plus | 5 | 199012 | 0.870155 | 0.157356 | STABLE |
| 002475 | dqn_with_marketwide_sentiment_nlp | vanilla_dqn | one_day_return | normalized_plus | 5 | 73567.5 | 0.52732 | 0.048037 | STABLE |
| 002475 | dqn_with_marketwide_sentiment_nlp | vanilla_dqn | three_day_return | normalized_plus | 5 | 251125 | 1.33774 | 0.206105 | UNSTABLE |
| 002475 | dqn_with_sector_impact_nlp | double_dueling_dqn | one_day_return | normalized_plus | 5 | 152870 | 0.96862 | 0.128619 | STABLE |
| 002475 | dqn_with_sector_impact_nlp | double_dueling_dqn | three_day_return | normalized_plus | 5 | 192988 | 1.06159 | 0.170994 | STABLE |
| 002475 | dqn_with_sector_impact_nlp | vanilla_dqn | one_day_return | normalized_plus | 5 | 123717 | 0.941628 | 0.0994353 | STABLE |
| 002475 | dqn_with_sector_impact_nlp | vanilla_dqn | three_day_return | normalized_plus | 5 | 302428 | 1.81223 | 0.245645 | UNSTABLE |
| 002475 | dqn_with_sector_sentiment_nlp | double_dueling_dqn | one_day_return | normalized_plus | 5 | 264154 | 1.31768 | 0.194061 | STABLE |
| 002475 | dqn_with_sector_sentiment_nlp | double_dueling_dqn | three_day_return | normalized_plus | 5 | 93761.7 | 0.542496 | 0.082747 | STABLE |
| 002475 | dqn_with_sector_sentiment_nlp | vanilla_dqn | one_day_return | normalized_plus | 5 | 220054 | 1.42791 | 0.152177 | STABLE |
| 002475 | dqn_with_sector_sentiment_nlp | vanilla_dqn | three_day_return | normalized_plus | 5 | 143384 | 1.28531 | 0.112309 | STABLE |
| 002475 | dqn_without_nlp | double_dueling_dqn | one_day_return | normalized_plus | 5 | 170517 | 1.07963 | 0.136823 | STABLE |
| 002475 | dqn_without_nlp | double_dueling_dqn | three_day_return | normalized_plus | 5 | 160154 | 0.657647 | 0.12336 | STABLE |
| 002475 | dqn_without_nlp | vanilla_dqn | one_day_return | normalized_plus | 5 | 157267 | 0.794702 | 0.131511 | STABLE |
| 002475 | dqn_without_nlp | vanilla_dqn | three_day_return | normalized_plus | 5 | 149688 | 1.09291 | 0.123383 | STABLE |

## 12. Key Findings
- Did the model-upgrade path solve flat/Hold-heavy behavior? Not fully; review HOLD_HEAVY/PARTIALLY_HOLD_HEAVY rows.
- Best model/reward/action/state combination: see top configurations table.
- NLP value-add: use NLP Incremental Effect Diagnosis; do not claim improvement unless labels say NLP_IMPROVES.
- Q-value confidence: no low-margin warning detected in available Q diagnostics.
- Seed stability: some configurations are unstable or insufficiently seeded.
- Report suitability: medium-test is preliminary; full-grid with sufficient seeds is preferred for formal claims.

## 13. Recommended Next Experiments
- Run full-grid with 20 seeds for the most promising configurations.
- Add or enable reward diagnostics for step-level reward attribution.
- Add paired NLP incremental effect analysis if missing.
- Run transaction-cost sensitivity before formal trading conclusions.
- Investigate any stock-specific HOLD_HEAVY rows before adding model complexity.
- Compare target_position against discrete_trade only after diagnostics are complete.

## 14. Report-Ready Summary Paragraph
The model-upgrade diagnostics evaluate DQN behavior across cached target-stock experiments using seed-level metrics, action distributions, environment dynamics, and Q-value traces where available. The dashboard and reports make it possible to inspect whether each DQN policy actively traded, how exposure evolved over time, whether NLP-enhanced groups improved over the no-NLP baseline, and whether performance was stable across seeds. Current conclusions should be treated as diagnostic unless full-grid seed coverage and paired NLP effect evidence are available.

## 15. Files Generated
| file | path | rows | targets |
| --- | --- | --- | --- |
| model_upgrade_summary.csv | outputs/model_upgrade/model_upgrade_summary.csv | 30 | 002475 |
| seed_level_metrics.csv | outputs/model_upgrade/seed_level_metrics.csv | 120 | 002475 |
| env_dynamics_diagnostics.csv | outputs/model_upgrade/env_dynamics_diagnostics.csv | 14520 | 002475 |
| action_distribution_diagnostics.csv | outputs/model_upgrade/action_distribution_diagnostics.csv | 1094 | 002475 |
| q_value_diagnostics.csv | outputs/model_upgrade/q_value_diagnostics.csv | 14520 | 002475 |
| exploration_diagnostics.csv | outputs/model_upgrade/exploration_diagnostics.csv | 5100 | 002475 |
| portfolio_curves.csv | outputs/model_upgrade/portfolio_curves.csv | 14520 | 002475 |
| reward_variant_comparison.csv | outputs/model_upgrade/reward_variant_comparison.csv | 30 | 002475 |
| state_feature_diagnostics.csv | outputs/model_upgrade/state_feature_diagnostics.csv | 720 | 002475 |
