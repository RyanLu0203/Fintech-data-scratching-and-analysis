# Model-Upgrade Experiment Rationale

## 1. Background

The original research framework remains unchanged: financial news and peer-trained NLP signals are combined with market data, passed into an NLP-enhanced DQN trading state, and evaluated through no-NLP versus NLP trading comparisons. The core research question is still whether peer-trained NLP sentiment and market-impact signals improve DQN-based trading decisions. The target-stock exclusion principle also remains unchanged: the target stock is held out from the peer NLP training corpus.

## 2. Problem With The Old Official DQN Experiment

The old official DQN path was too conservative for reliable interpretation. It produced too many Hold actions, low trading frequency, flat or nearly flat portfolio curves, weak exposure to market opportunities, and final performance that often did not improve over buy-and-hold. Because the dashboard mostly showed aggregate curves and aggregate Buy / Sell / Hold counts, it was difficult to determine whether NLP signals actually affected individual trading decisions.

## 3. Why Model-Upgrade Was Introduced

The model-upgrade path was introduced to improve DQN learnability and interpretability without changing the research framework. `target_position` gives the model explicit exposure choices rather than only simple Buy / Sell / Hold actions. `normalized_plus` improves feature scaling and learning stability. Reward variants allow one-day and three-day return learning comparisons. Model variants allow vanilla DQN and double-dueling DQN behavior to be compared. Environment dynamics and Q-value diagnostics record every trading step, and the trade-by-trade dashboard makes the learned behavior inspectable.

## 4. What Changed

- Action mode changed from only `discrete_trade` to model-upgrade `target_position` diagnostics.
- State feature mode expanded from the official raw state to `normalized_plus`.
- The experiment grid now compares model variants, reward variants, and seeds.
- Diagnostics now include environment dynamics, Q-values, action distributions, portfolio curves, and exploration metadata.
- Reporting now includes `README_diagnostics.md` and `CHATGPT_DIAGNOSTIC_SUMMARY.md`.
- Dashboard main experiment visualizations now use latest model-upgrade outputs.

## 5. What Did Not Change

- Crawler logic did not change.
- Peer NLP generation did not change.
- Target-stock exclusion did not change.
- The original research question did not change.
- The no-NLP versus NLP comparison logic did not change.
- The sentiment versus market-impact signal comparison did not change.
- The project remains a DQN-based trading framework.

## 6. How To Interpret The New Results

Latest model-upgrade results are not directly identical to old official results. Curves may differ because action space, state features, seeds, reward variants, and portfolio timing are different. The model-upgrade output should be interpreted as the improved experiment version. NLP improvement should only be claimed when paired incremental effect diagnostics support it. More active trading alone does not prove that NLP adds value.

## 7. Current Limitations

- Medium-test is diagnostic and is not a final full experiment.
- `nlp_incremental_effect.csv` may still be missing for some runs.
- `reward_diagnostics.csv` may still be missing for some runs.
- A full grid with more seeds is still needed for formal conclusions.
- Transaction-cost sensitivity may still be needed.

## 8. Report-Ready Explanation

The experiment was upgraded because the initial official DQN implementation produced overly conservative policies with excessive Hold actions and nearly flat portfolio curves, making it difficult to evaluate whether peer-trained NLP signals influenced trading behavior. The upgraded experiment preserves the original financial-news, peer-NLP, target-exclusion, and no-NLP versus NLP comparison framework, but improves the trading interface and diagnostics through target-position actions, normalized state features, reward variants, model variants, step-level environment dynamics, and Q-value logging. These changes make DQN behavior more interpretable and allow the dashboard to inspect whether each NLP signal changes exposure, trading frequency, and portfolio outcomes across seeds.
