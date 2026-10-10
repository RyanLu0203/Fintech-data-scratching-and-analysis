import numpy as np
import pandas as pd
import pytest

from src.evaluation.ablation import _metrics_from_log
from src.evaluation.model_upgrade import (
    NORMALIZED_PLUS_COLUMNS,
    _prepare_single_frame_pair,
)
from src.features.technical_indicators import add_trading_features
from src.nlp.chronological import (
    fit_frozen_peer_models,
    map_news_to_sessions,
    normalize_news,
    session_time,
)
from src.nlp.finbert_sentiment import FinBERTSentiment
from src.nlp.gold_label_evaluation import split_gold_labels, evaluate_models_on_gold
from src.nlp.peer_sentiment import _pseudo_label_peer_texts
from src.rl.trading_env import FinancialTradingEnv
from scripts.run_coursework import synthetic_inputs


def bars(opens=(10.0, 10.0, 10.0), closes=(10.0, 10.0, 10.0)):
    return pd.DataFrame(
        {
            "date": pd.bdate_range("2025-01-01", periods=len(opens)),
            "open": opens,
            "close": closes,
            "price": [9.0] * len(opens),
        }
    )


def test_constant_price_fees_reduce_reward_and_nav_and_process_final_day():
    env = FinancialTradingEnv(
        bars(), ["price"], initial_cash=1000, transaction_cost=0.01
    )
    rewards = []
    for action in [1, 0, 2]:
        _, reward, done, _ = env.step(action)
        rewards.append(reward)
    log = env.trading_log()
    assert done and len(log) == 3
    assert log.iloc[-1]["portfolio_value"] == pytest.approx(
        1000 - log["transaction_cost"].sum()
    )
    assert rewards[0] < 0 and rewards[2] < 0
    assert np.prod(1 + np.array(rewards)) == pytest.approx(
        log.iloc[-1]["portfolio_value"] / 1000
    )
    assert log["turnover"].sum() > log["transaction_cost"].sum() / 1000


def test_selling_at_open_preserves_overnight_gain_in_reward():
    env = FinancialTradingEnv(
        bars(opens=(10.0, 20.0, 20.0), closes=(10.0, 20.0, 20.0)),
        ["price"],
        initial_cash=1000,
        transaction_cost=0,
    )
    env.step(1)
    _, reward, _, _ = env.step(2)
    assert reward == pytest.approx(1.0)
    assert env.trading_log().iloc[-1]["portfolio_value"] == 2000


def test_insufficient_cash_buy_is_logged_as_hold():
    env = FinancialTradingEnv(bars(), ["price"], initial_cash=1, transaction_cost=0.01)
    _, _, _, info = env.step(1)
    assert info["mapped_action_label"] == "Buy"
    assert info["action"] == "Hold"
    assert env.trading_log().iloc[0]["executed_trade_value"] == 0


def test_account_ratios_do_not_read_same_day_close():
    data = bars()
    data["position_ratio"] = 0.0
    data["cash_ratio"] = 1.0
    env = FinancialTradingEnv(data, ["position_ratio", "cash_ratio"], initial_cash=1000)
    env.step(1)
    before = env._state()
    env.data.loc[1, "close"] = 100000
    np.testing.assert_array_equal(before, env._state())


def test_metrics_include_first_fee_loss_and_zero_volatility_is_undefined():
    log = pd.DataFrame(
        {
            "date": pd.date_range("2025-01-01", periods=3),
            "portfolio_value": [90.0, 90.0, 90.0],
            "reward": [-0.1, 0.0, 0.0],
            "position": 1.0,
            "action": ["Buy", "Hold", "Hold"],
        }
    )
    metric = _metrics_from_log("test", log, 100.0, 42)
    assert metric["max_drawdown"] == pytest.approx(0.1)
    assert metric["sharpe_ratio"] < 0
    log["portfolio_value"] = 100.0
    log["reward"] = 0.0
    assert np.isnan(_metrics_from_log("test", log, 100.0, 42)["sharpe_ratio"])


def test_normalized_features_ignore_same_day_close_and_zero_all_control_text():
    market, _, _, _ = synthetic_inputs()
    features = add_trading_features(market)
    assert len(features) > 30  # prevents vacuous no-lookahead checks on empty RSI
    features["sector_sentiment_score"] = 0.7
    train, test = features.iloc[:20], features.iloc[20:]
    before = _prepare_single_frame_pair(train, test, None, "normalized_plus", [])
    changed = test.copy()
    changed["close"] *= 9
    after = _prepare_single_frame_pair(train, changed, None, "normalized_plus", [])
    np.testing.assert_array_equal(
        before["test"][NORMALIZED_PLUS_COLUMNS], after["test"][NORMALIZED_PLUS_COLUMNS]
    )
    assert (
        (
            after["test"][
                [
                    "nlp_signal_score",
                    "abs_nlp_signal",
                    "nlp_signal_3day_mean",
                    "nlp_signal_change",
                ]
            ]
            == 0
        )
        .all()
        .all()
    )


def test_duplicate_grain_is_rejected():
    market, _, _, _ = synthetic_inputs()
    with pytest.raises(ValueError, match="one row"):
        add_trading_features(pd.concat([market, market.iloc[[0]]]))


def test_finbert_labels_do_not_collapse_probability_differences(monkeypatch):
    class Fake:
        def score(self, texts):
            from src.nlp.finbert_sentiment import FinBERTResult

            return FinBERTResult(
                ["positive", "negative", "neutral"][: len(texts)],
                [],
                [],
                [],
                [0.6, -0.7, 0.01][: len(texts)],
                "ok",
            )

    monkeypatch.setenv("ENABLE_FINBERT_PEER_LABELING", "1")
    monkeypatch.setattr("src.nlp.peer_sentiment.FinBERTSentiment", Fake)
    assert _pseudo_label_peer_texts(["a", "b", "c"])[0] == [1, -1, 0]


def test_finbert_missing_checkpoint_has_no_neutral_predictions(monkeypatch):
    scorer = FinBERTSentiment()
    monkeypatch.setattr(
        scorer, "_load", lambda: (_ for _ in ()).throw(FileNotFoundError("fixture"))
    )
    result = scorer.score(["profit growth"])
    assert result.status == "unavailable" and np.isnan(result.scores[0])
    assert result.labels == ["unavailable"]


def test_gold_duplicates_cannot_cross_test_boundary():
    labels = pd.DataFrame(
        {
            "text": ["Profit growth!", "profit  growth"],
            "gold_label_int": [1, 1],
            "split": ["train", "test"],
        }
    )
    with pytest.raises(ValueError, match="both train and test"):
        split_gold_labels(labels)


def test_all_gold_models_use_common_independent_test(monkeypatch):
    labels = pd.DataFrame(
        {
            "text": [f"profit growth {i}" for i in range(8)]
            + [f"loss risk {i}" for i in range(8)],
            "gold_label_int": [1] * 8 + [-1] * 8,
        }
    )
    train, test = split_gold_labels(labels)
    assert not set(train["text_key"]) & set(test["text_key"])
    monkeypatch.setattr(
        FinBERTSentiment,
        "_load",
        lambda _: (_ for _ in ()).throw(FileNotFoundError("fixture")),
    )
    rows = evaluate_models_on_gold(labels)
    assert rows["test_rows"].eq(len(test)).all()
    assert rows["training_rows"].eq(len(train)).all()
    assert len(rows.attrs["predictions"]) == len(test)


def test_after_close_and_weekend_news_wait_until_next_open():
    news = normalize_news(
        pd.DataFrame(
            {
                "news_id": ["a", "b"],
                "symbol": "002475",
                "text": ["profit growth", "loss risk"],
                "source": "fixture",
                "available_at": [
                    "2025-01-03T18:00:00+08:00",
                    "2025-01-05T10:00:00+08:00",
                ],
            }
        )
    )
    mapped = map_news_to_sessions(
        news, pd.Series(pd.to_datetime(["2025-01-03", "2025-01-06"]))
    )
    assert mapped["decision_date"].eq(pd.Timestamp("2025-01-06")).all()


def test_appended_future_peer_data_cannot_change_frozen_models():
    market, target, peers, peer_market = synthetic_inputs()
    peers = normalize_news(peers)
    fit_end = session_time(market["date"].iloc[[19]], 15).iloc[0]
    base_models, base_meta, base_labels = fit_frozen_peer_models(
        peers, peer_market, "002475", fit_end
    )
    future = peers[peers["available_at"] > fit_end].copy()
    future["text"] = "never seen future bankruptcy token"
    changed_market = peer_market.copy()
    changed_market.loc[changed_market["date"] > market["date"].iloc[19], "close"] *= 100
    new_models, new_meta, new_labels = fit_frozen_peer_models(
        pd.concat([peers[peers["available_at"] <= fit_end], future]),
        changed_market,
        "002475",
        fit_end,
    )
    assert base_meta == new_meta
    pd.testing.assert_frame_equal(base_labels, new_labels)
    for name in base_models:
        np.testing.assert_array_equal(
            base_models[name].predict_proba(["profit growth"]),
            new_models[name].predict_proba(["profit growth"]),
        )
    assert base_labels["label_end_at"].le(fit_end).all()
