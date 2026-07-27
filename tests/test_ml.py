"""Pure unit tests for the ML layer (no stack required)."""

import numpy as np
import pandas as pd

from ml.baselines import HAR_COLS, HarRV, mae, persistence, rmse
from ml.data import LABEL, time_split


def synthetic_gold(n=200, products=("BTC-USD", "ETH-USD")):
    rng = np.random.default_rng(7)
    ts = pd.date_range("2026-07-27 00:00", periods=n, freq="1min")
    frames = []
    for p in products:
        df = pd.DataFrame(
            {
                "product_id": p,
                "as_of_ts": ts,
                "label_ts": ts + pd.Timedelta(minutes=5),
                "rv_1m": rng.uniform(0.0005, 0.002, n),
                "rv_5m": rng.uniform(0.0005, 0.002, n),
                "rv_15m": rng.uniform(0.0005, 0.002, n),
                "rv_60m": rng.uniform(0.0005, 0.002, n),
            }
        )
        frames.append(df)
    return pd.concat(frames, ignore_index=True)


def test_time_split_is_purged():
    df = synthetic_gold()
    df[LABEL] = 0.001
    train, test, cut = time_split(df, test_frac=0.2)
    # every test row strictly at/after the cut; every train label fully
    # realized before the cut — no bar can appear on both sides
    assert (test["as_of_ts"] >= cut).all()
    assert (train["label_ts"] <= cut).all()
    assert train["as_of_ts"].max() < test["as_of_ts"].min()
    # the 5-minute purge gap actually removed rows
    assert len(train) + len(test) < len(df)
    # both products present on both sides
    assert set(train["product_id"]) == set(test["product_id"]) == {"BTC-USD", "ETH-USD"}


def test_har_recovers_linear_coefficients():
    df = synthetic_gold(n=500)
    true = np.array([0.5, 0.3, 0.15, 0.05])
    df[LABEL] = sum(c * df[col] for c, col in zip(true, HAR_COLS, strict=True)) + 1e-4
    fitted = HarRV().fit(df)
    np.testing.assert_allclose(fitted.coef_[:4], true, atol=1e-8)
    np.testing.assert_allclose(fitted.coef_[4], 1e-4, atol=1e-8)
    np.testing.assert_allclose(fitted.predict(df), df[LABEL], atol=1e-8)


def test_persistence_is_current_rv():
    df = synthetic_gold(n=10)
    np.testing.assert_array_equal(persistence(df), df["rv_5m"].to_numpy())


def test_metrics():
    y = np.array([1.0, 2.0, 3.0])
    pred = np.array([1.0, 2.0, 5.0])
    assert mae(y, pred) == 2.0 / 3
    assert rmse(y, pred) == np.sqrt(4.0 / 3)
