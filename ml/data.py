"""Gold table loading and leakage-safe time splitting."""

from __future__ import annotations

import pandas as pd

FEATURES = [
    "rv_1m",
    "rv_5m",
    "rv_15m",
    "rv_60m",
    "ofi_5m",
    "spread_mean_5m",
    "spread_max_5m",
    "depth_imb_5m",
    "trade_count_5m",
    "volume_5m",
    "notional_5m",
    "taker_imb_5m",
]
LABEL = "label_rv_5m"
MIN_BAR_COVERAGE = 290  # of 300 possible 1s bars per 5m window


def load_gold(host: str = "localhost", port: int = 8080) -> pd.DataFrame:
    from trino.dbapi import connect  # deferred: unit tests don't need trino

    conn = connect(host=host, port=port, user="ml", catalog="iceberg", schema="market")
    df = pd.read_sql(
        "SELECT * FROM features_5m "
        f"WHERE bar_count_5m >= {MIN_BAR_COVERAGE} "
        f"AND label_bar_count >= {MIN_BAR_COVERAGE} "
        "ORDER BY as_of_ts, product_id",
        conn,
    )
    return df.dropna(subset=[LABEL, "rv_5m"]).reset_index(drop=True)


def time_split(
    df: pd.DataFrame, test_frac: float = 0.2
) -> tuple[pd.DataFrame, pd.DataFrame, pd.Timestamp]:
    """Time-based split with purging, never random.

    The cut is a wall-clock instant (same for every product). Test rows
    have as_of_ts >= cut. Train rows must have label_ts <= cut: a train
    row whose forward-looking label window crosses the boundary would
    share bars with test features, so those rows are dropped entirely
    (a 5-minute purge gap).
    """
    ts = df["as_of_ts"].sort_values().unique()
    cut = pd.Timestamp(ts[int(len(ts) * (1 - test_frac))])
    train = df[df["label_ts"] <= cut]
    test = df[df["as_of_ts"] >= cut]
    return train, test, cut
