"""Unit tests for the online feature calculator, including the window
boundary semantics that must match scripts/gold_features.sql."""

import math
from datetime import datetime, timedelta

from service.feature_calc import compute_features

AS_OF = datetime(2026, 7, 27, 12, 0, 0)


def book_bar(ts, mid, spread_mean=1.0, spread_max=2.0, bid=10.0, ask=5.0, ofi=1.0):
    return {
        "bar_ts": ts,
        "mid_close": mid,
        "spread_mean": spread_mean,
        "spread_max": spread_max,
        "bid_depth5_mean": bid,
        "ask_depth5_mean": ask,
        "ofi_sum": ofi,
    }


def trade_bar(ts, count=2, volume=4.0, notional=100.0, buy=1.0, sell=3.0):
    return {
        "bar_ts": ts,
        "trade_count": count,
        "volume": volume,
        "notional": notional,
        "maker_buy_volume": buy,
        "maker_sell_volume": sell,
    }


def test_rv_and_window_boundaries():
    bars = [
        # exactly at as_of - 5m: excluded from the window but its mid is
        # the base of the first in-window return
        book_bar(AS_OF - timedelta(minutes=5), 100.0),
        book_bar(AS_OF - timedelta(seconds=2), 101.0),
        book_bar(AS_OF - timedelta(seconds=1), 102.0),
        book_bar(AS_OF, 103.0),  # bar at exactly as_of: included
    ]
    f = compute_features(bars, [], AS_OF)
    expected_rv = math.sqrt(
        math.log(101 / 100) ** 2 + math.log(102 / 101) ** 2 + math.log(103 / 102) ** 2
    )
    assert math.isclose(f["rv_5m"], expected_rv, rel_tol=1e-12)
    assert math.isclose(f["rv_60m"], expected_rv, rel_tol=1e-12)
    # rv_1m spans only the last three returns' bars (all within 1m here)
    assert math.isclose(f["rv_1m"], expected_rv, rel_tol=1e-12)
    assert f["bar_count_5m"] == 3  # boundary bar not counted
    assert f["ofi_5m"] == 3.0  # 1.0 per in-window bar


def test_bars_after_as_of_ignored():
    bars = [
        book_bar(AS_OF - timedelta(seconds=1), 100.0),
        book_bar(AS_OF, 101.0),
        book_bar(AS_OF + timedelta(seconds=1), 999.0),  # future: must not leak
    ]
    f = compute_features(bars, [], AS_OF)
    assert f["bar_count_5m"] == 2
    assert math.isclose(f["rv_5m"], abs(math.log(101 / 100)), rel_tol=1e-12)


def test_spread_and_depth_aggregation():
    bars = [
        book_bar(AS_OF - timedelta(seconds=2), 100.0, spread_mean=1.0, spread_max=3.0),
        book_bar(AS_OF - timedelta(seconds=1), 100.5, spread_mean=2.0, spread_max=5.0),
        book_bar(AS_OF, 101.0, spread_mean=3.0, spread_max=4.0),
    ]
    f = compute_features(bars, [], AS_OF)
    assert math.isclose(f["spread_mean_5m"], 2.0)
    assert f["spread_max_5m"] == 5.0
    assert math.isclose(f["depth_imb_5m"], (10 - 5) / 15)


def test_trade_features_and_taker_sign():
    book = [book_bar(AS_OF - timedelta(seconds=1), 100.0), book_bar(AS_OF, 101.0)]
    trades = [
        trade_bar(AS_OF - timedelta(seconds=30)),  # in window
        trade_bar(AS_OF - timedelta(minutes=5)),  # boundary: excluded
        trade_bar(AS_OF + timedelta(seconds=30)),  # future: excluded
    ]
    f = compute_features(book, trades, AS_OF)
    assert f["trade_count_5m"] == 2
    assert f["volume_5m"] == 4.0
    # maker sell 3, maker buy 1 -> taker-buy pressure positive
    assert math.isclose(f["taker_imb_5m"], (3 - 1) / 4)


def test_no_trades_gives_null_imbalance():
    book = [book_bar(AS_OF - timedelta(seconds=1), 100.0), book_bar(AS_OF, 101.0)]
    f = compute_features(book, [], AS_OF)
    assert f["taker_imb_5m"] is None
    assert f["volume_5m"] == 0.0


def test_empty_book_returns_none():
    assert compute_features([], [trade_bar(AS_OF)], AS_OF) is None
