"""Online feature computation — MUST mirror scripts/gold_features.sql.

Any change here without the matching SQL change (or vice versa) breaks
online/offline parity; tests/test_parity.py compares real rows from both
paths. Semantics mirrored deliberately:
- windows are half-open (as_of - X, as_of], bar at exactly as_of included
- returns are ln(mid_t / mid_prev_bar) over the ordered bar sequence, so
  the first in-window return reaches back to the bar before the window
- taker_imb is None when there were no trades (SQL NULL)
"""

from __future__ import annotations

import math
from datetime import datetime, timedelta

FEATURE_KEYS = [
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


def compute_features(book_bars: list[dict], trade_bars: list[dict], as_of: datetime) -> dict | None:
    """book_bars/trade_bars: bar dicts with parsed 'bar_ts' datetimes,
    covering at least (as_of - 61min, as_of]. Returns a feature dict or
    None when the 60m window has no usable returns."""
    lo_60 = as_of - timedelta(minutes=60)
    lo_15 = as_of - timedelta(minutes=15)
    lo_5 = as_of - timedelta(minutes=5)
    lo_1 = as_of - timedelta(minutes=1)

    book = sorted((b for b in book_bars if b["bar_ts"] <= as_of), key=lambda b: b["bar_ts"])
    sq = {"rv_1m": 0.0, "rv_5m": 0.0, "rv_15m": 0.0, "rv_60m": 0.0}
    ofi = 0.0
    spread_sum = 0.0
    spread_max = None
    bid_depth_sum = 0.0
    ask_depth_sum = 0.0
    bar_count_5m = 0
    prev_mid = None
    for b in book:
        ts = b["bar_ts"]
        mid = b["mid_close"]
        if prev_mid is not None and ts > lo_60:
            r2 = math.log(mid / prev_mid) ** 2
            sq["rv_60m"] += r2
            if ts > lo_15:
                sq["rv_15m"] += r2
            if ts > lo_5:
                sq["rv_5m"] += r2
            if ts > lo_1:
                sq["rv_1m"] += r2
        prev_mid = mid
        if ts > lo_5:
            bar_count_5m += 1
            ofi += b["ofi_sum"]
            spread_sum += b["spread_mean"]
            spread_max = b["spread_max"] if spread_max is None else max(spread_max, b["spread_max"])
            bid_depth_sum += b["bid_depth5_mean"]
            ask_depth_sum += b["ask_depth5_mean"]

    if bar_count_5m == 0 or sq["rv_60m"] == 0.0:
        return None

    bid_depth = bid_depth_sum / bar_count_5m
    ask_depth = ask_depth_sum / bar_count_5m
    depth_denom = bid_depth + ask_depth

    trade_count = 0
    volume = 0.0
    notional = 0.0
    maker_buy = 0.0
    maker_sell = 0.0
    for t in trade_bars:
        if lo_5 < t["bar_ts"] <= as_of:
            trade_count += t["trade_count"]
            volume += t["volume"]
            notional += t["notional"]
            maker_buy += t["maker_buy_volume"]
            maker_sell += t["maker_sell_volume"]

    return {
        "rv_1m": math.sqrt(sq["rv_1m"]),
        "rv_5m": math.sqrt(sq["rv_5m"]),
        "rv_15m": math.sqrt(sq["rv_15m"]),
        "rv_60m": math.sqrt(sq["rv_60m"]),
        "ofi_5m": ofi,
        "spread_mean_5m": spread_sum / bar_count_5m,
        "spread_max_5m": spread_max,
        "depth_imb_5m": (bid_depth - ask_depth) / depth_denom if depth_denom else None,
        "trade_count_5m": trade_count,
        "volume_5m": volume,
        "notional_5m": notional,
        "taker_imb_5m": (maker_sell - maker_buy) / volume if volume > 0 else None,
        "bar_count_5m": bar_count_5m,
    }
