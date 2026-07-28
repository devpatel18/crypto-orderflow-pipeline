"""Online/offline parity: feature rows written to Redis by the online
path (Kafka bars -> service/feature_calc.py) must match the gold rows
built by Trino SQL for the same (product, as_of) — same numbers, two
completely independent computation paths.

Integration test: needs the stack, the feature service having run, and
a fresh gold build. Skips otherwise.
"""

import json
import math
import subprocess

import pytest

from tests.test_gold import trino

TOL = 1e-7
COMPARE = [
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


def redis_features() -> dict[tuple[str, str], dict]:
    try:
        keys = subprocess.run(
            ["docker", "exec", "redis", "redis-cli", "--scan", "--pattern", "features:*"],
            capture_output=True,
            text=True,
            timeout=30,
            check=True,
        ).stdout.split()
    except (subprocess.SubprocessError, FileNotFoundError):
        return {}
    out = {}
    for key in keys:
        parts = key.split(":", 2)
        if len(parts) != 3 or parts[2] == "latest":
            continue
        raw = subprocess.run(
            ["docker", "exec", "redis", "redis-cli", "GET", key],
            capture_output=True,
            text=True,
            timeout=30,
        ).stdout.strip()
        if raw:
            # normalize "2026-07-27T22:19:00" -> "2026-07-27 22:19:00"
            out[(parts[1], parts[2].replace("T", " ")[:19])] = json.loads(raw)
    return out


def matched_pairs():
    online = redis_features()
    if not online:
        return []
    ts_list = ", ".join(f"timestamp '{ts}'" for _, ts in online)
    gold = trino(f"SELECT * FROM iceberg.market.features_5m WHERE as_of_ts IN ({ts_list})")
    pairs = []
    for g in gold:
        o = online.get((g["product_id"], g["as_of_ts"][:19]))
        if o:
            pairs.append((o, g))
    return pairs


def test_online_features_match_gold():
    pairs = matched_pairs()
    if len(pairs) < 3:
        pytest.skip("fewer than 3 overlapping online/offline rows yet")
    for online, gold in pairs:
        for col in COMPARE:
            g = gold[col]
            o = online[col]
            if g in ("", None) or o is None:
                assert (g in ("", None)) == (o is None), f"{col}: null mismatch"
                continue
            assert math.isclose(o, float(g), rel_tol=TOL, abs_tol=1e-12), (
                f"{col} parity broken at {gold['product_id']} {gold['as_of_ts']}: "
                f"online={o} gold={g}"
            )
