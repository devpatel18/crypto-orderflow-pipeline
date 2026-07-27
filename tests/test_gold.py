"""Point-in-time correctness of the gold feature table.

The strong test here re-derives sampled rows from the raw 1s bars:
- rv_5m must be reproducible using ONLY bars with bar_ts <= as_of_ts
- label_rv_5m must be reproducible using ONLY bars with bar_ts > as_of_ts
If either recomputation matches, the corresponding window cannot be
leaking data across the as_of_ts boundary.

Integration tests: they need the docker stack (Trino) and a built gold
table, and skip cleanly otherwise — CI runs them when the stack is up.
"""

import csv
import io
import math
import subprocess

import pytest


def trino(sql: str) -> list[dict]:
    proc = subprocess.run(
        ["docker", "exec", "trino", "trino", "--output-format=CSV_HEADER", "--execute", sql],
        capture_output=True,
        text=True,
        timeout=120,
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr[-500:])
    return list(csv.DictReader(io.StringIO(proc.stdout)))


def gold_available() -> bool:
    try:
        return int(trino("SELECT count(*) c FROM iceberg.market.features_5m")[0]["c"]) > 0
    except (RuntimeError, subprocess.SubprocessError, FileNotFoundError, IndexError):
        return False


pytestmark = pytest.mark.skipif(not gold_available(), reason="stack down or gold table empty")


def sample_rows(n: int = 3) -> list[dict]:
    return trino(
        "SELECT product_id, as_of_ts, label_ts, rv_5m, label_rv_5m, bar_count_5m "
        f"FROM iceberg.market.features_5m ORDER BY random() LIMIT {n}"
    )


def rv_from_bars(product: str, lo: str, hi: str) -> tuple[float, int]:
    """RV over (lo, hi] recomputed from book_bars_1s, mirroring the SQL:
    one bar before the window provides the boundary return's base."""
    rows = trino(
        "SELECT bar_ts, mid_close FROM iceberg.market.book_bars_1s "
        f"WHERE product_id = '{product}' AND bar_ts > timestamp '{lo}' - interval '10' second "
        f"AND bar_ts <= timestamp '{hi}' ORDER BY bar_ts"
    )
    acc, prev, count = 0.0, None, 0
    for r in rows:
        mid = float(r["mid_close"])
        if prev is not None and r["bar_ts"] > lo:
            ret = math.log(mid / prev)
            acc += ret * ret
            count += 1
        prev = mid
    return math.sqrt(acc), count


def test_features_use_only_past_data():
    for row in sample_rows():
        lo = row["as_of_ts"]  # window is (as_of - 5m, as_of]
        rv, _ = rv_from_bars(
            row["product_id"],
            trino(f"SELECT timestamp '{lo}' - interval '5' minute t")[0]["t"],
            lo,
        )
        assert math.isclose(rv, float(row["rv_5m"]), rel_tol=1e-9), (
            f"rv_5m not reproducible from past-only bars for {row}"
        )


def test_label_uses_only_future_data():
    for row in sample_rows():
        rv, _ = rv_from_bars(row["product_id"], row["as_of_ts"], row["label_ts"])
        assert math.isclose(rv, float(row["label_rv_5m"]), rel_tol=1e-9), (
            f"label_rv_5m not reproducible from future-only bars for {row}"
        )


def test_label_ts_is_five_minutes_forward():
    bad = trino(
        "SELECT count(*) c FROM iceberg.market.features_5m "
        "WHERE label_ts != as_of_ts + interval '5' minute"
    )
    assert int(bad[0]["c"]) == 0


def test_no_duplicate_keys():
    dup = trino(
        "SELECT count(*) - count(DISTINCT (product_id, as_of_ts)) c FROM iceberg.market.features_5m"
    )
    assert int(dup[0]["c"]) == 0


def test_window_coverage_and_ranges():
    row = trino(
        "SELECT min(bar_count_5m) b, min(label_bar_count) l, min(rv_5m) r, "
        "min(spread_max_5m - spread_mean_5m) s FROM iceberg.market.features_5m"
    )[0]
    assert int(row["b"]) >= 250, "feature window has large bar gaps"
    assert int(row["l"]) >= 250, "label window has large bar gaps"
    assert float(row["r"]) >= 0
    assert float(row["s"]) >= 0, "spread_max below spread_mean"
