"""Dagster assets: gold build (incremental — rerunning IS the backfill,
it catches up from wherever the bars tables are), data-quality gates,
and model retraining.

Run the UI:      make dagster            (http://localhost:3001)
One-off refresh: dagster asset materialize -f orchestration/definitions.py --select '*'
"""

from __future__ import annotations

from pathlib import Path

from dagster import (
    Definitions,
    MaterializeResult,
    ScheduleDefinition,
    asset,
    define_asset_job,
)

GOLD_SQL = Path(__file__).resolve().parent.parent / "scripts" / "gold_features.sql"


def _trino():
    from trino.dbapi import connect

    return connect(host="localhost", port=8080, user="dagster", catalog="iceberg", schema="market")


def _scalar(cur, sql: str):
    cur.execute(sql)
    return cur.fetchone()[0]


@asset(group_name="gold")
def gold_features(context) -> MaterializeResult:
    """Incrementally extend features_5m from the 1s bars. Idempotent:
    only minutes past each product's max(as_of_ts) are added, so this
    doubles as backfill after any pipeline downtime."""
    conn = _trino()
    cur = conn.cursor()
    before = _scalar(cur, "SELECT count(*) FROM features_5m")
    cur.execute(GOLD_SQL.read_text())
    cur.fetchall()  # drive the INSERT to completion
    after = _scalar(cur, "SELECT count(*) FROM features_5m")
    context.log.info(f"gold rows: {before} -> {after} (+{after - before})")
    return MaterializeResult(metadata={"rows_added": after - before, "rows_total": after})


@asset(deps=[gold_features], group_name="gold")
def gold_quality(context) -> MaterializeResult:
    """Hard quality gates on the gold table; raises on violation."""
    cur = _trino().cursor()
    dupes = _scalar(
        cur, "SELECT count(*) - count(DISTINCT (product_id, as_of_ts)) FROM features_5m"
    )
    bad_label_ts = _scalar(
        cur,
        "SELECT count(*) FROM features_5m WHERE label_ts != as_of_ts + interval '5' minute",
    )
    low_coverage = _scalar(
        cur,
        "SELECT count(*) FROM features_5m WHERE bar_count_5m < 250 OR label_bar_count < 250",
    )
    if dupes or bad_label_ts:
        raise ValueError(f"gold corrupt: dupes={dupes} bad_label_ts={bad_label_ts}")
    context.log.info(f"quality ok; low_coverage rows (excluded from training): {low_coverage}")
    return MaterializeResult(metadata={"low_coverage_rows": low_coverage})


@asset(deps=[gold_quality], group_name="model")
def volatility_model(context) -> MaterializeResult:
    """Retrain LightGBM vs baselines on all accumulated gold data and log
    to MLflow. The registry gets a new model version; the API picks it
    up on its next restart."""
    from ml.train import main

    rc = main()
    if rc != 0:
        raise RuntimeError("training aborted (insufficient gold rows?)")
    return MaterializeResult()


gold_job = define_asset_job("gold_refresh", selection=["gold_features", "gold_quality"])
retrain_job = define_asset_job("retrain", selection=["volatility_model"])

defs = Definitions(
    assets=[gold_features, gold_quality, volatility_model],
    jobs=[gold_job, retrain_job],
    schedules=[
        ScheduleDefinition(job=gold_job, cron_schedule="*/15 * * * *"),
        ScheduleDefinition(job=retrain_job, cron_schedule="0 7 * * *"),
    ],
)
