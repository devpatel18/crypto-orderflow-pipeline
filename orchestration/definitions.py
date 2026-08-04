"""Dagster assets: gold build (incremental — rerunning IS the backfill,
it catches up from wherever the bars tables are), data-quality gates,
and model retraining.

Run the UI:      make dagster            (http://localhost:3001)
One-off refresh: dagster asset materialize -f orchestration/definitions.py --select '*'
"""

from __future__ import annotations

from pathlib import Path

from dagster import (
    Backoff,
    DefaultScheduleStatus,
    Definitions,
    Jitter,
    MaterializeResult,
    RetryPolicy,
    ScheduleDefinition,
    asset,
    define_asset_job,
)

_SCRIPTS = Path(__file__).resolve().parent.parent / "scripts"
GOLD_SQL = _SCRIPTS / "gold_features.sql"
MAINT_SQL = _SCRIPTS / "iceberg_maintenance.sql"

# Trino gets recycled periodically (Docker/host on the dev laptop), dropping any
# in-flight connection mid-query — a single blip otherwise fails a whole run.
# Each retry re-runs the op with a fresh _trino() connection; the gold build is
# incremental+idempotent (only adds minutes past max(as_of_ts)) and Iceberg
# INSERTs commit atomically, so re-running is always safe. Exponential backoff
# 10/20/40s (+jitter) comfortably spans a Trino restart (~10-30s to healthy).
TRINO_RETRY = RetryPolicy(max_retries=3, delay=10, backoff=Backoff.EXPONENTIAL, jitter=Jitter.PLUS_MINUS)


def _statements(sql_text: str) -> list[str]:
    """Split a multi-statement .sql file into individual statements, dropping
    -- comment lines (the trino client executes one statement per call)."""
    out = []
    for chunk in sql_text.split(";"):
        stmt = "\n".join(ln for ln in chunk.splitlines() if not ln.strip().startswith("--")).strip()
        if stmt:
            out.append(stmt)
    return out


def _trino():
    from trino.dbapi import connect

    return connect(host="localhost", port=8080, user="dagster", catalog="iceberg", schema="market")


def _scalar(cur, sql: str):
    cur.execute(sql)
    return cur.fetchone()[0]


@asset(group_name="gold", retry_policy=TRINO_RETRY)
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


@asset(deps=[gold_features], group_name="gold", retry_policy=TRINO_RETRY)
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


@asset(group_name="maintenance", retry_policy=TRINO_RETRY)
def iceberg_maintenance(context) -> MaterializeResult:
    """Compact tiny streaming files (10s commits bloat the warehouse) and
    expire old snapshots so query planning stays fast and storage bounded.
    Independent of the gold/model chain; runs on its own 4h schedule."""
    cur = _trino().cursor()
    stmts = _statements(MAINT_SQL.read_text())
    for stmt in stmts:
        cur.execute(stmt)
        cur.fetchall()
    context.log.info(f"ran {len(stmts)} maintenance statements")
    return MaterializeResult(metadata={"statements": len(stmts)})


@asset(deps=[gold_quality], group_name="model", retry_policy=TRINO_RETRY)
def volatility_model(context) -> MaterializeResult:
    """Retrain LightGBM vs baselines on all accumulated gold data and log
    to MLflow. Promotion is gated: a new registry version is created only
    when the model beats persistence on the held-out split (else the
    current champion stands). A rejection is a healthy outcome, not a
    failure — only genuinely-insufficient data fails the asset. The API
    picks up a promoted model on its next restart."""
    from ml.train import run

    result = run()
    if result["status"] == "insufficient_data":
        raise RuntimeError(f"training aborted: only {result.get('n_rows')} usable gold rows")
    context.log.info(
        f"retrain {result['status']}: skill_vs_persistence={result['skill']:+.3f} "
        f"registered_version={result['version']}"
    )
    return MaterializeResult(
        metadata={
            "status": result["status"],
            "skill_vs_persistence": round(result["skill"], 4),
            "registered_version": (
                str(result["version"]) if result["version"] else "none (rejected)"
            ),
            "n_train": result["n_train"],
            "n_test": result["n_test"],
        }
    )


gold_job = define_asset_job("gold_refresh", selection=["gold_features", "gold_quality"])
retrain_job = define_asset_job("retrain", selection=["volatility_model"])
maintenance_job = define_asset_job("maintenance", selection=["iceberg_maintenance"])

defs = Definitions(
    assets=[gold_features, gold_quality, iceberg_maintenance, volatility_model],
    jobs=[gold_job, retrain_job, maintenance_job],
    schedules=[
        # default_status=RUNNING so they start firing as soon as the Dagster
        # daemon is up — no manual toggle in the UI. They still only run while
        # the daemon is alive (make dagster / a persistent process).
        ScheduleDefinition(
            job=gold_job,
            cron_schedule="*/15 * * * *",
            default_status=DefaultScheduleStatus.RUNNING,
        ),
        ScheduleDefinition(
            job=maintenance_job,
            cron_schedule="0 */4 * * *",  # every 4 hours
            default_status=DefaultScheduleStatus.RUNNING,
        ),
        ScheduleDefinition(
            job=retrain_job,
            cron_schedule="0 7 * * *",
            default_status=DefaultScheduleStatus.RUNNING,
        ),
    ],
)
