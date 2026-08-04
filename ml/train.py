"""Train LightGBM on the gold table, compare against persistence and
HAR-RV baselines on a purged time-based test split, log to MLflow.

Run: make train   (stack must be up; MLflow at localhost:5001)

Honest-results policy from SPEC.md: if LightGBM does not beat both
baselines, that is reported, not hidden. All three models are evaluated
on the identical test rows.

Promotion gate: every run is logged to MLflow, but a new *registered*
version is only created when the model beats persistence on the held-out
test split (skill >= PROMOTION_MIN_SKILL). The API serves
models:/vol-forecast-lgbm/latest, so a rejected run leaves the previous
champion in place instead of shipping a regression. (Bootstrap: if the
registry is empty, the first model is registered regardless so there is
something to serve.)
"""

from __future__ import annotations

import os
import sys

import lightgbm as lgb
import mlflow
import pandas as pd

from .baselines import HarRV, mae, persistence, rmse
from .data import FEATURES, LABEL, load_gold, time_split

TRACKING_URI = os.environ.get("MLFLOW_TRACKING_URI", "http://localhost:5001")
MODEL_NAME = "vol-forecast-lgbm"
# Minimum test-split skill vs persistence (1 - lgbm_mae/persistence_mae) for a
# model to be promoted. 0.0 = must at least tie persistence. Raise it (e.g.
# 0.02) to require a real margin and avoid churning on noise-level ties.
PROMOTION_MIN_SKILL = float(os.environ.get("PROMOTION_MIN_SKILL", "0.0"))

LGB_PARAMS = {
    # small trees + strong regularization: minute-level rows are few and
    # heavily autocorrelated, and n is tiny in early runs
    "n_estimators": 300,
    "learning_rate": 0.05,
    "num_leaves": 15,
    "min_child_samples": 10,
    "subsample": 0.9,
    "subsample_freq": 1,
    "colsample_bytree": 0.8,
    "reg_lambda": 1.0,
    "verbose": -1,
}


def evaluate(name: str, y, pred, per_product: pd.Series) -> dict[str, float]:
    out = {f"{name}_mae": mae(y, pred), f"{name}_rmse": rmse(y, pred)}
    for product in per_product.unique():
        m = (per_product == product).to_numpy()
        out[f"{name}_mae_{product.replace('-', '_')}"] = mae(y[m], pred[m])
    return out


def _has_registered_versions() -> bool:
    """True if the registry already holds a champion. Only a definitive empty
    result lets us bootstrap-promote; any error is treated conservatively as
    "an incumbent exists" so a registry hiccup can never bootstrap-promote a
    model that failed the skill gate. Requires the tracking URI to be set."""
    try:
        client = mlflow.MlflowClient()
        return len(client.search_model_versions(f"name = '{MODEL_NAME}'")) > 0
    except Exception as e:
        print(f"WARN: could not query registry ({e}); assuming an incumbent exists")
        return True


def run() -> dict:
    """Train, evaluate, log, and gate promotion. Returns a decision dict:
    status in {promoted, rejected, insufficient_data}."""
    # Point at the real MLflow server FIRST — the promotion gate queries the
    # registry, and an unset URI silently hits the empty local file store
    # (which would look like an empty registry and bootstrap-promote anything).
    mlflow.set_tracking_uri(TRACKING_URI)
    mlflow.set_experiment("vol-forecast")

    df = load_gold()
    if len(df) < 60:
        print(f"only {len(df)} usable gold rows — need more accumulated data")
        return {"status": "insufficient_data", "n_rows": len(df)}
    train, test, cut = time_split(df)
    print(f"rows={len(df)} train={len(train)} test={len(test)} cut={cut}")

    for frame in (train, test):
        frame["product_cat"] = frame["product_id"].astype("category")
    feature_cols = [*FEATURES, "product_cat"]
    y_test = test[LABEL].to_numpy()

    model = lgb.LGBMRegressor(**LGB_PARAMS)
    model.fit(train[feature_cols], train[LABEL], categorical_feature=["product_cat"])

    metrics: dict[str, float] = {}
    preds = {
        "persistence": persistence(test),
        "har": HarRV().fit(train).predict(test),
        "lgbm": model.predict(test[feature_cols]),
    }
    for name, pred in preds.items():
        metrics.update(evaluate(name, y_test, pred, test["product_id"]))
    for name in ("har", "lgbm"):
        metrics[f"{name}_skill_vs_persistence"] = (
            1 - metrics[f"{name}_mae"] / metrics["persistence_mae"]
        )

    # Promotion gate: beat persistence on the held-out split (or bootstrap an
    # empty registry). Decided before the run so we can tag it on the record.
    skill = metrics["lgbm_skill_vs_persistence"]
    bootstrap = not _has_registered_versions()
    promote = bool(skill >= PROMOTION_MIN_SKILL or bootstrap)

    version = None
    with mlflow.start_run():
        mlflow.log_params(LGB_PARAMS)
        mlflow.log_params(
            {
                "features": ",".join(feature_cols),
                "split_cut": str(cut),
                "n_train": len(train),
                "n_test": len(test),
                "products": ",".join(sorted(df["product_id"].unique())),
                "promotion_min_skill": PROMOTION_MIN_SKILL,
            }
        )
        mlflow.log_metrics(metrics)
        mlflow.log_metric("promoted", 1.0 if promote else 0.0)
        mlflow.set_tag("promoted", str(promote).lower())
        mlflow.set_tag("promotion_reason", "bootstrap" if bootstrap else "skill_gate")
        importance = pd.Series(model.feature_importances_, index=feature_cols).sort_values(
            ascending=False
        )
        mlflow.log_text(importance.to_string(), "feature_importance.txt")
        # Always log the artifact (full record); only REGISTER a new version
        # when the gate passes, so models:/…/latest never regresses.
        mlflow.lightgbm.log_model(model, artifact_path="model")
        run_id = mlflow.active_run().info.run_id
        if promote:
            version = mlflow.register_model(f"runs:/{run_id}/model", MODEL_NAME).version

    print(f"\nMLflow run {run_id}")
    print(f"{'model':<12} {'MAE':>12} {'RMSE':>12}")
    for name in ("persistence", "har", "lgbm"):
        print(f"{name:<12} {metrics[f'{name}_mae']:>12.3e} {metrics[f'{name}_rmse']:>12.3e}")
    print(
        f"\nskill vs persistence: har={metrics['har_skill_vs_persistence']:+.1%} lgbm={skill:+.1%}"
    )
    if promote:
        tag = "bootstrap" if bootstrap else f"skill {skill:+.1%} >= {PROMOTION_MIN_SKILL:+.1%}"
        print(
            f"\nPROMOTED -> registered {MODEL_NAME} v{version} ({tag}). "
            "API auto-reloads it within ~5 min (no restart)."
        )
    else:
        print(
            f"\nREJECTED -> skill {skill:+.1%} < gate {PROMOTION_MIN_SKILL:+.1%}; "
            "kept the current champion (no new version registered)."
        )
    print("\ntop features:")
    print(importance.head(6).to_string())
    return {
        "status": "promoted" if promote else "rejected",
        "skill": skill,
        "version": version,
        "n_train": len(train),
        "n_test": len(test),
    }


def main() -> int:
    """CLI entrypoint. Exit 0 on a clean run (promoted OR a healthy rejection);
    exit 1 only when there was not enough data to train at all."""
    result = run()
    return 1 if result["status"] == "insufficient_data" else 0


if __name__ == "__main__":
    sys.exit(main())
