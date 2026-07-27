"""Train LightGBM on the gold table, compare against persistence and
HAR-RV baselines on a purged time-based test split, log to MLflow.

Run: make train   (stack must be up; MLflow at localhost:5001)

Honest-results policy from SPEC.md: if LightGBM does not beat both
baselines, that is reported, not hidden. All three models are evaluated
on the identical test rows.
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


def main() -> int:
    df = load_gold()
    if len(df) < 60:
        print(f"only {len(df)} usable gold rows — need more accumulated data")
        return 1
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

    mlflow.set_tracking_uri(TRACKING_URI)
    mlflow.set_experiment("vol-forecast")
    with mlflow.start_run():
        mlflow.log_params(LGB_PARAMS)
        mlflow.log_params(
            {
                "features": ",".join(feature_cols),
                "split_cut": str(cut),
                "n_train": len(train),
                "n_test": len(test),
                "products": ",".join(sorted(df["product_id"].unique())),
            }
        )
        mlflow.log_metrics(metrics)
        importance = pd.Series(model.feature_importances_, index=feature_cols).sort_values(
            ascending=False
        )
        mlflow.log_text(importance.to_string(), "feature_importance.txt")
        mlflow.lightgbm.log_model(model, artifact_path="model", registered_model_name=MODEL_NAME)
        run_id = mlflow.active_run().info.run_id

    print(f"\nMLflow run {run_id}")
    print(f"{'model':<12} {'MAE':>12} {'RMSE':>12}")
    for name in ("persistence", "har", "lgbm"):
        print(f"{name:<12} {metrics[f'{name}_mae']:>12.3e} {metrics[f'{name}_rmse']:>12.3e}")
    print(
        f"\nskill vs persistence: har={metrics['har_skill_vs_persistence']:+.1%} "
        f"lgbm={metrics['lgbm_skill_vs_persistence']:+.1%}"
    )
    print("\ntop features:")
    print(importance.head(6).to_string())
    return 0


if __name__ == "__main__":
    sys.exit(main())
