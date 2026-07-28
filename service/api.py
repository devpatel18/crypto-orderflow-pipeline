"""FastAPI prediction service.

- Loads vol-forecast-lgbm from the MLflow registry at startup.
- A background loop polls Redis for new feature rows; each new
  (product, as_of) gets a model prediction plus the persistence
  baseline, written to Postgres and the `predictions` Kafka topic.
- Delayed label join: the features at minute T carry rv_5m over
  (T-5m, T] — that IS the realized label for the prediction made at
  T-5m, so labeling is a single UPDATE per new feature row.
- GET /accuracy reports rolling MAE/RMSE for model vs persistence over
  labeled predictions.

Run: make serve-api  (docs at http://localhost:8000/docs)
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timedelta

import mlflow.lightgbm
import pandas as pd
import psycopg
import redis.asyncio as aioredis
from aiokafka import AIOKafkaProducer
from fastapi import FastAPI, HTTPException

from ml.data import FEATURES
from producer.log import setup

log = logging.getLogger("service.api")

TRACKING_URI = os.environ.get("MLFLOW_TRACKING_URI", "http://localhost:5001")
MODEL_URI = os.environ.get("MODEL_URI", "models:/vol-forecast-lgbm/latest")
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:19092")
PG_DSN = os.environ.get("PG_DSN", "postgresql://market:market@localhost:5432/market")
PRODUCTS = [p.strip() for p in os.environ.get("PRODUCTS", "BTC-USD,ETH-USD").split(",")]
PREDICTIONS_TOPIC = "predictions"
MIN_BAR_COVERAGE = 250
POLL_S = 5.0

state: dict = {}


def feature_frame(feats: dict) -> pd.DataFrame:
    row = {k: feats.get(k) for k in FEATURES}
    # None (e.g. taker_imb_5m with no trades) must become NaN, not object dtype
    df = pd.DataFrame([row]).astype("float64")
    df["product_cat"] = pd.Categorical([feats["product_id"]], categories=sorted(PRODUCTS))
    return df


async def score_once(product: str) -> None:
    raw = await state["redis"].get(f"features:{product}:latest")
    if raw is None:
        return
    feats = json.loads(raw)
    as_of = datetime.fromisoformat(feats["as_of_ts"])
    if state["last_scored"].get(product) == as_of:
        return
    state["last_scored"][product] = as_of

    async with state["pg"].cursor() as cur:
        # label join first: this row's rv_5m realizes the T-5m prediction
        await cur.execute(
            "UPDATE predictions SET label_rv = %s, labeled_at = now() "
            "WHERE product_id = %s AND as_of_ts = %s AND label_rv IS NULL",
            (feats["rv_5m"], product, as_of - timedelta(minutes=5)),
        )
        if feats["bar_count_5m"] < MIN_BAR_COVERAGE:
            log.warning(
                "score.skipped_low_coverage",
                extra={"ctx": {"product": product, "as_of": feats["as_of_ts"]}},
            )
            return
        pred = float(state["model"].predict(feature_frame(feats))[0])
        record = {
            "product_id": product,
            "as_of_ts": feats["as_of_ts"],
            "label_ts": (as_of + timedelta(minutes=5)).isoformat(),
            "pred_rv": pred,
            "pred_persistence": feats["rv_5m"],
            "model_version": state["model_version"],
        }
        await cur.execute(
            "INSERT INTO predictions (product_id, as_of_ts, label_ts, pred_rv, "
            "pred_persistence, model_version, features) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (product_id, as_of_ts) DO NOTHING",
            (
                product,
                as_of,
                as_of + timedelta(minutes=5),
                pred,
                feats["rv_5m"],
                state["model_version"],
                json.dumps(feats),
            ),
        )
    await state["kafka"].send_and_wait(
        PREDICTIONS_TOPIC, json.dumps(record).encode(), key=product.encode()
    )
    log.info("score.predicted", extra={"ctx": record})


async def scoring_loop() -> None:
    while True:
        for product in PRODUCTS:
            try:
                await score_once(product)
            except Exception:
                log.exception("score.error")
        await asyncio.sleep(POLL_S)


@asynccontextmanager
async def lifespan(app: FastAPI):
    setup()
    mlflow.set_tracking_uri(TRACKING_URI)
    model = mlflow.lightgbm.load_model(MODEL_URI)
    client = mlflow.MlflowClient()
    version = client.get_latest_versions("vol-forecast-lgbm")[0].version
    state.update(
        model=model,
        model_version=str(version),
        redis=aioredis.from_url(REDIS_URL, decode_responses=True),
        pg=await psycopg.AsyncConnection.connect(PG_DSN, autocommit=True),
        kafka=AIOKafkaProducer(bootstrap_servers=BOOTSTRAP),
        last_scored={},
    )
    await state["kafka"].start()
    task = asyncio.create_task(scoring_loop())
    log.info("api.started", extra={"ctx": {"model_version": state["model_version"]}})
    yield
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task
    await state["kafka"].stop()
    await state["pg"].close()
    await state["redis"].aclose()


app = FastAPI(title="vol-forecast", lifespan=lifespan)


@app.get("/health")
async def health() -> dict:
    return {"status": "ok", "model_version": state.get("model_version")}


@app.get("/predictions/latest")
async def latest(product: str = "BTC-USD") -> dict:
    async with state["pg"].cursor() as cur:
        await cur.execute(
            "SELECT product_id, as_of_ts, label_ts, pred_rv, pred_persistence, "
            "label_rv, model_version FROM predictions WHERE product_id = %s "
            "ORDER BY as_of_ts DESC LIMIT 1",
            (product,),
        )
        row = await cur.fetchone()
    if row is None:
        raise HTTPException(404, f"no predictions for {product}")
    keys = [
        "product_id",
        "as_of_ts",
        "label_ts",
        "pred_rv",
        "pred_persistence",
        "label_rv",
        "model_version",
    ]
    return {
        k: (v.isoformat() if isinstance(v, datetime) else v) for k, v in zip(keys, row, strict=True)
    }


@app.get("/accuracy")
async def accuracy(window_minutes: int = 60) -> list[dict]:
    """Rolling MAE/RMSE of the model vs the persistence baseline over
    labeled predictions in the trailing window."""
    async with state["pg"].cursor() as cur:
        await cur.execute(
            "SELECT product_id, count(*), "
            "avg(abs(pred_rv - label_rv)), "
            "sqrt(avg(power(pred_rv - label_rv, 2))), "
            "avg(abs(pred_persistence - label_rv)), "
            "sqrt(avg(power(pred_persistence - label_rv, 2))) "
            "FROM predictions WHERE label_rv IS NOT NULL "
            "AND as_of_ts > now() - make_interval(mins => %s) GROUP BY 1",
            (window_minutes,),
        )
        rows = await cur.fetchall()
    return [
        {
            "product_id": r[0],
            "n_labeled": r[1],
            "model_mae": r[2],
            "model_rmse": r[3],
            "persistence_mae": r[4],
            "persistence_rmse": r[5],
            "skill_vs_persistence": (1 - r[2] / r[4]) if r[4] else None,
        }
        for r in rows
    ]
