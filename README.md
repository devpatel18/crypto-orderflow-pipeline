# Real-Time Market Microstructure Platform

Streaming platform that ingests Coinbase market data through Kafka (Redpanda),
computes order-flow features in Apache Flink, persists them to Apache Iceberg,
and serves 5-minute realized-volatility forecasts through a FastAPI
microservice — with a delayed-label evaluation loop producing live rolling
accuracy against a persistence baseline.

Full design: [docs/SPEC.md](docs/SPEC.md)

## Quick start

Requires Docker Desktop and ~6 GB of RAM available to Docker.

```sh
make up      # start Redpanda, Flink, MinIO, Trino, Postgres, Redis
make smoke   # health checks + Kafka produce/consume round trip
make down    # stop
```

## Status

| Phase | Scope | Status |
|---|---|---|
| 0 | Infrastructure: compose stack, health checks, smoke test | done |
| 1 | WebSocket → Kafka producer with reconnect + DLQ | done |
| 2 | Flink → Iceberg 1-second bars, exactly-once | done |
| 3 | Features + forward RV labels, point-in-time correct | done |
| 4 | LightGBM vs persistence and HAR-RV baselines | done — model now edges out persistence on held-out data as data accumulated (see Results); nightly retrain gated on beating the baseline |
| 5 | Online scoring + delayed label join | done — online/offline parity test passing; live rolling accuracy at `:8010/accuracy` |
| 6 | Dagster, Prometheus, Grafana | done — dashboard at `localhost:3000`, Dagster at `localhost:3001` |

## Results

Forecasting 5-minute realized volatility, evaluated honestly against a
**persistence** baseline (predict that the next 5 min looks like the last) —
a deliberately strong baseline, since volatility is highly autocorrelated.

- **Held-out skill vs persistence:** the current model beats persistence by
  **~+3.5%** MAE on a purged, time-based test split (skill = `1 − MAE_model /
  MAE_persistence`). Earlier retrains have landed in the **+3% to +9%** range as
  data accumulated.
- **Live rolling skill:** roughly a **tie-to-slight-edge** on streaming data —
  it oscillates within ±20–40% hour to hour and averages near zero, with
  BTC-USD modestly positive and ETH-USD roughly flat over recent windows.

The takeaway is deliberately unglamorous and honest: **a gradient-boosted model
with order-flow features marginally beats a strong baseline, and does not crush
it.** That is the expected result for short-horizon realized-vol forecasting,
and per the [honest-results policy](docs/SPEC.md) it is reported rather than
hidden. A **promotion gate** enforces this — the nightly retrain only registers
a new model version when it beats persistence on the held-out split, so the
served model can never silently regress below the baseline. Live accuracy is
exposed at `:8010/accuracy` and on the Grafana dashboard.
