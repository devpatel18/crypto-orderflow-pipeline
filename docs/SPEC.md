# Real-Time Market Microstructure Platform

Streaming data platform ingesting Coinbase market data through Kafka, computing
order-flow features in Flink, persisting to Iceberg, and serving short-horizon
volatility forecasts through a FastAPI microservice with a delayed-label
evaluation loop.

**Technologies:** Kafka, Flink, Iceberg, Trino, Dagster, Redis, FastAPI
microservices, MLflow, Prometheus/Grafana, exactly-once semantics, watermarking.

---

## Goal

Predict realized volatility over the next 5 minutes from order-flow and trade
features observed in the previous 5 minutes. Beat a persistence baseline.

The model is deliberately simple. The engineering around it is the point:
exactly-once streaming, point-in-time correct features, online/offline parity,
delayed label joins, and a live accuracy metric that updates as ground truth
arrives.

---

## Stack

| Layer | Choice | Why this and not the alternative |
|---|---|---|
| Broker | Redpanda | Kafka API, single binary, ~1GB RAM, no ZooKeeper |
| Stream processing | Apache Flink (Flink SQL + PyFlink) | True streaming engine with event-time semantics and stateful operators.|
| Table format | Apache Iceberg | Hidden partitioning, snapshot isolation, time travel |
| Object store | MinIO local, Cloudflare R2 deployed | S3 API; R2 has zero egress fees |
| Query engine | Trino | Federated SQL over Iceberg and Postgres.|
| Online store | Redis | Sub-millisecond feature reads at inference |
| Serving DB | Postgres | Predictions, model registry metadata, run history |
| API | FastAPI | Prediction microservice, health checks, OpenAPI docs |
| Model | LightGBM | Fast, tabular, CPU-only |
| Experiment tracking | MLflow | Model registry and metric logging |
| Orchestration | Dagster | Asset-based; retraining and backfill assets |
| Observability | Prometheus + Grafana | Lag, throughput, prediction latency, rolling accuracy |
| Infra | Docker Compose + Terraform | Terraform for the deployed R2 bucket and VPS |

---

## Data source

Coinbase Exchange public WebSocket. No authentication for public channels.

- Endpoint: `wss://ws-feed.exchange.coinbase.com`
- Channels: `matches` (executed trades, includes taker side), `level2_batch`
  (order book deltas)
- Products: `BTC-USD` and `ETH-USD` to start. Add more only after the pipeline
  is stable.

Verify the endpoint and channel names against Coinbase's current docs before
building. The API surface has changed historically.

**Reconnect handling is mandatory.** The socket will drop. On reconnect the L2
feed sends a fresh snapshot followed by deltas, and the consumer must handle the
snapshot/delta boundary or book state goes silently wrong. This is the single
most likely source of quiet data corruption in the project. Write a test that
simulates a mid-stream reconnect.

---

## Architecture

```
Coinbase WebSocket
        |
        v
Python asyncio producer  ──> dlq.malformed
        |
        v
   Redpanda topics
   - trades.raw
   - book.l2.raw
        |
        v
   Apache Flink
   - book state reconstruction (keyed state)
   - 1s tumbling windows, 30s watermark
   - OFI, trade imbalance, RV features
        |
   +----+----+----------+
   |         |          |
   v         v          v
Iceberg    Redis    predictions
(offline) (online)  topic
   |         |          |
   v         |          v
 Trino       |     FastAPI service
   |         +---------->|
   v                     v
 Dagster              Postgres
 (retrain,               |
  backfill)              v
   |              (5 min later)
   v              label join
 MLflow                  |
                         v
                  rolling accuracy
                         |
                         v
                  Grafana dashboard
```

---

## Feature definitions

### Realized volatility (the target)

Over intra-window mid prices `p_1 ... p_n`:

```
r_i = ln(p_i / p_{i-1})
RV  = sqrt( sum(r_i^2) )
```

Computed on 1-second mid-price bars. The target is RV over the *next* 5 minutes.
Annualize only for display, never for modeling.

### Order Flow Imbalance

Per Cont, Kukanov & Stoikov (2014). For consecutive best bid/ask states:

```
e_n =  1[P_bid_n >= P_bid_{n-1}] * Q_bid_n
     - 1[P_bid_n <= P_bid_{n-1}] * Q_bid_{n-1}
     - 1[P_ask_n <= P_ask_{n-1}] * Q_ask_n
     + 1[P_ask_n >= P_ask_{n-1}] * Q_ask_{n-1}

OFI_window = sum(e_n)
```

**Write unit tests against hand-computed examples before wiring this into
Flink.** The indicator directions are easy to invert and the resulting bug is
invisible downstream.

### Other features

- Trade sign imbalance: Coinbase `matches` gives taker `side` directly, so no
  Lee-Ready inference needed. `(buy_vol - sell_vol) / total_vol`
- Trade count and notional volume per window
- Bid-ask spread: mean and max
- Book depth imbalance at top 5 levels
- Lagged RV at 1m, 5m, 15m, 60m (HAR-RV components; expect these to dominate)

### Baselines

1. **Persistence:** `RV_hat(t+1) = RV(t)`. Volatility clusters, so this is
   strong.
2. **HAR-RV:** linear regression on lagged RV at multiple horizons.

If LightGBM does not beat both, report that. A documented negative result with
correct methodology is more credible than an unbeaten baseline that was never
run.

---

## Correctness requirements

These are what make this engineering rather than a notebook.

**Exactly-once.** Flink checkpointing plus Iceberg's atomic commits. Kill the
job mid-run and assert output tables are byte-identical after recovery.

**Watermarking.** 30s watermark on event time. Inject late events deliberately
in a test and document what is dropped versus included.

**Point-in-time correctness.** Every training row uses only data available at
prediction time. Enforce with an explicit `as_of_ts` column and assert
`feature_ts <= prediction_ts < label_ts` in a test that runs in CI.

**Online/offline parity.** Features computed in Flink for training and read from
Redis at inference must match. Write a parity test that recomputes a sample of
online features offline and asserts equality within tolerance.

**Dead letter queue.** Malformed events go to `dlq.malformed`, never silently
dropped. Expose DLQ rate as a Prometheus metric.

**Schema evolution.** Add a field to the producer mid-run and confirm Iceberg
absorbs it without a rebuild.

---

## Phases

Each phase ends with something demoable. Do not start a phase before the
previous one runs end to end.

### Phase 0 — Infrastructure
`docker-compose.yml` with Redpanda, Flink (JobManager + TaskManager), MinIO,
Trino, Postgres, Redis. Health checks. `make up` / `make down`. Produce and
consume a test message.

### Phase 1 — Ingestion
Async WebSocket producer to Kafka. Exponential-backoff reconnect. L2
snapshot/delta handling. DLQ. Structured logging. Run one hour, confirm no gaps.

### Phase 2 — Flink to Iceberg
Flink SQL job reading Kafka, reconstructing book state in keyed state, emitting
1-second bars to Iceberg. Checkpointing configured. Verify idempotency by
killing and restarting the job.

### Phase 3 — Features and labels
Feature windows and 5-minute-forward RV labels written to an Iceberg gold table.
Point-in-time assertions. Query through Trino to confirm it is readable.

### Phase 4 — Offline model
Train LightGBM on accumulated gold data. Time-based train/test split, never
random. Compare against both baselines. Log everything to MLflow.

### Phase 5 — Online scoring
FastAPI service loading the MLflow model, reading features from Redis, writing
predictions to Postgres and a Kafka topic. Parity test. Delayed label join
computing rolling MAE and RMSE against baselines.

### Phase 6 — Orchestration and observability
Dagster assets for retraining, backfill, and quality checks. Prometheus metrics
from the producer, Flink, and FastAPI. Grafana dashboard: consumer lag,
throughput, DLQ rate, prediction latency, rolling accuracy vs baselines.

---

## Correctness risk areas

Areas where correct-looking code can silently corrupt data, and which carry
dedicated tests:

- **The OFI formula.** A sign error poisons every downstream result silently;
  hand-computed unit tests pin the sign conventions.
- **Flink checkpointing and watermark config.** Correct-looking config can drop
  data. Verified with a kill/restart idempotency check.
- **L2 snapshot/delta boundaries.** The most likely silent bug; `conn_epoch`
  binds every delta to its snapshot across reconnects.
- **Online/offline feature parity.** The online calculator and the gold SQL are
  kept formula-identical and checked against each other.

---

## Deployment

Split collection from processing. A small VPS (Hetzner CX22, ~4 EUR/month, 4GB)
runs only the WebSocket producer writing to Cloudflare R2. Heavy Flink
processing runs locally against accumulated data.

This is cheaper, mirrors real architecture, and means a laptop restart does not
create a data gap. Do not attempt to run Flink on a 4GB VPS.

Terraform manages the R2 bucket and VPS provisioning.

---

## Deliverables

- Public repo with README, architecture diagram, and a "design decisions I would
  revisit" section
- Live Grafana dashboard screenshot showing rolling accuracy vs baselines
- One paragraph on a bug you found and how you found it

## Scope discipline

Phases 0 through 3 alone are a complete, defensible project. A working
four-phase pipeline beats a half-finished seven-phase one.
