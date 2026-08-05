# Real-Time Crypto Order-Flow & Volatility Pipeline

This project watches live Bitcoin and Ethereum trading on Coinbase, turns that
firehose into tidy per-second summaries, builds features from them, and every
minute predicts how volatile the next 5 minutes will be. Then it does the part
most demos skip: once those 5 minutes actually happen, it grades its own
prediction against what really occurred — continuously, in the open.

It's a full end-to-end streaming ML system, built to run the way something like
this would run in production rather than in a notebook.

Full design notes: [docs/SPEC.md](docs/SPEC.md)

## What it actually does

```
Coinbase WebSocket  →  Kafka (Redpanda)  →  Flink  →  Iceberg (on MinIO)
   trades + order       durable event        1-second      columnar tables
   book updates         log                  bars          queried by Trino
                                                                  │
                                                                  ▼
                                        Trino builds 5-min feature rows (gold)
                                                                  │
                                                                  ▼
                             LightGBM trains nightly ──► MLflow registry
                                                                  │
                                                                  ▼
                         FastAPI service predicts next-5-min volatility,
                         then joins the real outcome 5 minutes later and
                         reports live accuracy to Prometheus + Grafana
```

The whole thing is orchestrated by Dagster and monitored on a Grafana
dashboard, and it's designed to keep running unattended and pick itself back up
after failures.

## Why I built it

I wanted to build a real streaming data platform, not a Jupyter notebook with a
model in it. The interesting problems in ML systems usually aren't the model —
they're everything around it: getting live data in reliably, processing it
exactly once, storing it so you can both train on it and serve from it, keeping
training and serving features identical, shipping new models safely, and knowing
whether any of it is actually working. So I built one that does all of that, on
a genuinely hard forecasting problem, and held it to an honest standard.

## Is the model any good? (honest answer)

The task is forecasting 5-minute **realized volatility**, and the model is judged
against a **persistence baseline** — "assume the next 5 minutes look like the
last 5 minutes." That sounds trivial, but for short-horizon volatility it's a
brutally strong baseline, because volatility is highly autocorrelated.

- **On held-out data, the model beats persistence by roughly +3.5% to +9%** (in
  MAE) depending on the day — a real but modest edge.
- **Live, it's regime-dependent.** In calm, steady markets it beats persistence
  most of the time. During sharp volatility collapses it can do noticeably
  *worse*, because it leans on longer-window signals while persistence just
  tracks the drop.

So the honest verdict is unglamorous: **a gradient-boosted model with order-flow
features marginally beats a strong baseline — it doesn't crush it.** That is the
expected, respectable result for this problem, and the project reports it plainly
rather than cherry-picking a good hour. A **promotion gate** enforces the
honesty: the nightly retrain only ships a new model if it actually beats
persistence on a held-out split, so the served model can never quietly regress
below the baseline.
<img width="1897" height="993" alt="Screenshot 2026-08-04 at 2 53 30 PM" src="https://github.com/user-attachments/assets/ed3b2942-cff0-4be4-82b3-6e3a6e6546f6" />

## What I learned

Honestly, most of what I learned wasn't about the model.

- **A strong baseline keeps you honest.** It's easy to show a model with an
  impressive-looking error number. It's much harder — and much more useful — to
  show it beating a baseline a skeptic would actually pick. Half the value of
  this project is that it measures the right thing.
- **The model was the easy part; keeping the system alive was the real work.**
  The pipeline hit a string of failures that only show up when something runs
  for real, for days:
  - **Streaming storage bloats quietly.** Committing to Iceberg every few
    seconds left tens of thousands of tiny metadata files — ~35 GB of bookkeeping
    for ~180 MB of actual data — until it filled the disk and took the pipeline
    down. Fix: bound the metadata and commit less often.
  - **Some bugs are time bombs.** A query that built a per-minute grid over "all
    history" worked fine for a week, then broke the moment the data crossed a
    hard 10,000-row limit. Systems that are healthy at small scale can fail at a
    threshold you didn't know was there.

- **Design for recovery, not for never failing** — because it *will* fail. The
  pieces that saved me every time were the boring ones: idempotent jobs that
  re-run safely, checkpoints so the stream resumes exactly where it left off,
  retries around flaky calls, and restart policies. The system that recovers
  cleanly beats the one that's "never supposed to break."

## Architecture at a glance

| Layer | Tech | Job |
|---|---|---|
| Ingestion | Coinbase WebSocket → **Redpanda** (Kafka) | durable event log of trades + order-book updates, with reconnects and a dead-letter queue |
| Stream processing | **Apache Flink** (PyFlink + Flink SQL) | exactly-once 1-second bars, maintains the live order book, computes order-flow imbalance |
| Storage / lakehouse | **Apache Iceberg** on **MinIO**, catalog in **Postgres** | columnar tables for both training and serving |
| Query / features | **Trino** | builds point-in-time-correct 5-minute feature rows ("gold") |
| Modeling | **LightGBM** + **MLflow** | one pooled model for both assets; nightly retrain, versioned, promotion-gated |
| Serving | **FastAPI** + **Redis** | live predictions + delayed-label scoring vs the baseline |
| Orchestration | **Dagster** | schedules the gold build, retrain, and table maintenance |
| Monitoring | **Prometheus** + **Grafana** | data flow, prediction latency, live rolling accuracy |

A couple of details I'm proud of: the online (serving) features are verified to
match the offline (training) features exactly, so there's no train/serve skew;
and the whole labeling loop is point-in-time correct, so the model is never
accidentally shown the future.

## Running it

Heads up: this is a heavy stack. It wants **~10 GB of RAM given to Docker** —
comfortable on a 16 GB+ machine, tight below that.

```sh
make up      # start the infrastructure (Redpanda, Flink, MinIO, Trino, Postgres, Redis, ...)
make smoke   # health checks + a Kafka produce/consume round trip
make down    # stop everything (your data volumes are kept)
```

Once it's up: the Grafana dashboard is at `localhost:3000`, Dagster at
`localhost:3001`, and live model accuracy at `localhost:8010/accuracy`.
<img width="1904" height="990" alt="Screenshot 2026-08-04 at 2 53 40 PM" src="https://github.com/user-attachments/assets/843e057f-e35a-4177-8c3d-325c9b2ae4d4" />
<img width="1903" height="571" alt="Screenshot 2026-08-04 at 2 54 05 PM" src="https://github.com/user-attachments/assets/a8bc293d-1f0e-47e2-a442-4ef153e0fcde" />

## Build phases

| Phase | What got built |
|---|---|
| 0 | Infrastructure: Docker Compose stack, health checks, smoke test |
| 1 | WebSocket → Kafka producer with reconnects + dead-letter queue |
| 2 | Flink → Iceberg 1-second bars, exactly-once |
| 3 | Features + forward volatility labels, point-in-time correct |
| 4 | LightGBM vs persistence and HAR-RV baselines, tracked in MLflow |
| 5 | Online scoring + delayed-label evaluation (verified no train/serve skew) |
| 6 | Dagster orchestration, Prometheus metrics, Grafana dashboard |
