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
| 2 | Flink → Iceberg 1-second bars, exactly-once | — |
| 3 | Features + forward RV labels, point-in-time correct | — |
| 4 | LightGBM vs persistence and HAR-RV baselines | — |
| 5 | Online scoring + delayed label join | — |
| 6 | Dagster, Prometheus, Grafana | — |
