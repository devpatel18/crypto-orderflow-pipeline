"""Online feature updater: consumes 1s bars from Kafka (emitted by the
same Flink window subgraph that feeds Iceberg), maintains 62-minute
rolling buffers per product, and writes minute-boundary feature rows to
Redis:

    features:{product}:latest            (JSON, for the scoring service)
    features:{product}:{as_of isoformat} (JSON, 2h TTL, for parity tests)

Bars can arrive more than once (at-least-once Kafka sink): the buffers
are keyed by bar_ts, so replays are idempotent.

Run: make serve-features
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from collections import defaultdict
from datetime import datetime, timedelta

import redis.asyncio as aioredis
from aiokafka import AIOKafkaConsumer

from producer.log import setup

from .feature_calc import compute_features

log = logging.getLogger("service.features")

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:19092")
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")
BOOK_TOPIC = "bars.book.1s"
TRADE_TOPIC = "bars.trade.1s"
BUFFER = timedelta(minutes=62)
KEY_TTL_S = 7200


def parse_ts(s: str) -> datetime:
    return datetime.fromisoformat(s.replace(" ", "T"))


class FeatureUpdater:
    def __init__(self, r: aioredis.Redis):
        self.redis = r
        self.book: dict[str, dict[datetime, dict]] = defaultdict(dict)
        self.trade: dict[str, dict[datetime, dict]] = defaultdict(dict)
        self.last_as_of: dict[str, datetime] = {}
        self.pending: dict[str, datetime] = {}
        # Per-topic consumption high-water (max bar_ts seen). One consumer
        # reads both topics and Kafka gives no cross-topic ordering — during
        # replay the book backlog can arrive far ahead of trades, so emitting
        # on the book stream alone computes features with an empty trade
        # buffer (observed: trade_count_5m=0 vs gold 2056). A minute is only
        # emitted once BOTH topics have been consumed past it.
        self.hw: dict[str, datetime | None] = {BOOK_TOPIC: None, TRADE_TOPIC: None}
        self.written = 0

    async def handle(self, topic: str, bar: dict) -> None:
        product = bar["product_id"]
        ts = parse_ts(bar["bar_ts"])
        bar["bar_ts"] = ts
        buf = self.book[product] if topic == BOOK_TOPIC else self.trade[product]
        buf[ts] = bar
        if self.hw[topic] is None or ts > self.hw[topic]:
            self.hw[topic] = ts

        if topic == BOOK_TOPIC:
            candidate = ts.replace(second=0, microsecond=0)
            if candidate > self.last_as_of.get(product, datetime.min):
                self.pending[product] = candidate
        await self.flush_pending()

    def _both_streams_past(self, as_of: datetime) -> bool:
        margin = timedelta(seconds=2)
        return all(hw is not None and hw >= as_of + margin for hw in self.hw.values())

    async def flush_pending(self) -> None:
        for product, as_of in list(self.pending.items()):
            if self._both_streams_past(as_of):
                del self.pending[product]
                await self.emit(product, as_of)
                # Prune relative to the emitted minute, NEVER the arriving
                # bar's own ts: during replay one topic runs far ahead of the
                # other, and hw-based pruning deletes the slow stream's
                # window before it is ever emitted (observed: the trade
                # topic replayed ~1h ahead and 22:19's trades were pruned
                # before the book stream reached 22:19).
                cutoff = as_of - BUFFER
                for buf in (self.book[product], self.trade[product]):
                    for old in [t for t in buf if t < cutoff]:
                        del buf[old]

    async def emit(self, product: str, as_of: datetime) -> None:
        if self.last_as_of.get(product, datetime.min) >= as_of:
            return
        feats = compute_features(
            list(self.book[product].values()), list(self.trade[product].values()), as_of
        )
        if feats is None:
            return
        self.last_as_of[product] = as_of
        feats["product_id"] = product
        feats["as_of_ts"] = as_of.isoformat()
        payload = json.dumps(feats)
        await self.redis.set(f"features:{product}:latest", payload)
        await self.redis.set(f"features:{product}:{as_of.isoformat()}", payload, ex=KEY_TTL_S)
        self.written += 1
        if self.written % 10 == 1:
            log.info(
                "features.written",
                extra={
                    "ctx": {
                        "product": product,
                        "as_of": feats["as_of_ts"],
                        "bar_count_5m": feats["bar_count_5m"],
                        "total": self.written,
                    }
                },
            )


async def main() -> None:
    setup()
    r = aioredis.from_url(REDIS_URL, decode_responses=True)
    consumer = AIOKafkaConsumer(
        BOOK_TOPIC,
        TRADE_TOPIC,
        bootstrap_servers=BOOTSTRAP,
        group_id="online-features",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )
    await consumer.start()
    updater = FeatureUpdater(r)
    log.info("features.started", extra={"ctx": {"bootstrap": BOOTSTRAP}})
    try:
        async for msg in consumer:
            try:
                bar = json.loads(msg.value)
            except json.JSONDecodeError:
                continue
            await updater.handle(msg.topic, bar)
    finally:
        await consumer.stop()
        await r.aclose()


if __name__ == "__main__":
    asyncio.run(main())
