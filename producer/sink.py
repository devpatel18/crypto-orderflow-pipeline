from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Protocol

from aiokafka import AIOKafkaProducer
from aiokafka.errors import MessageSizeTooLargeError

log = logging.getLogger("producer.sink")

# Full L2 snapshots for BTC-USD serialize to >1MB, over the 1MB Kafka
# default. Topic max.message.bytes must match (see `make topics`).
MAX_REQUEST_SIZE = 10 * 1024 * 1024


class Sink(Protocol):
    async def send(self, topic: str, key: str | None, value: bytes) -> None: ...


class KafkaSink:
    def __init__(self, bootstrap: str, dlq_topic: str | None = None):
        self._dlq_topic = dlq_topic
        self._producer = AIOKafkaProducer(
            bootstrap_servers=bootstrap,
            acks="all",
            enable_idempotence=True,
            linger_ms=5,
            compression_type="gzip",
            max_request_size=MAX_REQUEST_SIZE,
        )

    async def start(self) -> None:
        await self._producer.start()

    async def stop(self) -> None:
        await self._producer.stop()

    async def send(self, topic: str, key: str | None, value: bytes) -> None:
        # send() enqueues and returns a delivery future; don't await delivery
        # inline (that would serialize every message), but never let a failure
        # pass silently either.
        try:
            fut = await self._producer.send(topic, value=value, key=key.encode() if key else None)
        except MessageSizeTooLargeError:
            # A poison message must not crash-loop the collector, and must
            # not vanish silently: DLQ a truncated copy.
            log.error(
                "kafka.message_too_large",
                extra={"ctx": {"topic": topic, "key": key, "bytes": len(value)}},
            )
            if self._dlq_topic and topic != self._dlq_topic:
                dlq_value = json.dumps(
                    {
                        "error": f"message too large ({len(value)} bytes) for {topic}",
                        "raw": value[:1024].decode(errors="replace") + "...",
                    }
                ).encode()
                await self.send(self._dlq_topic, key, dlq_value)
            return
        fut.add_done_callback(_log_delivery_failure)


def _log_delivery_failure(fut) -> None:
    if fut.cancelled() or fut.exception() is not None:
        log.error(
            "kafka.delivery_failed",
            extra={"ctx": {"error": str(fut.exception()) if not fut.cancelled() else "cancelled"}},
        )


@dataclass
class Record:
    topic: str
    key: str | None
    value: bytes


class MemorySink:
    """Test double."""

    def __init__(self) -> None:
        self.records: list[Record] = []

    async def send(self, topic: str, key: str | None, value: bytes) -> None:
        self.records.append(Record(topic, key, value))
