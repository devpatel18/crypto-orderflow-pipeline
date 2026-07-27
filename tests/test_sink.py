import json

from aiokafka.errors import MessageSizeTooLargeError

from producer.sink import KafkaSink


class OversizeRejectingProducer:
    """Stub of AIOKafkaProducer that rejects values over a threshold."""

    def __init__(self, limit: int):
        self.limit = limit
        self.sent: list[tuple[str, bytes]] = []

    async def send(self, topic, value=None, key=None):
        if len(value) > self.limit:
            raise MessageSizeTooLargeError("too big")
        self.sent.append((topic, value))

        class Done:
            def add_done_callback(self, cb):
                pass

        return Done()


async def test_oversized_message_goes_to_dlq_truncated():
    sink = KafkaSink("unused:9092", dlq_topic="dlq.malformed")
    sink._producer = OversizeRejectingProducer(limit=2048)

    big = json.dumps({"type": "snapshot", "bids": ["x" * 5000]}).encode()
    await sink.send("book.l2.raw", "BTC-USD", big)

    [(topic, value)] = sink._producer.sent
    assert topic == "dlq.malformed"
    out = json.loads(value)
    assert "message too large" in out["error"]
    assert len(value) <= 2048  # truncated, not the full payload


async def test_oversized_dlq_message_does_not_recurse():
    sink = KafkaSink("unused:9092", dlq_topic="dlq.malformed")
    sink._producer = OversizeRejectingProducer(limit=10)  # even DLQ copy too big

    await sink.send("book.l2.raw", "BTC-USD", b"x" * 500)
    assert sink._producer.sent == []  # dropped with an error log, no crash loop
