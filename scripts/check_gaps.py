"""Phase 1 acceptance check: consume trades.raw and dlq.malformed from the
beginning and report, per product: trade count, trade_id range, gaps in the
trade_id sequence, and connection epochs seen. Exits 1 on any gap or DLQ
message.

Usage: python scripts/check_gaps.py
"""

import asyncio
import json
import os
import sys
from collections import defaultdict

from aiokafka import AIOKafkaConsumer, TopicPartition

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:19092")


async def read_topic(topic: str):
    consumer = AIOKafkaConsumer(bootstrap_servers=BOOTSTRAP)
    await consumer.start()
    try:
        partitions = consumer.partitions_for_topic(topic)
        if not partitions:
            return
        tps = [TopicPartition(topic, p) for p in partitions]
        consumer.assign(tps)
        await consumer.seek_to_beginning(*tps)
        end = await consumer.end_offsets(tps)
        remaining = {tp: off for tp, off in end.items() if off > 0}
        while remaining:
            batches = await consumer.getmany(timeout_ms=2000, max_records=10000)
            if not batches:
                break
            for tp, msgs in batches.items():
                for m in msgs:
                    yield m
                if msgs and msgs[-1].offset >= remaining.get(tp, 0) - 1:
                    remaining.pop(tp, None)
    finally:
        await consumer.stop()


async def main() -> int:
    trade_ids: dict[str, list[int]] = defaultdict(list)
    epochs: dict[str, set[int]] = defaultdict(set)
    async for m in read_topic("trades.raw"):
        msg = json.loads(m.value)
        if msg.get("type") == "match" and "trade_id" in msg:
            trade_ids[msg["product_id"]].append(int(msg["trade_id"]))
            epochs[msg["product_id"]].add(msg.get("conn_epoch"))

    dlq_count = 0
    async for m in read_topic("dlq.malformed"):
        dlq_count += 1
        if dlq_count <= 5:
            print(f"DLQ sample: {m.value[:200]!r}")

    failed = False
    for product, tids in sorted(trade_ids.items()):
        tids.sort()
        gaps = [(a, b) for a, b in zip(tids, tids[1:], strict=False) if b - a > 1]
        missed = sum(b - a - 1 for a, b in gaps)
        print(
            f"{product}: {len(tids)} trades, trade_id {tids[0]}..{tids[-1]}, "
            f"gaps={len(gaps)} (missed {missed}), epochs={sorted(epochs[product])}"
        )
        for a, b in gaps[:5]:
            print(f"  gap: {a} -> {b}")
        if gaps:
            failed = True

    if not trade_ids:
        print("no match messages found in trades.raw")
        failed = True
    print(f"dlq.malformed: {dlq_count} messages")
    if dlq_count:
        failed = True
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
