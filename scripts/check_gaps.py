"""Phase 1 acceptance check: consume trades.raw and dlq.malformed from the
beginning and report, per product: trade count, trade_id range, gaps in the
trade_id sequence, and connection epochs seen. Exits 1 on any gap or DLQ
message.

Usage: python scripts/check_gaps.py
"""

import asyncio
import contextlib
import json
import os
import sys
from collections import defaultdict

from aiokafka import AIOKafkaConsumer

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:19092")


async def read_topic(topic: str) -> list:
    """Read a topic start-to-current-end and return the raw records.

    Subscribe mode (topic in constructor) rather than manual assign():
    with assign(), aiokafka never fetches partition metadata, so
    partitions_for_topic() returns None. Without a group_id, subscribing
    auto-assigns all partitions and commits nothing.
    """
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=BOOTSTRAP,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    await consumer.start()
    records = []
    try:
        async with asyncio.timeout(30):
            while not consumer.assignment():
                await asyncio.sleep(0.05)
        tps = list(consumer.assignment())
        end = await consumer.end_offsets(tps)
        remaining = {tp: off for tp, off in end.items() if off > 0}
        deadline = asyncio.get_running_loop().time() + 120
        while remaining:
            # The first polls can be empty while the fetcher warms up; only
            # positions reaching the end offsets mean we're done.
            batches = await consumer.getmany(timeout_ms=2000, max_records=10000)
            for msgs in batches.values():
                records.extend(msgs)
            for tp in list(remaining):
                if await consumer.position(tp) >= remaining[tp]:
                    remaining.pop(tp)
            if asyncio.get_running_loop().time() > deadline:
                raise TimeoutError(f"still {len(remaining)} partitions unread after 120s")
    finally:
        # aiokafka can leak a CancelledError from its internal coordinator
        # task during stop(); harmless for a read-only one-shot script.
        with contextlib.suppress(asyncio.CancelledError):
            await consumer.stop()
    return records


async def main() -> int:
    trade_ids: dict[str, list[int]] = defaultdict(list)
    epochs: dict[str, set[int]] = defaultdict(set)
    for m in await read_topic("trades.raw"):
        msg = json.loads(m.value)
        if msg.get("type") == "match" and "trade_id" in msg:
            trade_ids[msg["product_id"]].append(int(msg["trade_id"]))
            epochs[msg["product_id"]].add(msg.get("conn_epoch"))

    dlq = await read_topic("dlq.malformed")
    dlq_count = len(dlq)
    for m in dlq[:5]:
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
