"""Simulate a mid-stream disconnect: the producer must reconnect,
re-subscribe, and forward the fresh L2 snapshot with an incremented
conn_epoch so downstream can detect the snapshot/delta boundary."""

import asyncio
import json

import websockets

from producer.coinbase import Router
from producer.config import Config
from producer.runner import run
from producer.sink import MemorySink


class FakeFeed:
    """First connection: snapshot + delta, then drop. Second connection:
    snapshot + delta + a match, then hold open."""

    def __init__(self):
        self.connections = 0
        self.subscribes = []

    async def handler(self, ws):
        self.connections += 1
        n = self.connections
        self.subscribes.append(json.loads(await ws.recv()))
        await ws.send(json.dumps({"type": "subscriptions", "channels": []}))
        await ws.send(
            json.dumps(
                {"type": "snapshot", "product_id": "BTC-USD", "bids": [["100", "1"]], "asks": []}
            )
        )
        await ws.send(
            json.dumps(
                {"type": "l2update", "product_id": "BTC-USD", "changes": [["buy", "100", "2"]]}
            )
        )
        if n == 1:
            await ws.close()
            return
        await ws.send(
            json.dumps({"type": "match", "product_id": "BTC-USD", "trade_id": 7, "price": "100"})
        )
        await ws.wait_closed()  # hold open until the client goes away


async def _wait_until(predicate, timeout_s=5.0):
    async with asyncio.timeout(timeout_s):
        while not predicate():
            await asyncio.sleep(0.02)


async def test_reconnect_resubscribes_and_bumps_epoch():
    feed = FakeFeed()
    async with websockets.serve(feed.handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        cfg = Config(
            ws_url=f"ws://127.0.0.1:{port}",
            products=("BTC-USD",),
            backoff_base_s=0.01,
            backoff_cap_s=0.05,
        )
        sink = MemorySink()
        router = Router(cfg, sink)
        task = asyncio.create_task(run(cfg, sink, router, max_epochs=2))
        try:
            await _wait_until(lambda: any(r.topic == cfg.trades_topic for r in sink.records))
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    assert feed.connections == 2
    # re-subscribed identically on both connections
    assert feed.subscribes[0] == feed.subscribes[1]
    assert feed.subscribes[0]["type"] == "subscribe"
    assert set(feed.subscribes[0]["channels"]) == {"matches", "level2_batch", "heartbeat"}

    book = [json.loads(r.value) for r in sink.records if r.topic == cfg.book_topic]
    snapshots = [m for m in book if m["type"] == "snapshot"]
    assert [s["conn_epoch"] for s in snapshots] == [1, 2]
    # deltas carry the epoch of their connection, so a consumer can bind
    # each l2update to the snapshot that precedes it
    updates = [m for m in book if m["type"] == "l2update"]
    assert [u["conn_epoch"] for u in updates] == [1, 2]
    # nothing was misrouted or dropped to the DLQ
    assert router.counts["dlq"] == 0
