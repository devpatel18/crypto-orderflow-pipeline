"""Prometheus metrics for the producer: a small sync task diffs the
Router's counts dict into real Counters so rate() works in Grafana."""

from __future__ import annotations

import asyncio

from prometheus_client import Counter, start_http_server

COUNTERS = {
    "trades": Counter("producer_trades_total", "Trade messages routed to Kafka"),
    "book": Counter("producer_book_total", "Book messages routed to Kafka"),
    "heartbeat": Counter("producer_heartbeats_total", "Heartbeats received"),
    "dlq": Counter("producer_dlq_total", "Messages sent to the DLQ"),
    "trade_gaps": Counter("producer_trade_gaps_total", "Missed trades detected"),
    "reconnects": Counter("producer_reconnects_total", "WebSocket reconnects"),
}


def start(port: int) -> None:
    start_http_server(port)


async def sync_counts(counts: dict[str, int], interval_s: float = 5.0) -> None:
    last = dict.fromkeys(COUNTERS, 0)
    while True:
        await asyncio.sleep(interval_s)
        for key, counter in COUNTERS.items():
            current = counts.get(key, 0)
            if current > last[key]:
                counter.inc(current - last[key])
                last[key] = current
