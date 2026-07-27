from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    ws_url: str = "wss://ws-feed.exchange.coinbase.com"
    bootstrap: str = "localhost:19092"
    products: tuple[str, ...] = ("BTC-USD", "ETH-USD")
    trades_topic: str = "trades.raw"
    book_topic: str = "book.l2.raw"
    dlq_topic: str = "dlq.malformed"
    # Heartbeats arrive every 1s per product; 15s of silence means the
    # connection is dead even if TCP hasn't noticed.
    idle_timeout_s: float = 15.0
    backoff_base_s: float = 1.0
    backoff_cap_s: float = 60.0
    # Reset backoff only after a connection has proven stable this long,
    # so a server that accepts-then-drops still backs off.
    stable_after_s: float = 30.0

    @classmethod
    def from_env(cls) -> Config:
        return cls(
            ws_url=os.environ.get("WS_URL", cls.ws_url),
            bootstrap=os.environ.get("KAFKA_BOOTSTRAP", cls.bootstrap),
            products=tuple(
                p.strip()
                for p in os.environ.get("PRODUCTS", ",".join(cls.products)).split(",")
                if p.strip()
            ),
        )
