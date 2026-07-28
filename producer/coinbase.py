from __future__ import annotations

import json
import logging
from datetime import UTC, datetime

from .config import Config
from .sink import Sink

log = logging.getLogger("producer.router")

TRADE_TYPES = {"match", "last_match"}
BOOK_TYPES = {"snapshot", "l2update"}


def subscribe_message(products: tuple[str, ...]) -> str:
    return json.dumps(
        {
            "type": "subscribe",
            "product_ids": list(products),
            "channels": ["matches", "level2_batch", "heartbeat"],
        }
    )


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


class Router:
    """Validates raw feed messages and routes them to Kafka topics.

    Every routed record gets `ingest_ts` and `conn_epoch` injected;
    conn_epoch increments on each reconnect so downstream consumers can
    detect the L2 snapshot/delta boundary. Malformed or unrecognized
    messages go to the DLQ — nothing is silently dropped.

    Heartbeats carry last_trade_id, which is compared against the match
    stream to surface trades the matches channel dropped (Coinbase docs
    say it may drop).
    """

    def __init__(self, cfg: Config, sink: Sink):
        self._cfg = cfg
        self._sink = sink
        self._last_trade_id: dict[str, int] = {}
        self.counts = {
            "trades": 0,
            "book": 0,
            "heartbeat": 0,
            "dlq": 0,
            "trade_gaps": 0,
            "reconnects": 0,
        }

    async def route(self, raw: str | bytes, conn_epoch: int) -> None:
        try:
            msg = json.loads(raw)
            if not isinstance(msg, dict):
                raise ValueError("not a JSON object")
        except (json.JSONDecodeError, ValueError, UnicodeDecodeError) as e:
            await self._dlq(raw, f"invalid json: {e}")
            return

        mtype = msg.get("type")
        if mtype in TRADE_TYPES or mtype in BOOK_TYPES:
            product = msg.get("product_id")
            if not product:
                await self._dlq(raw, f"missing product_id in {mtype}")
                return
            msg["ingest_ts"] = _now_iso()
            msg["conn_epoch"] = conn_epoch
            if mtype in TRADE_TYPES:
                topic, count_key = self._cfg.trades_topic, "trades"
                self._track_trade_id(product, msg, mtype)
            else:
                topic, count_key = self._cfg.book_topic, "book"
            await self._sink.send(topic, product, json.dumps(msg).encode())
            self.counts[count_key] += 1
        elif mtype == "heartbeat":
            self._check_heartbeat(msg)
            self.counts["heartbeat"] += 1
        elif mtype == "subscriptions":
            log.info("ws.subscribed", extra={"ctx": {"channels": msg.get("channels")}})
        elif mtype == "error":
            await self._dlq(raw, f"feed error: {msg.get('message')} {msg.get('reason', '')}")
        else:
            await self._dlq(raw, f"unrecognized type: {mtype!r}")

    def _track_trade_id(self, product: str, msg: dict, mtype: str) -> None:
        try:
            tid = int(msg["trade_id"])
        except (KeyError, TypeError, ValueError):
            return
        prev = self._last_trade_id.get(product)
        if prev is not None and tid > prev + 1:
            missed = tid - prev - 1
            self.counts["trade_gaps"] += missed
            # A gap right after reconnect (last_match) is expected downtime,
            # not a feed drop, but both are data gaps worth surfacing.
            log.warning(
                "trades.gap",
                extra={
                    "ctx": {
                        "product": product,
                        "from_trade_id": prev,
                        "to_trade_id": tid,
                        "missed": missed,
                        "across_reconnect": mtype == "last_match",
                    }
                },
            )
        if prev is None or tid > prev:
            self._last_trade_id[product] = tid

    def _check_heartbeat(self, msg: dict) -> None:
        product = msg.get("product_id")
        try:
            hb_tid = int(msg["last_trade_id"])
        except (KeyError, TypeError, ValueError):
            return
        seen = self._last_trade_id.get(product)
        if seen is not None and hb_tid > seen:
            missed = hb_tid - seen
            self.counts["trade_gaps"] += missed
            log.warning(
                "trades.dropped_from_matches_channel",
                extra={"ctx": {"product": product, "missed": missed, "heartbeat_tid": hb_tid}},
            )
            self._last_trade_id[product] = hb_tid

    async def _dlq(self, raw: str | bytes, error: str) -> None:
        self.counts["dlq"] += 1
        log.warning("dlq.message", extra={"ctx": {"error": error}})
        value = json.dumps(
            {
                "raw": raw.decode(errors="replace") if isinstance(raw, bytes) else raw,
                "error": error,
                "ingest_ts": _now_iso(),
            }
        ).encode()
        await self._sink.send(self._cfg.dlq_topic, None, value)
