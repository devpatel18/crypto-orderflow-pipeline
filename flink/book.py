"""L2 order-book reconstruction from Coinbase snapshot/l2update messages.

Pure Python, no Flink imports: unit-tested locally, executed inside the
PyFlink job (shipped via --pyFiles). The Flink operator persists book
state; this module reports which levels changed so the operator can
write through to keyed state.

OFI (order flow imbalance) per Cont, Kukanov & Stoikov (2014), computed
incrementally over consecutive best-bid/ask states:

    e_n =  1[Pb_n >= Pb_{n-1}] * Qb_n
         - 1[Pb_n <= Pb_{n-1}] * Qb_{n-1}
         - 1[Pa_n <= Pa_{n-1}] * Qa_n
         + 1[Pa_n >= Pa_{n-1}] * Qa_{n-1}
"""

from __future__ import annotations

import heapq
from dataclasses import dataclass
from datetime import datetime

DEPTH_LEVELS = 5


def iso_to_ms(ts: str) -> int:
    """'2026-07-27T21:15:37.945472Z' or '...+00:00' -> epoch millis."""
    return int(datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp() * 1000)


@dataclass(frozen=True)
class BookTop:
    ts_ms: int
    product_id: str
    best_bid: float
    best_bid_qty: float
    best_ask: float
    best_ask_qty: float
    mid: float
    spread: float
    bid_depth5: float
    ask_depth5: float
    ofi_delta: float
    conn_epoch: int


@dataclass
class ApplyResult:
    top: BookTop | None  # None: nothing emittable (empty/crossed book, drop)
    snapshot: bool  # state should be fully rewritten from the book
    changes: list[tuple[str, float, float]]  # ("bid"|"ask", price, size); 0 = delete
    dropped: bool = False  # delta without a matching snapshot, not applied


class OrderBook:
    def __init__(self) -> None:
        self.bids: dict[float, float] = {}
        self.asks: dict[float, float] = {}
        self.epoch: int | None = None
        self.prev_top: tuple[float, float, float, float] | None = None

    # -- state round trip (for Flink keyed-state restore) --

    def meta(self) -> dict:
        return {"epoch": self.epoch, "prev_top": self.prev_top}

    @classmethod
    def restore(cls, bids: dict[float, float], asks: dict[float, float], meta: dict) -> OrderBook:
        book = cls()
        book.bids, book.asks = bids, asks
        book.epoch = meta.get("epoch")
        book.prev_top = meta.get("prev_top")
        return book

    # -- message application --

    def apply(self, msg: dict) -> ApplyResult:
        mtype = msg.get("type")
        epoch = msg.get("conn_epoch")
        if mtype == "snapshot":
            self.bids = {float(p): float(q) for p, q, *_ in msg.get("bids", [])}
            self.asks = {float(p): float(q) for p, q, *_ in msg.get("asks", [])}
            self.epoch = epoch
            # OFI must not bridge a reconnect: the first delta after a fresh
            # snapshot compares against the snapshot top, not the stale book.
            self.prev_top = None
            return ApplyResult(self._top(msg), snapshot=True, changes=[])
        if mtype == "l2update":
            if epoch != self.epoch:
                return ApplyResult(None, snapshot=False, changes=[], dropped=True)
            changes: list[tuple[str, float, float]] = []
            for side, price, size in msg.get("changes", []):
                book = self.bids if side == "buy" else self.asks
                p, q = float(price), float(size)
                if q == 0:
                    book.pop(p, None)
                else:
                    book[p] = q
                changes.append(("bid" if side == "buy" else "ask", p, q))
            return ApplyResult(self._top(msg), snapshot=False, changes=changes)
        return ApplyResult(None, snapshot=False, changes=[])

    def _top(self, msg: dict) -> BookTop | None:
        if not self.bids or not self.asks:
            return None
        bid_prices = heapq.nlargest(DEPTH_LEVELS, self.bids)
        ask_prices = heapq.nsmallest(DEPTH_LEVELS, self.asks)
        bb, ba = bid_prices[0], ask_prices[0]
        if bb >= ba:  # crossed or locked book: transient corruption, don't emit
            return None
        bbq, baq = self.bids[bb], self.asks[ba]
        ofi = self._ofi(bb, bbq, ba, baq)
        self.prev_top = (bb, bbq, ba, baq)
        ts = msg.get("time") or msg.get("ingest_ts")
        return BookTop(
            ts_ms=iso_to_ms(ts),
            product_id=msg["product_id"],
            best_bid=bb,
            best_bid_qty=bbq,
            best_ask=ba,
            best_ask_qty=baq,
            mid=(bb + ba) / 2,
            spread=ba - bb,
            bid_depth5=sum(self.bids[p] for p in bid_prices),
            ask_depth5=sum(self.asks[p] for p in ask_prices),
            ofi_delta=ofi,
            conn_epoch=self.epoch if self.epoch is not None else -1,
        )

    def _ofi(self, bb: float, bbq: float, ba: float, baq: float) -> float:
        if self.prev_top is None:
            return 0.0
        pbb, pbbq, pba, pbaq = self.prev_top
        e = 0.0
        if bb >= pbb:
            e += bbq
        if bb <= pbb:
            e -= pbbq
        if ba <= pba:
            e -= baq
        if ba >= pba:
            e += pbaq
        return e
