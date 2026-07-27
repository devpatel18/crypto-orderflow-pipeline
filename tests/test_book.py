"""OrderBook reconstruction and OFI tests.

OFI cases are hand-computed from Cont, Kukanov & Stoikov (2014):
    e_n =  1[Pb_n >= Pb_-1]*Qb_n - 1[Pb_n <= Pb_-1]*Qb_-1
         - 1[Pa_n <= Pa_-1]*Qa_n + 1[Pa_n >= Pa_-1]*Qa_-1
A sign error here silently poisons every downstream result — do not
"simplify" these tests.
"""

import pytest
from book import OrderBook, iso_to_ms

TS = "2026-07-27T21:15:37.945472Z"


def snapshot(bids, asks, epoch=1):
    return {
        "type": "snapshot",
        "product_id": "BTC-USD",
        "bids": bids,
        "asks": asks,
        "conn_epoch": epoch,
        "ingest_ts": TS,
    }


def update(changes, epoch=1):
    return {
        "type": "l2update",
        "product_id": "BTC-USD",
        "changes": changes,
        "conn_epoch": epoch,
        "time": TS,
    }


@pytest.fixture
def book():
    b = OrderBook()
    b.apply(
        snapshot(
            bids=[["100", "3"], ["99", "2"], ["98", "1"]],
            asks=[["102", "6"], ["103", "4"], ["104", "2"]],
        )
    )
    return b


# -- reconstruction --


def test_snapshot_top():
    b = OrderBook()
    r = b.apply(
        snapshot(
            bids=[["100", "3"], ["99", "2"]],
            asks=[["102", "6"], ["103", "4"]],
        )
    )
    assert r.snapshot
    t = r.top
    assert (t.best_bid, t.best_bid_qty) == (100.0, 3.0)
    assert (t.best_ask, t.best_ask_qty) == (102.0, 6.0)
    assert t.mid == 101.0
    assert t.spread == 2.0
    assert t.ofi_delta == 0.0
    assert t.ts_ms == iso_to_ms(TS)


def test_update_add_and_remove_levels(book):
    r = book.apply(update([["buy", "101", "5"], ["sell", "102", "0"]]))
    t = r.top
    assert (t.best_bid, t.best_bid_qty) == (101.0, 5.0)  # new best bid
    assert (t.best_ask, t.best_ask_qty) == (103.0, 4.0)  # 102 removed
    assert r.changes == [("bid", 101.0, 5.0), ("ask", 102.0, 0.0)]


def test_depth5_sums_only_top_five():
    b = OrderBook()
    bids = [[str(100 - i), "1"] for i in range(6)]  # 6 levels of qty 1
    r = b.apply(snapshot(bids=bids, asks=[["200", "1"]]))
    assert r.top.bid_depth5 == 5.0


def test_update_with_wrong_epoch_dropped(book):
    r = book.apply(update([["buy", "150", "9"]], epoch=2))
    assert r.dropped and r.top is None
    assert 150.0 not in book.bids  # book untouched


def test_new_snapshot_replaces_book(book):
    r = book.apply(snapshot(bids=[["50", "1"]], asks=[["51", "1"]], epoch=2))
    assert r.top.best_bid == 50.0
    assert book.bids == {50.0: 1.0}  # old levels gone
    # deltas from the new epoch now apply
    r = book.apply(update([["buy", "50.5", "2"]], epoch=2))
    assert r.top.best_bid == 50.5


def test_empty_side_emits_nothing():
    b = OrderBook()
    r = b.apply(snapshot(bids=[["100", "1"]], asks=[]))
    assert r.top is None


def test_crossed_book_emits_nothing(book):
    r = book.apply(update([["buy", "102", "1"]]))  # bid at ask price: locked
    assert r.top is None


def test_state_round_trip(book):
    book.apply(update([["buy", "100.5", "7"]]))
    restored = OrderBook.restore(dict(book.bids), dict(book.asks), book.meta())
    r1 = book.apply(update([["sell", "102", "0"]]))
    r2 = restored.apply(update([["sell", "102", "0"]]))
    assert r1.top == r2.top  # identical including ofi_delta


# -- OFI, hand-computed --
# fixture top: bid 100 qty 3, ask 102 qty 6


def ofi_after(book, changes):
    return book.apply(update(changes)).top.ofi_delta


def test_ofi_no_top_change_is_zero(book):
    assert ofi_after(book, [["buy", "98", "9"]]) == 0.0  # deep level only


def test_ofi_bid_qty_increase(book):
    # Pb 100->100 (>= and <=), Qb 3->5: e = +5 - 3 = +2
    assert ofi_after(book, [["buy", "100", "5"]]) == 2.0


def test_ofi_bid_price_up(book):
    # Pb 100->101 qty 5: 1[101>=100]*5 - 0 = +5; ask unchanged: -6+6 = 0
    assert ofi_after(book, [["buy", "101", "5"]]) == 5.0


def test_ofi_bid_price_down(book):
    # best bid removed -> new best 99. 1[99>=100]=0; -1[99<=100]*3 = -3
    assert ofi_after(book, [["buy", "100", "0"]]) == -3.0


def test_ofi_ask_price_down(book):
    # Pa 102->101 qty 4: -1[101<=102]*4 = -4; 1[101>=102]=0. bid: +3-3=0
    assert ofi_after(book, [["sell", "101", "4"]]) == -4.0


def test_ofi_ask_price_up(book):
    # best ask removed -> new best 103 qty 4: -0 + 1[103>=102]*6 = +6
    assert ofi_after(book, [["sell", "102", "0"]]) == 6.0


def test_ofi_ask_qty_increase(book):
    # Pa same, Qa 6->9: -9 + 6 = -3
    assert ofi_after(book, [["sell", "102", "9"]]) == -3.0


def test_ofi_both_sides(book):
    # bid qty 3->5 (+2) and ask qty 6->9 (-3) in one batch: -1
    assert ofi_after(book, [["buy", "100", "5"], ["sell", "102", "9"]]) == -1.0


def test_ofi_resets_after_reconnect_snapshot(book):
    book.apply(update([["buy", "100", "5"]]))
    r = book.apply(snapshot(bids=[["90", "1"]], asks=[["91", "1"]], epoch=2))
    assert r.top.ofi_delta == 0.0  # no OFI bridged across the reconnect
