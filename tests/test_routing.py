import json

import pytest

from producer.coinbase import Router
from producer.config import Config
from producer.sink import MemorySink

CFG = Config()


@pytest.fixture
def sink():
    return MemorySink()


@pytest.fixture
def router(sink):
    return Router(CFG, sink)


def msg(**kw):
    return json.dumps(kw)


async def test_match_routed_to_trades_with_envelope(router, sink):
    await router.route(
        msg(type="match", product_id="BTC-USD", trade_id=100, price="50000.1", size="0.5"),
        conn_epoch=3,
    )
    [r] = sink.records
    assert r.topic == CFG.trades_topic
    assert r.key == "BTC-USD"
    out = json.loads(r.value)
    assert out["price"] == "50000.1"  # original fields intact
    assert out["conn_epoch"] == 3
    assert "ingest_ts" in out


async def test_snapshot_and_l2update_routed_to_book(router, sink):
    await router.route(msg(type="snapshot", product_id="ETH-USD", bids=[], asks=[]), 1)
    await router.route(
        msg(type="l2update", product_id="ETH-USD", changes=[["buy", "3000", "1"]]), 1
    )
    assert [r.topic for r in sink.records] == [CFG.book_topic, CFG.book_topic]
    assert all(r.key == "ETH-USD" for r in sink.records)


async def test_invalid_json_goes_to_dlq(router, sink):
    await router.route(b"{not json", 1)
    [r] = sink.records
    assert r.topic == CFG.dlq_topic
    out = json.loads(r.value)
    assert out["raw"] == "{not json"
    assert "invalid json" in out["error"]
    assert router.counts["dlq"] == 1


async def test_missing_product_id_goes_to_dlq(router, sink):
    await router.route(msg(type="match", trade_id=1), 1)
    [r] = sink.records
    assert r.topic == CFG.dlq_topic


async def test_unrecognized_type_goes_to_dlq(router, sink):
    await router.route(msg(type="ticker", product_id="BTC-USD"), 1)
    [r] = sink.records
    assert r.topic == CFG.dlq_topic
    assert "unrecognized type" in json.loads(r.value)["error"]


async def test_heartbeat_not_forwarded(router, sink):
    await router.route(
        msg(type="heartbeat", product_id="BTC-USD", sequence=90, last_trade_id=20), 1
    )
    assert sink.records == []
    assert router.counts["heartbeat"] == 1


async def test_trade_id_gap_detected(router, sink):
    await router.route(msg(type="match", product_id="BTC-USD", trade_id=10), 1)
    await router.route(msg(type="match", product_id="BTC-USD", trade_id=13), 1)
    assert router.counts["trade_gaps"] == 2  # missed 11 and 12


async def test_contiguous_trades_no_gap(router, sink):
    for tid in (10, 11, 12):
        await router.route(msg(type="match", product_id="BTC-USD", trade_id=tid), 1)
    assert router.counts["trade_gaps"] == 0


async def test_gap_tracking_is_per_product(router, sink):
    await router.route(msg(type="match", product_id="BTC-USD", trade_id=10), 1)
    await router.route(msg(type="match", product_id="ETH-USD", trade_id=500), 1)
    await router.route(msg(type="match", product_id="BTC-USD", trade_id=11), 1)
    assert router.counts["trade_gaps"] == 0


async def test_heartbeat_detects_dropped_matches(router, sink):
    await router.route(msg(type="match", product_id="BTC-USD", trade_id=10), 1)
    await router.route(
        msg(type="heartbeat", product_id="BTC-USD", sequence=99, last_trade_id=12), 1
    )
    assert router.counts["trade_gaps"] == 2
    # and the next match at 13 is now contiguous, no double counting
    await router.route(msg(type="match", product_id="BTC-USD", trade_id=13), 1)
    assert router.counts["trade_gaps"] == 2
