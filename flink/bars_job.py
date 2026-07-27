"""Flink job: Kafka (trades.raw, book.l2.raw) -> 1-second bars -> Iceberg.

Two pipelines in one job:
- trades: pure Flink SQL, tumbling 1s event-time windows over match messages.
- book: PyFlink DataStream keyed by product; OrderBook (book.py) maintained
  in keyed state (MapState per side + pickled meta), emitting a top-of-book
  row per applied message, aggregated to 1s bars in SQL.

Exactly-once: 10s checkpoints, Kafka offsets in checkpointed state, Iceberg
commits bound to checkpoints. In-job restarts (e.g. TaskManager kill) resume
from the last checkpoint without duplicates. A *fresh* submit reprocesses
from the earliest offsets: truncate the bars tables first (make reset-bars).
Reading from earliest is deliberate — the book stream must start at a
snapshot message; a mid-stream start would silently drop every delta until
the producer next reconnects.

Iceberg tables are created via Trino (make tables) because Flink SQL DDL
cannot express hidden partitioning transforms like day(bar_ts).
"""

import json
import logging

from book import OrderBook
from pyflink.common import Duration, Row, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import CheckpointingMode, StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import MapStateDescriptor, ValueStateDescriptor
from pyflink.table import DataTypes, Schema, StreamTableEnvironment

BOOTSTRAP = "redpanda:9092"

TOP_FIELDS = [
    "ts_ms",
    "product_id",
    "best_bid",
    "best_bid_qty",
    "best_ask",
    "best_ask_qty",
    "mid",
    "spread",
    "bid_depth5",
    "ask_depth5",
    "ofi_delta",
    "conn_epoch",
]
TOP_TYPES = [
    Types.LONG(),
    Types.STRING(),
    Types.DOUBLE(),
    Types.DOUBLE(),
    Types.DOUBLE(),
    Types.DOUBLE(),
    Types.DOUBLE(),
    Types.DOUBLE(),
    Types.DOUBLE(),
    Types.DOUBLE(),
    Types.DOUBLE(),
    Types.LONG(),
]
TOP_ROW_TYPE = Types.ROW_NAMED(TOP_FIELDS, TOP_TYPES)

ICEBERG_CATALOG_DDL = """
CREATE CATALOG iceberg WITH (
  'type' = 'iceberg',
  'catalog-impl' = 'org.apache.iceberg.jdbc.JdbcCatalog',
  'uri' = 'jdbc:postgresql://postgres:5432/iceberg',
  'jdbc.user' = 'market',
  'jdbc.password' = 'market',
  'warehouse' = 's3://warehouse',
  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint' = 'http://minio:9000',
  's3.path-style-access' = 'true',
  's3.access-key-id' = 'minioadmin',
  's3.secret-access-key' = 'minioadmin',
  'client.region' = 'us-east-1'
)
"""

# scan.watermark.idle-timeout is essential: trades.raw has 3 partitions but
# only 2 products, so one partition is empty and per-split watermark
# alignment would hold the event-time clock at -inf forever without it.
TRADES_SRC_DDL = f"""
CREATE TEMPORARY TABLE trades_src (
  `type` STRING,
  product_id STRING,
  trade_id BIGINT,
  price STRING,
  `size` STRING,
  side STRING,
  `time` STRING,
  conn_epoch BIGINT,
  event_ts AS COALESCE(
    TO_TIMESTAMP(SUBSTRING(REPLACE(`time`, 'T', ' ') FROM 1 FOR 23),
                 'yyyy-MM-dd HH:mm:ss.SSS'),
    TIMESTAMP '1970-01-01 00:00:00'),
  WATERMARK FOR event_ts AS event_ts - INTERVAL '30' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'trades.raw',
  'properties.bootstrap.servers' = '{BOOTSTRAP}',
  'properties.group.id' = 'flink-bars-trades',
  'scan.startup.mode' = 'earliest-offset',
  'scan.watermark.idle-timeout' = '5 s',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
)
"""

# NOTE: Coinbase `side` on a match is the MAKER side (docs: sell = up-tick).
# Columns are named maker_* to keep that explicit; taker-signed imbalance is
# derived downstream as (maker_sell_volume - maker_buy_volume) / volume.
TRADES_INSERT = """
INSERT INTO iceberg.market.trade_bars_1s
SELECT
  product_id,
  CAST(window_start AS TIMESTAMP(6)) AS bar_ts,
  COUNT(*) AS trade_count,
  SUM(CAST(`size` AS DOUBLE)) AS volume,
  SUM(CAST(`size` AS DOUBLE) * CAST(price AS DOUBLE)) AS notional,
  SUM(CASE WHEN side = 'buy' THEN CAST(`size` AS DOUBLE) ELSE 0e0 END)
    AS maker_buy_volume,
  SUM(CASE WHEN side = 'sell' THEN CAST(`size` AS DOUBLE) ELSE 0e0 END)
    AS maker_sell_volume,
  MIN(CAST(price AS DOUBLE)) AS price_min,
  MAX(CAST(price AS DOUBLE)) AS price_max
FROM TABLE(TUMBLE(TABLE matches_only, DESCRIPTOR(event_ts), INTERVAL '1' SECOND))
GROUP BY product_id, window_start, window_end
"""

BOOK_INSERT = """
INSERT INTO iceberg.market.book_bars_1s
SELECT
  product_id,
  CAST(window_start AS TIMESTAMP(6)) AS bar_ts,
  LAST_VALUE(mid) AS mid_close,
  MIN(mid) AS mid_min,
  MAX(mid) AS mid_max,
  AVG(spread) AS spread_mean,
  MAX(spread) AS spread_max,
  AVG(bid_depth5) AS bid_depth5_mean,
  AVG(ask_depth5) AS ask_depth5_mean,
  SUM(ofi_delta) AS ofi_sum,
  COUNT(*) AS updates,
  MAX(conn_epoch) AS conn_epoch_max
FROM TABLE(TUMBLE(TABLE book_tops, DESCRIPTOR(event_ts), INTERVAL '1' SECOND))
GROUP BY product_id, window_start, window_end
"""


class BookStateFn(KeyedProcessFunction):
    """Applies snapshot/l2update messages to a per-product OrderBook.

    The book lives in a Python dict cache for fast top-5 extraction and is
    written through to Flink keyed state (only changed levels per update),
    so a restored job continues mid-epoch without waiting for the next
    snapshot. The cache is rebuilt from state on the first message per key.
    """

    def open(self, ctx: RuntimeContext):
        self.bids_state = ctx.get_map_state(
            MapStateDescriptor("bids", Types.DOUBLE(), Types.DOUBLE())
        )
        self.asks_state = ctx.get_map_state(
            MapStateDescriptor("asks", Types.DOUBLE(), Types.DOUBLE())
        )
        self.meta_state = ctx.get_state(ValueStateDescriptor("meta", Types.PICKLED_BYTE_ARRAY()))
        self.books: dict[str, OrderBook] = {}
        self.dropped = 0

    def process_element(self, value, ctx):
        try:
            msg = json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return
        key = ctx.get_current_key()
        book = self.books.get(key)
        if book is None:
            meta = self.meta_state.value()
            if meta is not None:  # restored from checkpoint: rebuild cache
                book = OrderBook.restore(
                    dict(self.bids_state.items()), dict(self.asks_state.items()), meta
                )
            else:
                book = OrderBook()
            self.books[key] = book

        res = book.apply(msg)
        if res.dropped:
            self.dropped += 1
            if self.dropped % 1000 == 1:
                logging.warning("book %s: %d deltas dropped awaiting snapshot", key, self.dropped)
        if res.snapshot:
            self.bids_state.clear()
            self.asks_state.clear()
            # put_all takes an iterable of (k, v) pairs, NOT a dict
            self.bids_state.put_all(list(book.bids.items()))
            self.asks_state.put_all(list(book.asks.items()))
            self.meta_state.update(book.meta())
        elif res.changes:
            for side, price, size in res.changes:
                state = self.bids_state if side == "bid" else self.asks_state
                if size == 0:
                    # the feed can remove a level we never stored
                    try:
                        state.remove(price)
                    except KeyError:
                        pass
                else:
                    state.put(price, size)
            self.meta_state.update(book.meta())

        t = res.top
        if t is not None:
            yield Row(
                t.ts_ms,
                t.product_id,
                t.best_bid,
                t.best_bid_qty,
                t.best_ask,
                t.best_ask_qty,
                t.mid,
                t.spread,
                t.bid_depth5,
                t.ask_depth5,
                t.ofi_delta,
                t.conn_epoch,
            )


def main() -> None:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.enable_checkpointing(10_000, CheckpointingMode.EXACTLY_ONCE)
    env.get_checkpoint_config().set_min_pause_between_checkpoints(5_000)

    t_env = StreamTableEnvironment.create(env)
    t_env.get_config().set("table.local-time-zone", "UTC")
    t_env.get_config().set("pipeline.name", "bars-1s")

    t_env.execute_sql(ICEBERG_CATALOG_DDL)
    t_env.execute_sql(TRADES_SRC_DDL)
    t_env.create_temporary_view(
        "matches_only",
        t_env.sql_query("SELECT * FROM trades_src WHERE `type` = 'match'"),
    )

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP)
        .set_topics("book.l2.raw")
        .set_group_id("flink-bars-book")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    # Watermarks MUST be generated inside the source (per split, from Kafka
    # record timestamps ~= ingest time) and propagated via SOURCE_WATERMARK().
    # A single post-source generator sees bursty per-partition reads during
    # replay: one partition races ahead, and the other product's windows all
    # get dropped as late. Observed as ~45% missing book bars on backfill.
    watermarks = WatermarkStrategy.for_bounded_out_of_orderness(
        Duration.of_seconds(30)
    ).with_idleness(Duration.of_seconds(5))
    tops = (
        env.from_source(source, watermarks, "book.l2.raw")
        .key_by(_product_key, key_type=Types.STRING())
        .process(BookStateFn(), output_type=TOP_ROW_TYPE)
    )
    t_env.create_temporary_view(
        "book_tops",
        t_env.from_data_stream(
            tops,
            Schema.new_builder()
            .column("ts_ms", DataTypes.BIGINT())
            .column("product_id", DataTypes.STRING())
            .column("best_bid", DataTypes.DOUBLE())
            .column("best_bid_qty", DataTypes.DOUBLE())
            .column("best_ask", DataTypes.DOUBLE())
            .column("best_ask_qty", DataTypes.DOUBLE())
            .column("mid", DataTypes.DOUBLE())
            .column("spread", DataTypes.DOUBLE())
            .column("bid_depth5", DataTypes.DOUBLE())
            .column("ask_depth5", DataTypes.DOUBLE())
            .column("ofi_delta", DataTypes.DOUBLE())
            .column("conn_epoch", DataTypes.BIGINT())
            .column_by_expression("event_ts", "TO_TIMESTAMP_LTZ(ts_ms, 3)")
            .watermark("event_ts", "SOURCE_WATERMARK()")
            .build(),
        ),
    )

    stmt = t_env.create_statement_set()
    stmt.add_insert_sql(TRADES_INSERT)
    stmt.add_insert_sql(BOOK_INSERT)
    stmt.execute()


def _product_key(raw: str) -> str:
    try:
        return json.loads(raw).get("product_id") or "unknown"
    except (json.JSONDecodeError, TypeError):
        return "unknown"


if __name__ == "__main__":
    main()
