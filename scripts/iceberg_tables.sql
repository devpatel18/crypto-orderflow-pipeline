CREATE SCHEMA IF NOT EXISTS iceberg.market;

CREATE TABLE IF NOT EXISTS iceberg.market.trade_bars_1s (
    product_id        varchar,
    bar_ts            timestamp(6),
    trade_count       bigint,
    volume            double,
    notional          double,
    maker_buy_volume  double,
    maker_sell_volume double,
    price_min         double,
    price_max         double
) WITH (partitioning = ARRAY['day(bar_ts)']);

CREATE TABLE IF NOT EXISTS iceberg.market.book_bars_1s (
    product_id       varchar,
    bar_ts           timestamp(6),
    mid_close        double,
    mid_min          double,
    mid_max          double,
    spread_mean      double,
    spread_max       double,
    bid_depth5_mean  double,
    ask_depth5_mean  double,
    ofi_sum          double,
    updates          bigint,
    conn_epoch_max   bigint
) WITH (partitioning = ARRAY['day(bar_ts)']);
