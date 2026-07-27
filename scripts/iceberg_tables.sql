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

-- Gold: one row per (product, minute). Features from (as_of_ts - 5m, as_of_ts],
-- HAR-RV lags from up to 60m back, label = RV over (as_of_ts, label_ts].
-- Built incrementally by scripts/gold_features.sql (make gold).
CREATE TABLE IF NOT EXISTS iceberg.market.features_5m (
    product_id       varchar,
    as_of_ts         timestamp(6),
    label_ts         timestamp(6),
    rv_1m            double,
    rv_5m            double,
    rv_15m           double,
    rv_60m           double,
    ofi_5m           double,
    spread_mean_5m   double,
    spread_max_5m    double,
    depth_imb_5m     double,
    trade_count_5m   bigint,
    volume_5m        double,
    notional_5m      double,
    taker_imb_5m     double,
    bar_count_5m     bigint,
    label_rv_5m      double,
    label_bar_count  bigint,
    built_at         timestamp(6)
) WITH (partitioning = ARRAY['day(as_of_ts)']);

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
