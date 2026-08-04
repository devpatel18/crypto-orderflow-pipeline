-- Incremental gold build: minute-grid feature/label rows from the 1s bars.
-- Idempotent: only grid minutes after each product's max(as_of_ts) are added.
-- Point-in-time correctness by construction:
--   features aggregate bars with bar_ts <= as_of_ts only,
--   the label aggregates bars with bar_ts > as_of_ts only,
--   and tests/test_gold.py re-derives sampled rows from raw bars to verify.
-- Grid bounds require a full 61m of history and a complete 5m label window.
-- Returns are ln(mid_t / mid_prev-bar): computed once over the full series,
-- so the first in-window return correctly reaches back to the boundary bar.

INSERT INTO iceberg.market.features_5m
WITH r AS (
    SELECT
        product_id, bar_ts, spread_mean, spread_max,
        bid_depth5_mean, ask_depth5_mean, ofi_sum,
        ln(mid_close / lag(mid_close)
            OVER (PARTITION BY product_id ORDER BY bar_ts)) AS ret
    FROM iceberg.market.book_bars_1s
),
bounds AS (
    SELECT product_id,
           date_trunc('minute', min(bar_ts)) + interval '61' minute AS start_t,
           date_trunc('minute', max(bar_ts)) - interval '5' minute  AS end_t
    FROM iceberg.market.book_bars_1s
    GROUP BY 1
),
existing AS (
    SELECT product_id, max(as_of_ts) AS mx
    FROM iceberg.market.features_5m
    GROUP BY 1
),
grid AS (
    -- Start the minute series at the first un-built minute, NOT the global data
    -- start, so sequence() spans a single refresh window (tens of rows) instead
    -- of the whole history. Trino hard-caps sequence() at 10000 entries and the
    -- full-history span crosses that once the pipeline has run ~7 days; the old
    -- `WHERE t > max(as_of_ts)` filtered AFTER sequence() had already generated
    -- (and overflowed on) every historical minute. The greatest(...) on the
    -- stop bound keeps start<=stop when there are no new minutes (sequence then
    -- yields the single start row, which the WHERE below drops).
    SELECT b.product_id, s.t AS as_of_ts
    FROM bounds b
    LEFT JOIN existing e ON e.product_id = b.product_id
    CROSS JOIN UNNEST(sequence(
        greatest(b.start_t, coalesce(e.mx + interval '1' minute, b.start_t)),
        greatest(b.start_t, coalesce(e.mx + interval '1' minute, b.start_t), b.end_t),
        interval '1' minute
    )) AS s(t)
    WHERE s.t > coalesce(e.mx, timestamp '1970-01-01')
      AND s.t <= b.end_t
),
book_agg AS (
    SELECT
        g.product_id, g.as_of_ts,
        sqrt(sum(CASE WHEN r.bar_ts > g.as_of_ts - interval '1' minute
                       AND r.bar_ts <= g.as_of_ts THEN r.ret * r.ret END)) AS rv_1m,
        sqrt(sum(CASE WHEN r.bar_ts > g.as_of_ts - interval '5' minute
                       AND r.bar_ts <= g.as_of_ts THEN r.ret * r.ret END)) AS rv_5m,
        sqrt(sum(CASE WHEN r.bar_ts > g.as_of_ts - interval '15' minute
                       AND r.bar_ts <= g.as_of_ts THEN r.ret * r.ret END)) AS rv_15m,
        sqrt(sum(CASE WHEN r.bar_ts <= g.as_of_ts THEN r.ret * r.ret END)) AS rv_60m,
        sum(CASE WHEN r.bar_ts > g.as_of_ts - interval '5' minute
                  AND r.bar_ts <= g.as_of_ts THEN r.ofi_sum END) AS ofi_5m,
        avg(CASE WHEN r.bar_ts > g.as_of_ts - interval '5' minute
                  AND r.bar_ts <= g.as_of_ts THEN r.spread_mean END) AS spread_mean_5m,
        max(CASE WHEN r.bar_ts > g.as_of_ts - interval '5' minute
                  AND r.bar_ts <= g.as_of_ts THEN r.spread_max END) AS spread_max_5m,
        avg(CASE WHEN r.bar_ts > g.as_of_ts - interval '5' minute
                  AND r.bar_ts <= g.as_of_ts THEN r.bid_depth5_mean END) AS bid_depth_5m,
        avg(CASE WHEN r.bar_ts > g.as_of_ts - interval '5' minute
                  AND r.bar_ts <= g.as_of_ts THEN r.ask_depth5_mean END) AS ask_depth_5m,
        count(CASE WHEN r.bar_ts > g.as_of_ts - interval '5' minute
                    AND r.bar_ts <= g.as_of_ts THEN 1 END) AS bar_count_5m,
        sqrt(sum(CASE WHEN r.bar_ts > g.as_of_ts THEN r.ret * r.ret END)) AS label_rv_5m,
        count(CASE WHEN r.bar_ts > g.as_of_ts THEN 1 END) AS label_bar_count
    FROM grid g
    JOIN r
      ON r.product_id = g.product_id
     AND r.bar_ts > g.as_of_ts - interval '60' minute
     AND r.bar_ts <= g.as_of_ts + interval '5' minute
    GROUP BY 1, 2
),
trade_agg AS (
    SELECT
        g.product_id, g.as_of_ts,
        sum(t.trade_count) AS trade_count_5m,
        sum(t.volume) AS volume_5m,
        sum(t.notional) AS notional_5m,
        -- taker-signed: Coinbase side is the MAKER side, so taker buys = maker sells
        (sum(t.maker_sell_volume) - sum(t.maker_buy_volume))
            / nullif(sum(t.volume), 0) AS taker_imb_5m
    FROM grid g
    LEFT JOIN iceberg.market.trade_bars_1s t
      ON t.product_id = g.product_id
     AND t.bar_ts > g.as_of_ts - interval '5' minute
     AND t.bar_ts <= g.as_of_ts
    GROUP BY 1, 2
)
SELECT
    b.product_id,
    b.as_of_ts,
    b.as_of_ts + interval '5' minute AS label_ts,
    b.rv_1m, b.rv_5m, b.rv_15m, b.rv_60m,
    b.ofi_5m, b.spread_mean_5m, b.spread_max_5m,
    (b.bid_depth_5m - b.ask_depth_5m)
        / nullif(b.bid_depth_5m + b.ask_depth_5m, 0) AS depth_imb_5m,
    coalesce(t.trade_count_5m, 0) AS trade_count_5m,
    coalesce(t.volume_5m, 0e0) AS volume_5m,
    coalesce(t.notional_5m, 0e0) AS notional_5m,
    t.taker_imb_5m,
    b.bar_count_5m,
    b.label_rv_5m,
    b.label_bar_count,
    localtimestamp(6) AS built_at
FROM book_agg b
LEFT JOIN trade_agg t
  ON t.product_id = b.product_id AND t.as_of_ts = b.as_of_ts
