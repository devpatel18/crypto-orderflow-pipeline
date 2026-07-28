-- Iceberg maintenance: compact tiny streaming files and reclaim old snapshots.
-- The Flink job commits every 10s, producing thousands of small parquet files
-- and snapshots that bloat storage and slow (eventually OOM) query planning.
-- Order matters: optimize writes big compacted files, expire_snapshots drops
-- the snapshots that still reference the tiny ones, remove_orphan_files then
-- deletes the now-unreferenced small files from object storage.
-- Requires iceberg.expire-snapshots.min-retention / remove-orphan-files
-- min-retention <= the thresholds below (set in iceberg.properties).

ALTER TABLE iceberg.market.book_bars_1s  EXECUTE optimize;
ALTER TABLE iceberg.market.trade_bars_1s EXECUTE optimize;
ALTER TABLE iceberg.market.features_5m   EXECUTE optimize;

ALTER TABLE iceberg.market.book_bars_1s  EXECUTE expire_snapshots(retention_threshold => '1h');
ALTER TABLE iceberg.market.trade_bars_1s EXECUTE expire_snapshots(retention_threshold => '1h');
ALTER TABLE iceberg.market.features_5m   EXECUTE expire_snapshots(retention_threshold => '1h');

ALTER TABLE iceberg.market.book_bars_1s  EXECUTE remove_orphan_files(retention_threshold => '1h');
ALTER TABLE iceberg.market.trade_bars_1s EXECUTE remove_orphan_files(retention_threshold => '1h');
ALTER TABLE iceberg.market.features_5m   EXECUTE remove_orphan_files(retention_threshold => '1h');
