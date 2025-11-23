-- Ensure price snapshots are deduplicated per market/timestamp
CREATE UNIQUE INDEX IF NOT EXISTS idx_prices_market_id_ts_unique ON prices(market_id, timestamp);
