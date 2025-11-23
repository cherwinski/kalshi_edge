ALTER TABLE signals
  ADD COLUMN IF NOT EXISTS execution_mode TEXT,
  ADD COLUMN IF NOT EXISTS order_id TEXT,
  ADD COLUMN IF NOT EXISTS sent_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS filled_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS executed_price DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS executed_size INTEGER,
  ADD COLUMN IF NOT EXISTS last_error TEXT;

CREATE INDEX IF NOT EXISTS idx_signals_status_market
  ON signals (status, market_ticker);

CREATE INDEX IF NOT EXISTS idx_signals_execution_mode
  ON signals (execution_mode);
