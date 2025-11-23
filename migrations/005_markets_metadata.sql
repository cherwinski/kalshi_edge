ALTER TABLE markets
  ADD COLUMN IF NOT EXISTS expiration_ts TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_markets_category ON markets (category);
CREATE INDEX IF NOT EXISTS idx_markets_expiration_ts ON markets (expiration_ts);
