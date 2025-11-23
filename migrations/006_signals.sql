CREATE TABLE IF NOT EXISTS signals (
  id             BIGSERIAL PRIMARY KEY,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  market_ticker  TEXT NOT NULL,
  side           TEXT NOT NULL,              -- "yes" or "no"
  threshold      DOUBLE PRECISION NOT NULL,
  category       TEXT,
  expiry_bucket  TEXT,
  p_mkt          DOUBLE PRECISION NOT NULL,
  p_true_est     DOUBLE PRECISION NOT NULL,
  expected_value DOUBLE PRECISION NOT NULL,
  size           INTEGER NOT NULL DEFAULT 1,
  status         TEXT NOT NULL DEFAULT 'pending'
);

CREATE INDEX IF NOT EXISTS idx_signals_created_at
  ON signals (created_at DESC);

CREATE INDEX IF NOT EXISTS idx_signals_status_created
  ON signals (status, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_signals_market_side
  ON signals (market_ticker, side, status);
