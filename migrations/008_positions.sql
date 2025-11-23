CREATE TABLE IF NOT EXISTS positions (
    id SERIAL PRIMARY KEY,
    market_ticker TEXT NOT NULL,
    side TEXT NOT NULL, -- 'yes' or 'no'
    size INTEGER NOT NULL,
    avg_entry_price DOUBLE PRECISION NOT NULL,
    realized_pnl DOUBLE PRECISION NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (market_ticker, side)
);

CREATE INDEX IF NOT EXISTS idx_positions_market ON positions (market_ticker);
