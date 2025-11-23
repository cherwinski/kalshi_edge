CREATE TABLE IF NOT EXISTS trades (
    id SERIAL PRIMARY KEY,
    signal_id INTEGER REFERENCES signals(id) ON DELETE SET NULL,
    market_ticker TEXT NOT NULL,
    side TEXT NOT NULL, -- 'yes' or 'no'
    size INTEGER NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    direction TEXT NOT NULL, -- 'buy' or 'sell'
    executed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_trades_market ON trades (market_ticker);
CREATE INDEX IF NOT EXISTS idx_trades_executed_at ON trades (executed_at);
