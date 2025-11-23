CREATE TABLE IF NOT EXISTS markets (
    id BIGSERIAL PRIMARY KEY,
    market_id TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    category TEXT NULL,
    resolution TEXT NULL,
    resolved_at TIMESTAMPTZ NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS prices (
    id BIGSERIAL PRIMARY KEY,
    market_id TEXT NOT NULL REFERENCES markets(market_id),
    timestamp TIMESTAMPTZ NOT NULL,
    bid_yes NUMERIC(10,4) NULL,
    ask_yes NUMERIC(10,4) NULL,
    last_yes NUMERIC(10,4) NULL,
    volume INTEGER NULL,
    open_interest INTEGER NULL
);

CREATE INDEX IF NOT EXISTS idx_markets_market_id ON markets(market_id);
CREATE INDEX IF NOT EXISTS idx_prices_market_id_ts ON prices(market_id, timestamp);
