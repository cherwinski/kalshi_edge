CREATE TABLE IF NOT EXISTS account_pnl (
    id SERIAL PRIMARY KEY,
    as_of_date DATE NOT NULL,
    realized_pnl DOUBLE PRECISION NOT NULL,
    unrealized_pnl DOUBLE PRECISION NOT NULL,
    total_equity DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (as_of_date)
);

CREATE INDEX IF NOT EXISTS idx_account_pnl_date ON account_pnl (as_of_date);
