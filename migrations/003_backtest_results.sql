CREATE TABLE IF NOT EXISTS backtest_results (
  id BIGSERIAL PRIMARY KEY,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  strategy_name TEXT NOT NULL,
  params JSONB NOT NULL,
  num_trades INTEGER NOT NULL,
  win_rate DOUBLE PRECISION NOT NULL,
  average_profit DOUBLE PRECISION NOT NULL,
  total_profit DOUBLE PRECISION NOT NULL,
  raw_summary JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_backtest_results_created_at
  ON backtest_results (created_at DESC);

CREATE INDEX IF NOT EXISTS idx_backtest_results_strategy_created
  ON backtest_results (strategy_name, created_at DESC);
