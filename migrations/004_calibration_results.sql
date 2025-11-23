CREATE TABLE IF NOT EXISTS calibration_results (
  id BIGSERIAL PRIMARY KEY,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  binning_mode TEXT NOT NULL,
  params JSONB NOT NULL,
  buckets JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_calibration_results_created_at
  ON calibration_results (created_at DESC);

CREATE INDEX IF NOT EXISTS idx_calibration_results_mode_created
  ON calibration_results (binning_mode, created_at DESC);
