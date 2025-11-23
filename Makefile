.PHONY: install lint test run-backtest run-backtest-0-90 run-backtest-0-10 run-calibration ingest-historical load-sample-data run-scheduler export-trades-0-90 export-trades-0-10

install:
	poetry install

lint:
	poetry run ruff check kalshi_edge || true

test:
	poetry run pytest

run-backtest: run-backtest-0-90

run-backtest-0-90:
	poetry run python -m kalshi_edge.backtest.strategy_0_90

run-backtest-0-10:
	poetry run python -m kalshi_edge.backtest.strategy_0_10

run-calibration:
	poetry run python -m kalshi_edge.backtest.calibration

ingest-historical:
	poetry run python -m kalshi_edge.ingest.historical_ingest --mode full --limit-markets 200

ingest-recent:
	poetry run python -m kalshi_edge.ingest.historical_ingest --mode recent --lookback-hours 1

load-sample-data:
	poetry run python scripts/load_sample_data.py

run-scheduler:
	poetry run python scripts/run_scheduler.py

export-trades-0-90:
	poetry run python -m kalshi_edge.backtest.strategy_0_90 --csv-path out/trades_0_90.csv

export-trades-0-10:
	poetry run python -m kalshi_edge.backtest.strategy_0_10 --csv-path out/trades_0_10.csv

run-api:
	poetry run python scripts/run_api.py

signals:
	poetry run python -m kalshi_edge.signals.generate_signals

execute-signals:
	poetry run python -m kalshi_edge.execution.execute_signals

exit-positions:
	poetry run python -m kalshi_edge.execution.exit_positions

sync-positions:
	poetry run python -m kalshi_edge.portfolio.sync_positions

snapshot-pnl:
	poetry run python -m kalshi_edge.portfolio.pnl

test-live-order:
	poetry run python scripts/test_live_order.py

dev:
	@echo "Start scheduler and API in separate terminals:"
	@echo "  Terminal 1 -> make run-scheduler"
	@echo "  Terminal 2 -> make run-api"
