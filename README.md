# kalshi_edge
Kalshi Bot

## Live trading (opt-in)

- Defaults are safe: keep `EXECUTION_MODE=simulate` in `.env`.
- To enable real orders:
  1. Set `KALSHI_ENV=demo` (or `live` if intended) and `EXECUTION_MODE=live` in `.env`.
  2. Ensure `KALSHI_API_KEY_ID` and `KALSHI_API_KEY_SECRET` (PEM path) are set.
  3. Verify risk caps: `MAX_RISK_PER_TRADE_USD`, `MAX_RISK_PER_MARKET_USD`, `MAX_RISK_TOTAL_USD`.
  4. Restart the scheduler service so it reloads env.
- Demo test order: `TEST_ORDER_TICKER=<ticker> TEST_ORDER_PRICE=0.50 EXECUTION_MODE=live poetry run python scripts/test_live_order.py`
- In live mode, scheduler will place orders for qualifying signals; monitor via `/signals` and systemd logs.

## Strategies & signals

- Threshold engine runs YES and NO sides using calibration-adjusted EV (ev_yes and ev_no). Positive NO EV produces buy-NO signals (sell-side of YES).
- Signals are stored in `signals` with EV, category, expiry bucket, and execution metadata; status is `simulated` in safe mode or updated with order IDs in live mode.

## Portfolio exposure & PnL

- Fills (sim/live) are logged in `trades`; positions are updated per market/side in `positions` with running `realized_pnl`.
- `sync_positions` pulls portfolio data from Kalshi (demo/live) to align local positions with the account.
- `snapshot_account_pnl` computes realized/unrealized PnL from positions + latest marks into `account_pnl` (equity curve).
- Dashboard shows positions, recent trades, signals, and an equity chart; calibration panel now shows sample count.

## Dynamic sizing & bankroll risk

- Configure bankroll: `INITIAL_BANKROLL_USD` (default 1000) and `MAX_RISK_FRACTION_PER_TRADE` (default 0.03 = 3%).
- Current bankroll uses the latest `account_pnl.total_equity` if available; otherwise falls back to `INITIAL_BANKROLL_USD`.
- Per-trade size is derived from `risk_per_contract` (price for YES, 1-price for NO) and capped by:
  - `MIN(per_trade_cap, market cap, total cap)` where `per_trade_cap = MIN(MAX_RISK_PER_TRADE_USD, MAX_RISK_FRACTION_PER_TRADE * bankroll)`.
- If the computed size is 0, the signal is ignored with a budget warning.
- Auto take-profit exits: positions are scanned each scheduler loop and closed when the price moves by `TAKE_PROFIT_FACTOR` (default 4.0× entry; e.g., 1% -> 4%).

## Make targets / helpers

- `make sync-positions` – pull portfolio positions from Kalshi into DB.
- `make snapshot-pnl` – recompute realized/unrealized PnL snapshot for today.
- `make signals` / `make execute-signals` – generate EV signals and execute them (simulate by default).
- `make exit-positions` – run the take-profit exit pass once.
- `make test-live-order` – tiny live/demo order smoke test (only with `EXECUTION_MODE=live`).
