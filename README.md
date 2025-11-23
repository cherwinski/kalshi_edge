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
