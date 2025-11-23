"""Take-profit exits for existing positions."""
from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List

from psycopg2.extras import RealDictCursor

from ..config import get_execution_mode, get_kalshi_env, get_take_profit_factor
from ..db import get_connection
from ..execution.client import ExecutionClient, OrderRequest
from ..portfolio.pnl import record_trade
from ..util.logging import get_logger

LOGGER = get_logger(__name__)
EXPIRY_HARD_LIMIT_HOURS = 24


def _fetch_positions_with_prices(cur: RealDictCursor) -> List[Dict[str, Any]]:
    cur.execute(
        """
        WITH latest_prices AS (
            SELECT DISTINCT ON (market_id) market_id, last_yes, timestamp
            FROM prices
            ORDER BY market_id, timestamp DESC
        )
        SELECT
            p.market_ticker,
            p.side,
            p.size,
            p.avg_entry_price,
            m.category,
            m.expiration_ts,
            lp.last_yes AS current_price
        FROM positions p
        LEFT JOIN markets m ON m.market_id = p.market_ticker
        LEFT JOIN latest_prices lp ON lp.market_id = p.market_ticker
        WHERE p.size <> 0
        """
    )
    return cur.fetchall()


def _should_take_profit(side: str, entry: float, current: float, factor: float) -> bool:
    if current is None or entry is None or entry <= 0:
        return False
    if side == "yes":
        return current >= entry * factor
    # side == "no": price fall is good
    return current <= entry / factor


def process_take_profit_exits() -> int:
    """Scan positions and submit exits when price moves favorably."""

    mode = get_execution_mode()
    env = get_kalshi_env()
    factor = get_take_profit_factor()
    now = datetime.now(timezone.utc)
    hard_cutoff = now + timedelta(hours=EXPIRY_HARD_LIMIT_HOURS)

    client = None
    if mode == "live":
        try:
            client = ExecutionClient()
            LOGGER.warning("Exit processing running in LIVE mode against %s", env)
        except Exception as exc:  # pragma: no cover - defensive
            LOGGER.exception("Failed to init ExecutionClient; falling back to simulate. %s", exc)
            mode = "simulate"

    processed = 0
    with get_connection() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        positions = _fetch_positions_with_prices(cur)
        for pos in positions:
            market = pos["market_ticker"]
            side = (pos["side"] or "").lower()
            size = int(pos["size"] or 0)
            entry = float(pos["avg_entry_price"] or 0.0)
            current = pos.get("current_price")
            exp_ts = pos.get("expiration_ts")
            cat = (pos.get("category") or "").lower()

            if size <= 0 or side not in ("yes", "no"):
                continue
            if not exp_ts or exp_ts < now or exp_ts > hard_cutoff:
                continue
            if current is None:
                continue
            current = float(current)

            college_fast_exit = (
                cat in {"college", "ncaa", "ncaaf", "ncaab"}
                and side == "yes"
                and entry <= 0.02
                and current >= 0.10
            )

            if not (_should_take_profit(side, entry, current, factor) or college_fast_exit):
                continue

            direction = "sell"  # closing reduces exposure
            if mode == "simulate":
                record_trade(
                    {
                        "signal_id": None,
                        "market_ticker": market,
                        "side": side,
                        "size": size,
                        "price": current,
                        "direction": direction,
                        "executed_at": now,
                    }
                )
                LOGGER.info("Simulated take-profit exit %s %s @ %.4f", side, market, current)
            else:
                try:
                    if client is None:
                        raise RuntimeError("Execution client unavailable for exits")
                    req = OrderRequest(
                        market_ticker=market,
                        side=side,
                        size=size,
                        price=current,
                        direction=direction,
                    )
                    resp = client.place_order(req)  # type: ignore[arg-type]
                    executed_price = float(resp.get("avg_price") or current)
                    executed_size = int(resp.get("filled_size") or size)
                    record_trade(
                        {
                            "signal_id": None,
                            "market_ticker": market,
                            "side": side,
                            "size": executed_size,
                            "price": executed_price,
                            "direction": direction,
                            "executed_at": now,
                        }
                    )
                    LOGGER.info(
                        "Placed take-profit exit %s %s size=%s price=%.4f order_id=%s",
                        side,
                        market,
                        executed_size,
                        executed_price,
                        resp.get("order_id"),
                    )
                except Exception as exc:  # pragma: no cover - defensive
                    LOGGER.exception("Failed exit for %s: %s", market, exc)
                    continue

            processed += 1
    return processed


def main() -> None:  # pragma: no cover - CLI helper
    count = process_take_profit_exits()
    print(f"Processed {count} take-profit exits")


if __name__ == "__main__":  # pragma: no cover - manual run
    main()
