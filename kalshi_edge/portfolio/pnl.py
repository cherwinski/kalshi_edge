"""Position and PnL maintenance helpers."""
from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Dict, Optional, Tuple

from psycopg2.extras import RealDictCursor

from ..db import connection_ctx, get_connection
from ..util.logging import get_logger

LOGGER = get_logger(__name__)


def _profit_yes(avg_price: float, trade_price: float, direction: str, size: int) -> float:
    """Profit for YES positions when closing size at trade_price."""
    delta = (trade_price - avg_price)
    return delta * size if direction == "sell" else -delta * size


def _profit_no(avg_yes_price: float, trade_yes_price: float, direction: str, size: int) -> float:
    """Profit for NO positions, using YES price representation."""
    delta = (avg_yes_price - trade_yes_price)
    return delta * size if direction == "buy" else -delta * size


def _update_position(cur: RealDictCursor, market_ticker: str, side: str, size_delta: int, price: float) -> Tuple[int, float, float]:
    """Update positions table and return (new_size, avg_price, realized_delta)."""
    cur.execute(
        """
        SELECT size, avg_entry_price, realized_pnl
        FROM positions
        WHERE market_ticker = %s AND side = %s
        """,
        (market_ticker, side),
    )
    row = cur.fetchone()
    size_prev = row["size"] if row else 0
    avg_prev = row["avg_entry_price"] if row else 0.0
    realized_prev = row["realized_pnl"] if row else 0.0

    realized_delta = 0.0
    size_new = size_prev + size_delta

    # Same direction extension
    if (size_prev >= 0 and size_delta >= 0) or (size_prev <= 0 and size_delta <= 0):
        total_cost = avg_prev * abs(size_prev) + price * abs(size_delta)
        denom = abs(size_prev) + abs(size_delta)
        avg_new = (total_cost / denom) if denom else 0.0
    else:
        # Closing/flip
        closing = min(abs(size_prev), abs(size_delta))
        if side == "yes":
            realized_delta = _profit_yes(avg_prev, price, "sell" if size_prev > 0 else "buy", closing)
        else:
            realized_delta = _profit_no(avg_prev, price, "buy" if size_prev < 0 else "sell", closing)
        # compute remaining
        if abs(size_delta) > abs(size_prev):
            # flipped direction
            remaining = abs(size_delta) - abs(size_prev)
            avg_new = price
            size_new = remaining if size_delta > 0 else -remaining
        else:
            avg_new = avg_prev

    cur.execute(
        """
        INSERT INTO positions (market_ticker, side, size, avg_entry_price, realized_pnl, updated_at)
        VALUES (%s, %s, %s, %s, %s, NOW())
        ON CONFLICT (market_ticker, side) DO UPDATE SET
          size = EXCLUDED.size,
          avg_entry_price = EXCLUDED.avg_entry_price,
          realized_pnl = positions.realized_pnl + EXCLUDED.realized_pnl,
          updated_at = NOW()
        """,
        (market_ticker, side, size_new, avg_new, realized_delta),
    )
    return size_new, avg_new, realized_delta


def record_trade(trade: Dict[str, Any]) -> None:
    """Persist trade and update positions/PnL."""
    with get_connection() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            INSERT INTO trades (signal_id, market_ticker, side, size, price, direction, executed_at)
            VALUES (%s, %s, %s, %s, %s, %s, COALESCE(%s, NOW()))
            """,
            (
                trade.get("signal_id"),
                trade["market_ticker"],
                trade["side"],
                trade["size"],
                trade["price"],
                trade["direction"],
                trade.get("executed_at"),
            ),
        )
        size_delta = trade["size"] if trade["direction"] == "buy" else -trade["size"]
        _update_position(cur, trade["market_ticker"], trade["side"], size_delta, trade["price"])
        conn.commit()


def snapshot_account_pnl(as_of: Optional[datetime] = None) -> None:
    """Compute realized/unrealized and store in account_pnl."""
    as_of = as_of or datetime.now(timezone.utc)
    as_of_date = as_of.date()

    with connection_ctx() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # realized
            cur.execute("SELECT COALESCE(SUM(realized_pnl),0) AS realized FROM positions")
            realized = float(cur.fetchone()["realized"])

            # latest prices per market
            cur.execute(
                """
                SELECT DISTINCT ON (market_id) market_id, last_yes
                FROM prices
                ORDER BY market_id, timestamp DESC
                """
            )
            latest = {row["market_id"]: row["last_yes"] for row in cur.fetchall()}

            # unrealized from positions
            cur.execute("SELECT market_ticker, side, size, avg_entry_price FROM positions")
            unrealized = 0.0
            for row in cur.fetchall():
                last = latest.get(row["market_ticker"])
                if last is None:
                    continue
                if row["side"] == "yes":
                    unrealized += (last - row["avg_entry_price"]) * row["size"]
                else:
                    unrealized += (row["avg_entry_price"] - last) * row["size"]

            total_equity = realized + unrealized
            cur.execute(
                """
                INSERT INTO account_pnl (as_of_date, realized_pnl, unrealized_pnl, total_equity)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (as_of_date) DO UPDATE SET
                  realized_pnl = EXCLUDED.realized_pnl,
                  unrealized_pnl = EXCLUDED.unrealized_pnl,
                  total_equity = EXCLUDED.total_equity
                """,
                (as_of_date, realized, unrealized, total_equity),
            )
            conn.commit()


__all__ = ["record_trade", "snapshot_account_pnl"]


def main() -> None:  # pragma: no cover - CLI helper
    snapshot_account_pnl()
    print("Snapshot PnL stored")


if __name__ == "__main__":  # pragma: no cover - manual run
    main()
