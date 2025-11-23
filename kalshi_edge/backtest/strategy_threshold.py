"""Generic threshold-based backtest engine."""
from __future__ import annotations

import operator
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple

from psycopg2.extras import RealDictCursor

from ..db import connection_ctx
from ..util.logging import get_logger
from .common import Trade, compute_profit, find_first_entry, max_drawdown

LOGGER = get_logger(__name__)


def _directional_profit(direction: str, resolution: str, yes_entry_price: float) -> float:
    """Compute profit depending on whether we bought YES or NO."""

    direction = direction.lower()
    if direction == "yes":
        return compute_profit(resolution, yes_entry_price)

    # Buying NO: price paid is (1 - yes_price); payout is 1 if NO resolves.
    no_price = 1.0 - yes_entry_price
    is_yes = (resolution or "").upper() == "YES"
    return -no_price if is_yes else (1.0 - no_price)


def _expiry_bucket_predicate(expiration_ts: Any, bucket: str | None) -> bool:
    if bucket is None:
        return True
    if expiration_ts is None:
        return False
    now = datetime.now(timezone.utc)
    delta = expiration_ts - now
    if bucket == "short":
        return delta <= timedelta(days=1)
    if bucket == "medium":
        return timedelta(days=1) < delta <= timedelta(days=7)
    if bucket == "long":
        return delta > timedelta(days=7)
    return True


def run_threshold_backtest(
    threshold: float,
    direction: str = "yes",
    category: str | None = None,
    expiry_bucket: str | None = None,
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """
    Backtest a simple threshold rule.

    direction = "yes": buy YES when yes_price >= threshold.
    direction = "no":  buy NO  when yes_price <= threshold.
    """

    direction = direction.lower()
    if direction not in ("yes", "no"):
        raise ValueError("direction must be 'yes' or 'no'")

    comparator = operator.ge if direction == "yes" else operator.le
    trades: List[Trade] = []

    with connection_ctx() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """
                SELECT market_id, resolution, category, expiration_ts
                FROM markets
                WHERE resolution IS NOT NULL
                """
            )
            markets = cursor.fetchall()
            for market in markets:
                if category and (market.get("category") or "").lower() != category.lower():
                    continue
                if not _expiry_bucket_predicate(market.get("expiration_ts"), expiry_bucket):
                    continue
                entry = find_first_entry(cursor, market["market_id"], threshold, comparator)
                if not entry:
                    continue

                # entry.entry_price holds the YES price; adjust if buying NO.
                yes_price = entry.entry_price
                entry_price_for_record = yes_price if direction == "yes" else (1.0 - yes_price)

                entry.resolution = market["resolution"] or "UNKNOWN"
                entry.profit = _directional_profit(direction, entry.resolution, yes_price)
                entry.entry_price = entry_price_for_record
                trades.append(entry)

    num_trades = len(trades)
    total_profit = sum(t.profit for t in trades)
    win_res = "YES" if direction == "yes" else "NO"
    wins = sum(1 for t in trades if (t.resolution or "").upper() == win_res)

    summary = {
        "threshold": threshold,
        "direction": direction,
        "category": category,
        "expiry_bucket": expiry_bucket,
        "num_trades": num_trades,
        "win_rate": (wins / num_trades) if num_trades else 0.0,
        "average_entry_price": (sum(t.entry_price for t in trades) / num_trades) if num_trades else 0.0,
        "average_profit": (total_profit / num_trades) if num_trades else 0.0,
        "total_profit": total_profit,
        "max_drawdown": max_drawdown(trades),
    }

    trade_dicts = [
        {
            "market_id": t.market_id,
            "entry_timestamp": t.entry_timestamp.isoformat()
            if hasattr(t.entry_timestamp, "isoformat")
            else t.entry_timestamp,
            "entry_price": t.entry_price,
            "resolution": t.resolution,
            "profit": t.profit,
            "direction": direction,
            "threshold": threshold,
        }
        for t in trades
    ]
    return summary, trade_dicts


__all__ = ["run_threshold_backtest"]
