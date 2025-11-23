"""Shared helpers for Kalshi backtests."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

from psycopg2.extras import RealDictCursor

MIN_OPEN_INTEREST = 10


@dataclass
class Trade:
    market_id: str
    entry_price: float
    resolution: str
    profit: float
    entry_timestamp: Any


def compute_mid_price(row: Dict[str, Any]) -> Optional[float]:
    bid = row.get("bid_yes")
    ask = row.get("ask_yes")
    last = row.get("last_yes")
    if bid is not None and ask is not None:
        return float(bid + ask) / 2.0
    if last is not None:
        return float(last)
    return None


def has_liquidity(row: Dict[str, Any], minimum_open_interest: int = MIN_OPEN_INTEREST) -> bool:
    open_interest = row.get("open_interest")
    if open_interest is None:
        return True
    return open_interest >= minimum_open_interest


def compute_profit(resolution: str, entry_price: float) -> float:
    is_yes = (resolution or "").upper() == "YES"
    return (1.0 - entry_price) if is_yes else (-entry_price)


def find_first_entry(
    cursor: RealDictCursor,
    market_id: str,
    threshold: float,
    comparator: Callable[[float, float], bool],
) -> Optional[Trade]:
    cursor.execute(
        """
        SELECT timestamp, bid_yes, ask_yes, last_yes, volume, open_interest
        FROM prices
        WHERE market_id = %s
        ORDER BY timestamp ASC
        """,
        (market_id,),
    )
    for row in cursor.fetchall():
        mid = compute_mid_price(row)
        if mid is None or not has_liquidity(row):
            continue
        if comparator(mid, threshold):
            return Trade(
                market_id=market_id,
                entry_price=mid,
                resolution="",  # filled later
                profit=0.0,
                entry_timestamp=row["timestamp"],
            )
    return None


def max_drawdown(trades: List[Trade]) -> float:
    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    for trade in sorted(trades, key=lambda t: t.entry_timestamp):
        equity += trade.profit
        peak = max(peak, equity)
        drawdown = peak - equity
        max_dd = max(max_dd, drawdown)
    return max_dd
