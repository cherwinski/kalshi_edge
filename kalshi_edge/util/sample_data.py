"""Utilities for loading synthetic sample data for testing."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Iterable, List, Tuple

from psycopg2.extensions import connection as PGConnection

SAMPLE_MARKETS = [
    {
        "market_id": "MKT001",
        "name": "Election odds surge",
        "category": "politics",
        "resolution": "YES",
    },
    {
        "market_id": "MKT002",
        "name": "Policy fails to pass",
        "category": "politics",
        "resolution": "NO",
    },
    {
        "market_id": "MKT003",
        "name": "Sports upset",
        "category": "sports",
        "resolution": "YES",
    },
    {
        "market_id": "MKT004",
        "name": "Weather event",
        "category": "weather",
        "resolution": "NO",
    },
    {
        "market_id": "MKT005",
        "name": "Economic indicator",
        "category": "economics",
        "resolution": "YES",
    },
]


def _price_path(
    start_price: float,
    deltas: Iterable[float],
    start_time: datetime,
    open_interest: int = 50,
) -> List[Tuple[datetime, float]]:
    prices = []
    price = start_price
    current_time = start_time
    for delta in deltas:
        price = max(0.01, min(0.99, price + delta))
        prices.append((current_time, round(price, 2)))
        current_time += timedelta(minutes=5)
    return prices


def _build_price_series() -> dict:
    now = datetime.now(timezone.utc)
    return {
        "MKT001": _price_path(0.60, [0.05, 0.07, 0.08, 0.07, 0.05], now),  # ramps to ~0.95 then YES
        "MKT002": _price_path(0.65, [0.02, 0.01, 0.00, -0.05, -0.10, -0.13], now),  # spikes near 0.90 but falls to NO
        "MKT003": _price_path(0.40, [0.03] * 10, now),  # slow grind up crossing 0.90 before YES
        "MKT004": _price_path(0.55, [-0.15, -0.10, 0.02, 0.01, -0.05], now),  # dips near 0.10 yet resolves NO
        "MKT005": _price_path(0.50, [0.0, 0.0, 0.0, 0.0, 0.0], now),  # flat around 0.50 resolves YES
    }


PRICE_SERIES = _build_price_series()


def load_sample_markets(conn: PGConnection) -> int:
    """Insert sample markets if they do not already exist."""

    inserted = 0
    now = datetime.now(timezone.utc)
    with conn.cursor() as cursor:
        for market in SAMPLE_MARKETS:
            cursor.execute(
                "SELECT 1 FROM markets WHERE market_id = %s",
                (market["market_id"],),
            )
            if cursor.fetchone():
                continue
            cursor.execute(
                """
                INSERT INTO markets (market_id, name, category, resolution, resolved_at, created_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    market["market_id"],
                    market["name"],
                    market["category"],
                    market["resolution"],
                    now + timedelta(hours=1),
                    now,
                ),
            )
            inserted += 1
    conn.commit()
    return inserted


def load_sample_prices(conn: PGConnection) -> int:
    """Insert synthetic price snapshots for sample markets."""

    inserted = 0
    with conn.cursor() as cursor:
        for market_id, price_series in PRICE_SERIES.items():
            for timestamp, price in price_series:
                cursor.execute(
                    "SELECT 1 FROM prices WHERE market_id = %s AND timestamp = %s",
                    (market_id, timestamp),
                )
                if cursor.fetchone():
                    continue
                bid = max(price - 0.02, 0.01)
                ask = min(price + 0.02, 0.99)
                cursor.execute(
                    """
                    INSERT INTO prices (market_id, timestamp, bid_yes, ask_yes, last_yes, volume, open_interest)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        market_id,
                        timestamp,
                        bid,
                        ask,
                        price,
                        10,
                        50,
                    ),
                )
                inserted += 1
    conn.commit()
    return inserted


__all__ = ["load_sample_markets", "load_sample_prices"]
