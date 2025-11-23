"""Historical Kalshi market ingestion using the official SDK."""
from __future__ import annotations

import argparse
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence

from kalshi_python.exceptions import NotFoundException

from ..db import connection_ctx
from ..util.logging import get_logger
from .kalshi_sdk_client import KalshiSDKClient

LOGGER = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest historical Kalshi data")
    parser.add_argument("--limit-markets", type=int, default=None, help="Only ingest first N markets")
    parser.add_argument(
        "--mode",
        choices=["full", "recent"],
        default="full",
        help="Full backfill or incremental recent ingest",
    )
    parser.add_argument("--lookback-hours", type=int, default=1, help="Lookback window for recent ingest")
    return parser.parse_args()


def _parse_dt(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=timezone.utc)
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None
    return None


def normalize_market(market: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "market_id": market.get("ticker") or market.get("market_id") or market.get("id"),
        "name": market.get("title") or market.get("name") or market.get("ticker"),
        "category": market.get("category"),
        "resolution": market.get("result") or market.get("status"),
        "resolved_at": _parse_dt(market.get("expiration_time") or market.get("close_time")),
        "created_at": _parse_dt(market.get("open_time")),
        "expiration_ts": _parse_dt(market.get("expiration_time") or market.get("close_time")),
        "series_ticker": market.get("series_ticker") or market.get("seriesTicker"),
    }


def upsert_market(cursor, market: Dict[str, Any]) -> None:
    cursor.execute(
        """
        INSERT INTO markets (market_id, name, category, resolution, resolved_at, created_at, expiration_ts)
        VALUES (%s, %s, %s, %s, %s, COALESCE(%s, NOW()), %s)
        ON CONFLICT (market_id) DO UPDATE SET
            name = EXCLUDED.name,
            category = EXCLUDED.category,
            resolution = EXCLUDED.resolution,
            resolved_at = EXCLUDED.resolved_at,
            expiration_ts = EXCLUDED.expiration_ts
        """,
        (
            market["market_id"],
            market["name"],
            market["category"],
            market["resolution"],
            market["resolved_at"],
            market["created_at"],
            market.get("expiration_ts"),
        ),
    )


def _normalize_yes_price(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    if value > 1:
        return float(value) / 100.0
    return float(value)


def _candles_to_price_rows(market_id: str, candles: Iterable[Dict[str, Any]]) -> Iterable[Dict[str, Any]]:
    for candle in candles:
        close_ts = candle.get("end_ts") or candle.get("close_ts") or candle.get("timestamp")
        if close_ts is None:
            continue
        timestamp = _parse_dt(close_ts)
        if timestamp is None:
            continue

        # SDK candlesticks expose OHLC in cents; use close as the representative YES price.
        yes_price = _normalize_yes_price(candle.get("close"))
        yield {
            "market_id": market_id,
            "timestamp": timestamp,
            "last_yes": yes_price,
            "bid_yes": None,
            "ask_yes": None,
            "volume": candle.get("volume"),
            "open_interest": None,
        }


def insert_price(cursor, row: Dict[str, Any]) -> bool:
    cursor.execute(
        """
        INSERT INTO prices (market_id, timestamp, bid_yes, ask_yes, last_yes, volume, open_interest)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (market_id, timestamp) DO NOTHING
        """,
        (
            row["market_id"],
            row["timestamp"],
            row["bid_yes"],
            row["ask_yes"],
            row["last_yes"],
            row["volume"],
            row["open_interest"],
        ),
    )
    return cursor.rowcount > 0


def _ingest_market_candles(
    cursor,
    client: KalshiSDKClient,
    market: Dict[str, Any],
    start_ts: Optional[int] = None,
    end_ts: Optional[int] = None,
) -> int:
    market_id = market.get("market_id")
    series_ticker = market.get("series_ticker") or market.get("seriesTicker")
    if not market_id or not series_ticker:
        LOGGER.warning("Skipping market without identifiers: %s", market)
        return 0

    candles = client.get_market_candles(
        series_ticker=series_ticker,
        market_ticker=market_id,
        start_ts=start_ts,
        end_ts=end_ts,
    )
    inserted = 0
    for row in _candles_to_price_rows(market_id, candles):
        if insert_price(cursor, row):
            inserted += 1
    return inserted


def backfill_full_history(
    status: str = "settled",
    max_markets: int | None = None,
) -> None:
    """Use the SDK to backfill historical prices for all settled markets."""

    client = KalshiSDKClient()
    start_ts_env = os.getenv("KALSHI_BACKFILL_START_TS")
    start_ts = int(start_ts_env) if start_ts_env else None

    with connection_ctx() as conn, conn.cursor() as cursor:
        for idx, market in enumerate(client.iter_markets_allow_invalid(status=status)):
            if max_markets is not None and idx >= max_markets:
                break

            normalized = normalize_market(market)
            if not normalized["market_id"]:
                LOGGER.warning("Skipping market without id: %s", market)
                continue

            upsert_market(cursor, normalized)
            inserted = _ingest_market_candles(cursor, client, normalized, start_ts=start_ts)
            if inserted:
                LOGGER.info("Market %s: inserted %d price rows", normalized["market_id"], inserted)
        conn.commit()


def _recent_market_ids(conn, cutoff: datetime) -> Sequence[str]:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT market_id
            FROM markets
            WHERE resolution IS NULL OR resolved_at >= %s
            """,
            (cutoff,),
        )
        return [row[0] for row in cursor.fetchall()]


def ingest_recent(
    lookback_hours: int = 1,
    status: str = "open",
    limit_markets: Optional[int] = None,
) -> None:
    """Incremental ingest using Kalshi candlesticks for the last N hours."""

    client = KalshiSDKClient()
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=lookback_hours)
    start_ts = int(cutoff.timestamp())
    end_ts = int(now.timestamp())
    series_lookup: Dict[str, Optional[str]] = {}

    with connection_ctx() as conn, conn.cursor() as cursor:
        for idx, market in enumerate(client.iter_markets_allow_invalid(status=status)):
            if limit_markets is not None and idx >= limit_markets:
                LOGGER.info("Stopping after %d markets due to limit", limit_markets)
                break
            normalized = normalize_market(market)
            if not normalized["market_id"]:
                continue
            upsert_market(cursor, normalized)
            series_lookup[normalized["market_id"]] = normalized.get("series_ticker")

        target_ids = _recent_market_ids(conn, cutoff)
        for market_id in target_ids:
            normalized_market = {
                "market_id": market_id,
                "series_ticker": series_lookup.get(market_id)
                or (market_id.split(".")[0] if "." in market_id else market_id),
            }
            try:
                inserted = _ingest_market_candles(
                    cursor,
                    client,
                    normalized_market,
                    start_ts=start_ts,
                    end_ts=end_ts,
                )
                if inserted:
                    LOGGER.info("Inserted %d recent prices for %s", inserted, market_id)
            except NotFoundException:
                LOGGER.warning("Skipping market %s: candlesticks not found", market_id)
                continue
        conn.commit()


def main() -> None:
    args = parse_args()
    if args.mode == "recent":
        ingest_recent(lookback_hours=args.lookback_hours, limit_markets=args.limit_markets)
    else:
        backfill_full_history(status="settled", max_markets=args.limit_markets)


if __name__ == "__main__":
    main()
