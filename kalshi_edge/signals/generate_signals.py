"""Generate trading signals based on calibration-adjusted probabilities."""
from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Any, Callable, Dict, List, Optional, Tuple

from psycopg2.extras import RealDictCursor, Json

from ..backtest.results_store import get_latest_calibration_result
from ..db import get_connection
from ..util.logging import get_logger

LOGGER = get_logger(__name__)
EV_THRESHOLD_DEFAULT = 0.02
MAX_SIGNALS_DEFAULT = 100
EXPIRY_HARD_LIMIT_HOURS = 24
PRO_SPORTS_LONGSHOT_THRESHOLD = 0.15
PRO_SPORTS_CATEGORIES = {"sports", "nfl", "nba", "nhl", "football", "basketball", "hockey"}
COLLEGE_LONGSHOT_THRESHOLD = 0.02
COLLEGE_CATEGORIES = {"college", "ncaa", "ncaaf", "ncaab"}
COLLEGE_MIN_REMAINING = timedelta(hours=1)
PRO_INPLAY_BAND_LOW = 0.88
PRO_INPLAY_BAND_HIGH = 0.92
SPORTS_INPLAY_MAX_REMAINING = timedelta(minutes=30)
SPORT_TICKER_HINTS = ("NFL", "NBA", "NHL", "MLB", "EPL", "MLS", "NCAAF", "NCAAB", "START", "GAME")


def _build_probability_lookup() -> Callable[[float], float]:
    """Return a function mapping p_mkt -> p_true_est using latest calibration; identity if none."""

    calib = get_latest_calibration_result(binning_mode="extreme")
    if not calib:
        LOGGER.warning("No calibration cached; using identity p_true=p_mkt")
        return lambda p: p

    buckets = calib["buckets"]
    # Use closest p_mkt_avg bucket; fall back to identity if missing data.
    def lookup(p_mkt: float) -> float:
        closest = None
        best_delta = None
        for b in buckets:
            if b.get("p_mkt_avg") is None or b.get("p_true") is None:
                continue
            delta = abs(b["p_mkt_avg"] - p_mkt)
            if best_delta is None or delta < best_delta:
                best_delta = delta
                closest = b
        if closest is None:
            return p_mkt
        return float(closest["p_true"])

    return lookup


def _latest_prices(cursor: RealDictCursor) -> List[Dict[str, Any]]:
    """Fetch latest price per market."""

    cursor.execute(
        """
        SELECT DISTINCT ON (market_id)
            market_id,
            last_yes AS p_mkt,
            timestamp
        FROM prices
        ORDER BY market_id, timestamp DESC
        """
    )
    return cursor.fetchall()


def _market_meta(cursor: RealDictCursor, market_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    cursor.execute(
        """
        SELECT market_id, category, expiration_ts
        FROM markets
        WHERE market_id = ANY(%s)
        """,
        (market_ids,),
    )
    return {row["market_id"]: row for row in cursor.fetchall()}


def _expiry_bucket(expiration_ts: Optional[datetime]) -> Optional[str]:
    if not expiration_ts:
        return None
    now = datetime.now(timezone.utc)
    delta = expiration_ts - now
    if delta <= timedelta(days=1):
        return "short"
    if delta <= timedelta(days=7):
        return "medium"
    return "long"


def generate_signals(ev_threshold: float = EV_THRESHOLD_DEFAULT, max_signals: int = MAX_SIGNALS_DEFAULT) -> int:
    """Generate EV-positive signals based on latest prices and calibration."""

    p_true_fn = _build_probability_lookup()
    created = 0
    now = datetime.now(timezone.utc)
    hard_cutoff = now + timedelta(hours=EXPIRY_HARD_LIMIT_HOURS)

    with get_connection() as conn, conn.cursor(cursor_factory=RealDictCursor) as cursor:
        prices = _latest_prices(cursor)
        meta = _market_meta(cursor, [p["market_id"] for p in prices])

        for row in prices:
            market_id = row["market_id"]
            p_mkt = row.get("p_mkt")
            if p_mkt is None:
                continue
            p_true = p_true_fn(float(p_mkt))

            # YES side EV
            ev_yes = p_true - float(p_mkt)
            # NO side EV (selling YES / buying NO)
            ev_no = (1.0 - p_true) - (1.0 - float(p_mkt))

            info = meta.get(market_id, {})
            cat = info.get("category")
            exp_ts = info.get("expiration_ts")
            # Hard 24h expiry rule: skip anything without an expiry, already expired,
            # or beyond the cutoff window.
            if not exp_ts or exp_ts < now or exp_ts > hard_cutoff:
                LOGGER.debug("Skipping %s due to expiry outside 24h window (exp_ts=%s)", market_id, exp_ts)
                continue
            bucket = _expiry_bucket(exp_ts)

            # Enforce high-probability band (88-92%) unless explicitly overridden.
            price = float(p_mkt)
            if price < PRO_INPLAY_BAND_LOW or price > PRO_INPLAY_BAND_HIGH:
                LOGGER.debug("Skipping %s due to price band constraint (p=%.3f)", market_id, price)
                continue

            candidates: List[Tuple[str, float, bool]] = []
            if ev_yes >= ev_threshold:
                candidates.append(("yes", ev_yes, False))
            if ev_no >= ev_threshold:
                candidates.append(("no", ev_no, False))

            for side, ev, forced in candidates:
                cursor.execute(
                    """
                    INSERT INTO signals (
                        market_ticker, side, threshold, category, expiry_bucket,
                        p_mkt, p_true_est, expected_value, size, status
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        market_id,
                        side,
                        p_mkt,
                        cat,
                        bucket,
                        p_mkt,
                        p_true,
                        ev,
                        1,
                        "pending",
                    ),
                )
                created += 1
                if created >= max_signals:
                    conn.commit()
                    return created

        conn.commit()
    return created


def main() -> None:
    created = generate_signals()
    print(f"Created {created} signals")


if __name__ == "__main__":
    main()
