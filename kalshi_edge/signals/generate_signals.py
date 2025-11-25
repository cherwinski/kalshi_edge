"""Generate trading signals based on calibration-adjusted probabilities."""
from __future__ import annotations

import re
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
WEATHER_MIN_P = 0.03
DATE_TOKEN_RE = re.compile(r"(\d{1,2})([A-Z]{3})(\d{2})")
MONTH_MAP = {
    "JAN": 1,
    "FEB": 2,
    "MAR": 3,
    "APR": 4,
    "MAY": 5,
    "JUN": 6,
    "JUL": 7,
    "AUG": 8,
    "SEP": 9,
    "OCT": 10,
    "NOV": 11,
    "DEC": 12,
}


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
        SELECT market_id, name, category, expiration_ts
        FROM markets
        WHERE market_id = ANY(%s)
        """,
        (market_ids,),
    )
    return {row["market_id"]: row for row in cursor.fetchall()}


def _parse_market_date(text: Optional[str]) -> Optional[datetime]:
    """Parse a date token like 25NOV24 from market text, adjusting to game day."""

    if not text:
        return None
    match = DATE_TOKEN_RE.search(text.upper())
    if not match:
        return None
    day_str, month_code, year_str = match.groups()
    try:
        day = int(day_str)
        month = MONTH_MAP[month_code]
        year = 2000 + int(year_str)
    except (KeyError, ValueError):
        return None

    # Kalshi sports tickers typically encode the settlement day; shift back to game day.
    parsed = datetime(year, month, day, 23, 59, tzinfo=timezone.utc) - timedelta(days=1)
    return parsed


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
    hard_cutoff_default = now + timedelta(hours=EXPIRY_HARD_LIMIT_HOURS)

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
            cat_lower = (cat or "").lower()
            ticker_upper = market_id.upper()
            is_pro_sport = cat_lower in PRO_SPORTS_CATEGORIES
            is_college = cat_lower in COLLEGE_CATEGORIES
            is_sport_any = (
                is_pro_sport
                or is_college
                or ("sport" in cat_lower)
                or any(hint in ticker_upper for hint in SPORT_TICKER_HINTS)
            )

            market_name = info.get("name") or market_id
            parsed_exp_ts = None
            if is_sport_any:
                parsed_exp_ts = _parse_market_date(market_name) or _parse_market_date(market_id)
                if parsed_exp_ts and parsed_exp_ts < now:
                    parsed_exp_ts = None
            exp_candidates = [ts for ts in (info.get("expiration_ts"), parsed_exp_ts) if ts]
            exp_ts = min(exp_candidates) if exp_candidates else None
            hard_cutoff = parsed_exp_ts or hard_cutoff_default
            if not exp_ts or exp_ts < now or exp_ts > hard_cutoff:
                LOGGER.debug(
                    "Skipping %s due to expiry window (exp_ts=%s, cutoff=%s, name=%s)",
                    market_id,
                    exp_ts,
                    hard_cutoff,
                    market_name,
                )
                continue
            bucket = _expiry_bucket(exp_ts)

            price = float(p_mkt)

            # Skip low-probability weather markets (<3%).
            if "weather" in cat_lower and price < WEATHER_MIN_P and p_true < WEATHER_MIN_P:
                LOGGER.debug("Skipping weather market %s due to low prob (p=%.3f, p_true=%.3f)", market_id, price, p_true)
                continue

            # Primary rule: only 88-92% band unless an explicit override applies.
            allow_band = PRO_INPLAY_BAND_LOW <= price <= PRO_INPLAY_BAND_HIGH

            # Overrides:
            longshot_yes = is_pro_sport and price <= PRO_SPORTS_LONGSHOT_THRESHOLD
            longshot_college = (
                is_college
                and price <= COLLEGE_LONGSHOT_THRESHOLD
                and (exp_ts - now) >= COLLEGE_MIN_REMAINING
            )
            remaining = exp_ts - now
            pro_inplay_override = (
                is_sport_any and allow_band and remaining <= SPORTS_INPLAY_MAX_REMAINING and remaining > timedelta(0)
            )

            if not (allow_band or longshot_yes or longshot_college or pro_inplay_override):
                LOGGER.debug("Skipping %s due to band/override constraints (p=%.3f)", market_id, price)
                continue

            candidates: List[Tuple[str, float, bool]] = []
            if allow_band:
                # Primary rule flipped: prefer NO when price is 88-92%.
                if ev_no >= ev_threshold:
                    candidates.append(("no", ev_no, False))
            else:
                if ev_yes >= ev_threshold:
                    candidates.append(("yes", ev_yes, False))
                if ev_no >= ev_threshold:
                    candidates.append(("no", ev_no, False))

            # Add override-driven signals
            if longshot_yes:
                candidates.append(("yes", ev_yes, True))
            if longshot_college:
                candidates.append(("yes", ev_yes, True))
            if pro_inplay_override and allow_band:
                candidates.append(("no", ev_no, True))

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
