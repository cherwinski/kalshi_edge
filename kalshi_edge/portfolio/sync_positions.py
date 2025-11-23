"""Sync positions from Kalshi Portfolio API into local positions table.

Note: kalshi_python Position model currently exposes `position` (YES count) and `total_cost`.
NO-side exposure is not directly provided; this sync captures YES positions only and leaves NO
positions untouched. Extend as the SDK surface improves.
"""
from __future__ import annotations

from typing import Any, Dict

from kalshi_python.api.portfolio_api import PortfolioApi

from ..config import get_kalshi_creds, get_kalshi_env
from ..db import connection_ctx
from ..util.logging import get_logger

LOGGER = get_logger(__name__)


def sync_positions() -> int:
    env = get_kalshi_env()
    key_id, key_secret = get_kalshi_creds()

    # Reuse ingest-style configuration
    from kalshi_python import ApiClient, Configuration

    cfg = Configuration()
    cfg.host = "https://demo-api.kalshi.co/trade-api/v2" if env == "demo" else "https://api.elections.kalshi.com/trade-api/v2"
    api_client = ApiClient(configuration=cfg)
    api_client.set_kalshi_auth(key_id, key_secret)
    api = PortfolioApi(api_client)

    resp = api.get_positions()
    positions = resp.positions or []
    updated = 0

    with connection_ctx() as conn, conn.cursor() as cur:
        # Reset to reflect current portfolio; avoids stale/cancelled orders lingering locally.
        cur.execute("TRUNCATE positions;")
        for pos in positions:
            ticker = getattr(pos, "ticker", None)
            count = getattr(pos, "position", None)
            cost_cents = getattr(pos, "total_cost", None)
            if ticker is None or count is None or cost_cents is None:
                continue
            if count == 0:
                continue
            avg_price = (cost_cents / 100.0) / abs(count)
            cur.execute(
                """
                INSERT INTO positions (market_ticker, side, size, avg_entry_price, updated_at)
                VALUES (%s, %s, %s, %s, NOW())
                ON CONFLICT (market_ticker, side) DO UPDATE SET
                  size = EXCLUDED.size,
                  avg_entry_price = EXCLUDED.avg_entry_price,
                  updated_at = NOW()
                """,
                (ticker, "yes", count, avg_price),
            )
            updated += 1
        conn.commit()

    return updated


if __name__ == "__main__":
    count = sync_positions()
    print(f"Synced {count} positions from Kalshi")
