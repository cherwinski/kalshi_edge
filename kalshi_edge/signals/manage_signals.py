"""Helpers to manage/cleanup signals."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from kalshi_edge.db import get_connection
from kalshi_edge.util.logging import get_logger

LOGGER = get_logger(__name__)


def cancel_stale_signals(max_age_minutes: int = 10) -> int:
    """
    Mark lingering open signals as cancelled to free risk budget.
    Cancels pending/resting/sent/simulated older than max_age_minutes.
    """

    cutoff = datetime.now(timezone.utc) - timedelta(minutes=max_age_minutes)
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE signals
            SET status = 'cancelled',
                last_error = 'auto-cancelled stale signal',
                executed_price = COALESCE(executed_price, 0),
                executed_size = COALESCE(executed_size, 0)
            WHERE status IN ('pending','resting','sent','simulated')
              AND created_at < %s
            """,
            (cutoff,),
        )
        cancelled = cur.rowcount
        conn.commit()
    if cancelled:
        LOGGER.info("Cancelled %s stale signals older than %s", cancelled, cutoff)
    return cancelled


__all__ = ["cancel_stale_signals"]
