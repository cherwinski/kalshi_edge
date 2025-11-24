"""Simple scheduler for recurring Kalshi Edge workflows."""
from __future__ import annotations

import time
from typing import Any, Callable

import schedule

from .backtest import calibration as calibration_mod
from .backtest.results_store import (
    save_backtest_result,
    save_calibration_result,
)
from .backtest.strategy_threshold import run_threshold_backtest
from .ingest import historical_ingest
from .signals.generate_signals import generate_signals
from .execution.execute_signals import execute_signals
from .execution.exit_positions import process_take_profit_exits
from .portfolio.sync_positions import sync_positions
from .portfolio.pnl import snapshot_account_pnl
from .util.logging import get_logger

LOGGER = get_logger(__name__)
# Fine-grained threshold grid:
# - Low side: 0.01 through 0.15 (inclusive) in 0.01 increments.
# - High side: 0.80 through 0.99 (inclusive) in 0.01 increments.
LOW_THRESHOLDS = [round(x / 100, 2) for x in range(1, 16)]
HIGH_THRESHOLDS = [round(x / 100, 2) for x in range(80, 100)]
# TODO: expand categories once payloads are confirmed; start with None only.
CATEGORIES = [None]
# To keep backtests fast for dashboard, only run unbucketed expiry.
EXPIRY_BUCKETS = [None]
BACKTEST_SINCE_HOURS = 24 * 90  # last ~90 days
BACKTEST_ALLOWED_CATEGORIES = {"sports", "football", "basketball", "hockey", "nfl", "nba", "nhl"}


def ingest_recent_data(lookback_hours: int = 1) -> None:
    """Run a lightweight ingest to keep the local DB fresh."""

    LOGGER.info("Scheduled ingest: fetching recent prices (lookback=%sh)", lookback_hours)
    historical_ingest.ingest_recent(lookback_hours=lookback_hours)


def minute_cycle() -> None:
    """Fast loop to ingest, generate/execute signals, exits, and resync positions."""

    try:
        ingest_recent_data(lookback_hours=1)
    except Exception:
        LOGGER.exception("minute_cycle: ingest failed")

    try:
        generated = generate_signals(ev_threshold=0.02, max_signals=100)
        LOGGER.info("minute_cycle: generated %s signals", generated)
    except Exception:
        LOGGER.exception("minute_cycle: generate_signals failed")

    try:
        processed = execute_signals()
        LOGGER.info("minute_cycle: executed %s signals", processed)
    except Exception:
        LOGGER.exception("minute_cycle: execute_signals failed")

    try:
        # Refresh backtests/calibration so dashboard summaries stay current.
        run_all_backtests()
    except Exception:
        LOGGER.exception("minute_cycle: backtests/calibration failed")

    try:
        exits = process_take_profit_exits()
        if exits:
            LOGGER.info("minute_cycle: processed %s take-profit exits", exits)
    except Exception:
        LOGGER.exception("minute_cycle: exits failed")

    try:
        synced = sync_positions()
        LOGGER.info("minute_cycle: synced %s positions", synced)
    except Exception:
        LOGGER.exception("minute_cycle: sync_positions failed")


def run_all_backtests() -> None:
    """Execute both strategies and refresh the calibration curve."""

    LOGGER.info("Running scheduled backtests and calibration")

    for cat in CATEGORIES:
        for bucket in EXPIRY_BUCKETS:
            # YES-buying thresholds
            for t in HIGH_THRESHOLDS:
                stats, _ = run_threshold_backtest(
                    threshold=t,
                    direction="yes",
                    category=cat,
                    expiry_bucket=bucket,
                    since_hours=BACKTEST_SINCE_HOURS,
                    allowed_categories=BACKTEST_ALLOWED_CATEGORIES,
                )
                save_backtest_result(
                    strategy_name=f"threshold_yes_{t:.2f}",
                    params={
                        "threshold": t,
                        "direction": "yes",
                        "category": cat,
                        "expiry_bucket": bucket,
                    },
                    summary=stats,
                )
                LOGGER.info(
                    "YES threshold %.2f cat=%s bucket=%s: trades=%s win_rate=%.2f%% total_profit=%.4f",
                    t,
                    cat,
                    bucket,
                    stats["num_trades"],
                    stats["win_rate"] * 100,
                    stats["total_profit"],
                )

            # NO-buying thresholds
            for t in LOW_THRESHOLDS:
                stats, _ = run_threshold_backtest(
                    threshold=t,
                    direction="no",
                    category=cat,
                    expiry_bucket=bucket,
                    since_hours=BACKTEST_SINCE_HOURS,
                    allowed_categories=BACKTEST_ALLOWED_CATEGORIES,
                )
                save_backtest_result(
                    strategy_name=f"threshold_no_{t:.2f}",
                    params={
                        "threshold": t,
                        "direction": "no",
                        "category": cat,
                        "expiry_bucket": bucket,
                    },
                    summary=stats,
                )
                LOGGER.info(
                    "NO threshold %.2f cat=%s bucket=%s: trades=%s win_rate=%.2f%% total_profit=%.4f",
                    t,
                    cat,
                    bucket,
                    stats["num_trades"],
                    stats["win_rate"] * 100,
                    stats["total_profit"],
                )

    bin_edges = calibration_mod.EXTREME_BIN_EDGES
    buckets = calibration_mod.compute_calibration_with_bins(bin_edges=bin_edges)
    save_calibration_result(
        binning_mode="extreme",
        params={"bin_edges": bin_edges},
        buckets=buckets,
    )
    LOGGER.info("Calibration refreshed with %s buckets", len(buckets))

    try:
        generated = generate_signals(ev_threshold=0.02, max_signals=100)
        LOGGER.info("Generated %s new signals", generated)
    except Exception as exc:  # pragma: no cover - defensive
        LOGGER.exception("Error generating signals: %s", exc)

    try:
        processed = execute_signals()
        LOGGER.info("Execution processed %s signals", processed)
    except Exception as exc:  # pragma: no cover - defensive
        LOGGER.exception("Error executing signals: %s", exc)

    try:
        exits = process_take_profit_exits()
        if exits:
            LOGGER.info("Processed %s take-profit exits", exits)
    except Exception as exc:  # pragma: no cover - defensive
        LOGGER.exception("Error processing take-profit exits: %s", exc)

    try:
        synced = sync_positions()
        LOGGER.info("Synced %s positions from Kalshi portfolio", synced)
    except Exception as exc:  # pragma: no cover - defensive
        LOGGER.exception("Error syncing positions: %s", exc)

    try:
        snapshot_account_pnl()
        LOGGER.info("Account PnL snapshot stored")
    except Exception as exc:  # pragma: no cover - defensive
        LOGGER.exception("Error snapshotting PnL: %s", exc)


def _safe_job(job: Callable[..., Any], *args: Any, **kwargs: Any) -> None:
    try:
        job(*args, **kwargs)
    except Exception:  # pragma: no cover - defensive logging path
        LOGGER.exception("Scheduled job %s failed", getattr(job, "__name__", str(job)))


def main() -> None:
    LOGGER.info("Starting kalshi_edge scheduler")
    # Run the fast loop every minute: ingest, signals, execution, exits, sync.
    schedule.every(1).minutes.do(_safe_job, minute_cycle)
    schedule.every().day.at("02:00").do(_safe_job, run_all_backtests)

    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":  # pragma: no cover - manual execution
    main()
