"""Helpers to persist and fetch cached backtest/calibration results."""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from psycopg2.extras import Json

from ..db import get_connection


def save_backtest_result(strategy_name: str, params: Dict[str, Any], summary: Dict[str, Any]) -> None:
    """Persist a single backtest summary into backtest_results."""

    num_trades = summary.get("num_trades")
    win_rate = summary.get("win_rate")
    average_profit = summary.get("average_profit")
    total_profit = summary.get("total_profit")

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO backtest_results (
                strategy_name,
                params,
                num_trades,
                win_rate,
                average_profit,
                total_profit,
                raw_summary
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                strategy_name,
                Json(params),
                num_trades,
                win_rate,
                average_profit,
                total_profit,
                Json(summary),
            ),
        )
        conn.commit()


def get_latest_backtest_results() -> Dict[str, Dict[str, Any]]:
    """Fetch the most recent backtest result per known strategy.

    Only returns strategies that have at least one row; callers should handle missing keys.
    """

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT ON (strategy_name)
                strategy_name,
                params,
                num_trades,
                win_rate,
                average_profit,
                total_profit,
                created_at
            FROM backtest_results
            ORDER BY strategy_name, created_at DESC
            """
        )
        rows = cur.fetchall()

    return {
        row[0]: {
            "params": row[1],
            "num_trades": row[2],
            "win_rate": row[3],
            "average_profit": row[4],
            "total_profit": row[5],
            "created_at": row[6],
        }
        for row in rows
    }


def get_all_latest_backtest_results() -> Dict[str, Dict[str, Any]]:
    """Alias to fetch all strategies' most recent results."""

    return get_latest_backtest_results()


def list_backtest_results(limit: int = 200) -> List[Dict[str, Any]]:
    """Return recent backtest results ordered by created_at desc."""

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT strategy_name, params, num_trades, win_rate, average_profit, total_profit, raw_summary, created_at
            FROM backtest_results
            ORDER BY created_at DESC
            LIMIT %s
            """,
            (limit,),
        )
        rows = cur.fetchall()
    keys = [
        "strategy_name",
        "params",
        "num_trades",
        "win_rate",
        "average_profit",
        "total_profit",
        "raw_summary",
        "created_at",
    ]
    return [dict(zip(keys, row)) for row in rows]


def save_calibration_result(
    binning_mode: str, params: Dict[str, Any], buckets: List[Dict[str, Any]]
) -> None:
    """Persist a calibration bucket set into calibration_results."""

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO calibration_results (
                binning_mode,
                params,
                buckets
            )
            VALUES (%s, %s, %s)
            """,
            (
                binning_mode,
                Json(params),
                Json(buckets),
            ),
        )
        conn.commit()


def get_latest_calibration_result(binning_mode: str = "extreme") -> Optional[Dict[str, Any]]:
    """Fetch the latest calibration result for the requested binning mode."""

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT binning_mode, params, buckets, created_at
            FROM calibration_results
            WHERE binning_mode = %s
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (binning_mode,),
        )
        row = cur.fetchone()
        if not row:
            return None
        mode, params, buckets, created_at = row
        return {
            "binning_mode": mode,
            "params": params,
            "buckets": buckets,
            "created_at": created_at,
        }


def list_calibration_results(limit: int = 100) -> List[Dict[str, Any]]:
    """Return recent calibration results ordered by created_at desc."""

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT binning_mode, params, buckets, created_at
            FROM calibration_results
            ORDER BY created_at DESC
            LIMIT %s
            """,
            (limit,),
        )
        rows = cur.fetchall()
    keys = ["binning_mode", "params", "buckets", "created_at"]
    return [dict(zip(keys, row)) for row in rows]


__all__ = [
    "save_backtest_result",
    "get_latest_backtest_results",
    "get_all_latest_backtest_results",
    "list_backtest_results",
    "save_calibration_result",
    "get_latest_calibration_result",
    "list_calibration_results",
]
