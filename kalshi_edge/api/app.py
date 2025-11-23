"""FastAPI app exposing backtest summaries and calibration."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates

from ..backtest.results_store import (
    get_all_latest_backtest_results,
    get_latest_calibration_result,
)
from ..db import get_connection

app = FastAPI(title="Kalshi Edge Dashboard")
templates = Jinja2Templates(directory=str(Path(__file__).resolve().parent.parent.parent / "templates"))


def _run_strategy_summary(run_fn) -> Dict[str, Any]:
    summary, _ = run_fn()
    return {
        "num_trades": summary["num_trades"],
        "win_rate": summary["win_rate"],
        "avg_profit": summary["average_profit"],
        "total_profit": summary["total_profit"],
    }


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.get("/summary")
def summary() -> Dict[str, Any]:
    latest = get_all_latest_backtest_results()
    # Backward-compatible keys for legacy dashboard cards.
    strategy_0_90 = (
        latest.get("strategy_0_90")
        or latest.get("threshold_yes_0.90")
        or latest.get("threshold_yes_0.9")
    )
    strategy_0_10 = (
        latest.get("strategy_0_10")
        or latest.get("threshold_no_0.10")
        or latest.get("threshold_no_0.1")
    )
    strategies = dict(latest)
    strategies["strategy_0_90"] = strategy_0_90
    strategies["strategy_0_10"] = strategy_0_10
    return {
        "strategies": strategies,
        "strategy_0_90": strategy_0_90,
        "strategy_0_10": strategy_0_10,
    }


@app.get("/calibration")
def calibration_buckets() -> List[Dict[str, Any]]:
    result = get_latest_calibration_result(binning_mode="extreme")
    if not result:
        return []
    return result["buckets"]


def get_recent_signals(limit: int = 100) -> List[Dict[str, Any]]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT created_at, market_ticker, side, threshold, category, expiry_bucket,
                   p_mkt, p_true_est, expected_value, size, status,
                   execution_mode, order_id, executed_price, executed_size, last_error
            FROM signals
            ORDER BY created_at DESC
            LIMIT %s
            """,
            (limit,),
        )
        rows = cur.fetchall()
    finally:
        conn.close()

    keys = [
        "created_at",
        "market_ticker",
        "side",
        "threshold",
        "category",
        "expiry_bucket",
        "p_mkt",
        "p_true_est",
        "expected_value",
        "size",
        "status",
        "execution_mode",
        "order_id",
        "executed_price",
        "executed_size",
        "last_error",
    ]
    return [dict(zip(keys, row)) for row in rows]


@app.get("/signals")
def list_signals(limit: int = 100) -> List[Dict[str, Any]]:
    return get_recent_signals(limit=limit)


@app.get("/")
def dashboard(request: Request) -> Any:
    latest = get_all_latest_backtest_results()
    strategy_0_90 = (
        latest.get("strategy_0_90")
        or latest.get("threshold_yes_0.90")
        or latest.get("threshold_yes_0.9")
    )
    strategy_0_10 = (
        latest.get("strategy_0_10")
        or latest.get("threshold_no_0.10")
        or latest.get("threshold_no_0.1")
    )
    thresholds = {k: v for k, v in latest.items() if k.startswith("threshold_")}
    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "summary": {
                "strategy_0_90": strategy_0_90,
                "strategy_0_10": strategy_0_10,
            },
            "thresholds": thresholds,
            "signals": get_recent_signals(limit=50),
        },
    )
