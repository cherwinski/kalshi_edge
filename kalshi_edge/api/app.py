"""FastAPI app exposing backtest summaries and calibration."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates
from datetime import datetime, timezone

from ..backtest.results_store import (
    get_all_latest_backtest_results,
    get_latest_calibration_result,
    list_backtest_results,
    list_calibration_results,
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


@app.get("/positions")
def list_positions() -> List[Dict[str, Any]]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT p.market_ticker, p.side, p.size, p.avg_entry_price, p.realized_pnl, m.category, m.expiration_ts
            FROM positions p
            LEFT JOIN markets m ON p.market_ticker = m.market_id
            ORDER BY p.updated_at DESC
            """
        )
        rows = cur.fetchall()
    finally:
        conn.close()
    keys = ["market_ticker", "side", "size", "avg_entry_price", "realized_pnl", "category", "expiration_ts"]
    return [dict(zip(keys, row)) for row in rows]


@app.get("/pnl/daily")
def list_daily_pnl(limit: int = 90) -> List[Dict[str, Any]]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT as_of_date, realized_pnl, unrealized_pnl, total_equity
            FROM account_pnl
            ORDER BY as_of_date DESC
            LIMIT %s
            """,
            (limit,),
        )
        rows = cur.fetchall()
    finally:
        conn.close()
    keys = ["as_of_date", "realized_pnl", "unrealized_pnl", "total_equity"]
    formatted: List[Dict[str, Any]] = []
    for row in rows:
        item = dict(zip(keys, row))
        if item.get("as_of_date") is not None:
            item["as_of_date"] = item["as_of_date"].isoformat()
        formatted.append(item)
    return formatted[::-1]


@app.get("/pnl/trades")
def list_trades(limit: int = 100) -> List[Dict[str, Any]]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT executed_at, market_ticker, side, direction, size, price
            FROM trades
            ORDER BY executed_at DESC
            LIMIT %s
            """,
            (limit,),
        )
        rows = cur.fetchall()
    finally:
        conn.close()
    keys = ["executed_at", "market_ticker", "side", "direction", "size", "price"]
    return [dict(zip(keys, row)) for row in rows]


def get_current_exposure() -> Dict[str, float]:
    """Estimate current risk in play from positions and open signals."""

    def _norm_price(price: float) -> float:
        try:
            p = float(price or 0.0)
        except Exception:
            return 0.0
        if p > 1.0:
            p = p / 100.0
        if p < 0:
            p = 0.0
        return p

    def _risk(side: str, price: float, size: int) -> float:
        side = (side or "").lower()
        price_f = _norm_price(price)
        size_i = abs(int(size or 0))
        if size_i <= 0:
            return 0.0
        return (1.0 - price_f) * size_i if side == "no" else price_f * size_i

    pos_risk = 0.0
    sig_risk = 0.0

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT p.side, p.avg_entry_price, p.size, m.expiration_ts, p.updated_at
            FROM positions p
            LEFT JOIN markets m ON p.market_ticker = m.market_id
            """
        )
        now = datetime.now(timezone.utc)
        stale_cutoff = now - timedelta(days=2)
        for side, avg_price, size, expiration_ts, updated_at in cur.fetchall():
            # Ignore positions with no known future expiry or very stale rows.
            if not expiration_ts or expiration_ts < now:
                continue
            if updated_at and updated_at < stale_cutoff:
                continue
            pos_risk += _risk(side, avg_price, size)

        cur.execute(
            """
            SELECT side, p_mkt, size
            FROM signals
            WHERE status IN ('pending', 'sent', 'resting', 'simulated')
            """
        )
        for side, p_mkt, size in cur.fetchall():
            sig_risk += _risk(side, p_mkt, size)
    finally:
        conn.close()

    total = pos_risk + sig_risk
    return {
        "total_exposure": total,
        "positions_exposure": pos_risk,
        "signals_exposure": sig_risk,
    }


@app.get("/exposure")
def exposure() -> Dict[str, float]:
    return get_current_exposure()


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
            "positions": list_positions(),
            "trades": list_trades(limit=50),
            "pnl_series": list_daily_pnl(limit=90),
            "exposure": get_current_exposure(),
        },
    )


@app.get("/reports")
def reports(request: Request) -> Any:
    backtests = list_backtest_results(limit=200)
    calibrations = list_calibration_results(limit=50)
    return templates.TemplateResponse(
        "reports.html",
        {
            "request": request,
            "backtests": backtests,
            "calibrations": calibrations,
        },
    )
