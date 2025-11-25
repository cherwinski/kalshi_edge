"""FastAPI app exposing backtest summaries and calibration."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List
from datetime import datetime, timezone, timedelta

from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates

from ..backtest.results_store import (
    get_all_latest_backtest_results,
    get_latest_calibration_result,
    list_backtest_results,
    list_calibration_results,
)
from ..db import get_connection
from ..signals.generate_signals import generate_signals
from ..execution.execute_signals import execute_signals
from ..config import get_initial_bankroll_usd

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
    def classify_rule(sig: Dict[str, Any]) -> str:
        """Lightweight justification tag based on pricing/rules."""

        side = (sig.get("side") or "").lower()
        p = float(sig.get("p_mkt") or 0.0)
        yes_prob = p if side == "yes" else (1.0 - p)

        if 0.88 <= yes_prob <= 0.92:
            return "Primary 88-92% rule"
        if yes_prob <= 0.02:
            return "College long-shot rule"
        if yes_prob <= 0.15:
            return "Pro long-shot rule"
        return "Other/override"

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT created_at, market_ticker, side, threshold, category, expiry_bucket,
                   p_mkt, p_true_est, expected_value, size, status,
                   execution_mode, order_id, executed_price, executed_size, last_error
            FROM signals
            ORDER BY
              CASE
                WHEN status = 'pending' THEN 0
                WHEN status = 'resting' THEN 1
                WHEN status = 'sent' THEN 2
                WHEN status = 'simulated' THEN 3
                WHEN status = 'executed' THEN 4
                WHEN status = 'ignored' THEN 5
                WHEN status = 'error' THEN 6
                ELSE 7
              END,
              created_at DESC
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
    out: List[Dict[str, Any]] = []
    for row in rows:
        sig = dict(zip(keys, row))
        sig["rule"] = classify_rule(sig)
        out.append(sig)
    return out


@app.get("/signals")
def list_signals(limit: int = 100) -> List[Dict[str, Any]]:
    return get_recent_signals(limit=limit)


@app.post("/signals/cancel_open")
def cancel_open_signals() -> Dict[str, Any]:
    """Cancel pending/resting/sent/simulated signals to free budget."""

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE signals
            SET status = 'cancelled',
                last_error = 'cancelled via dashboard',
                executed_price = COALESCE(executed_price, 0),
                executed_size = COALESCE(executed_size, 0)
            WHERE status IN ('pending','resting','sent','simulated')
            """
        )
        cancelled = cur.rowcount
        conn.commit()
    return {"cancelled": cancelled}


@app.post("/admin/generate_signals")
def admin_generate_signals() -> Dict[str, Any]:
    """Trigger signal generation."""

    created = generate_signals()
    return {"created": created}


@app.post("/admin/execute_signals")
def admin_execute_signals() -> Dict[str, Any]:
    """Trigger execution of pending signals."""

    processed = execute_signals()
    return {"processed": processed}


@app.post("/admin/reset_budget")
def admin_reset_budget() -> Dict[str, Any]:
    """Reset account_pnl to initial bankroll."""

    initial = get_initial_bankroll_usd()
    today = datetime.now(timezone.utc).date()
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("TRUNCATE account_pnl;")
        cur.execute(
            """
            INSERT INTO account_pnl (as_of_date, realized_pnl, unrealized_pnl, total_equity, created_at)
            VALUES (%s, %s, %s, %s, NOW())
            """,
            (today, 0.0, 0.0, initial),
        )
        conn.commit()
    return {"reset": True, "total_equity": initial}


def get_signal_status_summary() -> Dict[str, Any]:
    """Return counts by status and the latest signal timestamp."""

    conn = get_connection()
    counts: Dict[str, int] = {}
    latest_ts = None
    resting_risk = 0.0
    open_order_cost = 0.0
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT status, count(*)
            FROM signals
            GROUP BY status
            """
        )
        for status, cnt in cur.fetchall():
            counts[str(status)] = int(cnt)

        cur.execute("SELECT max(created_at) FROM signals")
        row = cur.fetchone()
        if row and row[0]:
            latest_ts = row[0]

        # Compute approximate risk for resting orders.
        cur.execute(
            """
            SELECT side, p_mkt, size
            FROM signals
            WHERE status = 'resting'
            """
        )
        for side, p_mkt, size in cur.fetchall():
            try:
                price = float(p_mkt or 0.0)
            except Exception:
                price = 0.0
            if price > 1.0:
                price = price / 100.0
            if price < 0:
                price = 0.0
            sz = abs(int(size or 0))
            if sz <= 0:
                continue
            if (side or "").lower() == "no":
                resting_risk += (1.0 - price) * sz
                open_order_cost += (1.0 - price) * sz
            else:
                resting_risk += price * sz
                open_order_cost += price * sz
    finally:
        conn.close()

    open_count = sum(
        counts.get(s, 0) for s in ("pending", "resting", "sent", "simulated")
    )
    message = ""
    if open_count == 0:
        if latest_ts:
            message = f"No open signals. Last signal at {latest_ts.isoformat()}"
        else:
            message = "No signals have been generated yet."

    return {
        "counts": counts,
        "latest_created_at": latest_ts.isoformat() if latest_ts else None,
        "message": message,
        "resting_risk": resting_risk,
        "open_order_cost": open_order_cost,
    }


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
    thresholds = {k: v for k, v in latest.items() if k.startswith("threshold_")}
    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "thresholds": thresholds,
            "signals": get_recent_signals(limit=50),
            "positions": list_positions(),
            "trades": list_trades(limit=50),
            "pnl_series": list_daily_pnl(limit=90),
            "exposure": get_current_exposure(),
            "signal_status": get_signal_status_summary(),
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
