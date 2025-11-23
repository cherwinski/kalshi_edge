"""Consume pending signals, enforce risk limits, and simulate or send orders."""
from __future__ import annotations

from typing import Any, Dict, List

from kalshi_edge.config import get_execution_mode, get_risk_limits
from kalshi_edge.db import get_connection
from kalshi_edge.execution.client import ExecutionClient, OrderRequest
from kalshi_edge.util.logging import get_logger

LOGGER = get_logger(__name__)


def fetch_pending_signals(limit: int = 50) -> List[Dict[str, Any]]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, created_at, market_ticker, side, threshold, category, expiry_bucket,
                   p_mkt, p_true_est, expected_value, size, status
            FROM signals
            WHERE status = 'pending'
            ORDER BY created_at ASC
            LIMIT %s
            """,
            (limit,),
        )
        rows = cur.fetchall()
    finally:
        conn.close()

    cols = [
        "id",
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
    ]
    return [dict(zip(cols, row)) for row in rows]


def estimate_trade_risk_usd(signal: Dict[str, Any]) -> float:
    p_mkt = float(signal["p_mkt"])
    size = int(signal["size"])
    side = (signal["side"] or "").lower()
    if side == "yes":
        return p_mkt * size
    return (1.0 - p_mkt) * size


def compute_existing_risk(conn) -> Dict[str, Any]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT market_ticker, side, p_mkt, size
        FROM signals
        WHERE status IN ('pending', 'sent')
        """
    )
    per_market: Dict[str, float] = {}
    total = 0.0
    for market_ticker, side, p_mkt, size in cur.fetchall():
        sig = {"market_ticker": market_ticker, "side": side, "p_mkt": float(p_mkt), "size": int(size)}
        r = estimate_trade_risk_usd(sig)
        per_market[market_ticker] = per_market.get(market_ticker, 0.0) + r
        total += r
    return {"total": total, "per_market": per_market}


def update_signal_execution(
    signal_id: int,
    *,
    status: str,
    execution_mode: str,
    order_id: str | None = None,
    executed_price: float | None = None,
    executed_size: int | None = None,
    error: str | None = None,
) -> None:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE signals
            SET status = %s,
                execution_mode = %s,
                order_id = COALESCE(%s, order_id),
                executed_price = COALESCE(%s, executed_price),
                executed_size = COALESCE(%s, executed_size),
                last_error = COALESCE(%s, last_error),
                sent_at = CASE WHEN %s IN ('sent', 'filled', 'simulated') AND sent_at IS NULL THEN NOW() ELSE sent_at END,
                filled_at = CASE WHEN %s = 'filled' AND filled_at IS NULL THEN NOW() ELSE filled_at END
            WHERE id = %s
            """,
            (
                status,
                execution_mode,
                order_id,
                executed_price,
                executed_size,
                error,
                status,
                status,
                signal_id,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def execute_signals(batch_limit: int = 50) -> int:
    mode = get_execution_mode()
    limits = get_risk_limits()
    signals = fetch_pending_signals(limit=batch_limit)
    if not signals:
        return 0

    client = ExecutionClient() if mode == "live" else None

    conn = get_connection()
    try:
        risk_state = compute_existing_risk(conn)
        total_risk = risk_state["total"]
        per_market = risk_state["per_market"]
    finally:
        conn.close()

    executed_count = 0

    for sig in signals:
        sig_id = sig["id"]
        market_ticker = sig["market_ticker"]
        risk_new = estimate_trade_risk_usd(sig)

        if risk_new > limits["max_risk_per_trade"]:
            update_signal_execution(
                sig_id,
                status="ignored",
                execution_mode=mode,
                error=f"Risk per trade {risk_new:.2f} exceeds limit {limits['max_risk_per_trade']:.2f}",
            )
            continue

        market_risk = per_market.get(market_ticker, 0.0)
        if market_risk + risk_new > limits["max_risk_per_market"]:
            update_signal_execution(
                sig_id,
                status="ignored",
                execution_mode=mode,
                error=f"Per-market risk {market_risk + risk_new:.2f} exceeds limit {limits['max_risk_per_market']:.2f}",
            )
            continue

        if total_risk + risk_new > limits["max_risk_total"]:
            update_signal_execution(
                sig_id,
                status="ignored",
                execution_mode=mode,
                error=f"Total risk {total_risk + risk_new:.2f} exceeds limit {limits['max_risk_total']:.2f}",
            )
            continue

        if mode == "simulate":
            update_signal_execution(
                sig_id,
                status="simulated",
                execution_mode=mode,
                executed_price=float(sig["p_mkt"]),
                executed_size=int(sig["size"]),
            )
        else:
            try:
                order_req = OrderRequest(
                    market_ticker=market_ticker,
                    side=sig["side"],
                    size=int(sig["size"]),
                    price=None,  # TODO: map to limit price if desired
                )
                resp = client.place_order(order_req)  # type: ignore[arg-type]
                order_id = str(resp.get("order_id") or resp.get("id") or "")
                executed_price = float(sig["p_mkt"])
                executed_size = int(sig["size"])
                update_signal_execution(
                    sig_id,
                    status="sent",
                    execution_mode=mode,
                    order_id=order_id,
                    executed_price=executed_price,
                    executed_size=executed_size,
                )
            except Exception as exc:  # pragma: no cover - defensive
                update_signal_execution(
                    sig_id,
                    status="error",
                    execution_mode=mode,
                    error=str(exc),
                )
                continue

        total_risk += risk_new
        per_market[market_ticker] = per_market.get(market_ticker, 0.0) + risk_new
        executed_count += 1

    return executed_count


def main() -> None:
    count = execute_signals()
    print(f"Processed {count} signals")


if __name__ == "__main__":
    main()
