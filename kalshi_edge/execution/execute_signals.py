"""Consume pending signals, enforce risk limits, and simulate or send orders."""
from __future__ import annotations

from typing import Any, Dict, List, Mapping
import math

from kalshi_edge.config import (
    get_execution_mode,
    get_risk_limits,
    get_kalshi_env,
    get_current_bankroll_usd,
    get_max_risk_fraction_per_trade,
)
from kalshi_edge.db import get_connection
from kalshi_edge.execution.client import ExecutionClient, OrderRequest
from kalshi_edge.util.logging import get_logger
from kalshi_edge.portfolio.pnl import record_trade

LOGGER = get_logger(__name__)
MAX_CONTRACTS_CAP = 1000  # hard safety cap on contract count per order


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
    def _norm_price(val: float) -> float:
        try:
            p = float(val)
        except Exception:
            return 0.0
        if p > 1.0:
            p = p / 100.0
        if p < 0:
            p = 0.0
        return p

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
        sig = {"market_ticker": market_ticker, "side": side, "p_mkt": _norm_price(p_mkt), "size": int(size)}
        r = estimate_trade_risk_usd(sig)
        per_market[market_ticker] = per_market.get(market_ticker, 0.0) + r
        total += r

    # include open positions risk (best-effort; table may be absent)
    try:
        cur.execute("SELECT market_ticker, side, size, avg_entry_price FROM positions")
        for market_ticker, side, size, avg_price in cur.fetchall():
            avg_price = _norm_price(avg_price)
            if side == "yes":
                r = abs(avg_price * size)
            else:
                r = abs((1.0 - avg_price) * size)
            per_market[market_ticker] = per_market.get(market_ticker, 0.0) + r
            total += r
    except Exception:
        pass
    return {"total": total, "per_market": per_market}


def compute_order_size_for_signal(
    signal: Mapping[str, Any],
    bankroll: float,
    risk_limits: Mapping[str, float],
    *,
    per_market_risk: float = 0.0,
    total_risk: float = 0.0,
    risk_fraction: float | None = None,
) -> tuple[int, float]:
    """Return (size, risk_per_contract) using bankroll-aware sizing."""

    side = (signal.get("side") or "").lower()
    price = float(signal.get("p_mkt") or 0.0)
    if side == "no":
        risk_per_contract = 1.0 - price
    else:
        risk_per_contract = price

    if risk_per_contract <= 0:
        return 0, risk_per_contract

    fraction = risk_fraction if risk_fraction is not None else get_max_risk_fraction_per_trade()
    per_trade_cap = min(risk_limits["max_risk_per_trade"], bankroll * fraction)
    remaining_market = risk_limits["max_risk_per_market"] - per_market_risk
    remaining_total = risk_limits["max_risk_total"] - total_risk
    max_risk = min(per_trade_cap, remaining_market, remaining_total)

    if max_risk <= 0:
        return 0, risk_per_contract

    # Default target risk is ~$3 per order unless caps force lower.
    target_risk = min(3.0, max_risk)
    size = int(math.ceil(target_risk / risk_per_contract))
    # Ensure we don't exceed max_risk
    if size * risk_per_contract > max_risk:
        size = int(max_risk // risk_per_contract)
    if size <= 0:
        return 0, risk_per_contract

    size = min(size, MAX_CONTRACTS_CAP)
    return size, risk_per_contract


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
    env = get_kalshi_env()
    limits = get_risk_limits()
    signals = fetch_pending_signals(limit=batch_limit)
    if not signals:
        return 0

    client = None
    if mode == "live":
        try:
            client = ExecutionClient()
            LOGGER.warning("Execution running in LIVE mode against %s", env)
        except Exception as exc:
            LOGGER.exception("Failed to initialize ExecutionClient; falling back to simulate. %s", exc)
            mode = "simulate"

    conn = get_connection()
    try:
        risk_state = compute_existing_risk(conn)
        total_risk = risk_state["total"]
        per_market = risk_state["per_market"]
        bankroll = get_current_bankroll_usd(conn)
    finally:
        conn.close()

    executed_count = 0

    for sig in signals:
        sig_id = sig["id"]
        market_ticker = sig["market_ticker"]
        trade_direction = "buy"  # buy YES or buy NO; selling paths can be added later.

        current_market_risk = per_market.get(market_ticker, 0.0)
        size, risk_per_contract = compute_order_size_for_signal(
            sig,
            bankroll,
            limits,
            per_market_risk=current_market_risk,
            total_risk=total_risk,
        )

        if size <= 0:
            update_signal_execution(
                sig_id,
                status="ignored",
                execution_mode=mode,
                error="Insufficient risk budget for dynamic sizing",
            )
            continue

        risk_new = risk_per_contract * size

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
                executed_size=size,
            )
            record_trade(
                {
                    "signal_id": sig_id,
                    "market_ticker": market_ticker,
                    "side": sig["side"],
                    "size": size,
                    "price": float(sig["p_mkt"]),
                    "direction": trade_direction,
                }
            )
        else:
            try:
                limit_price = float(sig["p_mkt"])
                order_req = OrderRequest(
                    market_ticker=market_ticker,
                    side=sig["side"],
                    size=size,
                    price=limit_price,
                    direction=trade_direction,
                )
                if client is None:
                    raise RuntimeError("Execution client not initialized; cannot send live orders")
                resp = client.place_order(order_req)  # type: ignore[arg-type]
                order_id = str(resp.get("order_id") or resp.get("id") or "")
                executed_price = float(resp.get("avg_price") or limit_price)
                executed_size = int(resp.get("filled_size") or size)
                status = resp.get("status") or "sent"
                update_signal_execution(
                    sig_id,
                    status=status,
                    execution_mode=mode,
                    order_id=order_id,
                    executed_price=executed_price,
                    executed_size=executed_size,
                )
                record_trade(
                    {
                        "signal_id": sig_id,
                        "market_ticker": market_ticker,
                        "side": sig["side"],
                        "size": executed_size,
                        "price": executed_price,
                        "direction": trade_direction,
                    }
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
