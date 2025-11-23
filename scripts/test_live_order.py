"""Tiny helper to place a demo order via Kalshi ExecutionClient.

Use only in demo env unless you truly intend to trade live.
"""
from __future__ import annotations

import os

from kalshi_edge.config import get_execution_mode, get_kalshi_env
from kalshi_edge.execution.client import ExecutionClient, OrderRequest
from kalshi_edge.util.logging import get_logger

LOGGER = get_logger(__name__)


def main() -> None:
    env = get_kalshi_env()
    mode = get_execution_mode()
    if mode != "live":
        raise SystemExit("Refusing to place orders: EXECUTION_MODE is not 'live'.")

    ticker = os.getenv("TEST_ORDER_TICKER") or ""
    if not ticker:
        raise SystemExit("Set TEST_ORDER_TICKER to a demo ticker to place an order.")

    side = os.getenv("TEST_ORDER_SIDE", "yes")
    size = int(os.getenv("TEST_ORDER_SIZE", "1"))
    price = float(os.getenv("TEST_ORDER_PRICE", "0.50"))

    client = ExecutionClient()
    LOGGER.warning("Placing test order in %s env: %s %s @ %.2f size %d", env, ticker, side, price, size)
    resp = client.place_order(OrderRequest(market_ticker=ticker, side=side, size=size, price=price))
    print("Response:", resp)


if __name__ == "__main__":
    main()
