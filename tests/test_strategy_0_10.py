from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from kalshi_edge.backtest import strategy_0_10
from tests.helpers import patch_connection_ctx


def _price_row(ts: datetime, bid: float, ask: float, open_interest: int = 20) -> dict:
    return {
        "timestamp": ts,
        "bid_yes": bid,
        "ask_yes": ask,
        "last_yes": None,
        "open_interest": open_interest,
        "volume": 5,
    }


def test_strategy_0_10_monitors_low_prices(monkeypatch):
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    markets = [
        {"market_id": "MKT_CHEAP_YES", "resolution": "YES"},
        {"market_id": "MKT_CHEAP_NO", "resolution": "NO"},
    ]
    prices = {
        "MKT_CHEAP_YES": [
            _price_row(base, 0.20, 0.22),
            _price_row(base + timedelta(minutes=5), 0.05, 0.07),
        ],
        "MKT_CHEAP_NO": [
            _price_row(base, 0.15, 0.17),
            _price_row(base + timedelta(minutes=5), 0.06, 0.08),
        ],
    }
    patch_connection_ctx(monkeypatch, strategy_0_10, markets, prices)

    summary, trades = strategy_0_10.run_backtest(threshold=0.10)
    assert summary["num_trades"] == 2

    trade_map = {trade["market_id"]: trade for trade in trades}
    yes_trade = trade_map["MKT_CHEAP_YES"]
    no_trade = trade_map["MKT_CHEAP_NO"]

    assert yes_trade["entry_price"] == pytest.approx(0.06)
    assert yes_trade["profit"] == pytest.approx(0.94)
    assert no_trade["profit"] == pytest.approx(-0.07)
