from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from kalshi_edge.backtest import strategy_0_90
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


def test_strategy_0_90_computes_profits(monkeypatch):
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    markets = [
        {"market_id": "MKT_YES", "resolution": "YES"},
        {"market_id": "MKT_NO", "resolution": "NO"},
    ]
    prices = {
        "MKT_YES": [
            _price_row(base, 0.80, 0.82),
            _price_row(base + timedelta(minutes=5), 0.91, 0.93),
        ],
        "MKT_NO": [
            _price_row(base, 0.40, 0.42),
            _price_row(base + timedelta(minutes=5), 0.94, 0.96),
        ],
    }
    patch_connection_ctx(monkeypatch, strategy_0_90, markets, prices)

    summary, trades = strategy_0_90.run_backtest(threshold=0.90)

    assert summary["num_trades"] == 2
    trade_map = {trade["market_id"]: trade for trade in trades}
    yes_trade = trade_map["MKT_YES"]
    no_trade = trade_map["MKT_NO"]

    assert yes_trade["profit"] == pytest.approx(1.0 - 0.92)
    assert no_trade["profit"] == pytest.approx(-0.95)
