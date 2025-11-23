from __future__ import annotations

from datetime import datetime, timedelta, timezone
import math

from kalshi_edge.backtest import calibration
from tests.helpers import patch_connection_ctx


def _price_row(ts, bid, ask):
    return {
        "timestamp": ts,
        "bid_yes": bid,
        "ask_yes": ask,
        "last_yes": None,
        "open_interest": 20,
        "volume": 5,
    }


def _find_bucket(buckets, target_low):
    for bucket in buckets:
        if math.isclose(bucket["bucket_low"], target_low, abs_tol=1e-9):
            return bucket
    raise AssertionError("Bucket not found")


def test_calibration_counts_buckets(monkeypatch):
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    markets = [
        {"market_id": "MKTH", "resolution": "YES", "resolved_at": base + timedelta(minutes=30)},
        {"market_id": "MKTL", "resolution": "NO", "resolved_at": base + timedelta(minutes=30)},
    ]
    prices = {
        "MKTH": [
            _price_row(base, 0.90, 0.94),
        ],
        "MKTL": [
            _price_row(base, 0.20, 0.24),
        ],
    }
    patch_connection_ctx(monkeypatch, calibration, markets, prices)

    buckets = calibration.compute_calibration(num_bins=10)
    high_bucket = _find_bucket(buckets, 0.9)
    low_bucket = _find_bucket(buckets, 0.2)

    assert high_bucket["n"] == 1
    assert high_bucket["n_yes"] == 1
    assert high_bucket["p_true"] == 1.0

    assert low_bucket["n"] == 1
    assert low_bucket["n_yes"] == 0
    assert low_bucket["p_true"] == 0.0
