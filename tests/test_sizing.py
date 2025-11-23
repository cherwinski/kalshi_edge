import pytest

from kalshi_edge.execution.execute_signals import compute_order_size_for_signal


def test_dynamic_sizing_yes_side_uses_fraction_cap():
    signal = {"side": "yes", "p_mkt": 0.5}
    risk_limits = {
        "max_risk_per_trade": 50.0,
        "max_risk_per_market": 200.0,
        "max_risk_total": 500.0,
    }
    size, rpc = compute_order_size_for_signal(
        signal,
        bankroll=1000.0,
        risk_limits=risk_limits,
        risk_fraction=0.03,  # 3% of bankroll = $30 cap
    )
    assert rpc == 0.5
    # Default sizing targets ~$3 risk, so ceil(3 / 0.5) = 6
    assert size == 6


def test_dynamic_sizing_no_side_uses_fraction_cap():
    signal = {"side": "no", "p_mkt": 0.8}
    risk_limits = {
        "max_risk_per_trade": 50.0,
        "max_risk_per_market": 200.0,
        "max_risk_total": 500.0,
    }
    size, rpc = compute_order_size_for_signal(
        signal,
        bankroll=1000.0,
        risk_limits=risk_limits,
        risk_fraction=0.03,
    )
    assert rpc == pytest.approx(0.2)  # 1 - price
    # Default sizing targets ~$3 risk, so ceil(3 / 0.2) ~ 16
    assert size == 16


def test_dynamic_sizing_respects_remaining_total_risk():
    signal = {"side": "yes", "p_mkt": 0.5}
    risk_limits = {
        "max_risk_per_trade": 50.0,
        "max_risk_per_market": 200.0,
        "max_risk_total": 30.0,
    }
    size, rpc = compute_order_size_for_signal(
        signal,
        bankroll=1000.0,
        risk_limits=risk_limits,
        risk_fraction=0.03,
        total_risk=28.0,  # only $2 of headroom
    )
    assert rpc == 0.5
    assert size == 4  # floor(2 / 0.5)
