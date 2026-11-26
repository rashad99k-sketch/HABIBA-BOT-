import pytest

from ggp_position_sizing import calculate_position_size


@pytest.mark.parametrize("classification,allocation", [("TREND", 0.20), ("STRONG", 0.30)])
def test_position_amount_is_exact_balance_allocation(classification, allocation):
    r = calculate_position_size(
        classification=classification, available_balance=34.4755,
        entry_price=1.0, stop_price=0.99, side="BUY", leverage=20,
        max_trade_risk_pct=10.0, stop_execution_buffer_pct=0.0)
    expected_margin = 34.4755 * allocation
    assert r.target_margin == pytest.approx(expected_margin)
    assert r.max_risk_usdt == pytest.approx(expected_margin)
    assert r.target_notional == pytest.approx(expected_margin * 20.0)
    assert r.final_notional == pytest.approx(expected_margin * 20.0)
    assert r.quantity == pytest.approx(expected_margin * 20.0)


def test_wide_stop_does_not_reduce_quantity():
    r = calculate_position_size(
        classification="TREND", available_balance=34.4755,
        entry_price=1.0, stop_price=0.97, side="BUY", leverage=20,
        max_trade_risk_pct=10.0, stop_execution_buffer_pct=0.0)
    assert r.binding_constraint == "ALLOCATION"
    assert r.margin_achieved_ratio == pytest.approx(1.0)
    assert r.quantity == pytest.approx(34.4755 * 0.20 * 20.0)


def test_sizing_uses_balance_once_then_leverage_once():
    r = calculate_position_size(
        classification="TREND", available_balance=100.0,
        entry_price=1.0, stop_price=0.999, side="BUY", leverage=20,
        max_trade_risk_pct=10.0, stop_execution_buffer_pct=0.0)
    assert r.max_risk_usdt == pytest.approx(20.0)
    assert r.quantity == pytest.approx(100.0 * 0.20 * 20.0)
