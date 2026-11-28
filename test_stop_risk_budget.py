import pytest

from ggp_position_sizing import validate_stop_risk_budget


def test_ten_percent_account_stop_risk_budget_is_accepted():
    risk = validate_stop_risk_budget(
        estimated_loss_usdt=10.0,
        available_balance=100.0,
        max_risk_pct=10.0,
    )
    assert risk == pytest.approx(10.0)


def test_stop_risk_above_ten_percent_is_rejected():
    with pytest.raises(ValueError, match="STRUCTURAL_STOP_RISK_EXCEEDS_BUDGET"):
        validate_stop_risk_budget(
            estimated_loss_usdt=10.01,
            available_balance=100.0,
            max_risk_pct=10.0,
        )


def test_stop_risk_budget_uses_final_exchange_rounded_loss_value():
    # The validator receives the calculated loss for the final rounded quantity.
    assert validate_stop_risk_budget(
        estimated_loss_usdt=9.999999,
        available_balance=100.0,
        max_risk_pct=10.0,
    ) == pytest.approx(9.999999)
