import pytest

from roro_institutional import StructuralStopLossEngine


def test_structural_stop_buffer_gives_half_atr_room_for_buy_and_sell():
    buy_stop, buy_reason = StructuralStopLossEngine.calculate(
        "BUY", "TEST", entry=100.0, extreme=99.0, atr=1.0,
    )
    sell_stop, sell_reason = StructuralStopLossEngine.calculate(
        "SELL", "TEST", entry=100.0, extreme=101.0, atr=1.0,
    )
    assert buy_stop == 98.5
    assert sell_stop == 101.5
    assert "risk_pct=1.5000" in buy_reason
    assert "risk_pct=1.5000" in sell_reason


def test_structural_stop_uses_two_tenths_percent_floor_when_atr_is_tiny():
    buy_stop, _ = StructuralStopLossEngine.calculate(
        "BUY", "TEST", entry=100.0, extreme=99.99, atr=0.01,
    )
    sell_stop, _ = StructuralStopLossEngine.calculate(
        "SELL", "TEST", entry=100.0, extreme=100.01, atr=0.01,
    )
    assert buy_stop == pytest.approx(99.79)
    assert sell_stop == pytest.approx(100.21)
