import math

import pytest

from ggp_profit_brain import (
    RANGE_PROFIT_TARGETS_PCT,
    TREND_TP1_MOVE_PCT,
    TREND_TP2_MOVE_PCT,
    exchange_pnl_at_move,
    profit_target_convention,
    range_target_price,
    select_range_target_pct,
    trend_target_prices,
)


def test_range_targets_are_requested_price_moves():
    assert RANGE_PROFIT_TARGETS_PCT == (0.25, 0.50, 1.00)


def test_trend_targets_are_one_and_three_percent_for_long():
    tp1, tp2 = trend_target_prices("BUY", 100.0)
    assert math.isclose(tp1, 102.0)
    assert math.isclose(tp2, 103.0)


def test_trend_targets_are_one_and_three_percent_for_short():
    tp1, tp2 = trend_target_prices("SELL", 100.0)
    assert math.isclose(tp1, 98.0)
    assert math.isclose(tp2, 97.0)


def test_trend_target_configuration_is_explicit():
    assert TREND_TP1_MOVE_PCT == 2.0
    assert TREND_TP2_MOVE_PCT == 3.0


def test_invalid_side_or_entry_fails_closed():
    assert trend_target_prices("HOLD", 100.0) == (0.0, 0.0)
    assert trend_target_prices("BUY", 0.0) == (0.0, 0.0)


def test_profit_convention_is_price_move_not_account_allocation():
    convention = profit_target_convention()
    assert convention["basis"] == "UNLEVERAGED_PRICE_MOVE_PCT_FROM_VERIFIED_ENTRY"
    assert convention["gross_roe_multiplier"] == 20
    assert convention["gross_roe_pct"]["0.25"] == pytest.approx(5.0)
    assert convention["gross_roe_pct"]["0.50"] == pytest.approx(10.0)
    assert convention["gross_roe_pct"]["1.00"] == pytest.approx(20.0)
    assert convention["net_pnl_is_estimate"] is True
    assert convention["realized_pnl_authority"] == "EXCHANGE_REPORTED_REALIZED_PNL"


def test_all_three_weak_targets_convert_gross_to_net_with_costs():
    for move in RANGE_PROFIT_TARGETS_PCT:
        target = range_target_price("BUY", 100.0, move)
        assert target == pytest.approx(100.0 * (1.0 + move / 100.0))
        pnl = exchange_pnl_at_move(side="BUY", entry_price=100.0, quantity=10.0, move_pct=move)
        assert pnl["target_price"] == pytest.approx(target)
        assert pnl["gross_roe_pct"] == pytest.approx(move * 20)
        assert pnl["gross_pnl"] > 0
        assert pnl["estimated_total_costs"] > 0
        assert pnl["estimated_net_pnl"] < pnl["gross_pnl"]
        assert pnl["basis"] == "PRE_TRADE_ESTIMATE_NOT_REALIZED_PNL"


def test_range_target_selector_reaches_all_three_levels():
    weak, _ = select_range_target_pct({
        "side": "BUY", "adx": 12.0, "continuation_probability": 0.30,
        "volume_ratio": 0.60, "di_plus": 20.0, "di_minus": 25.0,
    })
    moderate, _ = select_range_target_pct({
        "side": "BUY", "adx": 20.0, "continuation_probability": 0.60,
        "volume_ratio": 1.0, "di_plus": 25.0, "di_minus": 15.0,
        "aligned_closed_candles": 2,
    })
    strong, _ = select_range_target_pct({
        "side": "BUY", "adx": 25.0, "continuation_probability": 0.80,
        "volume_ratio": 1.3, "di_plus": 30.0, "di_minus": 10.0,
        "aligned_closed_candles": 3,
    })
    assert (weak, moderate, strong) == RANGE_PROFIT_TARGETS_PCT


def test_sell_pnl_mirrors_buy_and_never_reports_gross_as_net():
    buy = exchange_pnl_at_move(side="BUY", entry_price=100.0, quantity=10.0, move_pct=1.0)
    sell = exchange_pnl_at_move(side="SELL", entry_price=100.0, quantity=10.0, move_pct=1.0)
    assert sell["target_price"] == pytest.approx(99.0)
    assert sell["gross_pnl"] == pytest.approx(buy["gross_pnl"])
    assert sell["estimated_net_pnl"] < sell["gross_pnl"]
    assert "net" not in sell["basis"].lower() or "estimate" in sell["basis"].lower()


def test_pnl_helper_rejects_invalid_leverage_and_costs():
    with pytest.raises(ValueError):
        exchange_pnl_at_move(side="BUY", entry_price=100.0, quantity=1.0,
                             move_pct=1.0, leverage=0)
    with pytest.raises(ValueError):
        exchange_pnl_at_move(side="HOLD", entry_price=100.0, quantity=1.0, move_pct=1.0)
