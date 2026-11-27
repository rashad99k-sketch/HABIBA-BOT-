""" focused tests for adaptive SCALP/TREND management on the existing board.

The tests deliberately exercise pure policy helpers and TradeManagementBoard;
they never submit orders and never use live credentials.
"""
import pandas as pd
import pytest

import ggpbot
from ggp_profit_brain import (
    BEHAVIOR_DISTRIBUTION, BEHAVIOR_INSTITUTIONAL_EXIT, BEHAVIOR_MICRO_PULLBACK,
    BEHAVIOR_PULLBACK, BEHAVIOR_RETEST, BEHAVIOR_TREND_CONTINUATION,
    RANGE_MODE, STANDARD_MODE, TREND_MODE,
    SCALP_STATE, TREND_STATE, EXIT_REQUIRED_STATE,
    analyze_post_entry_behavior, map_profit_mode_to_regime, roe_target_price,
    select_scalp_roe_target, transition_trade_regime, verify_roe_inputs,
)


def _frame(rows):
    return pd.DataFrame(rows, columns=["open", "high", "low", "close", "volume"])


def _board():
    return ggpbot.TradeManagementBoard()


def _base_context(**overrides):
    context = {
        "symbol": "BTC/USDT:USDT", "side": "BUY", "entry": 100.0,
        "current_price": 102.0, "atr": 1.0, "roe": 0.0,
        "remaining_qty": 2.0, "initial_qty": 2.0,
        "smart_money": {}, "momentum": {},
        "adx": 20.0, "adx_slope": 0.0, "di_plus": 25.0, "di_minus": 15.0,
        "regime": "UNKNOWN", "trade_state": "UNKNOWN",
        "continuation_probability": 0.5, "momentum_strength": 50.0,
        "volume_ratio": 1.0, "thesis_failure_score": 0.0,
        "thesis_failure_reasons": [], "structural_invalidation": False,
        "max_price": 102.0, "min_price": 100.0,
        "trail_active": False, "trail_stop": 0.0,
        "tp1_hit": False, "tp2_hit": False, "tp1_price": 0.0, "tp2_price": 0.0,
        "synthetic_sl": 95.0, "synthetic_tp1": 0.0,
        "range_target_price": 0.0, "range_target_move_pct": 0.0,
        "profit_mode": RANGE_MODE, "profit_mode_reason": "",
        "tp2_decision": "HOLD", "tp2_decision_reason": "",
        "entry_type": "EMA9_VWAP", "be_done": False, "partial_done": False,
        "execution_unresolved": False, "close_reconciliation_required": False,
        "recovery_hold": False, "smart_trail_mult": 1.5,
        "peak_roe": 0.0, "peak_drawdown_roe": 0.0,
        "distribution_risk": 0.0, "exit_warning": 0,
        "ema_state": "INSUFFICIENT_DATA", "emergency_exit": False,
        "emergency_reason": "", "roe_ready": True, "roe_diagnostic": "",
        "trade_regime_state": SCALP_STATE, "post_entry_behavior": "RANGE_CHOP",
        "behavior_reason": "", "scalp_target_roe_pct": 5.0,
        "tp1_target_roe_pct": 0.0, "tp2_target_roe_pct": 0.0,
    }
    context.update(overrides)
    return context


# ---------------------------------------------------------------------------
# Classification and state transitions
# ---------------------------------------------------------------------------

def test_insufficient_trend_evidence_maps_to_scalp():
    assert map_profit_mode_to_regime(STANDARD_MODE) == SCALP_STATE
    assert map_profit_mode_to_regime(RANGE_MODE) == SCALP_STATE
    assert map_profit_mode_to_regime(TREND_MODE) == TREND_STATE


def test_scalp_to_trend_requires_two_confirmed_candles():
    first, _, count = transition_trade_regime(
        SCALP_STATE, TREND_STATE, BEHAVIOR_TREND_CONTINUATION, 1,
        {"aligned_closes": 2})
    assert first == SCALP_STATE
    assert count == 1
    second, reason, _ = transition_trade_regime(
        SCALP_STATE, TREND_STATE, BEHAVIOR_TREND_CONTINUATION, 2,
        {"aligned_closes": 2})
    assert second == TREND_STATE
    assert "confirmed" in reason


def test_profit_alone_does_not_upgrade_scalp_to_trend():
    state, _, _ = transition_trade_regime(
        SCALP_STATE, TREND_STATE, "RANGE_CHOP", 5,
        {"aligned_closes": 0, "adverse_rejection": True})
    assert state == SCALP_STATE


def test_trend_downgrade_needs_weakening_behavior():
    state, _, _ = transition_trade_regime(
        TREND_STATE, SCALP_STATE, BEHAVIOR_TREND_CONTINUATION, 2, {})
    assert state == TREND_STATE
    state, reason, _ = transition_trade_regime(
        TREND_STATE, SCALP_STATE, BEHAVIOR_DISTRIBUTION, 2, {})
    assert state == SCALP_STATE
    assert "weakening" in reason


def test_institutional_exit_overrides_hysteresis_immediately():
    state, _, _ = transition_trade_regime(
        SCALP_STATE, TREND_STATE, BEHAVIOR_INSTITUTIONAL_EXIT, 0, {})
    assert state == EXIT_REQUIRED_STATE


# ---------------------------------------------------------------------------
# ROE basis and price-target diagnostics
# ---------------------------------------------------------------------------

def test_verified_20x_margin_basis_computes_roe_and_price_target():
    context = {
        "side": "BUY", "entry": 100.0, "mark_price": 102.0, "qty": 2.0,
        "initial_margin": 10.0, "unrealized_pnl": 2.0,
        "actual_leverage": 20.0, "leverage_verified": True,
        "reported_roe_pct": 20.0,
    }
    check = verify_roe_inputs(context)
    assert check["ok"] is True
    assert check["computed_roe_pct"] == 20.0
    assert check["reason"] == "verified_20x_margin_basis"
    assert roe_target_price(context, 40.0) == 102.0


def test_unverified_leverage_blocks_roe_targets():
    check = verify_roe_inputs({
        "side": "BUY", "entry": 100.0, "mark_price": 102.0, "qty": 2.0,
        "initial_margin": 10.0, "unrealized_pnl": 2.0,
        "actual_leverage": 20.0, "leverage_verified": False,
    })
    assert check["ok"] is False
    assert check["reason"] == "actual_leverage_not_verified"


def test_non_20x_verified_leverage_stays_compatible_but_diagnosed():
    check = verify_roe_inputs({
        "side": "SELL", "entry": 100.0, "mark_price": 98.0, "qty": 2.0,
        "initial_margin": 20.0, "unrealized_pnl": 4.0,
        "actual_leverage": 10.0, "leverage_verified": True,
        "reported_roe_pct": 20.0,
    })
    assert check["ok"] is True
    assert "actual_leverage_10" in check["reason"]


def test_reported_roe_mismatch_blocks_targets():
    check = verify_roe_inputs({
        "side": "BUY", "entry": 100.0, "mark_price": 102.0, "qty": 2.0,
        "initial_margin": 10.0, "unrealized_pnl": 2.0,
        "actual_leverage": 20.0, "leverage_verified": True,
        "reported_roe_pct": 18.0,
    })
    assert check["ok"] is False
    assert "reported_roe_mismatch" in check["reason"]


# ---------------------------------------------------------------------------
# SCALP target selection and execution decisions
# ---------------------------------------------------------------------------

def test_scalp_target_selection_covers_5_10_and_20_percent_roe():
    weak_target, _ = select_scalp_roe_target({
        "adx": 14, "adx_slope": -1, "continuation_probability": 0.30,
        "volume_ratio": 0.50, "distance_to_opposing_pct": 0.15,
        "post_entry_behavior": BEHAVIOR_DISTRIBUTION,
    })
    moderate_target, _ = select_scalp_roe_target({
        "adx": 21, "adx_slope": 0.5, "continuation_probability": 0.62,
        "volume_ratio": 1.0, "distance_to_opposing_pct": 0.50,
        "aligned_closed_candles": 1, "di_aligned": True,
        "post_entry_behavior": BEHAVIOR_RETEST,
    })
    strong_target, _ = select_scalp_roe_target({
        "adx": 28, "adx_slope": 2, "continuation_probability": 0.80,
        "volume_ratio": 1.35, "distance_to_opposing_pct": 1.10,
        "aligned_closed_candles": 2, "di_aligned": True,
        "zone_strength": 0.8, "post_entry_behavior": BEHAVIOR_TREND_CONTINUATION,
    })
    assert (weak_target, moderate_target, strong_target) == (5.0, 10.0, 20.0)


def test_scalp_full_close_at_selected_target_and_no_partial_split():
    board = _board()
    decision = board.evaluate(_base_context(
        roe=5.2, scalp_target_roe_pct=5.0, profit_mode=RANGE_MODE,
        trade_regime_state=SCALP_STATE))
    assert decision.action is ggpbot.TradeDecisionType.FULL_CLOSE
    assert decision.params["close_reason"] == "SCALP_TARGET"
    assert decision.action is not ggpbot.TradeDecisionType.PARTIAL_CLOSE


def test_scalp_holds_before_selected_target():
    board = _board()
    decision = board.evaluate(_base_context(roe=4.8, scalp_target_roe_pct=5.0))
    assert decision.action is ggpbot.TradeDecisionType.HOLD


def test_roe_gate_holds_target_management_without_weakening_safety_exits():
    board = _board()
    held = board.evaluate(_base_context(
        roe=20.0, scalp_target_roe_pct=5.0, roe_ready=False,
        roe_diagnostic="actual_leverage_not_verified"))
    assert held.action is ggpbot.TradeDecisionType.HOLD
    assert "roe_target_blocked" in held.reason
    exit_decision = board.evaluate(_base_context(
        roe=20.0, roe_ready=False, synthetic_sl=103.0, current_price=102.0))
    assert exit_decision.action is ggpbot.TradeDecisionType.FULL_CLOSE


# ---------------------------------------------------------------------------
# TREND TP1/TP2 decisions
# ---------------------------------------------------------------------------

def test_trend_tp1_closes_exactly_half_at_40_percent_roe():
    board = _board()
    decision = board.evaluate(_base_context(
        profit_mode=TREND_MODE, trade_regime_state=TREND_STATE,
        roe=40.0, tp1_target_roe_pct=40.0, tp2_target_roe_pct=60.0))
    assert decision.action is ggpbot.TradeDecisionType.PARTIAL_CLOSE
    assert decision.params["ratio"] == 0.5
    assert decision.params["target_remaining_qty"] == 1.0
    assert decision.params["close_reason"] == "TP1"


def test_trend_tp2_closes_remaining_at_60_percent_roe():
    board = _board()
    decision = board.evaluate(_base_context(
        profit_mode=TREND_MODE, trade_regime_state=TREND_STATE,
        roe=60.0, tp1_hit=True, be_done=True, atr=0.0,
        tp1_target_roe_pct=40.0, tp2_target_roe_pct=60.0))
    assert decision.action is ggpbot.TradeDecisionType.FULL_CLOSE
    assert decision.params["close_reason"] == "TP2"


def test_tp1_and_tp2_are_not_repeated():
    board = _board()
    decision = board.evaluate(_base_context(
        profit_mode=TREND_MODE, trade_regime_state=TREND_STATE,
        roe=70.0, tp1_hit=True, tp2_hit=True, be_done=True, atr=0.0,
        tp1_target_roe_pct=40.0, tp2_target_roe_pct=60.0))
    assert decision.action is ggpbot.TradeDecisionType.HOLD


def test_unresolved_execution_prevents_any_new_target_decision():
    board = _board()
    decision = board.evaluate(_base_context(
        roe=20.0, scalp_target_roe_pct=5.0, execution_unresolved=True))
    assert decision.action is ggpbot.TradeDecisionType.HOLD
    assert decision.reason == "exchange_execution_or_recovery_unresolved"


# ---------------------------------------------------------------------------
# Retest / pullback / distribution behaviour labels
# ---------------------------------------------------------------------------

def test_retest_held_inside_entry_zone():
    frame = _frame([
        (100.0, 101.0, 99.9, 100.5, 100),
        (100.5, 101.2, 100.0, 101.0, 100),
        (101.0, 101.4, 99.6, 101.1, 100),
        (101.1, 101.5, 99.7, 101.3, 100),
        (101.3, 101.7, 99.8, 101.5, 100),
        (101.0, 101.3, 100.2, 100.9, 100),
    ])
    behavior, reason, _ = analyze_post_entry_behavior(
        frame, "BUY", 100.0, 1.0,
        {"continuation_probability": 0.62, "adx": 24, "zone_low": 99.5, "zone_high": 100.5})
    assert behavior == BEHAVIOR_RETEST
    assert "held" in reason


def test_micro_pullback_is_not_an_exit_signal():
    frame = _frame([
        (100.0, 100.4, 99.9, 100.3, 100),
        (100.3, 100.6, 99.8, 100.5, 100),
        (100.5, 100.8, 99.85, 100.6, 100),
        (100.6, 101.0, 99.9, 100.9, 100),
        (100.9, 101.2, 100.1, 101.1, 100),
        (101.1, 101.4, 100.2, 101.3, 100),
    ])
    behavior, _, evidence = analyze_post_entry_behavior(
        frame, "BUY", 100.0, 1.0,
        {"continuation_probability": 0.60, "adx": 16, "di_aligned": False,
         "structure_aligned": False, "zone_low": 99.5, "zone_high": 100.5})
    assert behavior == BEHAVIOR_MICRO_PULLBACK
    assert evidence["adverse_atr"] <= 0.35


def test_pullback_inside_intact_structure_is_not_institutional_exit():
    frame = _frame([
        (100.0, 101.0, 99.9, 100.8, 100),
        (100.8, 101.4, 100.0, 101.2, 100),
        (101.2, 101.6, 98.8, 100.4, 100),
        (100.4, 100.9, 98.9, 100.6, 100),
        (100.6, 101.1, 99.0, 100.9, 100),
        (100.9, 101.3, 99.2, 101.1, 100),
    ])
    behavior, _, _ = analyze_post_entry_behavior(
        frame, "BUY", 100.0, 1.0,
        {"continuation_probability": 0.58, "adx": 19, "di_aligned": False,
         "structure_aligned": True, "zone_low": 99.5, "zone_high": 100.5})
    assert behavior == BEHAVIOR_PULLBACK


def test_trend_continuation_requires_adx_di_and_structure():
    frame = _frame([
        (100.0, 100.6, 99.9, 100.5, 100),
        (100.5, 101.1, 100.4, 101.0, 100),
        (101.0, 101.6, 100.9, 101.5, 100),
        (101.5, 102.1, 101.4, 102.0, 100),
        (102.0, 102.6, 101.9, 102.5, 100),
        (102.5, 103.1, 102.4, 103.0, 100),
    ])
    behavior, _, _ = analyze_post_entry_behavior(
        frame, "BUY", 100.0, 1.0,
        {"continuation_probability": 0.75, "adx": 28, "di_aligned": True,
         "structure_aligned": True, "distribution_risk": 10,
         "zone_low": 99.5, "zone_high": 100.5})
    assert behavior == BEHAVIOR_TREND_CONTINUATION


def test_distribution_and_institutional_exit_are_distinct():
    frame = _frame([
        (100.0, 100.5, 99.9, 100.2, 100),
        (100.2, 100.6, 99.8, 100.1, 100),
        (100.1, 100.4, 99.7, 100.0, 100),
        (100.0, 100.3, 99.6, 99.9, 100),
        (99.9, 100.2, 99.5, 99.8, 200),
        (99.8, 100.1, 99.4, 99.7, 200),
    ])
    behavior, _, _ = analyze_post_entry_behavior(
        frame, "BUY", 100.0, 1.0,
        {"continuation_probability": 0.35, "distribution_risk": 70,
         "volume_ratio": 1.5, "zone_low": 99.5, "zone_high": 100.5})
    assert behavior == BEHAVIOR_DISTRIBUTION
    behavior, _, _ = analyze_post_entry_behavior(
        frame, "BUY", 100.0, 1.0, {"structural_invalidation": True})
    assert behavior == BEHAVIOR_INSTITUTIONAL_EXIT


def test_exit_required_regime_forces_full_close():
    decision = _board().evaluate(_base_context(trade_regime_state=EXIT_REQUIRED_STATE))
    assert decision.action is ggpbot.TradeDecisionType.FULL_CLOSE
    assert decision.params["close_reason"] == "EXIT_REQUIRED"


def test_adverse_rejection_blocks_scalp_to_trend_upgrade():
    state, reason, _ = transition_trade_regime(
        SCALP_STATE, TREND_STATE, BEHAVIOR_TREND_CONTINUATION, 2,
        {"adverse_rejection": True})
    assert state == SCALP_STATE
    assert "rejected" in reason


def test_short_roe_net_of_fees_uses_actual_leverage():
    context = {
        "side": "SELL", "entry": 100.0, "mark_price": 98.0, "qty": 2.0,
        "initial_margin": 20.0, "unrealized_pnl": 4.0,
        "actual_leverage": 10.0, "leverage_verified": True,
        "reported_roe_pct": 16.25, "fees_usdt": 0.5, "funding_usdt": 0.25,
    }
    check = verify_roe_inputs(context)
    assert check["ok"] is True
    assert check["computed_roe_pct"] == pytest.approx(16.25)
    assert check["net_unrealized_pnl"] == pytest.approx(3.25)
    # SHORT target is below entry; 2 USDT x 2 qty / 20 margin = 20% ROE.
    assert roe_target_price(check, 40.0) == pytest.approx(96.0)
    assert roe_target_price(check, 60.0) == pytest.approx(94.0)

    context["reported_roe_pct"] = 20.0  # gross ROE must not satisfy a net basis
    mismatch = verify_roe_inputs(context)
    assert mismatch["ok"] is False
    assert "reported_roe_mismatch" in mismatch["reason"]
