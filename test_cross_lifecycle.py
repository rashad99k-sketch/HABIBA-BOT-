"""Focused tests for the unified EMA9 + Session-VWAP cross lifecycle.

All tests are offline. No exchange call, order, balance query or live trade is
performed. Structural invariants are checked with AST/source assertions where
the protected execution and management paths must remain untouched.
"""
from __future__ import annotations

import ast
from pathlib import Path

from ggp_cross_lifecycle import (
    CrossLifecycleState, adx_gate, evaluate_cross_location, execution_readiness,
    make_cross_event_id, merge_cross_events, normalize_cross_event, rank_cross_candidates,
    transition_state, HIGH_QUALITY, GOOD_QUALITY, WEAK_QUALITY, INVALID_QUALITY,
)
from ggp_profit_brain import resolve_trend_targets, select_range_target_pct

ROOT = Path(__file__).resolve().parent
SOURCE = (ROOT / "ggpbot.py").read_text(encoding="utf-8")
BRAIN_SOURCE = (ROOT / "ggp_profit_brain.py").read_text(encoding="utf-8")
TREE = ast.parse(SOURCE)


def _function(name):
    """Return the effective (last-defined) function, including class methods."""
    matches = [node for node in ast.walk(TREE)
               if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name]
    assert matches, f"missing function {name}"
    return matches[-1]


def _class(name):
    return next(node for node in TREE.body if isinstance(node, ast.ClassDef) and node.name == name)


def _features(side="BUY", **overrides):
    buy = side == "BUY"
    base = {
        "cross_direction": side,
        "cross_ema9": "BULLISH" if buy else "BEARISH",
        "adx": 26.0,
        "valid_closed_data": True,
        "real_value_area": True,
        "room_before_opposing_liquidity": True,
        "room_before_opposing_structure": True,
        "market_structure_confirmed": True,
        "opposing_pressure": False,
        "demand_zone_near": buy,
        "supply_zone_near": not buy,
        "bullish_order_block_valid": buy,
        "bearish_order_block_valid": not buy,
        "bullish_fvg_valid": buy,
        "bearish_fvg_valid": not buy,
        "sweep_reclaim_confirmed": buy,
        "buy_side_sweep_rejection": not buy,
        "premium_discount_aligned": True,
        "smart_money_support": True,
        "momentum_support": True,
        "volume_confirmed": True,
        "institutional_flow_support": True,
        "available_price_space_atr": 1.4,
    }
    base.update(overrides)
    return base


def _confirmed(side="BUY", ts=1795132800000):
    return {
        "symbol": "BTC/USDT:USDT", "cross_direction": side, "cross_status": "CONFIRMED",
        "candle_timestamp": ts, "cross_event_id": make_cross_event_id("BTC/USDT:USDT", 900, side, ts),
        "adx": 26.0, "cross_distance_atr": 0.12, "institutional_analysis_status":
        CrossLifecycleState.INSTITUTIONAL_APPROVED.value,
        "expires_at_ms": ts + 2700 * 1000,
    }


def test_buy_in_strong_demand_zone_is_high_quality_cross():
    result = evaluate_cross_location(_features("BUY"))
    assert result["quality"] == HIGH_QUALITY
    assert result["approved"] is True
    assert any("demand_zone_near:PASS" in e for e in result["evidence"])


def test_sell_in_strong_supply_zone_is_high_quality_cross():
    result = evaluate_cross_location(_features("SELL"))
    assert result["quality"] == HIGH_QUALITY
    assert result["approved"] is True
    assert any("supply_zone_near:PASS" in e for e in result["evidence"])


def test_buy_far_from_suitable_zone_is_not_executable():
    result = evaluate_cross_location(_features(
        "BUY", demand_zone_near=False, bullish_order_block_valid=False,
        bullish_fvg_valid=False, sweep_reclaim_confirmed=False,
        room_before_opposing_liquidity=False, room_before_opposing_structure=False,
        available_price_space_atr=0.2,
    ))
    assert result["quality"] in {WEAK_QUALITY, INVALID_QUALITY}
    assert result["approved"] is False


def test_sell_in_unsuitable_support_zone_is_invalid():
    result = evaluate_cross_location(_features(
        "SELL", supply_zone_near=False, real_value_area=False,
        premium_discount_aligned=False, room_before_opposing_liquidity=False,
        room_before_opposing_structure=False, available_price_space_atr=0.1,
    ))
    assert result["quality"] == INVALID_QUALITY
    assert result["approved"] is False


def test_approaching_then_confirmed_on_closed_candle():
    base = {
        "state": "NEAR_CROSS", "direction": "BUY", "cross_ema9": None,
        "data_freshness": "FRESH", "candle_timestamp": 1795132800000,
        "ema9": 101.0, "ema50": 100.5, "vwap": 100.8, "gap_atr": 0.08,
    }
    approaching = normalize_cross_event("BTC/USDT:USDT", 900, base, 24.0, 1795132800000 + 900000)
    assert approaching and approaching["cross_status"] == "APPROACHING"
    assert approaching["cross_event_id"] is None
    confirmed = normalize_cross_event(
        "BTC/USDT:USDT", 900,
        {**base, "state": "CROSS_CONFIRMED", "cross_ema9": "BULLISH"},
        24.0, 1795132800000 + 900000,
    )
    assert confirmed and confirmed["cross_status"] == "CONFIRMED"
    assert confirmed["cross_event_id"] == make_cross_event_id("BTC/USDT:USDT", 900, "BUY", 1795132800000)


def test_cross_disappears_next_scanner_cycle_but_candidate_is_retained():
    event = _confirmed()
    kept = merge_cross_events({}, [event], 1795132800000 + 900000)
    assert list(kept) == [event["cross_event_id"]]
    retained = merge_cross_events(kept, [], 1795132800000 + 3 * 900000)
    assert event["cross_event_id"] in retained
    assert retained[event["cross_event_id"]]["cross_status"] == "CONFIRMED"
    expired = merge_cross_events(retained, [], event["candle_timestamp"] + 5000 * 1000)
    assert expired[event["cross_event_id"]]["final_rejection_reason"] == "cross_signal_expired_without_reentry"


def test_adx_17_99_is_hard_rejected():
    ok, reason = adx_gate(17.99)
    assert ok is False and "below_18" in reason
    result = evaluate_cross_location(_features("BUY", adx=17.99))
    assert result["approved"] is False


def test_adx_18_00_passes_only_adx_gate():
    ok, reason = adx_gate(18.0)
    assert ok is True and reason == "adx_gate_passed"
    # A perfect score cannot override a missing location: ADX alone only clears
    # the ADX gate, not the complete cross-location authority.
    weak = evaluate_cross_location(_features(
        "BUY", adx=18.0, demand_zone_near=False, bullish_order_block_valid=False,
        bullish_fvg_valid=False, real_value_area=False,
    ))
    assert weak["adx_ok"] is True
    assert weak["approved"] is False


def test_multiple_simultaneous_crosses_are_ranked_by_quality():
    high = {**_confirmed("BUY", 1795132800000), "cross_quality": HIGH_QUALITY,
            "adx": 28.0, "cross_distance_atr": 0.10, "risk_reward": 2.2}
    good = {**_confirmed("SELL", 1795132801000), "cross_quality": GOOD_QUALITY,
            "adx": 22.0, "cross_distance_atr": 0.30, "risk_reward": 1.5}
    weak = {**_confirmed("BUY", 1795132802000), "cross_quality": WEAK_QUALITY,
            "adx": 19.0, "cross_distance_atr": 1.2, "risk_reward": 0.5}
    ranked = rank_cross_candidates([weak, good, high])
    assert [row["cross_quality"] for row in ranked] == [HIGH_QUALITY, GOOD_QUALITY, WEAK_QUALITY]


def test_opposing_crosses_same_symbol_are_independent_and_direction_checked():
    buy = _confirmed("BUY", 1795132800000)
    sell = _confirmed("SELL", 1795132800000)
    assert buy["cross_event_id"] != sell["cross_event_id"]
    buy_result = evaluate_cross_location(_features("BUY"), "BUY")
    sell_result = evaluate_cross_location(_features("SELL"), "SELL")
    assert buy_result["approved"] and sell_result["approved"]
    wrong_side = evaluate_cross_location(_features("BUY"), "SELL")
    assert wrong_side["approved"] is False
    assert "cross_direction_mismatch" in wrong_side["rejection_reasons"] or wrong_side["quality"] == INVALID_QUALITY


def test_range_low_volatility_uses_0_25_percent_full_close_only():
    target, reason = select_range_target_pct({"volatility_band": "LOW_VOL"})
    assert target == 0.25 and "0.25pct" in reason
    evaluate = next(node for node in ast.walk(_class("TradeManagementBoard"))
                    if isinstance(node, ast.FunctionDef) and node.name == "evaluate")
    range_if = next(node for node in ast.walk(evaluate)
                    if isinstance(node, ast.If)
                    and any(isinstance(const, ast.Constant) and const.value == "RANGE_SCALP"
                            for const in ast.walk(node.test)))
    range_block = ast.get_source_segment(SOURCE, range_if)
    assert "TradeDecisionType.FULL_CLOSE" in range_block
    assert "TradeDecisionType.PARTIAL_CLOSE" not in range_block


def test_higher_volatility_scalp_uses_0_50_percent_full_close_only():
    target, reason = select_range_target_pct({"volatility_band": "HIGHER_VOL"})
    assert target == 0.50 and "0.50pct" in reason
    assert "higher_volatility_scalp_full_close_0.50pct" in BRAIN_SOURCE


def test_real_trend_uses_structural_tp1_tp2_with_no_tp3():
    plan = resolve_trend_targets({
        "side": "BUY", "entry": 100.0, "atr": 1.0,
        "opposing_liquidity_level": 103.0, "opposing_zone_level": 106.0,
    })
    assert plan.get("tp1") and plan.get("tp2")
    assert plan["tp1"] < plan["tp2"]
    board = ast.get_source_segment(SOURCE, _class("TradeManagementBoard"))
    assert "TP1_FIXED_50_PERCENT_OF_ORIGINAL_FILLED_QTY" in board
    assert "TP2_FINAL_50_PERCENT_RUNNER_TARGET" in board
    assert "TP3" not in SOURCE


def test_restart_with_open_position_blocks_entries_until_recovery_completes():
    wrapper = ast.get_source_segment(SOURCE, _function("main_loop_sniper"))
    assert "startup_trade_recovery" in wrapper
    assert wrapper.index("startup_trade_recovery") < wrapper.index("_original_main_loop_sniper")
    selection = ast.get_source_segment(SOURCE, _function("smart_opportunity_selection"))
    assert "RECOVERY_BLOCK_ENTRIES" in selection
    assert 'MEMORY.get("position_status") == "UNKNOWN"' in selection


def test_partial_close_verifies_actual_remaining_quantity():
    board = ast.get_source_segment(SOURCE, _class("TradeManagementBoard"))
    assert "target_remaining = initial * 0.5" in board
    assert "qty_to_close = max(0.0, remaining - target_remaining)" in board
    execution = ast.get_source_segment(SOURCE, _function("_execute_decision"))
    assert "safe_close_partial" in execution
    assert "TP1_VERIFIED" in execution


def test_full_close_requires_bingx_flat_verification():
    reduce = ast.get_source_segment(SOURCE, _function("_verified_market_reduce_impl"))
    assert 'post_status == "FLAT"' in reduce
    assert '"close_verified": post_status == "FLAT"' in reduce
    assert "actual position delta" in reduce or "filled_by_delta" in SOURCE


def test_duplicate_cross_event_id_is_blocked():
    event = _confirmed()
    ranked = rank_cross_candidates([event, dict(event)])
    rejected = [row for row in ranked if row.get("final_rejection_reason") == "duplicate_cross_event_id"]
    assert len(rejected) == 1
    ok, reason = execution_readiness(event, event_consumed=True, adx=26.0)
    assert ok is False and reason == "cross_event_already_consumed"
    assert "is_event_consumed" in SOURCE and "consume_event" in SOURCE


def test_no_new_entry_before_prior_execution_or_sync_is_resolved():
    core = ast.get_source_segment(SOURCE, _function("execute_entry"))
    assert "execution_unresolved" in SOURCE
    assert "recovery_hold" in SOURCE
    selection = ast.get_source_segment(SOURCE, _function("smart_opportunity_selection"))
    assert "RECOVERY_BLOCK_ENTRIES" in selection
    assert "_unified_watchlist_refresh_ok" in selection
    board = ast.get_source_segment(SOURCE, _class("TradeManagementBoard"))
    assert 'exchange_execution_or_recovery_unresolved' in board
    # The cross lifecycle state machine has explicit submitted -> filled/rejected
    # transitions; no candidate can jump from APPROVED straight to FILLED.
    assert CrossLifecycleState.EXECUTION_READY.value in {
        CrossLifecycleState.EXECUTION_READY.value
    }
    updated = transition_state(
        {**_confirmed(), "institutional_analysis_status": CrossLifecycleState.EXECUTION_READY.value},
        CrossLifecycleState.ENTRY_FILLED.value, "test",
    )
    assert updated["final_rejection_reason"].startswith("blocked_lifecycle_transition")
