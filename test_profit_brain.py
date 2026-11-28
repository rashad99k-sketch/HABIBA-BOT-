from ggp_profit_brain import (
    RANGE_MODE, TREND_MODE, STANDARD_MODE,
    classify_profit_mode, range_target_price, target_reached,
    select_range_target_pct, tp2_reassessment,
    advance_closed_candle_confirmation,
)
from ggp_market_intelligence import build_scalp_target


def test_unknown_or_missing_data_does_not_trigger_full_close_range_mode():
    mode, reason, _ = classify_profit_mode({"side": "BUY", "regime": "UNKNOWN"})
    assert mode == STANDARD_MODE
    assert "not_proven" in reason


def test_confirmed_trend_selects_trend_ride():
    mode, _, _ = classify_profit_mode({
        "side": "BUY", "regime": "STRONG_TREND", "trade_state": "TREND_RIDE",
        "adx": 30, "adx_slope": 1, "continuation_probability": 0.82,
        "di_plus": 32, "di_minus": 12, "momentum_strength": 78,
    })
    assert mode == TREND_MODE


def test_range_scalp_requires_multiple_weak_market_evidence():
    mode, _, evidence = classify_profit_mode({
        "side": "BUY", "regime": "CHOPPY", "trade_state": "RANGE_CHOP",
        "adx": 15, "adx_slope": -1.0, "continuation_probability": 0.38,
        "di_plus": 18, "di_minus": 17, "volume_ratio": 0.62,
        "momentum_strength": 20,
    })
    assert mode == RANGE_MODE
    assert "low_relative_volume" in evidence


def test_fresh_directional_vwap_cross_blocks_one_shot_scalp_classification():
    mode, reason, evidence = classify_profit_mode({
        "side": "BUY", "regime": "CHOPPY", "trade_state": "RANGE_CHOP",
        "adx": 15, "adx_slope": -1.0, "continuation_probability": 0.38,
        "di_plus": 18, "di_minus": 17, "volume_ratio": 0.62,
        "momentum_strength": 20,
        "fresh_ema9_vwap_cross": True, "vwap_cross_direction": "BULLISH",
        "vwap_cross_age_seconds": 60, "vwap_cross_max_age_seconds": 1800,
    })
    assert mode == STANDARD_MODE
    assert "fresh_directional_vwap_cross" in reason
    assert "fresh_directional_ema_vwap_cross" in evidence


def test_opposite_direction_vwap_cross_does_not_veto_range_classification():
    mode, _, _ = classify_profit_mode({
        "side": "BUY", "regime": "CHOPPY", "trade_state": "RANGE_CHOP",
        "adx": 15, "adx_slope": -1.0, "continuation_probability": 0.38,
        "di_plus": 18, "di_minus": 17, "volume_ratio": 0.62,
        "momentum_strength": 20,
        "fresh_ema50_vwap_cross": True, "vwap_cross_direction": "BEARISH",
    })
    assert mode == RANGE_MODE


def test_low_volume_alone_does_not_activate_range_scalp():
    mode, _, _ = classify_profit_mode({
        "side": "SELL", "regime": "UNKNOWN", "trade_state": "UNKNOWN",
        "adx": 24, "adx_slope": 1.0, "continuation_probability": 0.66,
        "di_plus": 15, "di_minus": 23, "volume_ratio": 0.60,
        "momentum_strength": 60,
    })
    assert mode == STANDARD_MODE


def test_range_target_is_conservative_when_thin_or_weak():
    target, _ = select_range_target_pct({
        "side": "BUY", "adx": 14, "continuation_probability": 0.30,
        "volume_ratio": 0.50, "thin_liquidity": True,
    })
    assert target == 0.25


def test_range_target_price_and_hit_are_directional():
    assert range_target_price("BUY", 100, 0.5) == 100.5
    assert range_target_price("SELL", 100, 0.5) == 99.5
    assert target_reached("BUY", 100.5, 100.5)
    assert target_reached("SELL", 99.5, 99.5)
    assert not target_reached("BUY", 100, 100.5)


def test_tp2_reassessment_closes_only_when_far_and_weak_or_structure_lost():
    decision, _ = tp2_reassessment("BUY", 100, 110, 2, 0.30, 17, -1, True)
    assert decision == "CLOSE"
    decision, _ = tp2_reassessment("SELL", 100, 90, 2, 0.82, 30, 1, True)
    assert decision == "HOLD"


def test_single_warning_does_not_force_close_losing_trade():
    from ggp_profit_brain import thesis_failure_action
    action, _ = thesis_failure_action("BUY", -1.0, 55, ["strong_reclaim"])
    assert action == "HOLD"


def test_two_independent_severe_signals_force_strict_exit():
    from ggp_profit_brain import thesis_failure_action
    action, reason = thesis_failure_action(
        "BUY", -2.0, 55, ["strong_reclaim", "di_flip_bearish"]
    )
    assert action == "EXIT"
    assert "multi_evidence_thesis_failure" in reason


def test_moderate_thesis_warning_on_positive_trade_requests_protection():
    from ggp_profit_brain import thesis_failure_action
    action, reason = thesis_failure_action("SELL", 3.0, 52, ["low_continuation_pressure"])
    assert action == "PROTECT"
    assert "protect_positive_trade" in reason


def test_structural_invalidation_forces_exit_even_below_score_threshold():
    from ggp_profit_brain import thesis_failure_action
    action, reason = thesis_failure_action("BUY", 4.0, 20, [], structural_invalidation=True)
    assert action == "EXIT"
    assert reason == "structural_invalidation"


def test_adaptive_trail_gives_confirmed_trend_more_room_than_distribution():
    from ggp_profit_brain import adaptive_trail_multiplier
    trend = adaptive_trail_multiplier(1.5, 0.85, 30, 20, 0, "TREND_RIDE", 2, True)
    risk = adaptive_trail_multiplier(1.5, 0.40, 16, 75, 4, "EMA50_BREAK_CONFIRMED", 30, True)
    assert trend > risk
    assert 0.55 <= risk <= 3.5


def test_ema_context_requires_completed_candle_history():
    from ggp_profit_brain import ema_trend_context
    import pandas as pd
    short = pd.DataFrame({"close": [1.0] * 20})
    assert ema_trend_context(short, "BUY")["state"] == "INSUFFICIENT_DATA"


def test_ema_context_classifies_aligned_uptrend():
    from ggp_profit_brain import ema_trend_context
    import pandas as pd
    closes = [100 + i * 0.5 for i in range(70)]
    frame = pd.DataFrame({"close": closes})
    result = ema_trend_context(frame, "BUY")
    assert result["state"] == "TREND_RIDE"
    assert result["trend_aligned"] is True


def test_buy_sell_opposing_liquidity_targets_trigger_in_correct_direction():
    buy = build_scalp_target("BUY", 100.0, 100.90)
    sell = build_scalp_target("SELL", 100.0, 99.10)
    assert buy["status"] == sell["status"] == "VALID"
    assert target_reached("BUY", 100.5, buy["target_price"])
    assert not target_reached("BUY", 100.4, buy["target_price"])
    assert target_reached("SELL", 99.5, sell["target_price"])
    assert not target_reached("SELL", 99.6, sell["target_price"])


def test_opposing_liquidity_scalp_plan_has_one_full_close_target():
    buy = build_scalp_target("BUY", 100.0, 100.90)
    sell = build_scalp_target("SELL", 100.0, 99.10)
    assert buy["status"] == sell["status"] == "VALID"
    assert buy["tp1_target_price"] == buy["tp1_target_pct"] == 0.0
    assert sell["tp1_target_price"] == sell["tp1_target_pct"] == 0.0
    assert buy["target_price"] < 100.90
    assert sell["target_price"] > 99.10
    assert build_scalp_target("BUY", 100.0, 100.20)["status"] == "NO_TARGET"



def test_range_targets_use_price_move_tiers_025_050():
    weak, _ = select_range_target_pct({
        "side": "BUY", "adx": 14, "continuation_probability": 0.30,
        "volume_ratio": 0.50, "thin_liquidity": True,
    })
    moderate, _ = select_range_target_pct({
        "side": "BUY", "adx": 19, "continuation_probability": 0.60,
        "volume_ratio": 0.95, "di_plus": 28, "di_minus": 14,
        "aligned_closed_candles": 2,
    })
    strong, _ = select_range_target_pct({
        "side": "SELL", "adx": 25, "continuation_probability": 0.78,
        "volume_ratio": 1.30, "di_plus": 12, "di_minus": 31,
        "aligned_closed_candles": 2,
    })
    assert (weak, moderate, strong) == (0.25, 0.50, 0.50)


def test_range_profit_targets_include_exceptional_one_percent_tier():
    from ggp_profit_brain import RANGE_PROFIT_TARGETS_PCT
    assert RANGE_PROFIT_TARGETS_PCT == (0.25, 0.50, 1.00)


def test_range_target_one_percent_requires_exceptional_closed_candle_evidence():
    target, reason = select_range_target_pct({
        "side": "BUY", "adx": 29, "continuation_probability": 0.85,
        "volume_ratio": 1.42, "di_plus": 31, "di_minus": 12,
        "aligned_closed_candles": 3,
    })
    assert target == 1.00
    assert reason == "exceptional_closed_candle_follow_through_1.00pct"


def test_range_target_does_not_select_one_percent_without_full_evidence():
    target, _ = select_range_target_pct({
        "side": "BUY", "adx": 29, "continuation_probability": 0.85,
        "volume_ratio": 1.42, "di_plus": 31, "di_minus": 12,
        "aligned_closed_candles": 2,
    })
    assert target == 0.50


def test_scalp_board_uses_two_stage_50_50_partial_tp1_and_runner_tp2():
    from pathlib import Path
    source = Path(__file__).with_name("ggpbot.py").read_text(encoding="utf-8")
    board_start = source.index("class TradeManagementBoard:")
    board_end = source.index("# ========== REAL EXCHANGE ORDERS", board_start)
    board = source[board_start:board_end]
    assert 'range_mode = profit_mode == "RANGE_SCALP"' in board
    assert "RANGE_SCALP_TP1_FIXED_50_PERCENT" in board
    assert "RANGE_SCALP_TP2_FINAL_50_PERCENT_RUNNER" in board
    assert "target_remaining = initial * 0.5" in board
    assert "range_scalp_waiting_for_two_stage_targets" in board
    assert "range_scalp_waiting_for_single_target" not in board



def test_only_recent_directional_vwap_cross_blocks_range_scalp():
    common = {
        "side": "BUY", "regime": "CHOPPY", "trade_state": "RANGE_CHOP",
        "adx": 15, "adx_slope": -1.0, "continuation_probability": 0.38,
        "di_plus": 18, "di_minus": 17, "volume_ratio": 0.62,
        "momentum_strength": 20,
        "fresh_ema9_vwap_cross": True, "vwap_cross_direction": "BULLISH",
    }
    recent_mode, recent_reason, _ = classify_profit_mode({
        **common, "vwap_cross_age_seconds": 60, "vwap_cross_max_age_seconds": 1800,
    })
    stale_mode, stale_reason, _ = classify_profit_mode({
        **common, "vwap_cross_age_seconds": 3600, "vwap_cross_max_age_seconds": 1800,
    })
    unknown_mode, _, _ = classify_profit_mode(common)

    assert recent_mode == STANDARD_MODE
    assert "fresh_directional_vwap_cross" in recent_reason
    assert stale_mode == RANGE_MODE
    assert "confirmed_range_weak_market" in stale_reason
    assert unknown_mode == RANGE_MODE




def test_exit_risk_closes_profitable_trade_only_on_multi_engine_confluence():
    from ggp_profit_brain import evaluate_exit_risk
    result = evaluate_exit_risk({
        "side": "BUY", "roe": 4.2, "di_plus": 12, "di_minus": 28,
        "momentum": {"flow_bias": "SELL", "momentum_decay": True, "exhaustion_risk": 72},
        "smart_money": {"distribution_risk": 68},
        "continuation_probability": 0.32,
    })
    assert result["action"] == "CLOSE_PROFIT"
    assert result["group_count"] >= 2


def test_exit_risk_does_not_close_profitable_trade_on_one_warning():
    from ggp_profit_brain import evaluate_exit_risk
    result = evaluate_exit_risk({
        "side": "BUY", "roe": 2.0,
        "momentum": {"momentum_decay": True},
    })
    assert result["action"] == "HOLD"


def test_exit_risk_hard_structural_invalidation_is_not_delayed_by_profit_mode():
    from ggp_profit_brain import evaluate_exit_risk
    result = evaluate_exit_risk({
        "side": "SELL", "roe": -1.0, "structural_invalidation": True,
    })
    assert result["action"] == "HARD_EXIT"


def test_institutional_flow_and_di_opposition_tighten_profitable_trade():
    from ggp_profit_brain import evaluate_exit_risk
    result = evaluate_exit_risk({
        "side": "SELL", "roe": 1.8, "di_plus": 29, "di_minus": 15,
        "smart_money": {"institutional_bias": "BUY"},
    })
    assert result["action"] == "LOCK_PROFIT"
    assert "DIRECTIONAL_FLOW" in result["groups"]
    assert "INSTITUTIONAL_FLOW" in result["groups"]


def test_opposing_sr_rejection_adds_independent_risk_evidence():
    from ggp_profit_brain import evaluate_exit_risk
    result = evaluate_exit_risk({
        "side": "BUY", "roe": 2.0, "opposing_zone_rejection": True,
        "opposing_zone_distance_atr": 0.25,
        "momentum": {"momentum_decay": True},
    })
    assert result["action"] == "LOCK_PROFIT"
    assert "SUPPORT_RESISTANCE" in result["groups"]


def test_distribution_pressure_is_not_exit_risk_for_aligned_short_trade():
    from ggp_profit_brain import evaluate_exit_risk
    result = evaluate_exit_risk({
        "side": "SELL", "roe": 2.0,
        "smart_money": {"distribution_risk": 82, "institutional_bias": "SELL"},
    })
    assert result["action"] == "HOLD"


def test_fresh_opposite_dual_vwap_cross_adds_risk_for_profitable_position():
    from ggp_profit_brain import evaluate_exit_risk
    result = evaluate_exit_risk({
        "side": "BUY", "roe": 2.0,
        "fresh_ema9_vwap_cross": True, "fresh_ema50_vwap_cross": True,
        "vwap_cross_direction": "BEARISH",
        "vwap_cross_age_seconds": 60, "vwap_cross_max_age_seconds": 1800,
        "momentum": {"momentum_decay": True},
    })
    assert result["action"] == "LOCK_PROFIT"
    assert "CROSS_STRUCTURE" in result["groups"]


def test_confirmed_bearish_trend_selects_trend_ride():
    mode, _, _ = classify_profit_mode({
        "side": "SELL", "regime": "STRONG_TREND", "trade_state": "TREND_RIDE",
        "adx": 31, "adx_slope": 1.2, "continuation_probability": 0.84,
        "di_plus": 11, "di_minus": 34, "momentum_strength": 82,
        "trend_expansion": True,
    })
    assert mode == TREND_MODE


def test_momentum_engine_uses_direction_neutral_strength_and_separate_flow_bias():
    from pathlib import Path
    source = Path(__file__).with_name("ggpbot.py").read_text(encoding="utf-8")
    assert "abs_spread = abs(float(momentum_spread.iloc[-1]))" in source
    assert "trend_expansion = abs_spread > 0.35" in source
    assert 'flow_bias = "SELL"' in source



def test_unconfirmed_momentum_collapse_state_does_not_force_hard_exit():
    from ggp_profit_brain import evaluate_exit_risk
    result = evaluate_exit_risk({
        "side": "BUY", "roe": -0.07,
        "trade_state": "MOMENTUM_COLLAPSE",
        "momentum_collapse_confirmed": False,
        "momentum": {"momentum_decay": False, "flow_bias": "BUY"},
    })
    assert result["action"] == "HOLD"
    assert result["hard_exit"] is False
    assert "unconfirmed_momentum_collapse_deferred" in result["reasons"]


def test_confirmed_momentum_collapse_still_forces_hard_exit():
    from ggp_profit_brain import evaluate_exit_risk
    result = evaluate_exit_risk({
        "side": "BUY", "roe": -0.07,
        "trade_state": "MOMENTUM_COLLAPSE",
        "momentum_collapse_confirmed": True,
    })
    assert result["action"] == "HARD_EXIT"
    assert result["hard_exit"] is True
    assert "confirmed_momentum_collapse" in result["reasons"]


def test_panic_liquidity_and_structural_hard_exits_remain_immediate():
    from ggp_profit_brain import evaluate_exit_risk
    for context in (
        {"trade_state": "PANIC_EXIT"},
        {"trade_state": "LIQUIDITY_EXHAUSTION"},
        {"structural_invalidation": True},
    ):
        result = evaluate_exit_risk({"side": "BUY", "roe": -0.1, **context})
        assert result["action"] == "HARD_EXIT"


def test_plain_momentum_decay_warning_is_not_a_hard_exit():
    from ggp_profit_brain import evaluate_exit_risk
    result = evaluate_exit_risk({
        "side": "BUY", "roe": -0.07,
        "momentum": {"momentum_decay": True},
    })
    assert result["action"] == "HOLD"
    assert result["hard_exit"] is False


def test_momentum_engine_distinguishes_quiet_market_from_confirmed_decay():
    from pathlib import Path
    source = Path(__file__).with_name("ggpbot.py").read_text(encoding="utf-8")
    assert "prior_spreads = momentum_spread.iloc[-5:-1].abs().dropna()" in source
    assert "prior_peak_spread >= 0.20" in source
    assert "prior_peak_spread * 0.35" in source
    assert '"momentum_collapse_confirmed": bool(momentum_decay)' in source


def test_opposing_liquidity_selector_is_imported_where_live_manager_calls_it():
    import ast
    from pathlib import Path
    source = Path(__file__).with_name("ggpbot.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported = any(
        isinstance(node, ast.ImportFrom)
        and node.module == "ggp_market_intelligence"
        and any(alias.name == "select_opposing_liquidity_pool" for alias in node.names)
        for node in tree.body
    )
    called = any(
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "select_opposing_liquidity_pool"
        for node in ast.walk(tree)
    )
    assert imported and called


def test_loss_exit_confirmation_advances_only_on_distinct_closed_candles():
    first = advance_closed_candle_confirmation(
        True, 900, last_candle_ts=0, streak=0, max_gap_seconds=1350
    )
    repeated = advance_closed_candle_confirmation(
        True, 900, last_candle_ts=first["last_candle_ts"],
        streak=first["count"], max_gap_seconds=1350
    )
    second = advance_closed_candle_confirmation(
        True, 1800, last_candle_ts=repeated["last_candle_ts"],
        streak=repeated["count"], max_gap_seconds=1350
    )
    assert first["count"] == 1 and first["confirmed"] is False
    assert repeated["count"] == 1 and repeated["confirmed"] is False
    assert repeated["advanced"] is False
    assert second["count"] == 2 and second["confirmed"] is True


def test_loss_exit_confirmation_resets_after_non_signal_or_data_gap():
    first = advance_closed_candle_confirmation(
        True, 900, last_candle_ts=0, streak=0, max_gap_seconds=1350
    )
    no_signal = advance_closed_candle_confirmation(
        False, 1800, last_candle_ts=first["last_candle_ts"],
        streak=first["count"], max_gap_seconds=1350
    )
    assert no_signal["count"] == 0
    assert no_signal["confirmed"] is False

    after_gap = advance_closed_candle_confirmation(
        True, 3600, last_candle_ts=900, streak=1, max_gap_seconds=1350
    )
    assert after_gap["count"] == 1
    assert after_gap["confirmed"] is False


def test_unconfirmed_high_thesis_score_does_not_hard_exit_losing_trade():
    from ggp_profit_brain import evaluate_exit_risk
    result = evaluate_exit_risk({
        "side": "BUY", "roe": -2.0,
        "thesis_failure_score": 85,
        "thesis_failure_confirmed": False,
    })
    assert result["action"] == "HOLD"
    assert result["hard_exit"] is False
    assert "unconfirmed_thesis_failure_deferred" in result["reasons"]


def test_confirmed_high_thesis_score_remains_hard_exit():
    from ggp_profit_brain import evaluate_exit_risk
    result = evaluate_exit_risk({
        "side": "SELL", "roe": -2.0,
        "thesis_failure_score": 85,
        "thesis_failure_confirmed": True,
    })
    assert result["action"] == "HARD_EXIT"
    assert result["hard_exit"] is True
    assert "confirmed_thesis_failure_score_70_plus" in result["reasons"]


def test_live_board_has_no_unconfirmed_momentum_collapse_bypass_and_range_is_two_stage():
    from pathlib import Path
    source = Path(__file__).with_name("ggpbot.py").read_text(encoding="utf-8")
    assert 'if trade_state in ("PANIC_EXIT", "LIQUIDITY_EXHAUSTION"):' in source
    assert 'if trade_state in ("PANIC_EXIT", "MOMENTUM_COLLAPSE", "LIQUIDITY_EXHAUSTION"):' not in source
    board_start = source.index("class TradeManagementBoard:")
    board_end = source.index("# ========== REAL EXCHANGE ORDERS", board_start)
    board = source[board_start:board_end]
    assert "RANGE_SCALP_TP1_FIXED_50_PERCENT" in board
    assert "RANGE_SCALP_TP2_FINAL_50_PERCENT_RUNNER" in board
    assert "range_scalp_waiting_for_two_stage_targets" in board
    assert "loss_exit_waiting_for_second_closed_candle_confirmation" in board


def test_trade_state_machine_uses_distinct_closed_candle_collapse_confirmation():
    from pathlib import Path
    source = Path(__file__).with_name("ggpbot.py").read_text(encoding="utf-8")
    machine_start = source.index("class TradeStateMachine:")
    machine_end = source.index("# ========== TRADE STATE & PERFORMANCE TRACKING", machine_start)
    machine = source[machine_start:machine_end]
    assert "advance_closed_candle_confirmation(" in machine
    assert "momentum_collapse_confirmed" in machine
    assert "def reset(self):" in machine
    assert 'return self.current_state in ("PANIC_EXIT", "LIQUIDITY_EXHAUSTION")' in machine
    assert 'new_state = "RANGE_CHOP" if self.current_state == "MOMENTUM_COLLAPSE" else self.current_state' in machine
