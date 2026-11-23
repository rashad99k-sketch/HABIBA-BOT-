from ggp_profit_brain import (
    RANGE_MODE, TREND_MODE, STANDARD_MODE, classify_profit_mode,
    select_range_target_pct, adaptive_trail_multiplier,
)


def strong_trend():
    return {
        "side": "BUY", "adx": 31, "adx_slope": 1.0,
        "di_plus": 32, "di_minus": 11, "ema9_slope_atr": .35,
        "vwap_slope_atr": .24, "structure_score": 82,
        "continuation_probability": .81, "momentum_strength": 78,
        "volume_ratio": 1.35, "atr_expansion_ratio": 1.2,
        "candle_follow_through": True,
    }


def test_high_adx_alone_does_not_prove_trend():
    mode, _, _ = classify_profit_mode({
        "side": "BUY", "adx": 38, "adx_slope": 4, "regime": "UNKNOWN",
        "di_plus": 15, "di_minus": 18, "volume_ratio": .55,
        "momentum_strength": 20, "continuation_probability": .32,
        "atr_expansion_ratio": .7, "ema9_slope_atr": .01,
        "vwap_slope_atr": -.02, "structure_score": 25, "structure_broken": True,
    })
    assert mode != TREND_MODE


def test_low_adx_alone_does_not_force_scalp():
    mode, _, _ = classify_profit_mode({
        "side": "BUY", "adx": 12, "adx_slope": -1, "di_plus": 30, "di_minus": 10,
        "volume_ratio": 1.4, "momentum_strength": 75, "continuation_probability": .8,
        "ema9_slope_atr": .3, "vwap_slope_atr": .2, "structure_score": 75,
        "atr_expansion_ratio": 1.3, "candle_follow_through": True,
    })
    assert mode == TREND_MODE


def test_multi_factor_trend_selects_trend_ride():
    mode, _, evidence = classify_profit_mode(strong_trend())
    assert mode == TREND_MODE
    assert {"directional_di", "ema_vwap_slope", "structure", "continuation"}.issubset(set(evidence))


def test_weak_market_can_have_adx_29():
    mode, _, evidence = classify_profit_mode({
        "side": "SELL", "regime": "CHOPPY", "adx": 29, "adx_slope": -2,
        "di_plus": 21, "di_minus": 20, "volume_ratio": .55,
        "momentum_strength": 18, "continuation_probability": .30,
        "ema9_slope_atr": .01, "vwap_slope_atr": -.01,
        "structure_score": 20, "atr_expansion_ratio": .7,
        "liquidity_quality": "THIN", "opposing_liquidity_near": True,
        "adverse_divergence": True,
    })
    assert mode == RANGE_MODE
    assert len(evidence) >= 5


def test_unknown_data_keeps_two_stage_not_scalp():
    assert classify_profit_mode({"side": "BUY", "adx": 30})[0] == STANDARD_MODE


def test_scalp_targets_require_cost_room_and_entry_quality():
    base = {
        "side": "BUY", "estimated_round_trip_cost_pct": .04,
        "minimum_net_profit_pct": .10, "opposing_liquidity_distance_pct": 1.5,
        "entry_quality_score": 80, "momentum_strength": 70,
        "continuation_probability": .75, "volume_ratio": 1.3,
        "di_plus": 30, "di_minus": 10,
    }
    assert select_range_target_pct(base)[0] == 1.0
    assert select_range_target_pct({**base, "entry_quality_score": 60})[0] == .50
    assert select_range_target_pct({**base, "entry_quality_score": 30})[0] == .25


def test_scalp_blocked_when_costs_or_room_make_edge_impossible():
    assert select_range_target_pct({
        "side": "BUY", "estimated_round_trip_cost_pct": .20,
        "minimum_net_profit_pct": .10, "opposing_liquidity_distance_pct": .18,
    })[0] == 0.0


def test_trailing_uses_trend_evidence_not_adx_alone():
    weak = adaptive_trail_multiplier(1.5, .5, 40, 20, 0, "TREND_RIDE", 0, True, 20)
    strong = adaptive_trail_multiplier(1.5, .8, 15, 20, 0, "TREND_RIDE", 0, True, 80)
    assert strong > weak

from ggp_profit_brain import plan_trend_targets


def test_trend_targets_are_capped_before_opposing_liquidity():
    tp1, tp2, reason = plan_trend_targets("BUY", 100.0, 1.0, [101.2, 103.0], 1.0, 3.0, 1.5)
    assert tp1 < 101.2
    assert tp2 <= 103.0
    assert tp2 > tp1
    assert "liquidity" in reason


def test_trend_target2_is_blocked_when_no_farther_liquidity_exists():
    tp1, tp2, _ = plan_trend_targets("BUY", 100.0, 1.0, [101.0], 1.0, 3.0, 1.5)
    assert tp1 > 100.0
    assert tp2 == 0.0


def test_trend_targets_are_directional_for_short_positions():
    tp1, tp2, _ = plan_trend_targets("SELL", 100.0, 1.0, [99.0, 97.0], 1.0, 3.0, 1.5)
    assert tp1 < 100.0
    assert tp2 < tp1


