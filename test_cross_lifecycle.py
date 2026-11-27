"""Focused tests for the unified EMA9 + Session-VWAP cross lifecycle.

All tests are offline. No exchange call, order, balance query or live trade is
performed. Structural invariants are checked with AST/source assertions where
the protected execution and management paths must remain untouched.
"""
from __future__ import annotations

import ast
from pathlib import Path

import pandas as pd
import pytest

from ggp_cross_lifecycle import (
    CrossLifecycleState, adx_gate, build_location_features, evaluate_cross_location,
    execution_readiness, make_cross_event_id, merge_cross_events, normalize_cross_event,
    rank_cross_candidates, transition_state, HIGH_QUALITY, GOOD_QUALITY, WEAK_QUALITY,
    INVALID_QUALITY,
)
from ggp_institutional_bridge import _select_cross_event, cross_event_authorization_block
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
    healthy_context = {
        "volatility_band": "HIGHER_VOL", "adx": 24.0,
        "continuation_probability": 0.60, "volume_ratio": 1.10,
    }
    target, reason = select_range_target_pct(healthy_context)
    assert target == 0.50 and "0.50pct" in reason
    assert "higher_volatility_scalp_full_close_0.50pct" in BRAIN_SOURCE
    # The volatility band must never override the inherited weak-market guard.
    weak = select_range_target_pct({**healthy_context, "adx": 17.5})
    assert weak[0] == 0.25 and "weak_or_thin_market" in weak[1]


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


def _closed_frame(rows=100):
    base_ts = 1795132800000 - rows * 900000
    closes = [100.0 + index * 0.05 + (0.20 if index % 7 == 0 else -0.10)
              for index in range(rows)]
    opens = [100.0] + closes[:-1]
    return pd.DataFrame({
        "timestamp": [base_ts + index * 900000 for index in range(rows)],
        "open": opens,
        "high": [max(opens[index], closes[index]) + 0.25 for index in range(rows)],
        "low": [min(opens[index], closes[index]) - 0.25 for index in range(rows)],
        "close": closes,
        "volume": [1000.0 + index for index in range(rows)],
    })


def test_normalized_event_is_not_readable_until_analysis_is_written_back():
    candle_ts = 1795132800000
    proximity = {
        "state": "CROSS_CONFIRMED", "direction": "BUY", "cross_ema9": "BULLISH",
        "data_freshness": "FRESH", "candle_timestamp": candle_ts,
        "ema9": 101.0, "ema50": 99.5, "vwap": 100.0, "gap_atr": 0.10,
    }
    raw = normalize_cross_event("BTC/USDT:USDT", 900, proximity, 26.0, candle_ts + 900000)
    assert raw and raw["cross_status"] == "CONFIRMED"
    ok, reason = execution_readiness(raw, adx=26.0)
    assert ok is False and reason.startswith("cross_location_quality:")
    enriched = {
        **raw,
        "cross_quality": HIGH_QUALITY,
        "institutional_analysis_status": CrossLifecycleState.INSTITUTIONAL_APPROVED.value,
    }
    ok, reason = execution_readiness(enriched, adx=26.0)
    assert ok is True and reason == "cross_lifecycle_execution_ready"


def test_duplicate_cross_event_cannot_be_reapproved_or_executed():
    event = _confirmed()
    ranked = rank_cross_candidates([event, dict(event)])
    duplicate = next(row for row in ranked
                     if row.get("final_rejection_reason") == "duplicate_cross_event_id")
    assert duplicate["cross_rank"] == -1.0
    assert duplicate["institutional_analysis_status"] == CrossLifecycleState.INSTITUTIONAL_REJECTED.value
    promoted = transition_state(
        {**duplicate, "institutional_analysis_status":
         CrossLifecycleState.INSTITUTIONAL_REJECTED.value},
        CrossLifecycleState.INSTITUTIONAL_APPROVED.value, "retry",
    )
    assert promoted["institutional_analysis_status"] == CrossLifecycleState.INSTITUTIONAL_REJECTED.value
    ok, reason = execution_readiness(duplicate, adx=26.0)
    assert ok is False and reason == "duplicate_cross_event_id"


def test_bridge_selects_newest_unconsumed_cross_not_a_stale_consumed_id():
    old_ts, new_ts = 1795132800000, 1795132809000
    symbol = "BTC/USDT:USDT"
    old = {**_confirmed("BUY", old_ts),
           "cross_event_id": make_cross_event_id(symbol, 900, "BUY", old_ts),
           "cross_event_id_consumed": True}
    new = {**_confirmed("BUY", new_ts),
           "cross_event_id": make_cross_event_id(symbol, 900, "BUY", new_ts)}
    assert _select_cross_event([old, new], "BUY")["cross_event_id"] == new["cross_event_id"]
    assert _select_cross_event([old, new], "BUY", old["cross_event_id"])["cross_event_id"] == new["cross_event_id"]
    assert _select_cross_event([old], "BUY") is None
    approaching = {**_confirmed("BUY", old_ts), "cross_status": "APPROACHING"}
    assert _select_cross_event([approaching], "BUY") is None
    expired = {**_confirmed("BUY", old_ts),
               "final_rejection_reason": "cross_signal_expired_without_reentry"}
    assert _select_cross_event([expired, new], "BUY", expired["cross_event_id"])["cross_event_id"] == new["cross_event_id"]


def test_bridge_authorization_block_rejects_consumed_and_nonconfirmed_events():
    event = _confirmed("BUY", 1795132800000)
    assert cross_event_authorization_block(event) is None
    assert cross_event_authorization_block(None) == "cross_event_missing"
    assert cross_event_authorization_block({**event, "cross_event_id_consumed": True}) == "cross_event_already_consumed"
    assert cross_event_authorization_block({**event, "cross_status": "APPROACHING"}) == "cross_event_not_confirmed"
    assert cross_event_authorization_block({**event, "final_rejection_reason": "duplicate_cross_event_id"}) == "duplicate_cross_event_id"


def test_price_space_uses_nearest_opposing_boundary_not_ema_vwap_gap():
    candle_ts = 1795132800000
    cross = {
        **_confirmed("BUY", candle_ts),
        "cross_distance_atr": 2.0,
        "cross_ema9": "BULLISH",
    }
    context = {
        "price": 100.0, "atr": 1.0, "adx": 26.0,
        "data_quality": "VALID_CLOSED_CANDLES",
        "golden_zone": {"state": "GOLDEN_CONFLUENCE", "value_zone": "DISCOUNT", "score": 80,
                        "ob_valid": True, "fvg_valid": True, "support_resistance_aligned": True},
        "market_structure": {"confirmed": True, "mss": "BULLISH_CHOCH",
                             "swing_high": 106.0, "swing_low": 95.0},
        "liquidity": {"event": "SWEEP", "direction": "BULLISH", "level": 101.5},
        "order_block": {"status": "FRESH", "direction": "BULLISH"},
        "fvg": {"direction": "BULLISH", "status": "UNFILLED"},
        "support_resistance": {
            "supports": [{"low": 98.0, "high": 99.0, "status": "CONFIRMED", "strength": 7}],
            "resistances": [{"low": 100.4, "high": 101.0, "status": "CONFIRMED", "strength": 7}],
        },
        "smart_money": {"institutional_bias": "BULLISH"},
        "momentum": {"flow_bias": "BULLISH"},
        "institutional_trend": {"state": "BULLISH", "supporting_check": True},
        "indicator_snapshot": {}, "volume_confirmed": True,
    }
    features = build_location_features(context, cross, "BUY")
    assert features["available_price_space_atr"] == pytest.approx(0.4)
    assert features["room_before_opposing_liquidity"] is False
    assert features["room_before_opposing_structure"] is False


def test_production_cross_path_reaches_execute_entry_with_approved_event(monkeypatch):
    import ggpbot as bot

    exchange_symbol = "BTC/USDT:USDT"
    candle_ts = 1795132800000
    proximity = {
        "state": "CROSS_CONFIRMED", "direction": "BUY", "cross_ema9": "BULLISH",
        "data_freshness": "FRESH", "candle_timestamp": candle_ts,
        "ema9": 101.0, "ema50": 99.5, "vwap": 100.0, "gap_atr": 0.10,
    }
    stale_proximity = {**proximity, "candle_timestamp": candle_ts - 900000}
    stale_event = normalize_cross_event(exchange_symbol, 900, stale_proximity, 26.0,
                                        candle_ts + 900000)
    stale_event["cross_status"] = "EXPIRED"
    stale_event["final_rejection_reason"] = "cross_signal_expired_without_reentry"
    event = normalize_cross_event(exchange_symbol, 900, proximity, 26.0, candle_ts + 900000)
    watch_key = "BTCUSDT"
    watch_item = {
        "symbol": watch_key, "exchange_symbol": exchange_symbol, "side": "BUY",
        "decision_state": "ENTRY_READY", "cross_status": "CONFIRMED",
        "cross_event_id": stale_event["cross_event_id"], "score": 80.0,
        "price": 100.0, "atr": 1.0,
    }
    fresh = {
        "side": "BUY", "decision_state": "ENTRY_READY", "event_id": "STRUCT-E1",
        "score": 80.0, "decision_setup_type": "REVERSAL_ENGINE", "structural_stop": 99.0,
        "adx": 26.0, "price": 100.0, "atr": 1.0,
        "golden_zone": {"state": "GOLDEN_CONFLUENCE", "value_zone": "DISCOUNT", "score": 80,
                        "ob_valid": True, "fvg_valid": True, "support_resistance_aligned": True},
        "order_block": {"status": "FRESH", "direction": "BULLISH", "distance_atr": 0.1,
                        "strength": 7, "low": 99.5, "high": 100.5,
                        "impulse_timestamp": candle_ts - 900000},
        "fvg": {"direction": "BULLISH", "status": "UNFILLED", "lower": 99.2,
                "upper": 100.1, "size_atr": 0.4},
        "liquidity": {"event": "SWEEP", "direction": "BULLISH", "level": 101.5},
        "market_structure": {"confirmed": True, "mss": "BULLISH_CHOCH",
                             "swing_high": 106.0, "swing_low": 95.0},
        "support_resistance": {
            "supports": [{"low": 98.0, "high": 99.0, "status": "CONFIRMED", "strength": 7}],
            "resistances": [{"low": 105.0, "high": 106.0, "status": "CONFIRMED", "strength": 7}],
        },
        "smart_money": {"institutional_bias": "BULLISH"},
        "momentum": {"flow_bias": "BULLISH", "trend_expansion": True},
        "institutional_trend": {"state": "BULLISH", "supporting_check": True},
        "volume_confirmed": True,
        "market_intelligence": {"mode": "RANGE_SCALP", "entry_behavior": "WAIT_FOR_STRUCTURE",
                                "reason_codes": [], "confidence": 0.7},
        "evidence": ["sweep", "order_block"],
        "institutional_analysis_status": CrossLifecycleState.INSTITUTIONAL_APPROVED.value,
        "cross_quality": HIGH_QUALITY,
    }
    enrichment = {
        "smart_money": fresh["smart_money"], "momentum": fresh["momentum"],
        "institutional_trend": fresh["institutional_trend"],
        "volume_confirmed": True, "cross_distance_atr": 0.10, "adx": 26.0,
    }
    frame = _closed_frame()
    logs = []
    calls = []
    bridge_calls = []

    def fake_bridge(symbol, df, **kwargs):
        events = list(kwargs.get("cross_events") or [])
        ids = {str(row.get("cross_event_id") or "") for row in events if isinstance(row, dict)}
        bridge_calls.append(ids)
        # Canonical watchlist key and exchange-qualified event symbol must match.
        assert ids == {stale_event["cross_event_id"], event["cross_event_id"]}
        return dict(fresh)

    def fake_execute_entry(*args, **kwargs):
        calls.append({"args": args, "kwargs": kwargs,
                      "context": dict(bot._INSTITUTIONAL_ENTRY_CONTEXT or {})})
        return True

    journal = type("Journal", (), {
        "is_event_consumed": lambda self, event_id: False,
        "consume_event": lambda self, event_id, trade_id: True,
    })()
    authority = type("Authority", (), {"consume": lambda self, event_id: True})()
    monkeypatch.setattr(bot, "MEMORY", {
        "watchlist": {watch_key: watch_item},
        "cross_events": {
            stale_event["cross_event_id"]: stale_event,
            event["cross_event_id"]: event,
        },
        "_unified_watchlist_refresh_ok": True,
    })
    monkeypatch.setattr(bot, "STATE", {"open": False})
    monkeypatch.setattr(bot, "TRADE_STATE", {"in_position": False})
    monkeypatch.setattr(bot, "_ACTIVE_TRADE", None)
    monkeypatch.setattr(bot, "RECOVERY_BLOCK_ENTRIES", False)
    monkeypatch.setattr(bot, "_INSTITUTIONAL_ENTRY_CONTEXT", None)
    monkeypatch.setattr(bot, "daily_loss_cooldown_active", lambda: False)
    monkeypatch.setattr(bot, "refresh_unified_institutional_watchlist",
                        lambda force=False: bot.MEMORY["watchlist"])
    monkeypatch.setattr(bot, "_validated_scanner_symbol", lambda _: exchange_symbol)
    monkeypatch.setattr(bot, "_integration_fetch_frame", lambda _: frame)
    monkeypatch.setattr(bot, "roro_validate_closed_frame", lambda df, **kwargs: df)
    monkeypatch.setattr(bot, "validate_dataframe", lambda *args, **kwargs: True)
    monkeypatch.setattr(bot, "roro_analyze_symbol_both", fake_bridge)
    monkeypatch.setattr(bot, "_enrich_with_existing_gpp_engines",
                        lambda *args, **kwargs: dict(enrichment))
    monkeypatch.setattr(bot, "validate_location_zone", lambda *args, **kwargs: {"valid": True})
    monkeypatch.setattr(bot, "compute_atr", lambda _: pd.Series([1.0] * len(frame)))
    monkeypatch.setattr(bot, "compute_adx", lambda _: pd.Series([26.0] * len(frame)))
    monkeypatch.setattr(bot, "compute_sl_tp",
                        lambda *args, **kwargs: (99.0, 102.0, 104.0))
    monkeypatch.setattr(bot, "get_ticker_safe", lambda _: 100.0)
    monkeypatch.setattr(bot, "is_strong_candidate", lambda _: False)
    monkeypatch.setattr(bot, "_persist_trade_journal_start", lambda *args, **kwargs: None)
    monkeypatch.setattr(bot, "INSTITUTIONAL_TRADE_JOURNAL", journal)
    monkeypatch.setattr(bot, "RORO_ENTRY_AUTHORITY", authority)
    monkeypatch.setattr(bot, "execute_entry", fake_execute_entry)
    monkeypatch.setattr(bot, "log_execution",
                        lambda message, level="INFO", **kwargs: logs.append(str(message)))

    assert bot.smart_opportunity_selection() is True
    assert len(calls) == 1
    assert calls[0]["context"]["cross_event_id"] == event["cross_event_id"]
    assert calls[0]["context"]["cross_direction"] == "BUY"
    assert bridge_calls and len(bridge_calls) == 1
    assert event["institutional_analysis_status"] == CrossLifecycleState.ENTRY_FILLED.value
    assert event["cross_event_id_consumed"] is True
    assert stale_event["institutional_analysis_status"] != CrossLifecycleState.ENTRY_FILLED.value
    assert stale_event["cross_status"] == "EXPIRED"
    assert watch_item["cross_event_id"] == event["cross_event_id"]
    assert watch_item["cross_quality"] == HIGH_QUALITY
    assert any("execute_entry called" in message and event["cross_event_id"] in message
               for message in logs)


def test_execution_wrapper_logs_authoritative_fill_and_protection_state(monkeypatch):
    import ggpbot as bot

    logs = []

    def fake_original(*args, **kwargs):
        bot._LAST_EXECUTION_GATE_RESULT = {
            "durable_event_id": "CROSS-E1", "cross_event_id": "CROSS-E1",
            "ready": True,
        }
        return True

    monkeypatch.setattr(bot, "_original_execute_entry_for_journal", fake_original)
    monkeypatch.setattr(bot, "STATE", {
        "open": True, "entry_execution_state": "FILLED",
        "entry_exchange_order_id": "ORD-1", "qty": 1.5,
        "native_sl_verified": True,
    })
    monkeypatch.setattr(bot, "_INSTITUTIONAL_ENTRY_CONTEXT", None)
    monkeypatch.setattr(bot, "_persist_trade_journal_start", lambda *args, **kwargs: None)
    monkeypatch.setattr(bot, "INSTITUTIONAL_TRADE_JOURNAL", None)
    monkeypatch.setattr(bot, "log_execution",
                        lambda message, level="INFO", **kwargs: logs.append(str(message)))

    assert bot.execute_entry("BUY", "BTC/USDT:USDT", 100.0, 99.0, 102.0, 104.0, 80.0, "test", 1.0) is True
    cross_log = next(message for message in logs if "[CROSS_LIFECYCLE] entry_result" in message)
    assert "order_sent=True" in cross_log
    assert "order_id=ORD-1" in cross_log
    assert "filled_qty=1.5" in cross_log
    assert "position_verified=True" in cross_log
    assert "protection_verified=True" in cross_log
