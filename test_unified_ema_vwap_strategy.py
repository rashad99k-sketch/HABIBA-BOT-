"""Acceptance tests for EMA9 x Session-VWAP entries inside causal zones."""
import logging

import numpy as np
import pandas as pd
import pytest

from unified_ema_vwap_strategy import (
    DIAGNOSTIC_TAGS,
    REJECT_CODES,
    Setup,
    UnifiedEMAVWAPPullbackEngine,
    _clean_closed,
    _find_causal_zone,
    assess_zone_quality,
    emit_entry_diagnostic,
    execution_handoff_allowed,
    find_causal_zones,
    indicator_series,
    indicators,
)

BASE_TS = 1_800_000_000_000
TF_MS = 900_000


def crossover_frame(side):
    """Deterministic closed-candle cross with a causal FVG at the cross price."""
    n = 70
    ts = BASE_TS + np.arange(n) * TF_MS
    o = np.full(n, 100.0)
    h = np.full(n, 100.1)
    low = np.full(n, 99.9)
    close = np.full(n, 100.0)
    volume = np.full(n, 10.0)

    # Session VWAP anchor: high-volume candle tunes VWAP without moving price far.
    close[55] = 100.25 if side == "BUY" else 99.75
    h[55] = max(100.1, close[55] + 0.1)
    low[55] = min(99.9, close[55] - 0.1)

    if side == "BUY":
        # Bullish FVG [99.80, 99.90] formed at index 63.
        o[61], h[61], low[61], close[61] = 99.8, 99.8, 99.7, 99.8
        o[62], h[62], low[62], close[62] = 99.75, 100.05, 99.70, 100.0
        o[63], h[63], low[63], close[63] = 100.0, 100.20, 99.90, 100.1
        for i in range(64, 69):
            o[i], h[i], low[i], close[i] = 100.0, 100.2, 100.0, 100.1
        # Final closed cross candle resolves inside the FVG.
        o[69], h[69], low[69], close[69] = 98.9, 99.95, 98.8, 99.85
    else:
        # Bearish FVG [100.10, 100.20] formed at index 63.
        o[61], h[61], low[61], close[61] = 100.2, 100.3, 100.2, 100.2
        o[62], h[62], low[62], close[62] = 100.25, 100.30, 99.95, 100.0
        o[63], h[63], low[63], close[63] = 100.0, 100.10, 99.80, 99.9
        for i in range(64, 69):
            o[i], h[i], low[i], close[i] = 100.0, 100.0, 99.8, 99.9
        o[69], h[69], low[69], close[69] = 101.1, 101.2, 100.05, 100.15

    volume[69] = 1000.0
    return pd.DataFrame({
        "timestamp": ts.astype(np.int64), "open": o, "high": h,
        "low": low, "close": close, "volume": volume,
    })


def fresh_engine(tmp_path, name="watch.json"):
    return UnifiedEMAVWAPPullbackEngine(str(tmp_path / name))


def seed_ready(engine, symbol, side="BUY", **overrides):
    label = "DEMAND_ZONE" if side == "BUY" else "SUPPLY_ZONE"
    ob = "BULLISH_ORDER_BLOCK" if side == "BUY" else "BEARISH_ORDER_BLOCK"
    fields = dict(
        symbol=symbol, status="ENTRY_READY", entry_status="NOT_READY",
        direction=side, crossover_ts=BASE_TS + 69 * TF_MS, crossover_index=69,
        crossover_price=100.0, ema9=100.0, ema15=100.0, ema50=100.0,
        vwap=99.9, atr14=1.0, causal_level=100.0,
        causal_zone_low=99.5, causal_zone_high=100.5,
        causal_zone_type="FVG", causal_zone_ts=BASE_TS + 63 * TF_MS,
        zone_label=label, zone_quality="FRESH_ZONE", order_block_type=ob,
        last_candle_ts=BASE_TS + 69 * TF_MS, data_source="TEST",
    )
    fields.update(overrides)
    engine._persist(Setup(**fields))
    return engine.get(symbol)


def ready_cross(tmp_path, side, name=None):
    engine = fresh_engine(tmp_path, name or f"{side.lower()}.json")
    frame = crossover_frame(side)
    result = engine.evaluate(side, frame, now_ms=int(frame.timestamp.iloc[-1] + TF_MS))
    assert result["status"] == "ENTRY_READY"
    return engine, frame, result


# 1) Session VWAP uses OHLC4 and resets at the UTC day boundary.
def test_session_vwap_uses_ohlc4_and_resets_by_utc_date():
    day = 1_800_000_000_000
    rows = []
    for day_offset in (0, 86_400_000):
        for k in range(30):
            rows.append({"timestamp": day + day_offset + k * TF_MS,
                         "open": 20.0, "high": 22.0, "low": 18.0,
                         "close": 21.0, "volume": 1.0})
    frame = pd.DataFrame(rows)
    got = indicators(frame)
    assert got["vwap"] == pytest.approx((20 + 22 + 18 + 21) / 4)
    assert got["timezone"] == "UTC"


# 2) EMA9 is derived from closed Close values, not OHLC4 or an unclosed bar.
def test_ema9_uses_closed_close_series_not_ohlc4():
    frame = crossover_frame("BUY")
    series = indicator_series(frame)
    expected = frame.close.ewm(span=9, adjust=False, min_periods=9).mean().iloc[-1]
    ohlc4 = frame[["open", "high", "low", "close"]].mean(axis=1)
    ohlc4_ema = ohlc4.ewm(span=9, adjust=False, min_periods=9).mean().iloc[-1]
    assert series["ema9"].iloc[-1] == pytest.approx(expected)
    assert abs(float(series["ema9"].iloc[-1] - ohlc4_ema)) > 1e-6


# 3) A forming-candle-only cross is rejected with INCOMPLETE_CANDLE.
def test_cross_on_unclosed_bar_is_never_promoted(tmp_path):
    frame = crossover_frame("BUY")
    frame.loc[69, ["open", "high", "low", "close", "volume"]] = [100.0, 100.1, 99.9, 100.0, 10.0]
    forming = frame.iloc[[-1]].copy()
    forming["timestamp"] = forming["timestamp"].iloc[0] + TF_MS
    forming.loc[forming.index[0], ["open", "high", "low", "close", "volume"]] = [
        98.9, 99.95, 98.8, 99.85, 1000.0]
    frame = pd.concat([frame, forming], ignore_index=True)
    now = int(frame.timestamp.iloc[-1] + TF_MS - 1)
    result = fresh_engine(tmp_path, "forming.json").evaluate("BUY", frame, now_ms=now)
    assert result["reason_code"] == "INCOMPLETE_CANDLE"
    assert result["crossover_ts"] is None


# 4) BUY cross inside a causal demand zone is authorized immediately.
def test_buy_cross_inside_demand_zone_is_ready(tmp_path):
    engine, frame, result = ready_cross(tmp_path, "BUY")
    assert result["direction"] == "BUY"
    assert result["zone_label"] == "DEMAND_ZONE"
    assert result["causal_zone_type"] == "FVG"
    assert result["crossover_ts"] == int(frame.timestamp.iloc[-1])
    assert result["zone_quality"] in {"FRESH_ZONE", "TESTED_ZONE", "VALID_ZONE"}


# 5) SELL cross inside a causal supply zone is authorized symmetrically.
def test_sell_cross_inside_supply_zone_is_ready(tmp_path):
    engine, frame, result = ready_cross(tmp_path, "SELL")
    assert result["direction"] == "SELL"
    assert result["zone_label"] == "SUPPLY_ZONE"
    assert result["causal_zone_type"] == "FVG"
    assert result["crossover_ts"] == int(frame.timestamp.iloc[-1])


# 6) EMA15 is observational and never a mandatory entry gate.
def test_ema15_does_not_gate_the_ema9_vwap_cross(tmp_path):
    engine, frame, result = ready_cross(tmp_path, "BUY")
    series = indicator_series(frame)
    assert float(series["ema9"].iloc[-1]) <= float(series["ema15"].iloc[-1])
    assert result["status"] == "ENTRY_READY"
    assert result["trend_evidence"]["ema15_alignment"] == "OBSERVATIONAL_ONLY"


# 7) No pullback, retest or rejection confirmation is required after the cross.
def test_no_extra_pullback_or_confirmation_gate(tmp_path):
    engine, frame, result = ready_cross(tmp_path, "BUY")
    assert result["rejection_ts"] is None
    assert result["confirmation_ts"] is None
    assert result["status"] == "ENTRY_READY"
    gate = engine.authorize("BUY", float(frame.close.iloc[-1]), 100.5,
                            side="BUY", now_ms=int(frame.timestamp.iloc[-1] + TF_MS))
    assert gate["allowed"] is True


# 8) Zone engine is causal: future candles cannot create or alter a zone.
def test_zone_detection_uses_only_causal_history():
    frame = crossover_frame("BUY")
    cross_index = len(frame) - 1
    zone = _find_causal_zone(frame, cross_index, "BUY", float(indicators(frame)["atr14"]))
    assert zone is not None
    assert zone["timestamp"] <= int(frame.timestamp.iloc[cross_index])
    mutated = frame.copy()
    future = pd.DataFrame([{
        "timestamp": int(frame.timestamp.iloc[-1]) + TF_MS,
        "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 1e9,
    }])
    with_future = pd.concat([mutated, future], ignore_index=True)
    assert find_causal_zones(with_future, cross_index, "BUY",
                             float(indicators(frame)["atr14"])) == find_causal_zones(
        frame, cross_index, "BUY", float(indicators(frame)["atr14"]))


# 9) All documented zone-quality states are classified from causal evidence.
def test_zone_quality_states_are_classified():
    frame = crossover_frame("BUY")
    atr = float(indicators(frame)["atr14"])
    price = float(frame.close.iloc[-1])
    base = {"low": 99.8, "high": 99.9, "index": 63,
            "timestamp": int(frame.timestamp.iloc[63]),
            "displacement_atr": 0.8, "zone_strength": 6.0}

    fresh = dict(base, index=68, low=100.1, high=100.3)
    assert assess_zone_quality(frame, 69, "BUY", atr, 100.2, fresh)["status"] == "FRESH_ZONE"
    tested = assess_zone_quality(frame, 69, "BUY", atr, price, base)
    assert tested["status"] == "TESTED_ZONE"
    weak = dict(base, displacement_atr=0.1)
    assert assess_zone_quality(frame, 69, "BUY", atr, price, weak)["status"] == "WEAK_ZONE"
    invalid_frame = frame.copy()
    invalid_frame.loc[69, ["open", "high", "low", "close"]] = [100.5, 100.6, 98.9, 99.0]
    invalid = dict(base, low=99.5, high=99.6)
    assert assess_zone_quality(invalid_frame, 69, "BUY", atr, 99.0, invalid)[
        "status"] == "INVALIDATED_ZONE"
    missed = dict(base, low=110.0, high=110.2)
    assert assess_zone_quality(frame, 69, "BUY", atr, price, missed)["status"] == "ENTRY_WINDOW_MISSED"


# 10) Stale market data is rejected and never authorized.
def test_stale_market_data_is_rejected(tmp_path):
    frame = crossover_frame("BUY")
    now = int(frame.timestamp.iloc[-1] + 2_000_000)
    result = fresh_engine(tmp_path, "stale.json").evaluate("BUY", frame, now_ms=now)
    assert result["reason_code"] == "STALE_MARKET_DATA"
    assert result["status"] == "REJECTED"


# 11) One closed candle can produce exactly one signal, including after restart.
def test_same_closed_candle_is_duplicate_safe(tmp_path):
    engine, frame, _ = ready_cross(tmp_path, "BUY", "duplicate.json")
    now = int(frame.timestamp.iloc[-1] + TF_MS)
    second = engine.evaluate("BUY", frame, now_ms=now)
    assert second["reason_code"] == "DUPLICATE_SIGNAL"
    restarted = UnifiedEMAVWAPPullbackEngine(str(tmp_path / "duplicate.json"))
    third = restarted.evaluate("BUY", frame, now_ms=now)
    assert third["reason_code"] == "DUPLICATE_SIGNAL"


# 12) A zone missed by price is rejected as ENTRY_WINDOW_MISSED, never chased.
def test_no_chase_outside_the_causal_zone(tmp_path):
    engine = fresh_engine(tmp_path, "chase.json")
    seed_ready(engine, "CHASE", "BUY")
    decision = engine.authorize("CHASE", 102.0, 103.0, side="BUY",
                                now_ms=BASE_TS + 70 * TF_MS)
    assert decision["reason_code"] == "ENTRY_WINDOW_MISSED"
    assert decision["allowed"] is False


# 13) Authorization is bound to symbol, side, price, target, zone and data age.
def test_authorization_is_exactly_bound_to_the_authorized_setup(tmp_path):
    engine = fresh_engine(tmp_path, "binding.json")
    seed_ready(engine, "BTC/USDT", "BUY", causal_zone_low=99.5,
               causal_zone_high=100.5, last_candle_ts=BASE_TS + 69 * TF_MS)
    now = BASE_TS + 70 * TF_MS
    assert engine.authorize("BTC/USDT", 100.0, 101.0, side="BUY", now_ms=now)["allowed"] is True
    engine = fresh_engine(tmp_path, "binding2.json")
    seed_ready(engine, "BTC/USDT", "BUY")
    assert engine.authorize("BTC/USDT", 100.0, 101.0, side="SELL", now_ms=now)[
        "reason_code"] == "WRONG_ZONE_DIRECTION"
    engine = fresh_engine(tmp_path, "binding3.json")
    seed_ready(engine, "BTC/USDT", "BUY", zone_label="SUPPLY_ZONE")
    assert engine.authorize("BTC/USDT", 100.0, 101.0, side="BUY", now_ms=now)[
        "reason_code"] == "WRONG_ZONE_DIRECTION"
    engine = fresh_engine(tmp_path, "binding4.json")
    seed_ready(engine, "BTC/USDT", "BUY")
    assert engine.authorize("BTC/USDT", 100.0, 101.0, side="BUY", now_ms=now + 2_000_000)[
        "reason_code"] == "STALE_MARKET_DATA"


# 14) Execution lifecycle emits the exact tags and records a one-time handoff.
def test_execution_lifecycle_and_exact_diagnostic_tags(tmp_path, caplog):
    with caplog.at_level(logging.INFO, logger="ema9_vwap_zone_entry"):
        engine, frame, result = ready_cross(tmp_path, "BUY", "lifecycle.json")
        gate = engine.authorize("BUY", float(frame.close.iloc[-1]), 100.5,
                                side="BUY", now_ms=int(frame.timestamp.iloc[-1] + TF_MS))
        assert gate["allowed"] is True
        assert engine.mark_executing("BUY") is True
        context = {"ema_vwap_authorized": True, "canonical_symbol": "BUY",
                   "side": "BUY", "entry_price": float(frame.close.iloc[-1]),
                   "tp1": 100.5}
        assert execution_handoff_allowed(
            engine, "BUY", "BUY", float(frame.close.iloc[-1]), 100.5, context)
        assert engine.mark_open("BUY") is True
        assert engine.mark_handoff("BUY", "handoff-1") is True
        assert engine.mark_handoff("BUY", "handoff-2") is False

    emitted = caplog.text
    # Rejection and Brain-classification tags are emitted on their own paths.
    success_tags = set(DIAGNOSTIC_TAGS) - {"[ENTRY_REJECTED]", "[TRADE_CLASSIFIED]"}
    for tag in success_tags:
        assert tag in emitted


# 15) Legacy/unconfirmed execution and all rejections use the exact code whitelist.
def test_all_reason_codes_are_in_the_exact_whitelist(tmp_path, caplog):
    assert {
        "NO_CROSS", "INVALID_ZONE", "WRONG_ZONE_DIRECTION", "STALE_MARKET_DATA",
        "INCOMPLETE_CANDLE", "DUPLICATE_SIGNAL", "ENTRY_WINDOW_MISSED",
        "EXECUTION_REJECTED", "POSITION_NOT_CONFIRMED", "TRADE_HANDOFF_FAILED",
    }.issubset(REJECT_CODES)
    engine = fresh_engine(tmp_path, "codes.json")
    seed_ready(engine, "X", "BUY", status="REJECTED", entry_status="BLOCKED")
    with caplog.at_level(logging.INFO, logger="ema9_vwap_zone_entry"):
        decision = engine.authorize("X", 100.0, 101.0, side="BUY",
                                    now_ms=BASE_TS + 70 * TF_MS)
        engine.mark_rejected_execution("X", "EXECUTION_REJECTED", "blocked")
    assert decision["reason_code"] in REJECT_CODES
    assert "[ENTRY_REJECTED]" in caplog.text


# 16) Causal FVG detection ignores future structure and keeps exact boundaries.
def test_causal_fvg_boundaries_are_exact_and_not_invented():
    frame = crossover_frame("BUY")
    zone = _find_causal_zone(frame, 69, "BUY", float(indicators(frame)["atr14"]))
    assert zone is not None
    assert zone["type"] == "FVG"
    assert zone["low"] == pytest.approx(99.8)
    assert zone["high"] == pytest.approx(99.9)
    assert zone["timestamp"] == int(frame.timestamp.iloc[63])
