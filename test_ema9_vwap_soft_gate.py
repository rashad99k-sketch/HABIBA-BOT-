import numpy as np
import pandas as pd

from roro_confluence import AUTH_EMA9_EMA15_TRIGGER_VALID, validate_dual_ema_vwap_entry_gate


def _frame(count=80):
    start = 1795132800000  # 2026-11-20 00:00 UTC
    rows = []
    for i in range(count):
        rows.append({
            "timestamp": start + i * 900_000,
            "open": 100.0,
            "high": 100.2,
            "low": 99.8,
            "close": 100.0,
            "volume": 1000.0,
        })
    frame = pd.DataFrame(rows)
    now_ms = int(frame.timestamp.iloc[-1] + 900_000 + 1_000)
    return frame, now_ms


def _bullish_ema9_ema15_cross(values, period):
    """EMA9 crosses above EMA15 on the last closed candle; VWAP is 100.0."""
    if period == 9:
        out = pd.Series(99.8, index=values.index, dtype=float)
        out.iloc[-1] = 100.2
        return out
    if period == 15:
        return pd.Series(100.0, index=values.index, dtype=float)
    if period == 50:
        # No EMA50/VWAP cross: EMA50 stays below VWAP on every closed candle.
        return pd.Series(99.8, index=values.index, dtype=float)
    raise AssertionError(f"unexpected EMA period {period}")


def _bearish_ema9_ema15_cross(values, period):
    """Mirror image of the bullish trigger."""
    if period == 9:
        out = pd.Series(100.2, index=values.index, dtype=float)
        out.iloc[-1] = 99.8
        return out
    if period == 15:
        return pd.Series(100.0, index=values.index, dtype=float)
    if period == 50:
        return pd.Series(100.2, index=values.index, dtype=float)
    raise AssertionError(f"unexpected EMA period {period}")


def test_bullish_ema9_crosses_above_ema15_with_vwap_confirmation_passes():
    frame, now_ms = _frame()
    result = validate_dual_ema_vwap_entry_gate(
        frame, "BUY", _bullish_ema9_ema15_cross, timeframe_seconds=900, now_ms=now_ms,
    )
    assert result["ready"] is True
    assert result["trigger"] == "EMA9_EMA15"
    assert result["authorization_state"] == AUTH_EMA9_EMA15_TRIGGER_VALID
    assert result["reason"] == "fresh_closed_ema9_ema15_cross_vwap_confirmed_ema50_advisory"
    assert result["vwap_confirmation"] == "CONFIRMED"
    assert result["ema9"] == 100.2
    assert result["ema15"] == 100.0
    assert result["ema9_cross"] == "BULLISH"
    assert result["ema9_15_cross"] == "BULLISH"
    assert result["ema9_cross_timestamp"] == int(frame.timestamp.iloc[-1])
    assert result["ema9_15_cross_timestamp"] == result["ema9_cross_timestamp"]
    assert result["signal_candle_timestamp"] == result["ema9_15_cross_timestamp"]
    assert result["signal_candle_id"] == f"BUY:{result['signal_candle_timestamp']}"
    assert result["ema50_cross"] is None
    assert result["cross_quality"] == "UNRATED_PENDING_CALIBRATION"


def test_bearish_ema9_crosses_below_ema15_with_vwap_confirmation_passes():
    frame, now_ms = _frame()
    result = validate_dual_ema_vwap_entry_gate(
        frame, "SELL", _bearish_ema9_ema15_cross, timeframe_seconds=900, now_ms=now_ms,
    )
    assert result["ready"] is True
    assert result["authorization_state"] == AUTH_EMA9_EMA15_TRIGGER_VALID
    assert result["ema9"] == 99.8
    assert result["ema15"] == 100.0
    assert result["ema9_15_cross"] == "BEARISH"
    assert result["ema9_cross"] == "BEARISH"
    assert result["vwap_confirmation"] == "CONFIRMED"
    assert result["signal_candle_id"].startswith("SELL:")
    assert result["quality_features"]["ema9_ema15_gap_atr"] > 0


def test_ema9_ema15_cross_below_vwap_is_rejected():
    frame, now_ms = _frame()

    def below_vwap(values, period):
        if period == 9:
            out = pd.Series(99.6, index=values.index, dtype=float)
            out.iloc[-1] = 99.9  # above EMA15 (99.8) but still below VWAP (100.0)
            return out
        if period == 15:
            return pd.Series(99.8, index=values.index, dtype=float)
        return pd.Series(99.6, index=values.index, dtype=float)

    result = validate_dual_ema_vwap_entry_gate(
        frame, "BUY", below_vwap, timeframe_seconds=900, now_ms=now_ms,
    )
    assert result["ready"] is False
    assert result["reason"] == "latest_closed_ema9_vwap_relationship_not_directional"
    assert result["ema9_15_cross"] is None
    assert result["authorization_state"] != AUTH_EMA9_EMA15_TRIGGER_VALID


def test_vwap_relationship_equal_is_not_directional():
    frame, now_ms = _frame()

    def touching_vwap(values, period):
        if period == 9:
            out = pd.Series(99.6, index=values.index, dtype=float)
            out.iloc[-1] = 100.0  # EMA9 crosses EMA15 but only touches VWAP
            return out
        if period == 15:
            return pd.Series(99.8, index=values.index, dtype=float)
        return pd.Series(99.6, index=values.index, dtype=float)

    result = validate_dual_ema_vwap_entry_gate(
        frame, "BUY", touching_vwap, timeframe_seconds=900, now_ms=now_ms,
    )
    assert result["ready"] is False
    assert result["reason"] == "latest_closed_ema9_vwap_relationship_not_directional"
    assert result["vwap_confirmation"] == "NOT_CONFIRMED"
    assert result["authorization_state"] != AUTH_EMA9_EMA15_TRIGGER_VALID


def test_cross_candle_without_vwap_confirmation_is_rejected_even_when_latest_is_directional():
    frame, now_ms = _frame()

    def late_vwap_alignment(values, period):
        if period == 9:
            out = pd.Series(99.0, index=values.index, dtype=float)
            out.iloc[-2] = 99.8   # EMA9 crosses above EMA15 (99.5) but below VWAP (100.0)
            out.iloc[-1] = 100.2  # latest closed candle is above VWAP
            return out
        if period == 15:
            return pd.Series(99.5, index=values.index, dtype=float)
        return pd.Series(99.0, index=values.index, dtype=float)

    result = validate_dual_ema_vwap_entry_gate(
        frame, "BUY", late_vwap_alignment, timeframe_seconds=900, now_ms=now_ms,
    )
    assert result["ready"] is False
    assert result["reason"] == "cross_candle_vwap_direction_not_confirmed"
    assert result["vwap_confirmation"] == "NOT_CONFIRMED"
    assert result["authorization_state"] != AUTH_EMA9_EMA15_TRIGGER_VALID


def test_touch_without_strict_ema9_ema15_cross_is_not_authorized():
    frame, now_ms = _frame()

    def touching(values, period):
        if period == 9:
            out = pd.Series(99.8, index=values.index, dtype=float)
            out.iloc[-1] = 100.2
            return out
        if period == 15:
            out = pd.Series(100.0, index=values.index, dtype=float)
            out.iloc[-1] = 100.2  # equality, not a strict cross
            return out
        return pd.Series(99.8, index=values.index, dtype=float)

    result = validate_dual_ema_vwap_entry_gate(
        frame, "BUY", touching, timeframe_seconds=900, now_ms=now_ms,
    )
    assert result["ready"] is False
    assert result["reason"] == "missing_fresh_directional_ema9_ema15_cross"
    assert result["ema9_15_cross"] is None


def test_ema50_only_cross_never_passes():
    frame, now_ms = _frame()

    def ema50_only(values, period):
        if period == 9:
            return pd.Series(100.2, index=values.index, dtype=float)
        if period == 15:
            return pd.Series(100.2, index=values.index, dtype=float)
        out = pd.Series(99.8, index=values.index, dtype=float)
        out.iloc[-1] = 100.2  # EMA50 crosses VWAP; EMA9/EMA15 never cross
        return out

    result = validate_dual_ema_vwap_entry_gate(
        frame, "BUY", ema50_only, timeframe_seconds=900, now_ms=now_ms,
    )
    assert result["ready"] is False
    assert result["reason"] == "missing_fresh_directional_ema9_ema15_cross"
    assert result["ema9_15_cross"] is None
    assert result["ema50_cross"] == "BULLISH"
    assert result["authorization_state"] != AUTH_EMA9_EMA15_TRIGGER_VALID


def test_strongly_opposed_ema50_is_advisory_and_never_vetoes():
    frame, now_ms = _frame()

    def opposed_ema50(values, period):
        if period == 9:
            return _bullish_ema9_ema15_cross(values, period)
        if period == 15:
            return _bullish_ema9_ema15_cross(values, period)
        return pd.Series(np.linspace(101.0, 100.5, len(values)), index=values.index)

    result = validate_dual_ema_vwap_entry_gate(
        frame, "BUY", opposed_ema50, timeframe_seconds=900, now_ms=now_ms,
    )
    assert result["ready"] is True
    assert result["authorization_state"] == AUTH_EMA9_EMA15_TRIGGER_VALID
    assert result["ema50_alignment"] == "OPPOSED"
    assert result["ema50_distance_atr"] > 0
    assert result["quality_features"]["ema50_alignment"] == "OPPOSED"


def test_ema50_unavailable_does_not_block_valid_ema9_ema15_cross():
    frame, now_ms = _frame(count=35)

    def ema9_ema15_only(values, period):
        assert period in (9, 15)
        if period == 9:
            out = pd.Series(99.8, index=values.index, dtype=float)
            out.iloc[-1] = 100.2
            return out
        return pd.Series(100.0, index=values.index, dtype=float)

    result = validate_dual_ema_vwap_entry_gate(
        frame, "BUY", ema9_ema15_only, timeframe_seconds=900, now_ms=now_ms,
    )
    assert result["ready"] is True
    assert result["ema50"] is None
    assert result["ema50_cross"] is None
    assert result["ema50_alignment"] == "UNAVAILABLE"


def test_stale_closed_candle_data_is_rejected():
    frame, now_ms = _frame()
    result = validate_dual_ema_vwap_entry_gate(
        frame, "BUY", _bullish_ema9_ema15_cross, timeframe_seconds=900,
        now_ms=now_ms + 4 * 900_000,
    )
    assert result["ready"] is False
    assert result["reason"] == "stale_closed_candle_data"
    assert result["data_age_seconds"] > result["max_data_age_seconds"]


def test_forming_candle_cannot_supply_the_cross():
    frame, now_ms = _frame()

    def cross_only_when_latest_bar_is_included(values, period):
        if len(values) != 80:
            return pd.Series(100.2, index=values.index, dtype=float)
        return _bullish_ema9_ema15_cross(values, period)

    # now_ms equals the last candle's open time, so that candle is still forming.
    result = validate_dual_ema_vwap_entry_gate(
        frame, "BUY", cross_only_when_latest_bar_is_included,
        timeframe_seconds=900, now_ms=int(frame.timestamp.iloc[-1]),
    )
    assert result["ready"] is False
    assert result["reason"] == "missing_fresh_directional_ema9_ema15_cross"
    assert result["latest_closed_timestamp"] == int(frame.timestamp.iloc[-2])
    assert result["authorization_state"] != AUTH_EMA9_EMA15_TRIGGER_VALID


def test_cross_candle_age_is_checked_separately_from_latest_data_age():
    frame, now_ms = _frame()

    def older_cross(values, period):
        if period == 9:
            out = pd.Series(99.8, index=values.index, dtype=float)
            out.iloc[-2:] = 100.2
            return out
        if period == 15:
            return pd.Series(100.0, index=values.index, dtype=float)
        return pd.Series(99.8, index=values.index, dtype=float)

    # The latest closed candle is only 20 minutes old (within the 30-minute
    # data freshness limit), but the cross candle itself is 35 minutes old.
    result = validate_dual_ema_vwap_entry_gate(
        frame, "BUY", older_cross, timeframe_seconds=900,
        now_ms=now_ms + 20 * 60_000,
    )
    assert result["ready"] is False
    assert result["reason"] == "stale_ema9_ema15_cross"
    assert result["data_age_seconds"] <= result["max_data_age_seconds"]
    assert result["cross_age_seconds"] > result["max_data_age_seconds"]


def test_invalid_ohlcv_fails_closed():
    frame, now_ms = _frame()
    frame.loc[5, "close"] = np.nan
    result = validate_dual_ema_vwap_entry_gate(
        frame, "BUY", _bullish_ema9_ema15_cross, timeframe_seconds=900, now_ms=now_ms,
    )
    assert result["ready"] is False
    assert result["reason"] == "invalid_ohlcv"
    assert result["authorization_state"] != AUTH_EMA9_EMA15_TRIGGER_VALID
