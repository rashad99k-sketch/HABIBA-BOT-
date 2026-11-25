import numpy as np
import pandas as pd

from roro_confluence import (
    AUTH_EMA9_VWAP_TRIGGER_VALID,
    validate_ema9_vwap_entry_gate,
)


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


def _ema9_cross(values, period, side="BUY"):
    if period == 9:
        out = pd.Series(99.8 if side == "BUY" else 100.2, index=values.index, dtype=float)
        out.iloc[-1] = 100.2 if side == "BUY" else 99.8
        return out
    if period == 50:
        return pd.Series(99.8 if side == "BUY" else 100.2, index=values.index, dtype=float)
    raise AssertionError(f"EMA{period} must not be requested by the EMA9/VWAP gate")


def test_bullish_closed_ema9_session_vwap_cross_authorizes_buy():
    frame, now_ms = _frame()
    periods = []

    def ema(values, period):
        periods.append(period)
        return _ema9_cross(values, period, "BUY")

    result = validate_ema9_vwap_entry_gate(frame, "BUY", ema, 900, now_ms)
    assert result["ready"] is True
    assert result["trigger"] == "EMA9_SESSION_VWAP"
    assert result["authorization_state"] == AUTH_EMA9_VWAP_TRIGGER_VALID
    assert result["ema9_vwap_cross"] == "BULLISH"
    assert result["ema9_cross"] == "BULLISH"
    assert result["ema9_15_cross"] is None
    assert result["ema9"] > result["vwap"]
    assert 15 not in periods
    assert set(periods).issubset({9, 50})


def test_bearish_closed_ema9_session_vwap_cross_authorizes_sell():
    frame, now_ms = _frame()

    result = validate_ema9_vwap_entry_gate(
        frame, "SELL", lambda values, period: _ema9_cross(values, period, "SELL"),
        900, now_ms,
    )
    assert result["ready"] is True
    assert result["authorization_state"] == AUTH_EMA9_VWAP_TRIGGER_VALID
    assert result["ema9_vwap_cross"] == "BEARISH"
    assert result["ema9"] < result["vwap"]


def test_ema9_above_vwap_without_fresh_cross_is_not_authorized():
    frame, now_ms = _frame()

    def no_cross(values, period):
        return pd.Series(100.2 if period == 9 else 99.8, index=values.index, dtype=float)

    result = validate_ema9_vwap_entry_gate(frame, "BUY", no_cross, 900, now_ms)
    assert result["ready"] is False
    assert result["reason"] == "missing_fresh_directional_ema9_vwap_cross"
    assert result["authorization_state"] != AUTH_EMA9_VWAP_TRIGGER_VALID


def test_touching_vwap_is_not_a_directional_cross():
    frame, now_ms = _frame()

    def touch(values, period):
        out = pd.Series(99.8, index=values.index, dtype=float)
        if period == 9:
            out.iloc[-1] = 100.0
        return out

    result = validate_ema9_vwap_entry_gate(frame, "BUY", touch, 900, now_ms)
    assert result["ready"] is False
    assert result["reason"] in {
        "missing_fresh_directional_ema9_vwap_cross",
        "latest_closed_ema9_vwap_relationship_not_directional",
    }
    assert result["authorization_state"] != AUTH_EMA9_VWAP_TRIGGER_VALID


def test_ema50_cross_alone_never_authorizes_entry():
    frame, now_ms = _frame()

    def ema50_only(values, period):
        if period == 9:
            return pd.Series(100.2, index=values.index, dtype=float)
        out = pd.Series(99.8, index=values.index, dtype=float)
        out.iloc[-1] = 100.2
        return out

    result = validate_ema9_vwap_entry_gate(frame, "BUY", ema50_only, 900, now_ms)
    assert result["ready"] is False
    assert result["ema50_cross"] == "BULLISH"
    assert result["authorization_state"] != AUTH_EMA9_VWAP_TRIGGER_VALID


def test_ema50_opposition_is_advisory_and_does_not_veto_ema9_vwap_cross():
    frame, now_ms = _frame()

    def opposed_ema50(values, period):
        if period == 9:
            return _ema9_cross(values, period, "BUY")
        return pd.Series(np.linspace(101.0, 100.5, len(values)), index=values.index)

    result = validate_ema9_vwap_entry_gate(frame, "BUY", opposed_ema50, 900, now_ms)
    assert result["ready"] is True
    assert result["authorization_state"] == AUTH_EMA9_VWAP_TRIGGER_VALID
    assert result["ema50_alignment"] == "OPPOSED"


def test_gate_never_calculates_ema15():
    frame, now_ms = _frame()
    requested = []

    def ema(values, period):
        requested.append(period)
        if period == 9:
            return _ema9_cross(values, period, "BUY")
        if period == 50:
            return pd.Series(99.8, index=values.index, dtype=float)
        raise AssertionError(f"Unexpected EMA period: {period}")

    result = validate_ema9_vwap_entry_gate(frame, "BUY", ema, 900, now_ms)
    assert result["ready"] is True
    assert 15 not in requested


def test_forming_candle_cannot_supply_the_cross():
    frame, _ = _frame()

    def cross_only_on_full_frame(values, period):
        if period == 9 and len(values) == len(frame):
            return _ema9_cross(values, period, "BUY")
        return pd.Series(100.2 if period == 9 else 99.8, index=values.index, dtype=float)

    result = validate_ema9_vwap_entry_gate(
        frame, "BUY", cross_only_on_full_frame, 900,
        int(frame.timestamp.iloc[-1]),
    )
    assert result["ready"] is False
    assert result["latest_closed_timestamp"] == int(frame.timestamp.iloc[-2])
    assert result["authorization_state"] != AUTH_EMA9_VWAP_TRIGGER_VALID


def test_stale_closed_candle_data_is_rejected():
    frame, now_ms = _frame()
    result = validate_ema9_vwap_entry_gate(
        frame, "BUY", lambda values, period: _ema9_cross(values, period),
        900, now_ms + 4 * 900_000,
    )
    assert result["ready"] is False
    assert result["reason"] == "stale_closed_candle_data"
    assert result["data_age_seconds"] > result["max_data_age_seconds"]


def test_invalid_ohlcv_fails_closed():
    frame, now_ms = _frame()
    frame.loc[5, "close"] = np.nan
    result = validate_ema9_vwap_entry_gate(
        frame, "BUY", lambda values, period: _ema9_cross(values, period),
        900, now_ms,
    )
    assert result["ready"] is False
    assert result["reason"] == "invalid_ohlcv"
    assert result["authorization_state"] != AUTH_EMA9_VWAP_TRIGGER_VALID
