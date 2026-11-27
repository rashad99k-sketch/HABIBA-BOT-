import pytest

from ggp_entry_authorization import validate_ema9_vwap_cross_authorization


def valid_context(side="BUY"):
    signal_id = f"{side}:1795744800000"
    return {
        "side": side,
        "setup_type": "EMA9_VWAP_CROSS",
        "decision_state": "ENTRY_READY",
        "event_id": f"MASK/USDT:USDT:15m:{signal_id}",
        "evidence": [
            "closed_ema9_session_vwap_cross",
            "adx_at_entry:22.40",
            "cross_candle:1795744800000",
            "post_entry_management_owns_retest_pullback_and_liquidity_decisions",
        ],
        "adx": 22.4,
        "gate": {
            "ready": True,
            "frame_source": "UNIFIED_WATCHLIST_AUTHORIZED_FRAME",
            "ema9_cross": "BULLISH" if side == "BUY" else "BEARISH",
            "signal_candle_id": signal_id,
            "ema9_cross_timestamp": 1795744800000,
        },
        "candle_source": "UNIFIED_WATCHLIST_AUTHORIZED_FRAME",
        "cross_timestamp": 1795744800000,
    }


@pytest.mark.parametrize("side", ["BUY", "SELL"])
def test_immediate_ema9_vwap_cross_authorizes_without_pre_entry_retest(side):
    ctx = valid_context(side)
    ok, reason = validate_ema9_vwap_cross_authorization(
        side=ctx["side"], setup_type=ctx["setup_type"],
        decision_state=ctx["decision_state"], event_id=ctx["event_id"],
        evidence=ctx["evidence"], adx=ctx["adx"], gate=ctx["gate"],
        candle_source=ctx["candle_source"], cross_timestamp=ctx["cross_timestamp"],
    )
    assert ok is True
    assert reason == "authorized_closed_ema9_session_vwap_cross"


@pytest.mark.parametrize(("field", "value", "expected"), [
    ("decision_state", "WAITING_FOR_RETEST", "decision_not_entry_ready"),
    ("candle_source", "UNVERIFIED_FRAME", "unauthorized_candle_source"),
    ("adx", 17.99, "adx_below_18_or_invalid"),
    ("event_id", "MASK:old-event", "durable_event_identity_mismatch"),
])
def test_ema9_vwap_cross_authorization_fails_closed(field, value, expected):
    ctx = valid_context()
    ctx[field] = value
    ok, reason = validate_ema9_vwap_cross_authorization(
        side=ctx["side"], setup_type=ctx["setup_type"],
        decision_state=ctx["decision_state"], event_id=ctx["event_id"],
        evidence=ctx["evidence"], adx=ctx["adx"], gate=ctx["gate"],
        candle_source=ctx["candle_source"], cross_timestamp=ctx["cross_timestamp"],
    )
    assert ok is False
    assert reason == expected


def test_ema9_vwap_cross_authorization_rejects_wrong_direction_or_missing_evidence():
    ctx = valid_context("BUY")
    ctx["gate"]["ema9_cross"] = "BEARISH"
    ok, reason = validate_ema9_vwap_cross_authorization(
        side=ctx["side"], setup_type=ctx["setup_type"],
        decision_state=ctx["decision_state"], event_id=ctx["event_id"],
        evidence=ctx["evidence"], adx=ctx["adx"], gate=ctx["gate"],
        candle_source=ctx["candle_source"], cross_timestamp=ctx["cross_timestamp"],
    )
    assert not ok and reason == "cross_direction_mismatch"
    ctx = valid_context("BUY")
    ctx["evidence"] = ["adx_at_entry:22.4"]
    ok, reason = validate_ema9_vwap_cross_authorization(
        side=ctx["side"], setup_type=ctx["setup_type"],
        decision_state=ctx["decision_state"], event_id=ctx["event_id"],
        evidence=ctx["evidence"], adx=ctx["adx"], gate=ctx["gate"],
        candle_source=ctx["candle_source"], cross_timestamp=ctx["cross_timestamp"],
    )
    assert not ok and reason == "closed_cross_evidence_missing"


def test_ema9_vwap_cross_authorization_rejects_timestamp_mismatch_inside_signal_identity():
    ctx = valid_context("BUY")
    ctx["gate"]["signal_candle_id"] = "BUY:1795744800001"
    ok, reason = validate_ema9_vwap_cross_authorization(
        side=ctx["side"], setup_type=ctx["setup_type"],
        decision_state=ctx["decision_state"], event_id=ctx["event_id"],
        evidence=ctx["evidence"], adx=ctx["adx"], gate=ctx["gate"],
        candle_source=ctx["candle_source"], cross_timestamp=ctx["cross_timestamp"],
    )
    assert ok is False
    assert reason == "signal_candle_timestamp_mismatch"
