"""Fail-closed authorization for the immediate closed-candle EMA9/Session VWAP entry."""

from math import isfinite
from typing import Any, Dict, Iterable, Tuple


def validate_ema9_vwap_cross_authorization(
    *,
    side: str,
    setup_type: str,
    decision_state: Any,
    event_id: Any,
    evidence: Iterable[Any],
    adx: Any,
    gate: Dict[str, Any],
    candle_source: Any,
    cross_timestamp: Any = None,
) -> Tuple[bool, str]:
    """Authorize only the unified-watchlist closed-candle cross event.

    This setup intentionally enters on the confirmed cross; pullback/retest and
    liquidity decisions belong to post-entry management, not a pre-entry veto.
    """
    direction = str(side or "").strip().upper()
    if direction not in {"BUY", "SELL"}:
        return False, "invalid_side"
    if str(setup_type or "").strip().upper() != "EMA9_VWAP_CROSS":
        return False, "unsupported_setup_type"
    if str(decision_state or "").strip().upper() != "ENTRY_READY":
        return False, "decision_not_entry_ready"
    source = str(candle_source or "").strip()
    if source != "UNIFIED_WATCHLIST_AUTHORIZED_FRAME":
        return False, "unauthorized_candle_source"
    gate = gate if isinstance(gate, dict) else {}
    if gate.get("ready") is not True:
        return False, "closed_ema9_vwap_gate_not_ready"
    if str(gate.get("frame_source") or "").strip() != source:
        return False, "gate_frame_source_mismatch"
    expected_cross = "BULLISH" if direction == "BUY" else "BEARISH"
    if str(gate.get("ema9_cross") or "").strip().upper() != expected_cross:
        return False, "cross_direction_mismatch"
    signal_id = str(gate.get("signal_candle_id") or "").strip()
    if not signal_id.startswith(f"{direction}:"):
        return False, "signal_candle_identity_invalid"
    gate_cross_ts = gate.get("ema9_cross_timestamp")
    try:
        gate_cross_ts = int(gate_cross_ts)
    except (TypeError, ValueError, OverflowError):
        return False, "cross_timestamp_missing"
    if gate_cross_ts <= 0:
        return False, "cross_timestamp_invalid"
    if cross_timestamp is not None:
        try:
            if int(cross_timestamp) != gate_cross_ts:
                return False, "cross_timestamp_mismatch"
        except (TypeError, ValueError, OverflowError):
            return False, "context_cross_timestamp_invalid"
    durable_event_id = str(event_id or "").strip()
    if not durable_event_id or not durable_event_id.endswith(signal_id):
        return False, "durable_event_identity_mismatch"
    normalized_evidence = [str(item or "").strip().lower() for item in evidence]
    if "closed_ema9_session_vwap_cross" not in normalized_evidence:
        return False, "closed_cross_evidence_missing"
    try:
        adx_value = float(adx)
    except (TypeError, ValueError, OverflowError):
        return False, "adx_unavailable"
    if not isfinite(adx_value) or adx_value < 18.0:
        return False, "adx_below_18_or_invalid"
    return True, "authorized_closed_ema9_session_vwap_cross"
