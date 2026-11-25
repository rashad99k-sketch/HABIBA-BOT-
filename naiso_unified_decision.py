"""Read-only unified projection for Scanner -> Watchlist -> Decision -> Entry Gate.

This module is deliberately analysis-only. It does not create orders, indicators,
API calls, snapshots, journals, or a second decision authority. It only makes the
existing evidence explicit and separates states that were previously collapsed
under ambiguous labels such as ``WAITING_FOR_CONFIRMATION`` and ``Confluence``.

All inputs are expected to come from already validated closed-candle sources:
the durable cross journal, the unified watchlist, and the existing proximity
ranking feed. A ranking-only row can be classified, but it never becomes a
durable cross event and never produces ``conditions_complete=True``.
"""
from __future__ import annotations

import hashlib
import json
import math
import time
from typing import Any, Dict, Iterable, List, Mapping, Optional

from ggp_market_intelligence import normalize_entry_type
from ggp_institutional_strategy import build_institutional_report
from roro_confluence import AUTH_EMA9_EMA15_TRIGGER_VALID

SCHEMA_VERSION = "NAISO_UNIFIED_DECISION_CONTEXT_V1"

CROSS_STATES = (
    "CONFIRMED_BUY_CROSS",
    "CONFIRMED_SELL_CROSS",
    "CONVERGING",
    "ALIGNED_NOT_CROSSING",
    "NO_CROSS",
    "INVALID_DATA",
    "CROSS_ALREADY_CONSUMED",
)
CROSS_LIFECYCLE_STATES = ("ACTIVE", "ENTERED", "INVALIDATED", "EXPIRED")
CROSS_PHASES = ("WAITING_FOR_PULLBACK", "PULLBACK_CONFIRMED", "REJECTION_RECORDED", "BREAKOUT_CONFIRMED")

# Entry type is a dashboard-facing projection of the one canonical classifier in
# ggp_market_intelligence. These aliases are the only accepted display names.
ENTRY_TYPES = ("IMMEDIATE_ENTRY", "MICRO_PULLBACK", "PULLBACK", "RETEST")
WATCHLIST_LIFECYCLE_STATES = (
    "CROSS_DETECTED", "ACTIVE_WATCHLIST", "INSTITUTIONAL_ANALYSIS",
    "READY_FOR_IMMEDIATE_ENTRY", "WAITING_FOR_MICRO_PULLBACK",
    "WAITING_FOR_PULLBACK", "WAITING_FOR_RETEST", "INVALIDATED",
    "EXPIRED", "ENTERED",
)
_ENTRY_TYPE_LIFECYCLE = {
    "IMMEDIATE_ENTRY": "READY_FOR_IMMEDIATE_ENTRY",
    "MICRO_PULLBACK": "WAITING_FOR_MICRO_PULLBACK",
    "PULLBACK": "WAITING_FOR_PULLBACK",
    "RETEST": "WAITING_FOR_RETEST",
}

DECISION_STATES = (
    "ENTRY_READY",
    "WAITING_FOR_CROSS",
    "WAITING_FOR_PULLBACK",
    "WAITING_FOR_RETEST",
    "WAITING_FOR_LIQUIDITY_CONFIRMATION",
    "WAITING_FOR_MSS",
    "WAITING_FOR_REJECTION_CANDLE",
    "WAITING_FOR_BREAK_CONFIRMATION",
    "BLOCKED_INVALID_ZONE",
    "BLOCKED_MARKET_CHOP",
    "BLOCKED_CONFLICTING_CONTEXT",
    "REJECTED_INVALIDATED_SETUP",
    "EXPIRED",
    "DATA_STALE",
    "DATA_INCOMPLETE",
)

LIQUIDITY_STATES = (
    "LIQUIDITY_SWEEP",
    "LIQUIDITY_GRAB",
    "FAILED_AUCTION",
    "RECLAIM",
    "NO_LIQUIDITY_EVENT",
)
LIQUIDITY_STAGES = ("DETECTED", "CONFIRMED", "RECLAIMED", "PENDING", "INVALIDATED")

RANKING_ONLY_ROLE = "RANKING_ONLY_NOT_ENTRY_GATE"
CONFIRMED_CROSS_ROLE = "CONFIRMED_CROSS_EVENT"
CONTEXT_ONLY_ROLE = "CONTEXT_ONLY_NOT_AUTHORITY"

REQUIRED_NEXT_EVENT = {
    "ENTRY_READY": "NONE_ALL_CONDITIONS_COMPLETE",
    "WAITING_FOR_CROSS": "CLOSED_DIRECTIONAL_EMA9_VWAP_CROSS",
    "WAITING_FOR_PULLBACK": "CLOSED_POST_CROSS_PULLBACK",
    "WAITING_FOR_RETEST": "CLOSED_INSTITUTIONAL_ZONE_RETEST",
    "WAITING_FOR_LIQUIDITY_CONFIRMATION": "CLOSED_LIQUIDITY_EVENT_CONFIRMATION",
    "WAITING_FOR_MSS": "CLOSED_MSS_OR_CHOCH_CONFIRMATION",
    "WAITING_FOR_REJECTION_CANDLE": "CLOSED_REJECTION_CANDLE",
    "WAITING_FOR_BREAK_CONFIRMATION": "CLOSED_REJECTION_LEVEL_BREAK",
    "BLOCKED_INVALID_ZONE": "VALID_NEARBY_CAUSAL_ZONE_REQUIRED",
    "BLOCKED_MARKET_CHOP": "TRENDING_REGIME_CONFIRMATION_REQUIRED",
    "BLOCKED_CONFLICTING_CONTEXT": "OPPOSING_HYPOTHESES_RESOLUTION_REQUIRED",
    "REJECTED_INVALIDATED_SETUP": "NEW_SETUP_REQUIRED",
    "EXPIRED": "NEW_SETUP_REQUIRED",
    "DATA_STALE": "FRESH_CLOSED_CANDLE_FRAME_REQUIRED",
    "DATA_INCOMPLETE": "COMPLETE_CLOSED_CANDLE_FRAME_REQUIRED",
}

SETUP_STAGE = {
    "ENTRY_READY": "CONDITIONS_COMPLETE",
    "WAITING_FOR_CROSS": "AWAIT_CROSS",
    "WAITING_FOR_PULLBACK": "AWAIT_PULLBACK",
    "WAITING_FOR_RETEST": "AWAIT_ZONE_RETEST",
    "WAITING_FOR_LIQUIDITY_CONFIRMATION": "AWAIT_LIQUIDITY",
    "WAITING_FOR_MSS": "AWAIT_STRUCTURE_SHIFT",
    "WAITING_FOR_REJECTION_CANDLE": "AWAIT_REJECTION",
    "WAITING_FOR_BREAK_CONFIRMATION": "AWAIT_BREAKOUT",
    "BLOCKED_INVALID_ZONE": "ZONE_BLOCKED",
    "BLOCKED_MARKET_CHOP": "REGIME_BLOCKED",
    "BLOCKED_CONFLICTING_CONTEXT": "CONTEXT_CONFLICT",
    "REJECTED_INVALIDATED_SETUP": "SETUP_INVALIDATED",
    "EXPIRED": "SETUP_EXPIRED",
    "DATA_STALE": "DATA_STALE",
    "DATA_INCOMPLETE": "DATA_INCOMPLETE",
}

_INVALID_DATA_TOKENS = (
    "invalid_ohlcv", "insufficient", "stale", "indicator_failed", "indicator_calculation_failed",
    "session_vwap_unavailable", "atr_unavailable", "invalid_data", "data_unavailable",
    "unfinished", "forming", "intrabar", "missing_closed",
)
_CHOP_TOKENS = ("chop", "range_bound", "rangebound", "sideways", "mixed_swings", "transition")
_INVALID_ZONE_TOKENS = (
    "invalid_zone", "zone_invalid", "order_block_invalid", "fvg_invalid",
    "no_actionable_zone", "entry_window_missed", "overextended", "no_chase",
    "outside_institutional_zone", "structural_stop_missing",
)
_INVALIDATED_TOKENS = ("invalidated", "post_sweep_invalidated", "cross_event_invalidated")
_LIQUIDITY_TOKENS = (
    "liquidity_sweep", "liquidity_grab", "liquidity_confirm", "waiting_liquidity",
    "liquidity_event", "sweep_confirm", "grab_confirm", "failed_auction",
    "sweep_equal_high", "sweep_equal_low", "liquidity_equal_high", "liquidity_equal_low",
    "reclaim_confirm", "waiting_reclaim",
)
_DATA_GAP_TOKENS = (
    "liquidity_context_unavailable", "context_unavailable", "data_gap",
    "missing_closed", "insufficient_closed",
)
_MSS_TOKENS = ("mss", "choch", "structure_shift", "post_sweep_structure")
_REJECTION_TOKENS = ("rejection", "wick", "displacement_candle")
_BREAK_TOKENS = ("breakout", "break_confirm", "broke_level", "break_level")
_RETEST_TOKENS = ("retest", "re-test", "pullback")
_RANKING_ROLES = {RANKING_ONLY_ROLE, "DISCOVERY_ONLY", "RADAR", "RF_HINT", "SCANNER_HINT"}


def _mapping(value: Any) -> Dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _text(value: Any) -> str:
    return str(value).strip() if value is not None else ""


def _upper(value: Any) -> str:
    return _text(value).upper()


def _number(value: Any) -> Optional[float]:
    if value is None or isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return result if math.isfinite(result) else None


def _items(value: Any) -> List[str]:
    if value is None:
        return []
    pending = list(value) if isinstance(value, (list, tuple, set)) else [value]
    out: List[str] = []
    while pending:
        item = pending.pop(0)
        if isinstance(item, (list, tuple, set)):
            pending[0:0] = list(item)
            continue
        text = _text(item)
        if text and text not in out:
            out.append(text)
    return out


def _direction_from_cross(value: Any) -> str:
    text = _upper(value)
    if text in {"BUY", "BULLISH", "LONG"}:
        return "BUY"
    if text in {"SELL", "BEARISH", "SHORT"}:
        return "SELL"
    return ""


def _confirmed_state(direction: str) -> str:
    return "CONFIRMED_BUY_CROSS" if direction == "BUY" else "CONFIRMED_SELL_CROSS" if direction == "SELL" else "NO_CROSS"


def _stable_id(parts: Iterable[Any]) -> str:
    payload = json.dumps([_text(p) for p in parts], separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def snapshot_id(row: Any, *, side: str = "", timestamp_ms: Any = "") -> str:
    """Stable source snapshot identity; never a new exchange or journal record."""
    source = _mapping(row)
    # Every producer already carries a closed-candle timestamp under a different
    # field name. Include all of them so two snapshots of the same symbol/timeframe
    # never collapse into one ID. No timestamp is computed here.
    stamp = timestamp_ms if timestamp_ms not in (None, "") else (
        source.get("source_snapshot_timestamp_ms")
        or source.get("snapshot_last_closed_timestamp_ms")
        or source.get("engine_candle_timestamp")
        or source.get("origin_timestamp")
        or source.get("signal_candle_timestamp")
        or source.get("cross_event_timestamp")
        or source.get("closed_candle_timestamp")
        or source.get("latest_closed_timestamp")
        or source.get("candle_timestamp")
        or source.get("timestamp")
        or source.get("detected_ts")
        or ""
    )
    return _stable_id((
        source.get("exchange_symbol") or source.get("symbol") or "",
        _upper(side or source.get("side") or source.get("direction")),
        source.get("timeframe") or source.get("cross_event_timeframe") or source.get("timeframe_seconds") or "",
        stamp,
    ))


def classify_cross_state(record: Any = None, *, proximity: Any = None, gate: Any = None,
                         now_ts: Optional[float] = None, role: str = "",
                         ranking_only: bool = False) -> Dict[str, Any]:
    """Classify one observed cross state without creating or reusing an event.

    A durable event is required for authorization. Ranking/proximity rows may be
    labelled as a confirmed observation, but ``authoritative`` stays False and
    ``durable_event`` stays False so they can never become an entry cross event.
    """
    now = float(now_ts if now_ts is not None else time.time())
    row = _mapping(record)
    # Callers either pass one already-merged row, or pass the proximity/gate
    # payload separately. Merge both views so field naming never changes a state.
    prox = {**row, **_mapping(proximity)} if proximity is not None else row
    gate_map = {**row, **_mapping(gate)} if gate is not None else row
    declared_role = _upper(role or row.get("role") or prox.get("role") or gate_map.get("role"))
    is_ranking = bool(ranking_only or declared_role in _RANKING_ROLES)
    event_id = _text(row.get("event_id") or row.get("cross_event_id"))
    status = _upper(row.get("status") or row.get("cross_event_status") or "ACTIVE")
    if status == "INACTIVE":
        status = "EXPIRED"
    metadata = _mapping(row.get("metadata"))
    phase = _upper(metadata.get("phase") or row.get("cross_event_phase") or "WAITING_FOR_PULLBACK")
    if phase and phase not in CROSS_PHASES:
        phase = "WAITING_FOR_PULLBACK"
    entry_ready = metadata.get("entry_ready", row.get("cross_event_entry_ready")) is True
    direction = _direction_from_cross(
        row.get("side") or row.get("direction") or row.get("cross_event_direction")
    )
    if not direction:
        direction = _direction_from_cross(gate_map.get("ema9_cross") or prox.get("cross_any"))
    reason = _text(row.get("reason") or metadata.get("reason") or gate_map.get("reason") or "")
    intrabar = bool(row.get("intrabar") or gate_map.get("intrabar") or prox.get("intrabar")
                    or "forming" in reason.lower() or "unfinished" in reason.lower())
    event_timestamp = (row.get("signal_candle_timestamp") or row.get("cross_event_timestamp")
                       or row.get("closed_candle_timestamp") or row.get("latest_closed_timestamp")
                       or row.get("candle_timestamp") or 0)
    base = {
        "schema": SCHEMA_VERSION,
        "cross_state": "NO_CROSS",
        "cross_direction": direction,
        "cross_event_id": event_id,
        "cross_event_status": status,
        "cross_lifecycle_status": status if status in CROSS_LIFECYCLE_STATES else "ACTIVE",
        "cross_event_phase": phase,
        "cross_event_entry_ready": entry_ready,
        "cross_event_timeframe": row.get("timeframe") or row.get("cross_event_timeframe") or "",
        "event_timestamp": event_timestamp,
        "durable_event": bool(event_id and not is_ranking),
        "authoritative": bool(event_id and not is_ranking and status == "ACTIVE"),
        "consumed": bool(row.get("consumed_trade_id") or status == "ENTERED"),
        "intrabar": intrabar,
        "role": RANKING_ONLY_ROLE if is_ranking else (CONFIRMED_CROSS_ROLE if event_id else CONTEXT_ONLY_ROLE),
        "reason": reason or "no_directional_cross_observation",
        "source_snapshot_id": snapshot_id(row or prox or gate_map, side=direction),
    }
    invalid_reason = next((token for token in _INVALID_DATA_TOKENS if token in reason.lower()), "")
    prox_state = _upper(prox.get("state") or prox.get("data_freshness") or "")
    if prox_state in {"STALE", "STALE_DATA", "DATA_STALE"}:
        invalid_reason = invalid_reason or "stale_closed_frame"
    if intrabar:
        invalid_reason = invalid_reason or "intrabar_candle_not_closed"
    if invalid_reason:
        base.update({"cross_state": "INVALID_DATA", "reason": invalid_reason,
                     "durable_event": False, "authoritative": False})
        return base

    if event_id and not is_ranking:
        # Scanner/journal rows use ``expires_ts``; the synced Watchlist feed uses
        # ``cross_event_expires_ts``. Both must expire an event identically.
        expires = _number(row.get("expires_ts") or row.get("cross_event_expires_ts")
                          or row.get("cross_event_expiry_ts"))
        if base["consumed"] or status == "ENTERED":
            base.update({"cross_state": "CROSS_ALREADY_CONSUMED", "authoritative": False,
                         "reason": "cross_event_consumed_once"})
            return base
        if status in {"INVALIDATED", "EXPIRED", "INACTIVE"}:
            base.update({"cross_state": "NO_CROSS", "cross_lifecycle_status": status,
                         "authoritative": False, "reason": f"cross_event_{status.lower()}"})
            return base
        if status != "ACTIVE" or (expires is not None and expires <= now):
            base.update({"cross_state": "NO_CROSS", "cross_lifecycle_status": "EXPIRED",
                         "authoritative": False, "reason": "cross_event_not_active"})
            return base
        base.update({"cross_state": _confirmed_state(direction), "reason": "durable_closed_cross_event"})
        return base

    # Read-only projection. The strategy branch authorizes only the new
    # EMA9/EMA15 state; the legacy token is retained here solely so historical
    # rows still classify as observations. Neither can become authoritative
    # without a durable, unconsumed cross event.
    _OBSERVED_AUTHORIZATION_STATES = {
        _upper(AUTH_EMA9_EMA15_TRIGGER_VALID), "EMA9_TRIGGER_VALID",
    }
    gate_confirmed = bool(gate_map.get("ready") is True
                          and _upper(gate_map.get("authorization_state")) in _OBSERVED_AUTHORIZATION_STATES)
    prox_confirmed = bool(prox.get("cross_any") or prox.get("cross_ema9") or prox.get("cross_ema50"))
    if prox.get("cross_conflict") is True or prox_state == "CROSS_CONFLICT":
        base.update({"cross_state": "INVALID_DATA", "reason": "cross_conflict",
                     "durable_event": False, "authoritative": False})
        return base
    if gate_confirmed or prox_confirmed:
        base.update({"cross_state": _confirmed_state(direction),
                     "reason": "observation_only_durable_event_required"})
        return base
    if prox_state in {"NEAR_CROSS", "CONVERGING"} or prox.get("converging") is True:
        base.update({"cross_state": "CONVERGING", "reason": "gap_narrowing_on_closed_candles"})
        return base
    gap = _number(prox.get("gap"))
    if gap is not None and gap != 0:
        base.update({"cross_state": "ALIGNED_NOT_CROSSING", "reason": "directional_alignment_without_cross"})
        return base
    if _upper(gate_map.get("ema9_cross")) in {"BULLISH", "BEARISH"}:
        base.update({"cross_state": _confirmed_state(direction), "reason": "observation_only_durable_event_required"})
        return base
    base["reason"] = reason or "no_closed_cross_or_convergence"
    return base


def classify_liquidity(record: Any = None) -> Dict[str, Any]:
    """Project an existing liquidity result into explicit state and stage fields."""
    row = _mapping(record)
    liquidity = _mapping(row.get("liquidity"))
    raw = _upper(liquidity.get("event") or row.get("liquidity_event") or row.get("event") or "UNKNOWN")
    reasons = _items(liquidity.get("reasons") or row.get("rejection_reasons"))
    setup = _upper(row.get("decision_setup_type") or row.get("setup_type"))
    blob = " ".join(reasons + [raw, setup]).lower()
    confidence = _number(liquidity.get("confidence"))
    timestamp = liquidity.get("candle_timestamp") or row.get("liquidity_timestamp")
    result = {
        "schema": SCHEMA_VERSION,
        "state": "NO_LIQUIDITY_EVENT",
        "stage": "PENDING",
        "raw_event": raw,
        "direction": _upper(liquidity.get("direction") or "NEUTRAL"),
        "level": liquidity.get("level"),
        "confidence": confidence,
        "confirmation_timestamp": timestamp,
        "reclaim_timestamp": None,
        "pending_reason": "",
        "evidence": reasons,
    }
    if raw in {"INVALID", "INVALID_DATA"} or "invalid_data" in blob:
        result.update({"stage": "INVALIDATED", "pending_reason": "invalid_liquidity_data"})
        return result
    if "failed_auction" in blob or setup == "FAILED_AUCTION_REVERSAL":
        result.update({"state": "FAILED_AUCTION", "stage": "CONFIRMED" if timestamp or reasons else "DETECTED",
                       "direction": _upper(row.get("side") or result["direction"])})
        return result
    if "reclaim" in blob:
        result.update({"state": "RECLAIM", "stage": "RECLAIMED", "reclaim_timestamp": timestamp,
                       "confirmation_timestamp": timestamp})
        return result
    if raw in {"SWEEP", "GRAB", "RUN"}:
        state = "LIQUIDITY_SWEEP" if raw == "SWEEP" else "LIQUIDITY_GRAB"
        result["state"] = state
        confirmed = bool(timestamp and (confidence is None or confidence >= 0.45))
        if "touch" in blob and "confirm" not in blob:
            confirmed = False
        result["stage"] = "CONFIRMED" if confirmed else "DETECTED"
        result["pending_reason"] = "" if confirmed else "touch_or_grab_not_sweep_confirmation"
        return result
    if raw in {"NONE", "UNKNOWN", ""} and reasons:
        result["pending_reason"] = reasons[0]
    return result


def project_structure(record: Any = None, *, closed_timestamp: Any = None) -> Dict[str, Any]:
    """Expose closed-candle BOS/MSS confirmation provenance without recomputing swings."""
    row = _mapping(record)
    structure = _mapping(row.get("market_structure"))
    bos = _upper(structure.get("bos"))
    mss = _upper(structure.get("mss") or structure.get("choch"))
    confirmed = bool(structure.get("confirmed")) and bool(bos or mss)
    break_level = None
    previous_swing = None
    direction = _upper(structure.get("direction"))
    if "BULLISH" in bos or "BULLISH" in mss or direction == "BULLISH":
        break_level = structure.get("swing_high")
        previous_swing = structure.get("swing_low")
    elif "BEARISH" in bos or "BEARISH" in mss or direction == "BEARISH":
        break_level = structure.get("swing_low")
        previous_swing = structure.get("swing_high")
    stamp = closed_timestamp if closed_timestamp not in (None, "") else (
        row.get("engine_candle_timestamp") or row.get("origin_timestamp")
        or row.get("signal_candle_timestamp") or row.get("cross_event_timestamp")
        or row.get("closed_candle_timestamp") or row.get("latest_closed_timestamp")
        or row.get("candle_timestamp")
    )
    return {
        "schema": SCHEMA_VERSION,
        "state": "CONFIRMED" if confirmed else "PENDING",
        "pattern": _upper(structure.get("pattern") or "UNKNOWN"),
        "direction": direction or "UNKNOWN",
        "bos": bos or None,
        "mss": mss or None,
        "previous_swing": previous_swing,
        "break_level": break_level,
        "confirmation_timestamp": stamp if confirmed else None,
        "confirmation_status": "CONFIRMED" if confirmed else "PENDING",
        "pending_reason": "" if confirmed else "no_closed_bos_or_mss_break_of_confirmed_swing",
        "source": "CLOSED_CANDLE_FRAME",
    }


def project_zone_context(record: Any = None) -> Dict[str, Any]:
    """Separate zone quality from decision readiness; never invent a new zone."""
    row = _mapping(record)
    golden = _mapping(row.get("golden_zone"))
    ob = _mapping(row.get("order_block"))
    fvg = _mapping(row.get("fvg"))
    indicators = _mapping(row.get("indicator_snapshot"))
    zone_quality = _number(row.get("zone_quality"))
    if zone_quality is None:
        zone_quality = _number(golden.get("score")) or 0.0
    ob_status = _upper(ob.get("status") or row.get("order_block_status") or "UNKNOWN")
    ob_valid = bool(ob and (ob.get("valid") is True
                            or ob_status in {"FRESH", "UNTESTED", "VALID"})
                    and ob_status not in {"MITIGATED", "INVALIDATED", "EXPIRED"})
    fvg_status = _upper(fvg.get("status") or "UNKNOWN")
    fvg_valid = bool(fvg and fvg_status in {"UNFILLED", "PARTIALLY_FILLED"})
    lower = _number(fvg.get("lower"))
    upper = _number(fvg.get("upper"))
    ob_low, ob_high = _number(ob.get("low")), _number(ob.get("high"))
    overlap = bool(ob_valid and fvg_valid and None not in (lower, upper, ob_low, ob_high)
                   and max(float(ob_low), float(lower)) <= min(float(ob_high), float(upper)))
    freshness = _upper(indicators.get("quality") or row.get("data_freshness") or "UNKNOWN")
    adx = _number(row.get("adx") or indicators.get("adx"))
    points = 0.0
    points += 25.0 if ob_valid else 0.0
    points += 20.0 if fvg_valid else 0.0
    points += 15.0 if overlap or golden.get("ob_fvg_overlap") else 0.0
    points += 15.0 if classify_liquidity(row)["state"] not in {"NO_LIQUIDITY_EVENT"} else 0.0
    points += 10.0 if project_structure(row)["state"] == "CONFIRMED" else 0.0
    points += 10.0 if freshness == "FRESH" else 0.0
    points += 5.0 if adx is not None and adx >= 20.0 else 0.0
    return {
        "schema": SCHEMA_VERSION,
        "zone_quality_score": round(zone_quality, 2),
        "context_quality_score": round(min(100.0, points), 2),
        "meaning": "evidence_quality_only_not_entry_authorization",
        "order_block": {
            "valid": ob_valid,
            "freshness": ob_status,
            "mitigation_pct": _number(ob.get("fill_pct")),
            "distance_atr": _number(ob.get("distance_atr")),
            "inside_zone": bool(ob_valid and _number(ob.get("distance_atr")) is not None
                                and float(_number(ob.get("distance_atr")) or 999) <= 1.5),
            "low": ob_low,
            "high": ob_high,
        },
        "fvg": {
            "valid": fvg_valid,
            "status": fvg_status,
            "freshness": "FRESH" if fvg_valid else "INVALIDATED",
            "fill_pct": _number(fvg.get("fill_pct")),
            "lower": lower,
            "upper": upper,
            "direction": _upper(fvg.get("direction")),
            "overlap_with_order_block": bool(overlap or golden.get("ob_fvg_overlap")),
        },
        "supply_demand": {
            "zone_type": _upper(row.get("zone_type") or ("DEMAND" if _upper(row.get("side")) == "BUY" else "SUPPLY")),
            "valid": bool(ob_valid or fvg_valid),
            "status": "VALID" if (ob_valid or fvg_valid) else "UNAVAILABLE_OR_INVALID",
        },
    }


def project_timeframe_view(record: Any = None, *, ranking: bool = False) -> Dict[str, Any]:
    """Keep independently sourced states separate; expose conflicts explicitly."""
    row = _mapping(record)
    indicators = _mapping(row.get("indicator_snapshot"))
    proximity = _mapping(row.get("ema9_vwap_proximity"))
    adx_di = _mapping(row.get("adx_di"))
    momentum = _mapping(row.get("momentum"))
    institutional_trend = _mapping(row.get("institutional_trend"))
    regime = _upper(row.get("market_regime") or row.get("entry_regime") or "UNKNOWN")
    trend = _upper(row.get("trend_direction") or row.get("regime_direction") or "UNKNOWN")
    trade_state = _upper(row.get("trade_state") or "UNKNOWN")
    local = project_structure(row)
    htf_raw = (row.get("higher_timeframe_bias") or row.get("ema50_alignment")
               or row.get("htf_bias") or proximity.get("ema50_direction"))
    htf = _upper(htf_raw) or "UNAVAILABLE_NO_EXTRA_HTF_FETCH"
    ema50_direction = _upper(row.get("ema50_direction") or proximity.get("ema50_direction") or "UNKNOWN")
    vwap_position = _upper(row.get("vwap_position") or indicators.get("vwap_position") or "UNKNOWN")
    strength_candidate = (row.get("trend_strength") if row.get("trend_strength") is not None
                          else institutional_trend.get("health") if institutional_trend
                          else row.get("adx") if row.get("adx") is not None
                          else indicators.get("adx"))
    trend_strength = _number(strength_candidate)
    adx = _number(row.get("adx") if row.get("adx") is not None else indicators.get("adx"))
    conflicts: List[str] = []
    local_bias = local["direction"]
    if htf in {"SUPPORTIVE", "BULLISH", "BUY"} and local_bias == "BEARISH":
        conflicts.append("TIMEFRAME_CONFLICT:LOCAL_BEARISH_VS_HTF_SUPPORTIVE")
    if htf in {"OPPOSED", "BEARISH", "SELL"} and local_bias == "BULLISH":
        conflicts.append("TIMEFRAME_CONFLICT:LOCAL_BULLISH_VS_HTF_OPPOSED")
    if regime in _CHOP_TOKENS or any(token in regime.lower() for token in _CHOP_TOKENS):
        conflicts.append("REGIME_CHOP_CONTEXT")
    scanner_tf = _text(row.get("timeframe") or row.get("cross_event_timeframe") or row.get("timeframe_seconds"))
    institutional_tf = _text(row.get("institutional_timeframe") or row.get("bridge_timeframe"))
    timeframe_alignment = "NOT_APPLICABLE"
    if scanner_tf and institutional_tf:
        if scanner_tf == institutional_tf:
            timeframe_alignment = "ALIGNED"
        else:
            timeframe_alignment = "MISMATCH"
            conflicts.append(f"TIMEFRAME_CONFLICT:SCANNER_{scanner_tf}_VS_INSTITUTIONAL_{institutional_tf}")
    return {
        "schema": SCHEMA_VERSION,
        "market_regime": regime,
        "trend_direction": trend,
        "trend_strength": trend_strength,
        "trade_state": trade_state,
        "local_structure": local["state"],
        "local_structure_direction": local_bias,
        "higher_timeframe_bias": htf,
        "ema50_direction": ema50_direction,
        "vwap_position": vwap_position,
        "adx": adx,
        "di_plus": _number(adx_di.get("di_plus") or row.get("di_plus")),
        "di_minus": _number(adx_di.get("di_minus") or row.get("di_minus")),
        "di_dominant": _upper(adx_di.get("di_dominant") or row.get("di_dominant") or "UNKNOWN"),
        "momentum": {
            "flow_bias": _upper(momentum.get("flow_bias") or row.get("momentum_bias") or "UNKNOWN"),
            "continuation_strength": _number(momentum.get("continuation_strength")),
            "trend_expansion": bool(momentum.get("trend_expansion")) if momentum else None,
        },
        "timeframe_alignment": timeframe_alignment,
        "scanner_timeframe": scanner_tf or "UNKNOWN",
        "institutional_timeframe": institutional_tf or "UNKNOWN",
        "timeframe_conflict": "PRESENT" if conflicts else "NONE",
        "conflict_reasons": conflicts,
        "scope": "RANKING_ONLY" if ranking else "PER_SYMBOL_CLOSED_CANDLE_HYPOTHESIS",
        "source": "EXISTING_FIELDS_NO_EXTRA_FETCH",
    }


def project_execution_readiness(record: Any, *, decision_state: str) -> Dict[str, Any]:
    """Describe completeness only. The existing execute_entry gate remains authority."""
    row = _mapping(record)
    missing: List[str] = []
    stop = _number(row.get("structural_stop"))
    if stop is None or stop <= 0:
        missing.append("STRUCTURAL_STOP_UNKNOWN")
    if decision_state == "DATA_STALE":
        missing.append("FRESH_CLOSED_FRAME")
    elif decision_state == "DATA_INCOMPLETE":
        missing.append("COMPLETE_CLOSED_FRAME")
    elif decision_state != "ENTRY_READY":
        missing.append(decision_state)
    if row.get("cross_event_id") and decision_state != "ENTRY_READY":
        missing.append("CROSS_CONDITIONS_INCOMPLETE")
    score = max(0.0, 100.0 - 25.0 * len(missing))
    return {
        "schema": SCHEMA_VERSION,
        "state": "ENTRY_READY" if not missing else "NOT_READY",
        "conditions_complete": not missing,
        "score": round(score, 2),
        "missing": missing,
        "sl_valid": bool(stop is not None and stop > 0),
        "authoritative_gate": "UNCHANGED_EXECUTE_ENTRY",
        "role": CONTEXT_ONLY_ROLE,
    }


def _map_decision_state(row: Dict[str, Any], cross: Dict[str, Any], zones: Dict[str, Any],
                        liquidity: Dict[str, Any], legacy: str, reason_blob: str) -> str:
    # Terminal and blocking conditions are decided first. They are facts about the
    # durable event/context, not a generic "still waiting" label.
    if row.get("conflicting_directions") or "conflicting_opposing_setups" in reason_blob:
        return "BLOCKED_CONFLICTING_CONTEXT"
    if cross.get("cross_state") == "CROSS_ALREADY_CONSUMED":
        return "REJECTED_INVALIDATED_SETUP"
    if legacy == "INVALIDATED" or any(token in reason_blob for token in _INVALIDATED_TOKENS):
        return "REJECTED_INVALIDATED_SETUP"
    if legacy == "EXPIRED" or "expired" in reason_blob:
        return "EXPIRED"
    regime = _upper(row.get("market_regime") or row.get("entry_regime"))
    if any(token in regime.lower() for token in _CHOP_TOKENS) or "market_chop" in reason_blob:
        return "BLOCKED_MARKET_CHOP"
    explicit_zone_invalid = (
        zones["order_block"]["freshness"] in {"MITIGATED", "INVALIDATED", "EXPIRED"}
        or zones["fvg"]["status"] in {"FILLED", "INVALIDATED"}
    )
    if any(token in reason_blob for token in _INVALID_ZONE_TOKENS) or explicit_zone_invalid:
        return "BLOCKED_INVALID_ZONE"

    # Entry readiness is checked before the waiting heuristics so a complete,
    # authoritative breakout is never downgraded merely because no liquidity
    # event exists in the same frame.
    if legacy == "ENTRY_READY" \
            and cross.get("cross_state") in {"CONFIRMED_BUY_CROSS", "CONFIRMED_SELL_CROSS"} \
            and cross.get("authoritative") \
            and cross.get("cross_event_phase") == "BREAKOUT_CONFIRMED" \
            and cross.get("cross_event_entry_ready") is True \
            and project_execution_readiness(row, decision_state="ENTRY_READY")["conditions_complete"]:
        return "ENTRY_READY"

    # Cross phases are only meaningful when an actual durable event exists or the
    # upstream producer explicitly named a phase. A plain institutional hypothesis
    # must not inherit the default WAITING_FOR_PULLBACK phase.
    metadata = _mapping(row.get("metadata"))
    has_cross = bool(cross.get("durable_event") or row.get("cross_event_id") or row.get("event_id"))
    phase = _upper(metadata.get("phase") or row.get("cross_event_phase") or row.get("phase")) if has_cross else ""
    if phase == "WAITING_FOR_PULLBACK" or "waiting_for_pullback" in reason_blob:
        return "WAITING_FOR_PULLBACK"
    if phase == "PULLBACK_CONFIRMED":
        return "WAITING_FOR_REJECTION_CANDLE"
    if phase == "REJECTION_RECORDED":
        return "WAITING_FOR_BREAK_CONFIRMATION"
    if phase == "BREAKOUT_CONFIRMED" and cross.get("cross_event_entry_ready") is not True:
        return "WAITING_FOR_BREAK_CONFIRMATION"

    if any(token in reason_blob for token in _REJECTION_TOKENS):
        return "WAITING_FOR_REJECTION_CANDLE"
    if any(token in reason_blob for token in _BREAK_TOKENS):
        return "WAITING_FOR_BREAK_CONFIRMATION"
    if any(token in reason_blob for token in _RETEST_TOKENS):
        return "WAITING_FOR_RETEST"
    liquidity_pending = (liquidity["state"] in {
        "LIQUIDITY_SWEEP", "LIQUIDITY_GRAB", "FAILED_AUCTION", "RECLAIM"
    } and liquidity["stage"] in {"DETECTED", "PENDING"})
    if any(token in reason_blob for token in _LIQUIDITY_TOKENS) or liquidity_pending:
        return "WAITING_FOR_LIQUIDITY_CONFIRMATION"
    if any(token in reason_blob for token in _MSS_TOKENS):
        return "WAITING_FOR_MSS"
    if cross.get("cross_state") in {"NO_CROSS", "INVALID_DATA", "CONVERGING", "ALIGNED_NOT_CROSSING"}:
        return "WAITING_FOR_CROSS"
    if legacy.startswith("WAITING") or legacy in {"REJECTED", "NO_CHASE", "SHADOW_ONLY"}:
        return "WAITING_FOR_CROSS"
    return "WAITING_FOR_CROSS"


def project_entry_behavior(record: Any, decision: Any = None) -> Dict[str, Any]:
    """Project the canonical market-intelligence entry behavior for display only."""
    row = _mapping(record)
    canonical = _mapping(row.get("market_intelligence"))
    raw_behavior = _text(canonical.get("entry_behavior"))
    reason_codes = _items(canonical.get("reason_codes") or row.get("entry_behavior_reasons"))
    confidence = _number(canonical.get("confidence")
                         if canonical.get("confidence") is not None
                         else row.get("entry_behavior_confidence"))
    entry_type = normalize_entry_type(raw_behavior or row.get("entry_behavior"))
    decision_map = _mapping(decision)
    readiness = _mapping(decision_map.get("entry_readiness"))
    invalidations = _items(row.get("invalidations"))
    if row.get("invalidation_reason"):
        invalidations.append(_text(row.get("invalidation_reason")))
    if row.get("structural_stop_status") not in (None, "", "verified_current_analysis"):
        invalidations.append(f"structural_stop_{row.get('structural_stop_status')}")
    lifecycle_hint = _upper(_mapping(row.get("metadata")).get("lifecycle_status")
                            or row.get("cross_event_lifecycle_status"))
    authorized = bool(readiness.get("conditions_complete") is True and readiness.get("sl_valid") is True)
    result = {
        "schema": SCHEMA_VERSION,
        "entry_type": entry_type,
        "classification_status": "CLASSIFIED" if entry_type else "NOT_CLASSIFIED",
        "classification_source": "GGP_MARKET_INTELLIGENCE" if raw_behavior else "NONE",
        "raw_behavior": raw_behavior or _text(row.get("entry_behavior")),
        "reason": reason_codes[0] if reason_codes else ("entry_behavior_not_proven" if not entry_type else "classifier_reason_unavailable"),
        "reason_codes": reason_codes,
        "confirmation": confidence,
        "confirmation_basis": "classifier_confidence",
        "invalidation": invalidations,
        "waiting_condition": _ENTRY_TYPE_LIFECYCLE.get(entry_type or "", ""),
        "stop_valid": readiness.get("sl_valid") is True,
        "final_authorization": "UNIFIED_ENTRY_AUTHORITY_UNCHANGED" if authorized else "NOT_READY",
        "authoritative_gate": readiness.get("authoritative_gate") or "UNIFIED_ENTRY_AUTHORITY_UNCHANGED",
        "lifecycle_hint": lifecycle_hint if lifecycle_hint in WATCHLIST_LIFECYCLE_STATES else "",
    }
    result["entry_state"] = (
        ("READY_FOR_IMMEDIATE_ENTRY" if authorized else "NOT_READY")
        if entry_type == "IMMEDIATE_ENTRY"
        else result["waiting_condition"] if entry_type in ENTRY_TYPES else "NOT_READY"
    )
    return result


def project_watchlist_lifecycle(record: Any, *, cross: Any = None, decision: Any = None,
                                entry_behavior: Any = None) -> Dict[str, Any]:
    """Derive one Active Watchlist lifecycle state without writing any new store."""
    row = _mapping(record)
    cross_map = _mapping(cross)
    decision_map = _mapping(decision)
    behavior = _mapping(entry_behavior or project_entry_behavior(row, decision_map))
    status = _upper(cross_map.get("cross_event_status") or row.get("cross_event_status")
                    or row.get("status") or "ACTIVE")
    if status in {"ENTERED", "CROSS_ALREADY_CONSUMED"}:
        state, reason = "ENTERED", "cross_event_consumed_once"
    elif status in {"INVALIDATED", "REJECTED"}:
        state, reason = "INVALIDATED", _text(row.get("invalidation_reason")) or "cross_event_invalidated"
    elif status in {"EXPIRED", "INACTIVE", "CROSS_ALREADY_EXPIRED"}:
        state, reason = "EXPIRED", "cross_event_expired"
    elif not cross_map.get("durable_event"):
        state, reason = "ACTIVE_WATCHLIST", "watchlist_observation_no_durable_event"
    elif not any(row.get(key) for key in (
            "market_intelligence", "decision_state", "decision_context",
            "unified_decision_context", "structural_stop", "entry_behavior_context")):
        hint = _upper(behavior.get("lifecycle_hint"))
        state = hint if hint in {"CROSS_DETECTED", "ACTIVE_WATCHLIST", "INSTITUTIONAL_ANALYSIS"} else "CROSS_DETECTED"
        reason = "durable_cross_without_analysis_snapshot"
    elif decision_map.get("analysis_status") not in (None, "FRESH"):
        analysis = _upper(decision_map.get("analysis_status"))
        state, reason = "INSTITUTIONAL_ANALYSIS", (
            "analysis_in_progress" if analysis == "PENDING" else f"analysis_{analysis.lower()}"
        )
    elif not behavior.get("entry_type"):
        state, reason = "INSTITUTIONAL_ANALYSIS", "entry_behavior_not_proven"
    else:
        state = behavior.get("waiting_condition") or "INSTITUTIONAL_ANALYSIS"
        reason = behavior.get("reason") or "canonical_entry_behavior"
    hint = _upper(behavior.get("lifecycle_hint"))
    if state in {"ACTIVE_WATCHLIST", "INSTITUTIONAL_ANALYSIS"} and hint == "CROSS_DETECTED" and not decision_map:
        state, reason = "CROSS_DETECTED", "durable_cross_not_yet_analyzed"
    return {
        "schema": SCHEMA_VERSION,
        "lifecycle_status": state if state in WATCHLIST_LIFECYCLE_STATES else "INSTITUTIONAL_ANALYSIS",
        "reason": reason,
        "entry_type": behavior.get("entry_type"),
        "waiting_condition": behavior.get("waiting_condition") or "",
        "analysis_status": decision_map.get("analysis_status") or "PENDING",
        "durable_event": bool(cross_map.get("durable_event")),
    }


def project_decision_context(record: Any, *, cross: Any = None, now_ts: Optional[float] = None,
                             ranking: bool = False) -> Dict[str, Any]:
    """One explicit projected decision per row with reasons and next required event.

    The legacy engine state is preserved untouched; this projection explains it
    and replaces generic waiting labels with the exact missing condition.
    """
    row = _mapping(record)
    cross_map = _mapping(cross) if cross is not None else classify_cross_state(row, now_ts=now_ts)
    liquidity = classify_liquidity(row)
    structure = project_structure(row)
    zones = project_zone_context(row)
    legacy_state = _upper(row.get("decision_state") or row.get("entry_status") or "UNKNOWN")
    primary = _text(row.get("decision_reason") or row.get("rejection_reason") or "not_available")
    secondary = _items(row.get("evidence"))
    blocking = _items(row.get("rejection_reasons") or row.get("rejection_reason"))
    blob = " ".join([primary] + secondary + blocking).lower()
    freshness = _upper(row.get("data_freshness") or (row.get("indicator_snapshot") or {}).get("quality")
                       if isinstance(row.get("indicator_snapshot"), Mapping) else row.get("data_freshness") or "UNKNOWN")
    data_quality = _upper(row.get("data_quality") or "UNKNOWN")
    analysis_status = "FRESH"
    if freshness in {"STALE", "STALE_DATA", "DATA_STALE"} or "stale" in blob:
        analysis_status = "DATA_STALE"
    elif ("insufficient" in data_quality.lower() or "insufficient" in blob
          or data_quality == "DATA_UNAVAILABLE" or any(token in blob for token in _DATA_GAP_TOKENS)):
        analysis_status = "DATA_INCOMPLETE"

    if analysis_status != "FRESH":
        state = analysis_status
        primary = "stale_closed_frame" if state == "DATA_STALE" else "incomplete_closed_frame"
        blocking = [primary] + [b for b in blocking if b != primary]
    else:
        state = _map_decision_state(row, cross_map, zones, liquidity, legacy_state, blob)
    readiness = project_execution_readiness(row, decision_state=state)
    entry_behavior_context = project_entry_behavior(
        row, {"entry_readiness": readiness, "blocking_reasons": blocking},
    )
    lifecycle_context = project_watchlist_lifecycle(
        row, cross=cross_map, decision={"analysis_status": analysis_status},
        entry_behavior=entry_behavior_context,
    )
    execution_state = "ENTRY_READY" if (
        readiness.get("conditions_complete") is True and readiness.get("sl_valid") is True
    ) else "ENTRY_WINDOW_MISSED" if any(
        token in blob for token in ("entry_window_missed", "no_chase", "outside_institutional_zone")
    ) else "NOT_READY"
    timestamp = (row.get("origin_timestamp") or row.get("signal_candle_timestamp")
                 or row.get("cross_event_timestamp") or row.get("engine_candle_timestamp")
                 or row.get("closed_candle_timestamp") or row.get("latest_closed_timestamp")
                 or row.get("candle_timestamp") or 0)
    timeframe_view = project_timeframe_view(row, ranking=ranking)
    # Read-only institutional evidence report. Reuse the report already produced
    # by Watchlist enrichment when present; otherwise build the same pure report
    # from the closed-candle row and its projected cross context. No API, cache,
    # journal, order or authority is involved.
    institutional_report = row.get("institutional_report")
    if not isinstance(institutional_report, dict) or not institutional_report:
        report_row = dict(row)
        report_row["cross_context"] = cross_map
        institutional_report = build_institutional_report(
            report_row, side=_upper(row.get("side") or row.get("direction")), now_ts=now_ts,
        )
    market_context = {
        **timeframe_view,
        "liquidity": liquidity,
        "market_structure": structure,
        "order_block": zones["order_block"],
        "fvg": zones["fvg"],
        "supply_demand": zones["supply_demand"],
    }
    return {
        "schema": SCHEMA_VERSION,
        "role": CONTEXT_ONLY_ROLE,
        "authority": "UNIFIED_ENTRY_AUTHORITY_UNCHANGED",
        "legacy_decision_state": legacy_state,
        "decision_state": state,
        "analysis_status": analysis_status,
        "setup_stage": SETUP_STAGE.get(state, "UNCLASSIFIED"),
        "primary_reason": primary,
        "secondary_reasons": secondary,
        "blocking_reasons": _items(blocking),
        "required_next_event": REQUIRED_NEXT_EVENT.get(state, "SETUP_DIAGNOSTICS_REQUIRED"),
        "event_timestamp": timestamp,
        "event_id": row.get("event_id") or row.get("cross_event_id") or None,
        "data_freshness": freshness,
        "source_snapshot_id": (row.get("source_snapshot_id") or cross_map.get("source_snapshot_id")
                               or snapshot_id(row, side=_upper(row.get("side") or row.get("direction")))),
        "cross_context": cross_map,
        "liquidity_context": liquidity,
        "structure_context": structure,
        "zone_context": zones,
        "timeframe_view": timeframe_view,
        "market_context": market_context,
        "entry_readiness": readiness,
        "entry_behavior_context": entry_behavior_context,
        "institutional_report": institutional_report,
        "entry_type": entry_behavior_context["entry_type"],
        "lifecycle_status": lifecycle_context["lifecycle_status"],
        "lifecycle_context": lifecycle_context,
        "waiting_condition": lifecycle_context["waiting_condition"],
        "execution_gate_state": execution_state,
        "final_authorization": entry_behavior_context["final_authorization"],
        "stop_validation_status": row.get("structural_stop_status") or "UNSPECIFIED",
        "zone_quality_score": zones["zone_quality_score"],
        "context_quality_score": zones["context_quality_score"],
    }


def project_watchlist_row(record: Any, *, now_ts: Optional[float] = None,
                          ranking_only: bool = False) -> Dict[str, Any]:
    """Project one watchlist/ranking row into the unified dashboard-facing schema."""
    row = _mapping(record)
    cross = classify_cross_state(row, now_ts=now_ts, ranking_only=ranking_only)
    display_state = cross["cross_state"]
    if ranking_only and display_state in {"CONFIRMED_BUY_CROSS", "CONFIRMED_SELL_CROSS"}:
        display_state = f"RANKING_ONLY_{display_state}_OBSERVATION"
    cross["display_cross_state"] = display_state
    # project_decision_context() shallow-copies the cross map, so set the display
    # label first to keep the nested decision_context schema consistent.
    decision = project_decision_context(row, cross=cross, now_ts=now_ts, ranking=ranking_only)
    return {
        "schema": SCHEMA_VERSION,
        "symbol": row.get("symbol") or "",
        "exchange_symbol": row.get("exchange_symbol") or "",
        "side": _upper(row.get("side") or row.get("direction")),
        "cross_state_display": display_state,
        "cross_context": cross,
        "decision_context": decision,
        "liquidity_context": decision["liquidity_context"],
        "structure_context": decision["structure_context"],
        "zone_context": decision["zone_context"],
        "timeframe_view": decision["timeframe_view"],
        "entry_readiness": decision["entry_readiness"],
        "entry_behavior_context": decision["entry_behavior_context"],
        "institutional_report": decision["institutional_report"],
        "institutional_location_gate": dict(decision["institutional_report"].get("location_gate") or {}),
        "entry_type": decision["entry_type"],
        "lifecycle_status": decision["lifecycle_status"],
        "lifecycle_context": decision["lifecycle_context"],
        "waiting_condition": decision["waiting_condition"],
        "execution_gate_state": decision["execution_gate_state"],
        "source_snapshot_id": decision["source_snapshot_id"],
    }


def project_dashboard_sections(watchlist: Any = None, confirmed_crosses: Any = None,
                               ranking: Any = None, now_ts: Optional[float] = None,
                               cross_delivery: Any = None) -> Dict[str, Any]:
    """Build four separated dashboard sections from existing feeds only."""
    watch = _mapping(watchlist)
    crosses = list(confirmed_crosses or [])
    ranks = list(ranking or [])

    confirmed_rows: List[Dict[str, Any]] = []
    for item in crosses:
        projected = project_watchlist_row(item, now_ts=now_ts)
        cross = projected["cross_context"]
        if cross["durable_event"] and cross["cross_state"] in {"CONFIRMED_BUY_CROSS", "CONFIRMED_SELL_CROSS"} \
                and cross["cross_event_status"] == "ACTIVE":
            confirmed_rows.append(projected)

    ranking_rows: List[Dict[str, Any]] = []
    for item in ranks:
        projected = project_watchlist_row(item, now_ts=now_ts, ranking_only=True)
        if projected["cross_context"]["cross_state"] not in {"NO_CROSS", "CROSS_ALREADY_CONSUMED"}:
            ranking_rows.append(projected)

    institutional_rows = [project_watchlist_row(item, now_ts=now_ts) for item in watch.values()
                          if isinstance(item, Mapping)]
    execution_rows = []
    for item in institutional_rows:
        readiness = dict(item["entry_readiness"])
        decision = item["decision_context"]
        readiness.update({
            "symbol": item["symbol"], "side": item["side"],
            "decision_state": decision["decision_state"],
            "required_next_event": decision["required_next_event"],
            "source_snapshot_id": decision["source_snapshot_id"],
            "structural_stop_valid": readiness["sl_valid"],
            "entry_type": decision["entry_type"],
            "entry_behavior_context": decision["entry_behavior_context"],
            "lifecycle_status": decision["lifecycle_status"],
            "waiting_condition": decision["waiting_condition"],
            "execution_gate_state": decision["execution_gate_state"],
            "final_authorization": decision["final_authorization"],
            "stop_validation_status": decision["stop_validation_status"],
        })
        execution_rows.append(readiness)
    execution_rows.sort(key=lambda row: (not row["conditions_complete"], row["symbol"]))
    return {
        "schema": SCHEMA_VERSION,
        "generated_at": float(now_ts if now_ts is not None else time.time()),
        "confirmed_crosses": confirmed_rows,
        "watchlist_ranking": ranking_rows,
        "institutional_analysis": institutional_rows,
        "institutional_report": [row["institutional_report"] for row in institutional_rows],
        "execution_readiness": execution_rows,
        "cross_delivery": dict(_mapping(cross_delivery)),
        "note": "Projection only. No new authority, order path, indicator or API call.",
    }
